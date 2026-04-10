"""Tier 2 LLM-powered analysis layer.

``LLMAnalyzer`` sits on top of the Tier 1 static-analysis results and
enriches them with context-aware explanations, cross-file pattern detection,
and an executive-level health summary — all produced by calling the
configured LLM provider.

Usage::

    from spark_perf_lint.config import LintConfig
    from spark_perf_lint.llm.analyzer import LLMAnalyzer

    config = LintConfig.load()
    analyzer = LLMAnalyzer.from_config(config)
    result = analyzer.analyze(report, source_map={"jobs/etl.py": source_text})

    for finding in result.enriched_findings:
        if finding.llm_insight:
            print(finding.llm_insight)

    print(result.executive_summary)

All LLM calls are optional: if the ``anthropic`` package is absent, if the
API key is not set, or if ``config.llm_enabled`` is ``False``, the analyzer
degrades gracefully and returns the original findings unmodified.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from spark_perf_lint.types import Finding, Severity

if TYPE_CHECKING:
    from spark_perf_lint.config import LintConfig
    from spark_perf_lint.llm.provider import LLMProvider
    from spark_perf_lint.types import AuditReport

logger = logging.getLogger(__name__)

# Lines to include before and after the finding's line when building snippets.
_SNIPPET_CONTEXT = 10


# =============================================================================
# Result dataclass
# =============================================================================


@dataclass
class LLMAnalysisResult:
    """Aggregated output of a Tier 2 LLM analysis run.

    Attributes:
        enriched_findings: The original ``Finding`` list with ``llm_insight``
            populated on findings that were sent to the LLM.  Findings that
            were filtered (below ``min_severity``) or skipped due to the
            ``max_llm_calls`` cap retain ``llm_insight = None``.
        cross_file_insights: Free-text output of the cross-file pattern
            analysis, or an empty string when skipped.
        executive_summary: Free-text management-level health summary,
            or an empty string when skipped.
        calls_made: Total number of LLM API calls that were issued.
        model: Identifier of the model that was used.
        errors: List of non-fatal error messages encountered during analysis
            (e.g., per-finding call failures when ``fail_fast=False``).
    """

    enriched_findings: list[Finding]
    cross_file_insights: str = ""
    executive_summary: str = ""
    calls_made: int = 0
    model: str = ""
    errors: list[str] = field(default_factory=list)


# =============================================================================
# LLMAnalyzer
# =============================================================================


class LLMAnalyzer:
    """Enriches Tier 1 findings with context-aware LLM analysis (Tier 2).

    The analyzer respects three budget controls from ``LintConfig``:

    - ``llm.max_llm_calls``: hard cap on total API calls per scan
    - ``llm.min_severity_for_llm``: findings below this severity are skipped
    - ``llm.batch_size``: max findings analysed per file (avoids flooding a
      single noisy file while starving others)

    The call budget is split as follows:
    - 1 call reserved for the cross-file analysis (if ≥ 2 files have findings)
    - 1 call reserved for the executive summary
    - Remaining budget allocated to per-finding deep-dives

    Args:
        config: Fully resolved ``LintConfig`` for this scan run.
        provider: An ``LLMProvider`` instance.  When ``None``, a
            ``ClaudeLLMProvider`` is constructed from *config* on first use.
    """

    def __init__(
        self,
        config: LintConfig,
        provider: LLMProvider | None = None,
    ) -> None:
        self._config = config
        self._provider = provider  # built lazily to defer the ImportError

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, config: LintConfig) -> LLMAnalyzer:
        """Construct an analyzer that builds its provider from *config*.

        The provider is created lazily (on first use) so that constructing
        an ``LLMAnalyzer`` never raises ``ImportError`` even when the
        ``anthropic`` package is absent.

        Args:
            config: Resolved ``LintConfig`` for the current scan.

        Returns:
            A ready-to-use ``LLMAnalyzer`` instance.
        """
        return cls(config=config, provider=None)

    def analyze(
        self,
        report: AuditReport,
        source_map: dict[str, str] | None = None,
    ) -> LLMAnalysisResult:
        """Run all Tier 2 analyses and return a consolidated result.

        Orchestrates three sub-analyses in order:

        1. **Per-finding enrichment** — ``enrich_findings()`` annotates each
           qualifying finding with a detailed LLM explanation.
        2. **Cross-file analysis** — identifies systemic patterns when more
           than one file has findings.
        3. **Executive summary** — produces a management-level health report.

        API calls are bounded by ``llm.max_llm_calls``; if the budget is
        exhausted the remaining steps are skipped gracefully.

        Args:
            report: ``AuditReport`` produced by the Tier 1 scan.
            source_map: Optional mapping of ``file_path → source_text``.
                When provided, exact source snippets are included in prompts.
                When ``None``, the analyzer attempts to read files from disk.

        Returns:
            ``LLMAnalysisResult`` with enriched findings and analysis text.
        """
        llm_cfg = self._config.raw.get("llm", {})
        max_calls: int = int(llm_cfg.get("max_llm_calls", 20))

        try:
            model = self._get_provider().model
        except Exception:  # noqa: BLE001 — provider unavailable; degrade gracefully
            model = ""

        result = LLMAnalysisResult(
            enriched_findings=list(report.findings),
            model=model,
        )

        # Reserve 1 call each for cross-file and exec summary (if budget allows)
        calls_for_findings = max(0, max_calls - 2)

        # 1. Per-finding enrichment
        result.enriched_findings = self.enrich_findings(
            result.enriched_findings,
            source_map=source_map,
            max_calls=calls_for_findings,
        )
        result.calls_made = sum(
            1 for f in result.enriched_findings if f.llm_insight is not None
        )

        remaining = max_calls - result.calls_made

        # 2. Cross-file analysis (≥ 2 files, budget permitting)
        files_with_findings = {f.file_path for f in result.enriched_findings}
        if remaining >= 1 and len(files_with_findings) >= 2:
            try:
                result.cross_file_insights = self._cross_file_analysis(
                    result.enriched_findings
                )
                result.calls_made += 1
                remaining -= 1
            except Exception as exc:  # noqa: BLE001
                msg = f"Cross-file analysis failed: {exc}"
                logger.warning(msg)
                result.errors.append(msg)

        # 3. Executive summary (always attempt if budget allows)
        if remaining >= 1:
            try:
                result.executive_summary = self._executive_summary(report)
                result.calls_made += 1
            except Exception as exc:  # noqa: BLE001
                msg = f"Executive summary failed: {exc}"
                logger.warning(msg)
                result.errors.append(msg)

        logger.info(
            "Tier 2 analysis complete: %d LLM calls, %d findings enriched",
            result.calls_made,
            sum(1 for f in result.enriched_findings if f.llm_insight),
        )
        return result

    def enrich_findings(
        self,
        findings: list[Finding],
        source_map: dict[str, str] | None = None,
        max_calls: int | None = None,
    ) -> list[Finding]:
        """Annotate qualifying findings with LLM-generated explanations.

        Each finding that passes the severity filter and the per-file
        ``batch_size`` cap is sent to the LLM individually.  The response is
        stored in ``finding.llm_insight``.  Findings that are skipped retain
        ``llm_insight = None``.

        The original list is mutated in-place *and* returned so callers can
        use this either as a mutation or a functional transform.

        Args:
            findings: List of ``Finding`` objects to enrich.
            source_map: ``{file_path: source_text}`` for snippet extraction.
                Files absent from the map are read from disk if possible.
            max_calls: Override the call budget for this invocation.
                When ``None``, ``llm.max_llm_calls`` from config is used.

        Returns:
            The same ``findings`` list, with ``llm_insight`` set on
            qualifying entries.
        """
        llm_cfg = self._config.raw.get("llm", {})
        batch_size: int = int(llm_cfg.get("batch_size", 5))
        min_sev_name: str = llm_cfg.get("min_severity_for_llm", "WARNING")
        try:
            min_severity = Severity[min_sev_name.upper()]
        except KeyError:
            min_severity = Severity.WARNING

        budget = (
            max_calls
            if max_calls is not None
            else int(llm_cfg.get("max_llm_calls", 20))
        )

        calls_used = 0
        # Track per-file usage to respect batch_size
        file_call_counts: dict[str, int] = {}

        for finding in findings:
            if calls_used >= budget:
                logger.debug("LLM call budget exhausted (%d calls)", budget)
                break

            if finding.severity < min_severity:
                continue

            file_key = finding.file_path
            if file_call_counts.get(file_key, 0) >= batch_size:
                continue

            snippet = self._extract_snippet(finding, source_map)
            try:
                from spark_perf_lint.llm.prompts import PromptTemplates

                messages = PromptTemplates.finding_analysis(finding, snippet)
                response = self._get_provider().complete(
                    messages, system=PromptTemplates.SYSTEM
                )
                finding.llm_insight = response.strip()
                file_call_counts[file_key] = file_call_counts.get(file_key, 0) + 1
                calls_used += 1
                logger.debug(
                    "Enriched finding %s in %s (call %d/%d)",
                    finding.rule_id,
                    finding.file_path,
                    calls_used,
                    budget,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to enrich finding %s: %s", finding.rule_id, exc
                )

        return findings

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _cross_file_analysis(self, findings: list[Finding]) -> str:
        """Call the LLM for a cross-file pattern analysis.

        Args:
            findings: All enriched findings from the scan.

        Returns:
            Raw LLM response text.
        """
        from spark_perf_lint.llm.prompts import PromptTemplates

        by_file: dict[str, list[Finding]] = {}
        for f in findings:
            by_file.setdefault(f.file_path, []).append(f)

        messages = PromptTemplates.cross_file_analysis(by_file)
        return self._get_provider().complete(
            messages, system=PromptTemplates.SYSTEM
        ).strip()

    def _executive_summary(self, report: AuditReport) -> str:
        """Call the LLM for a management-level executive summary.

        Args:
            report: The complete ``AuditReport``.

        Returns:
            Raw LLM response text.
        """
        from spark_perf_lint.llm.prompts import PromptTemplates

        messages = PromptTemplates.executive_summary(report)
        return self._get_provider().complete(
            messages, system=PromptTemplates.SYSTEM
        ).strip()

    def _get_provider(self) -> LLMProvider:
        """Return the provider, building it lazily from config if needed.

        Raises:
            ImportError: If ``anthropic`` is not installed and no explicit
                provider was supplied.
            ValueError: If the API key cannot be resolved.
        """
        if self._provider is None:
            from spark_perf_lint.llm.provider import ClaudeLLMProvider

            self._provider = ClaudeLLMProvider.from_config(self._config)
        return self._provider

    @staticmethod
    def _extract_snippet(
        finding: Finding,
        source_map: dict[str, str] | None,
    ) -> str:
        """Return the source lines around *finding* as a formatted string.

        Attempts to read from *source_map* first, then falls back to reading
        the file from disk.  Returns a placeholder string if neither source
        is available.

        Args:
            finding: The finding whose line number guides the extraction.
            source_map: Optional pre-loaded source texts.

        Returns:
            Up to ``2 * _SNIPPET_CONTEXT + 1`` lines centred on the finding,
            prefixed with 1-based line numbers.
        """
        source: str | None = None

        if source_map:
            source = source_map.get(finding.file_path)

        if source is None:
            try:
                path = Path(finding.file_path)
                if path.is_file():
                    source = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                pass

        if source is None:
            return f"# Source not available for {finding.file_path}"

        lines = source.splitlines()
        # Convert to 0-based index; clamp to valid range
        idx = max(0, finding.line_number - 1)
        start = max(0, idx - _SNIPPET_CONTEXT)
        end = min(len(lines), idx + _SNIPPET_CONTEXT + 1)

        numbered = [
            f"{lineno:4d} | {line}"
            for lineno, line in enumerate(lines[start:end], start=start + 1)
        ]
        return "\n".join(numbered)

    def __repr__(self) -> str:
        provider_repr = repr(self._provider) if self._provider else "<lazy>"
        return f"LLMAnalyzer(provider={provider_repr})"
