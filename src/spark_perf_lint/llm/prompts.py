"""Prompt templates for Tier 2 LLM analysis.

Every public method on ``PromptTemplates`` returns a ``messages`` list
ready to pass directly to ``LLMProvider.complete()``.  The system prompt
is exposed as ``PromptTemplates.SYSTEM`` so callers can pass it as the
``system=`` argument.

Four analysis types are covered:

``finding_analysis``   — deep-dive on a single finding with source context
``cross_file_analysis``— patterns and hotspots across multiple files
``config_coherence``   — coherence review of Spark configuration findings
``executive_summary``  — management-level health summary of a full scan
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spark_perf_lint.types import AuditReport, Finding, SparkConfigEntry

# ---------------------------------------------------------------------------
# System prompt — injected into every LLM call as the "system" role.
# ---------------------------------------------------------------------------

_SYSTEM = """\
You are a senior Apache Spark performance engineer conducting a code review.

Your analysis must be:
- Grounded in specific Spark internals (execution model, DAG, shuffle, \
Catalyst optimizer, AQE, JVM GC)
- Quantified wherever possible (e.g. "3–10× slowdown on datasets > 1 M rows")
- Actionable: concrete code or config changes, not generic advice
- Concise: developers read this in a pull-request review context
- Honest about uncertainty: say "depends on data volume" rather than guessing

When you reference Spark concepts explain them in one sentence so a \
mid-level Python engineer can follow along.\
"""


class PromptTemplates:
    """Static factory for all LLM prompt payloads used by ``LLMAnalyzer``.

    All methods are static; the class is a namespace only.  Every method
    returns a ``list[dict[str, str]]`` (the ``messages`` argument for
    ``LLMProvider.complete``).

    Callers should also pass ``PromptTemplates.SYSTEM`` as the ``system=``
    keyword argument so that the system instructions are applied consistently.
    """

    SYSTEM: str = _SYSTEM

    # ------------------------------------------------------------------
    # 1. Single-finding deep-dive
    # ------------------------------------------------------------------

    @staticmethod
    def finding_analysis(finding: Finding, source_snippet: str) -> list[dict[str, str]]:
        """Build a prompt for a deep-dive analysis of one finding.

        The prompt embeds the static-analysis context (rule ID, severity,
        explanation, before/after examples) alongside the actual source lines
        so the model can give file-specific, actionable advice.

        Args:
            finding: The ``Finding`` produced by a Tier 1 rule.
            source_snippet: The relevant source lines around the finding
                (typically ±10 lines from ``finding.line_number``).

        Returns:
            A ``messages`` list suitable for ``LLMProvider.complete()``.
        """
        parts: list[str] = [
            f"## Finding {finding.rule_id}  [{finding.severity.name}]",
            f"**Dimension**: {finding.dimension.display_name}",
            f"**Location**: `{finding.file_path}`, line {finding.line_number}",
            f"**Static analyser message**: {finding.message}",
            "",
            "### Source context",
            "```python",
            source_snippet.rstrip(),
            "```",
            "",
            "### Static analyser context",
            f"*Explanation*: {finding.explanation}",
            f"*Recommendation*: {finding.recommendation}",
        ]

        if finding.estimated_impact:
            parts += [f"*Estimated impact*: {finding.estimated_impact}"]

        if finding.before_code:
            parts += [
                "",
                "**Anti-pattern** (what was detected):",
                "```python",
                finding.before_code,
                "```",
            ]
        if finding.after_code:
            parts += [
                "**Corrected pattern** (suggested fix):",
                "```python",
                finding.after_code,
                "```",
            ]

        parts += [
            "",
            "---",
            "Please provide a focused analysis with these four sections:",
            "",
            "**1. Root cause** — Why does this exact pattern hurt Spark performance? "
            "Reference the specific internal mechanism (e.g., full shuffle, driver OOM, "
            "Catalyst plan explosion).",
            "",
            "**2. Concrete fix** — A specific change to *this* code at *this* location, "
            "not a generic rewrite. Show the minimal delta.",
            "",
            "**3. Impact estimate** — Quantify the expected improvement: query time, "
            "shuffle bytes, memory pressure, or GC overhead. State the data-size "
            "assumptions.",
            "",
            "**4. Caveats** — Edge cases or risks the developer should verify before "
            "applying the fix (e.g., semantics change, dependency on AQE config).",
        ]

        return [{"role": "user", "content": "\n".join(parts)}]

    # ------------------------------------------------------------------
    # 2. Cross-file pattern analysis
    # ------------------------------------------------------------------

    @staticmethod
    def cross_file_analysis(
        findings_by_file: dict[str, list[Finding]],
    ) -> list[dict[str, str]]:
        """Build a prompt for identifying patterns across multiple files.

        Groups all findings by file so the model can spot team-wide habits,
        architectural issues, and cross-cutting hotspots.

        Args:
            findings_by_file: Mapping from file path to the list of
                ``Finding`` objects detected in that file.

        Returns:
            A ``messages`` list suitable for ``LLMProvider.complete()``.
        """
        total = sum(len(fs) for fs in findings_by_file.values())
        parts: list[str] = [
            "## Cross-File Spark Performance Analysis",
            "",
            f"A static scan found **{total} finding(s)** across "
            f"**{len(findings_by_file)} file(s)**.",
            "",
        ]

        for file_path, findings in findings_by_file.items():
            parts.append(f"### `{file_path}` — {len(findings)} finding(s)")
            for f in sorted(findings, key=lambda x: (x.severity.value * -1, x.line_number)):
                parts.append(
                    f"- `[{f.severity.name}]` **{f.rule_id}** "
                    f"(line {f.line_number}): {f.message}"
                )
            parts.append("")

        parts += [
            "---",
            "Please identify:",
            "",
            "**1. Systemic patterns** — Anti-patterns that repeat across multiple files. "
            "What team-wide habit or architectural decision is causing them?",
            "",
            "**2. Hotspot ranking** — List the files in priority order by performance risk, "
            "with one sentence justifying each ranking.",
            "",
            "**3. Top 3 quick wins** — The three changes that would have the largest "
            "performance impact for the least engineering effort. Be specific: name "
            "the file, line range, and the change.",
            "",
            "**4. Root cause hypothesis** — Is there a single underlying cause "
            "(e.g., AQE not enabled, no caching strategy, wrong join pattern) that "
            "explains most of the findings?",
        ]

        return [{"role": "user", "content": "\n".join(parts)}]

    # ------------------------------------------------------------------
    # 3. Spark configuration coherence
    # ------------------------------------------------------------------

    @staticmethod
    def config_coherence(config_entries: list[SparkConfigEntry]) -> list[dict[str, str]]:
        """Build a prompt for Spark configuration coherence analysis.

        Reviews multiple ``SparkConfigEntry`` findings together so the model
        can surface parameter interactions and ordering dependencies that
        per-entry analysis would miss.

        Args:
            config_entries: Configuration audit results from the scan.

        Returns:
            A ``messages`` list suitable for ``LLMProvider.complete()``.
        """
        parts: list[str] = [
            "## Spark Configuration Coherence Review",
            "",
            f"The following **{len(config_entries)} configuration issue(s)** were detected:",
            "",
        ]

        for entry in config_entries:
            parts += [
                f"### `{entry.parameter}`",
                f"- **Current value**: `{entry.current_value}`",
                f"- **Recommended value**: `{entry.recommended_value}`",
                f"- **Reason**: {entry.reason}",
                f"- **Impact**: {entry.impact}",
                "",
            ]

        parts += [
            "---",
            "Please assess:",
            "",
            "**1. Coherence** — Do these config values work well together, or do some "
            "conflict or undermine each other? Give concrete examples.",
            "",
            "**2. Dependency order** — Which changes must be applied first because others "
            "depend on them (e.g., enabling AQE before tuning AQE sub-parameters)?",
            "",
            "**3. Missing configs** — What related parameters are commonly set alongside "
            "these but are absent? Include the recommended value and reason.",
            "",
            "**4. Cluster sizing signal** — Based on these configs, is the cluster sized "
            "appropriately for typical Spark batch workloads?",
        ]

        return [{"role": "user", "content": "\n".join(parts)}]

    # ------------------------------------------------------------------
    # 4. Executive summary
    # ------------------------------------------------------------------

    @staticmethod
    def executive_summary(report: AuditReport) -> list[dict[str, str]]:
        """Build a prompt for a management-level summary of the full scan.

        Packages the aggregated counts and dimension breakdown so the model
        can write a non-technical health assessment suitable for a tech lead
        or engineering manager.

        Args:
            report: The complete ``AuditReport`` produced by Tier 1 analysis.

        Returns:
            A ``messages`` list suitable for ``LLMProvider.complete()``.
        """
        summary = report.summary
        by_sev = summary.get("by_severity", {})
        by_dim = summary.get("by_dimension", {})

        # Top 5 dimensions by finding count (exclude zero-count entries)
        active_dims = sorted(
            [(dim, cnt) for dim, cnt in by_dim.items() if cnt > 0],
            key=lambda x: -x[1],
        )

        parts: list[str] = [
            "## Spark Performance Audit — Executive Summary",
            "",
            "### Scan statistics",
            f"- Files scanned   : {report.files_scanned}",
            f"- Total findings  : {summary.get('total', 0)}",
            f"- CRITICAL        : {by_sev.get('CRITICAL', 0)}",
            f"- WARNING         : {by_sev.get('WARNING', 0)}",
            f"- INFO            : {by_sev.get('INFO', 0)}",
            f"- Scan duration   : {report.scan_duration_seconds:.1f}s",
            "",
            "### Findings by performance dimension (top areas)",
        ]

        for dim, count in active_dims[:6]:
            parts.append(f"- {dim}: {count} finding(s)")
        if len(active_dims) > 6:
            remaining = sum(cnt for _, cnt in active_dims[6:])
            parts.append(f"- … {len(active_dims) - 6} more dimensions ({remaining} findings)")

        parts += [
            "",
            "---",
            "Write an executive summary (3–5 paragraphs) for a technical lead or "
            "engineering manager who is *not* a Spark expert. Cover:",
            "",
            "**1. Overall health** — Is the Spark usage in this codebase healthy, "
            "concerning, or critical? Give a one-sentence verdict.",
            "",
            "**2. Top 3 performance risks** — Specific patterns that could cause "
            "production incidents (OOMs, hour-long queries, job failures) with a "
            "brief explanation of the mechanism.",
            "",
            "**3. Immediate actions** — The 2–3 highest-priority remediations, each "
            "with an engineering-effort estimate (hours/days).",
            "",
            "**4. Positive signals** — Any dimensions with zero findings or patterns "
            "that indicate good Spark engineering practice.",
            "",
            "**5. Risk score** — Rate the overall Spark performance risk as one of: "
            "LOW / MEDIUM / HIGH / CRITICAL. Justify in one sentence.",
            "",
            "Briefly explain any Spark jargon you use (shuffle, AQE, broadcast join, "
            "etc.) so a non-Spark reader can follow along.",
        ]

        return [{"role": "user", "content": "\n".join(parts)}]
