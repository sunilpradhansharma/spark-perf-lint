"""Main scan orchestrator for spark-perf-lint.

``ScanOrchestrator`` is the single entry point for the linting engine.
It ties together file discovery, AST parsing, rule execution, and report
building into a single ``scan()`` call.

Execution flow::

    ScanOrchestrator(config)
        .scan(paths)
            ├── FileScanner.from_paths(paths, config)  → ScanTarget list
            ├── for each PySpark file:
            │       ASTAnalyzer.from_source(content, filename)
            │       for each enabled rule:
            │           rule.check(analyzer, config)   → list[Finding]
            ├── sort findings: CRITICAL first, then file path, then line
            ├── apply max_findings cap
            └── AuditReport(findings, files_scanned, duration, config)

Usage::

    from spark_perf_lint.config import LintConfig
    from spark_perf_lint.engine.orchestrator import ScanOrchestrator

    config = LintConfig.load()
    orchestrator = ScanOrchestrator(config)
    report = orchestrator.scan(["src/jobs"])
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer, ParseError
from spark_perf_lint.engine.file_scanner import FileScanner, ScanTarget
from spark_perf_lint.observability.tracer import BaseTracer, TracerFactory
from spark_perf_lint.rules.base import BaseRule
from spark_perf_lint.rules.registry import RuleRegistry
from spark_perf_lint.types import AuditReport, Finding, Severity

logger = logging.getLogger(__name__)

# Severity ordering used for sort key (CRITICAL first = lowest sort value)
_SEVERITY_ORDER: dict[Severity, int] = {
    Severity.CRITICAL: 0,
    Severity.WARNING: 1,
    Severity.INFO: 2,
}

# Parallel execution tuning.
# Below _PARALLEL_THRESHOLD files the thread-pool overhead costs more than
# it saves; above it each file is dispatched to its own worker thread so that
# I/O wait and GIL-releasing C extensions (ast.parse) run concurrently.
_PARALLEL_THRESHOLD: int = 3  # files; serial below this count
_MAX_WORKERS: int = 4  # threads; enough for a typical 20-file commit


# Regex for inline suppression comments.
# Matches:  noqa: SPL-D03-001  or  noqa: SPL-D03-001,SPL-D08-002
_NOQA_RE = re.compile(r"#\s*noqa:\s*(SPL-[A-Z0-9,\s\-]+)", re.IGNORECASE)


def _build_noqa_map(content: str) -> dict[int, set[str]]:
    """Parse ``# noqa: SPL-DXX-XXX`` suppression comments from source text.

    A comment of the form ``# noqa: SPL-D03-001`` on a source line tells the
    engine to discard any finding for that rule on that line.  Multiple rule
    IDs can be comma-separated: ``# noqa: SPL-D03-001, SPL-D08-002``.

    Args:
        content: Full Python source text.

    Returns:
        Mapping of 1-based line number → set of suppressed rule IDs.
        Empty dict when no noqa comments are present.
    """
    noqa_map: dict[int, set[str]] = {}
    for lineno, line in enumerate(content.splitlines(), start=1):
        m = _NOQA_RE.search(line)
        if m:
            ids = {r.strip().upper() for r in m.group(1).split(",") if r.strip()}
            if ids:
                noqa_map[lineno] = ids
    return noqa_map


class ScanOrchestrator:
    """Ties together file discovery, AST parsing, rule execution, and reporting.

    One orchestrator instance is typically created per CLI invocation and
    reused across all scanned paths so the rule registry and configuration
    are resolved only once.

    A ``BaseTracer`` is constructed automatically from the config when
    ``observability.enabled = true``; a ``NullTracer`` is used otherwise so
    the rest of the code never needs to branch on tracer presence.

    Args:
        config: Fully resolved ``LintConfig`` for this scan run.
        tracer: Optional explicit tracer.  When ``None`` (the default),
            ``TracerFactory.from_config(config)`` is called to select and
            construct the correct backend.
    """

    def __init__(
        self,
        config: LintConfig,
        tracer: BaseTracer | None = None,
    ) -> None:
        self._config = config
        self._registry = RuleRegistry.instance()
        self._tracer: BaseTracer = (
            tracer if tracer is not None else TracerFactory.from_config(config)
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def scan(self, paths: list[str | Path]) -> AuditReport:
        """Scan a list of file/directory paths and return an ``AuditReport``.

        Directories are walked recursively; only ``.py`` files that contain
        PySpark code are analysed.  Files matching ``ignore.files`` or inside
        ``ignore.directories`` are skipped.

        When observability is enabled the scan is bracketed by
        ``tracer.start_run`` / ``tracer.end_run`` calls and per-file stats
        are forwarded via ``tracer.record_file``.  Tracer failures are caught
        and logged so they never abort the scan.

        Args:
            paths: File or directory paths to scan.  Accepts strings or
                ``Path`` objects.

        Returns:
            ``AuditReport`` containing all findings, timing, and summary
            counts.
        """
        str_paths: list[str | Path] = [str(p) for p in paths]
        start = time.monotonic()

        run_id = TracerFactory.new_run_id()
        try:
            self._tracer.start_run(run_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Tracer.start_run failed: %s", exc)

        targets, _summary = FileScanner.from_paths(str_paths, config=self._config)
        scannable = FileScanner.scannable(targets)

        findings = self._run_rules_on_targets(scannable)
        findings = self._sort_findings(findings)
        findings = self._apply_cap(findings)

        elapsed = time.monotonic() - start
        report = AuditReport(
            findings=findings,
            files_scanned=len(scannable),
            scan_duration_seconds=elapsed,
            config_used=self._config.raw,
        )

        try:
            self._tracer.record_findings(findings)
            self._tracer.end_run(report)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Tracer.end_run failed: %s", exc)

        return report

    def scan_content(self, content: str, filename: str = "<string>") -> AuditReport:
        """Lint a source string directly, without touching the filesystem.

        Useful for:

        - Unit tests that construct code programmatically.
        - CI pipelines that pipe file content through stdin.
        - Editor integrations that want inline diagnostics on unsaved buffers.

        Args:
            content: Full Python source text to analyse.
            filename: Label used in ``Finding.file_path`` and error messages.
                Defaults to ``"<string>"``.

        Returns:
            ``AuditReport`` containing all findings for *content*.
        """
        start = time.monotonic()
        target = ScanTarget(
            path=Path(filename),
            content=content,
            is_pyspark=True,  # trust the caller; no filesystem check needed
            relative_path=filename,
            size_bytes=len(content.encode()),
        )
        findings = self._run_rules_on_target(target)
        findings = self._sort_findings(findings)
        findings = self._apply_cap(findings)

        elapsed = time.monotonic() - start
        return AuditReport(
            findings=findings,
            files_scanned=1,
            scan_duration_seconds=elapsed,
            config_used=self._config.raw,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_rules_on_targets(self, targets: list[ScanTarget]) -> list[Finding]:
        """Run all enabled rules across a list of scan targets.

        Pre-computes the enabled-rule list once so the registry is not
        accessed from multiple threads simultaneously.  For small batches
        (< ``_PARALLEL_THRESHOLD`` files) the serial path is used to avoid
        thread-pool overhead.  Larger batches are dispatched to a
        ``ThreadPoolExecutor`` so that AST parsing and I/O-bound work
        can proceed concurrently.

        Args:
            targets: PySpark files to analyse.

        Returns:
            Flat list of all findings from all targets (order not
            guaranteed when parallel execution is used).
        """
        if not targets:
            return []

        # Resolve once — registry access is not thread-safe under mutation.
        enabled_rules = self._registry.get_enabled_rules(self._config)

        if len(targets) < _PARALLEL_THRESHOLD:
            # Serial fast-path: zero thread overhead for tiny batches.
            all_findings: list[Finding] = []
            for target in targets:
                all_findings.extend(self._run_rules_on_target(target, enabled_rules))
            return all_findings

        # Parallel path: dispatch each file to a worker thread.
        all_findings = []
        n_workers = min(len(targets), _MAX_WORKERS)
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {
                pool.submit(self._run_rules_on_target, target, enabled_rules): target
                for target in targets
            }
            for future in as_completed(futures):
                try:
                    all_findings.extend(future.result())
                except Exception as exc:  # noqa: BLE001
                    tgt = futures[future]
                    logger.error(
                        "Worker failed unexpectedly on %s: %s",
                        tgt.relative_path,
                        exc,
                    )
        return all_findings

    def _run_rules_on_target(
        self,
        target: ScanTarget,
        enabled_rules: list[BaseRule] | None = None,
    ) -> list[Finding]:
        """Parse and run all enabled rules against a single ``ScanTarget``.

        Parse errors are logged as warnings and produce a synthetic INFO
        finding rather than aborting the whole scan.

        Per-file timing is forwarded to the tracer via ``record_file`` after
        every file, regardless of whether findings were produced.

        Args:
            target: A single file target with pre-loaded content.
            enabled_rules: Pre-computed rule list from the caller.  When
                ``None`` (e.g. from ``scan_content``), the list is resolved
                from the registry.  Pass an explicit list when calling from a
                thread pool to avoid repeated registry lookups.

        Returns:
            All findings for *target*, or a single parse-error finding if
            the file cannot be parsed.
        """
        file_start = time.monotonic()

        try:
            analyzer = ASTAnalyzer.from_source(target.content, filename=target.relative_path)
        except ParseError as exc:
            logger.warning("Parse error in %s: %s", target.relative_path, exc)
            result = [self._make_parse_error_finding(target.relative_path, exc)]
            self._emit_file_stat(target.relative_path, len(result), file_start)
            return result
        except Exception as exc:  # noqa: BLE001 — broad catch: never abort the scan
            logger.error("Unexpected error parsing %s: %s", target.relative_path, exc)
            self._emit_file_stat(target.relative_path, 0, file_start)
            return []

        if enabled_rules is None:
            enabled_rules = self._registry.get_enabled_rules(self._config)

        findings: list[Finding] = []
        for rule in enabled_rules:
            try:
                rule_findings = rule.check(analyzer, self._config)
                findings.extend(rule_findings)
            except Exception as exc:  # noqa: BLE001 — a broken rule must not crash the scan
                logger.error(
                    "Rule %s raised an unexpected error on %s: %s",
                    rule.rule_id,
                    target.relative_path,
                    exc,
                )

        if findings:
            noqa_map = _build_noqa_map(target.content)
            if noqa_map:
                findings = [
                    f for f in findings if f.rule_id not in noqa_map.get(f.line_number, set())
                ]

        self._emit_file_stat(target.relative_path, len(findings), file_start)
        return findings

    def _emit_file_stat(
        self,
        file_path: str,
        finding_count: int,
        file_start: float,
    ) -> None:
        """Forward per-file timing to the tracer; swallow tracer errors.

        Args:
            file_path: Repo-relative path of the file that was scanned.
            finding_count: Number of findings produced for this file.
            file_start: ``time.monotonic()`` value recorded before scanning.
        """
        try:
            self._tracer.record_file(
                file_path,
                finding_count,
                time.monotonic() - file_start,
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Tracer.record_file failed for %s: %s", file_path, exc)

    @staticmethod
    def _sort_findings(findings: list[Finding]) -> list[Finding]:
        """Sort findings: CRITICAL first, then by file path, then by line.

        Args:
            findings: Unsorted list of findings.

        Returns:
            New sorted list (original is not mutated).
        """
        return sorted(
            findings,
            key=lambda f: (
                _SEVERITY_ORDER.get(f.severity, 99),
                f.file_path,
                f.line_number,
            ),
        )

    def _apply_cap(self, findings: list[Finding]) -> list[Finding]:
        """Truncate the findings list if ``max_findings`` is set.

        Args:
            findings: Already-sorted findings list.

        Returns:
            Truncated list, or the original list if ``max_findings == 0``.
        """
        cap = self._config.max_findings
        if cap > 0 and len(findings) > cap:
            logger.debug("Truncating findings from %d to max_findings=%d", len(findings), cap)
            return findings[:cap]
        return findings

    @staticmethod
    def _make_parse_error_finding(filename: str, exc: ParseError) -> Finding:
        """Produce a synthetic Finding for a file that could not be parsed.

        Args:
            filename: Path label for the unparseable file.
            exc: The ``ParseError`` raised by the AST analyzer.

        Returns:
            An INFO-severity Finding that surfaces the syntax error.
        """
        from spark_perf_lint.types import Dimension, EffortLevel

        return Finding(
            rule_id="SPL-D00-000",
            severity=Severity.INFO,
            dimension=Dimension.D01_CLUSTER_CONFIG,  # placeholder; no own dimension
            file_path=filename,
            line_number=max(exc.lineno or 1, 1),
            message=f"Syntax error — file could not be parsed: {exc.msg}",
            explanation=(
                "spark-perf-lint could not parse this file due to a Python syntax error. "
                "No rules were run against it. Fix the syntax error to enable analysis."
            ),
            recommendation="Fix the reported syntax error.",
            effort_level=EffortLevel.MINOR_CODE_CHANGE,
        )

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"ScanOrchestrator("
            f"rules={len(self._registry)}, "
            f"threshold={self._config.severity_threshold.name}, "
            f"tracer={self._tracer.__class__.__name__})"
        )
