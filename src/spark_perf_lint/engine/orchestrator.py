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
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer, ParseError
from spark_perf_lint.engine.file_scanner import FileScanner, ScanTarget
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
_PARALLEL_THRESHOLD: int = 3   # files; serial below this count
_MAX_WORKERS: int = 4           # threads; enough for a typical 20-file commit


class ScanOrchestrator:
    """Ties together file discovery, AST parsing, rule execution, and reporting.

    One orchestrator instance is typically created per CLI invocation and
    reused across all scanned paths so the rule registry and configuration
    are resolved only once.

    Args:
        config: Fully resolved ``LintConfig`` for this scan run.
    """

    def __init__(self, config: LintConfig) -> None:
        self._config = config
        self._registry = RuleRegistry.instance()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def scan(self, paths: list[str | Path]) -> AuditReport:
        """Scan a list of file/directory paths and return an ``AuditReport``.

        Directories are walked recursively; only ``.py`` files that contain
        PySpark code are analysed.  Files matching ``ignore.files`` or inside
        ``ignore.directories`` are skipped.

        Args:
            paths: File or directory paths to scan.  Accepts strings or
                ``Path`` objects.

        Returns:
            ``AuditReport`` containing all findings, timing, and summary
            counts.
        """
        str_paths = [str(p) for p in paths]
        start = time.monotonic()

        targets, _summary = FileScanner.from_paths(str_paths, config=self._config)
        scannable = FileScanner.scannable(targets)

        findings = self._run_rules_on_targets(scannable)
        findings = self._sort_findings(findings)
        findings = self._apply_cap(findings)

        elapsed = time.monotonic() - start
        return AuditReport(
            findings=findings,
            files_scanned=len(scannable),
            scan_duration_seconds=elapsed,
            config_used=self._config.raw,
        )

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
        enabled_rules: list | None = None,
    ) -> list[Finding]:
        """Parse and run all enabled rules against a single ``ScanTarget``.

        Parse errors are logged as warnings and produce a synthetic INFO
        finding rather than aborting the whole scan.

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
        try:
            analyzer = ASTAnalyzer.from_source(target.content, filename=target.relative_path)
        except ParseError as exc:
            logger.warning("Parse error in %s: %s", target.relative_path, exc)
            return [self._make_parse_error_finding(target.relative_path, exc)]
        except Exception as exc:  # noqa: BLE001 — broad catch: never abort the scan
            logger.error("Unexpected error parsing %s: %s", target.relative_path, exc)
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
        return findings

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
            f"threshold={self._config.severity_threshold.name})"
        )
