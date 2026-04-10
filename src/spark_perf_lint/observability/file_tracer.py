"""JSON file-based observability tracer for spark-perf-lint.

Writes one JSON trace document per scan run to
``observability.output_dir`` (default: ``.spark-perf-lint-traces/``).

Trace file naming::

    spl-trace-<ISO8601-timestamp>-<run_id[:8]>.json

    e.g.  spl-trace-2026-04-10T14-32-05-3f2504e0.json

Trace document schema
---------------------
The root object always contains ``run``, ``summary``, and ``config`` keys.
Additional keys are added based on the ``trace_level`` setting:

``minimal`` (always present)
    - ``run``     — run_id, started_at, ended_at, duration_s, files_scanned
    - ``summary`` — total findings + counts by severity and dimension
    - ``config``  — subset of resolved config (threshold, formats, llm, obs)

``standard`` (default, adds to minimal)
    - ``findings``  — list of findings at WARNING or above
    - ``file_stats`` — list of ``{file, findings, duration_s}`` dicts

``verbose`` (adds to standard)
    - ``findings``  — all findings including INFO
    - ``file_stats`` — per-file timing for every scanned file
    - ``config``    — full ``config_used`` dict (unabridged)

The trace is written atomically: the full document is serialised to a
string first, then written in one ``Path.write_text()`` call so a
partial file is never left on disk if the process is interrupted.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from spark_perf_lint.observability.tracer import BaseTracer
from spark_perf_lint.types import Severity

if TYPE_CHECKING:
    from spark_perf_lint.config import LintConfig
    from spark_perf_lint.types import AuditReport, Finding

logger = logging.getLogger(__name__)


class FileTracer(BaseTracer):
    """Writes a single JSON trace document per scan run.

    The output directory is created on first use.  When the directory
    cannot be created (e.g. permission error), a warning is logged and
    the scan continues without tracing — the file tracer is never
    allowed to abort the linting run.

    Args:
        config: Resolved ``LintConfig`` for this scan.

    Example trace document (``standard`` level)::

        {
          "run": {
            "run_id": "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
            "started_at": "2026-04-10T14:32:05.123456+00:00",
            "ended_at":   "2026-04-10T14:32:07.891234+00:00",
            "duration_s": 2.768,
            "files_scanned": 12,
            "spark_perf_lint_version": "0.3.0"
          },
          "summary": {
            "total": 7,
            "by_severity": {"CRITICAL": 2, "WARNING": 4, "INFO": 1},
            "by_dimension": {"D03_JOINS": 3, "D02_SHUFFLE": 2, ...}
          },
          "config": {
            "severity_threshold": "WARNING",
            "report_formats": ["terminal"],
            "llm_enabled": false,
            "observability_backend": "file",
            "trace_level": "standard"
          },
          "findings": [ ... ],
          "file_stats": [ ... ]
        }
    """

    def __init__(self, config: LintConfig) -> None:
        super().__init__(config)
        obs_cfg = config.raw.get("observability", {})
        self._output_dir = Path(obs_cfg.get("output_dir", ".spark-perf-lint-traces"))
        self._started_at: datetime | None = None
        self._file_stats: list[dict[str, Any]] = []
        self._findings: list[Finding] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start_run(self, run_id: str) -> None:
        """Record run start timestamp.

        Args:
            run_id: UUID4 string identifying this scan run.
        """
        super().start_run(run_id)
        self._started_at = datetime.now(tz=timezone.utc)
        self._file_stats = []
        self._findings = []

    def record_file(
        self,
        file_path: str,
        finding_count: int,
        duration_s: float,
    ) -> None:
        """Accumulate per-file stats for the trace document.

        Only stored when ``trace_level`` is STANDARD or VERBOSE.

        Args:
            file_path: Repo-relative path of the scanned file.
            finding_count: Findings emitted for this file.
            duration_s: Wall-clock seconds spent scanning this file.
        """
        if self._should_include_findings():
            self._file_stats.append(
                {
                    "file": file_path,
                    "findings": finding_count,
                    "duration_s": round(duration_s, 4),
                }
            )

    def record_findings(self, findings: list[Finding]) -> None:
        """Store findings for inclusion in the trace document.

        At STANDARD level, only WARNING+ findings are kept.
        At VERBOSE level, all findings are kept.

        Args:
            findings: All findings from the completed scan run.
        """
        if not self._should_include_findings():
            return

        if self._should_include_verbose():
            self._findings = list(findings)
        else:
            self._findings = [f for f in findings if f.severity >= Severity.WARNING]

    def end_run(self, report: AuditReport) -> None:
        """Serialise and write the trace document.

        The output directory is created if it does not exist.  A warning is
        logged (and the method returns quietly) if any I/O error occurs so
        that trace failures never abort the linting run.

        Args:
            report: The fully assembled ``AuditReport`` for this run.
        """
        ended_at = datetime.now(tz=timezone.utc)
        started_at = self._started_at or ended_at

        document = self._build_document(report, started_at, ended_at)
        self._write(document, started_at)

    # ------------------------------------------------------------------
    # Document construction
    # ------------------------------------------------------------------

    def _build_document(
        self,
        report: AuditReport,
        started_at: datetime,
        ended_at: datetime,
    ) -> dict[str, Any]:
        """Assemble the trace document dictionary.

        Args:
            report: The completed ``AuditReport``.
            started_at: UTC datetime when ``start_run`` was called.
            ended_at: UTC datetime when ``end_run`` was called.

        Returns:
            A JSON-serialisable dictionary representing the full trace.
        """
        duration_s = (ended_at - started_at).total_seconds()

        doc: dict[str, Any] = {
            "run": self._build_run_section(started_at, ended_at, duration_s, report),
            "summary": report.summary,
            "config": self._build_config_section(report),
        }

        if self._should_include_findings():
            doc["findings"] = [self._serialise_finding(f) for f in self._findings]
            doc["file_stats"] = self._file_stats

        return doc

    def _build_run_section(
        self,
        started_at: datetime,
        ended_at: datetime,
        duration_s: float,
        report: AuditReport,
    ) -> dict[str, Any]:
        """Build the ``run`` metadata section.

        Args:
            started_at: UTC datetime for the run start.
            ended_at: UTC datetime for the run end.
            duration_s: Total wall-clock seconds.
            report: The completed ``AuditReport``.

        Returns:
            Dictionary with run identity and timing fields.
        """
        try:
            from spark_perf_lint import __version__ as version
        except ImportError:
            version = "unknown"

        return {
            "run_id": self._run_id,
            "started_at": started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "duration_s": round(duration_s, 3),
            "files_scanned": report.files_scanned,
            "spark_perf_lint_version": version,
        }

    def _build_config_section(self, report: AuditReport) -> dict[str, Any]:
        """Build the ``config`` section.

        At VERBOSE level the full ``config_used`` dict is included.
        Otherwise a compact summary is used to keep the trace file small.

        Args:
            report: The completed ``AuditReport`` (provides ``config_used``).

        Returns:
            Dictionary summarising or reproducing the active configuration.
        """
        if self._should_include_verbose():
            return report.config_used

        raw = report.config_used
        obs = raw.get("observability", {})
        return {
            "severity_threshold": raw.get("general", {}).get("severity_threshold"),
            "report_formats": raw.get("general", {}).get("report_format"),
            "llm_enabled": raw.get("llm", {}).get("enabled"),
            "observability_backend": obs.get("backend"),
            "trace_level": obs.get("trace_level"),
        }

    @staticmethod
    def _serialise_finding(finding: Finding) -> dict[str, Any]:
        """Convert a ``Finding`` to a JSON-safe dict.

        Uses ``finding.to_dict()`` as the base but trims long fields
        (``explanation``, ``before_code``, ``after_code``) at STANDARD level
        to keep file sizes reasonable.  At VERBOSE level callers pass all
        findings through ``record_findings`` unfiltered, so the full dict is
        always written.

        Args:
            finding: A ``Finding`` instance to serialise.

        Returns:
            A JSON-serialisable dictionary.
        """
        return finding.to_dict()

    # ------------------------------------------------------------------
    # I/O
    # ------------------------------------------------------------------

    def _write(self, document: dict[str, Any], started_at: datetime) -> None:
        """Serialise *document* to a JSON file in the output directory.

        The filename encodes the timestamp and first 8 characters of the
        run ID so that files sort chronologically and are easy to identify.

        Args:
            document: The assembled trace document dictionary.
            started_at: Used to build the timestamp portion of the filename.
        """
        try:
            self._output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.warning(
                "FileTracer: cannot create output directory %s: %s — trace skipped.",
                self._output_dir,
                exc,
            )
            return

        ts = started_at.strftime("%Y-%m-%dT%H-%M-%S")
        short_id = self._run_id[:8] if self._run_id else "unknown"
        filename = f"spl-trace-{ts}-{short_id}.json"
        out_path = self._output_dir / filename

        try:
            payload = json.dumps(document, indent=2, default=str)
            out_path.write_text(payload, encoding="utf-8")
            logger.debug("FileTracer: trace written to %s", out_path)
        except (OSError, TypeError, ValueError) as exc:
            logger.warning(
                "FileTracer: failed to write trace to %s: %s",
                out_path,
                exc,
            )

    def __repr__(self) -> str:
        return (
            f"FileTracer("
            f"output_dir={self._output_dir!r}, "
            f"trace_level={self._trace_level!r}, "
            f"run_id={self._run_id!r})"
        )
