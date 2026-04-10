"""JSON reporter for spark-perf-lint.

Serialises a complete ``AuditReport`` to structured, machine-readable JSON.
The output envelope wraps findings with run metadata (timestamp, tool version,
config source) so downstream consumers — CI dashboards, audit databases, or
other tooling — have everything they need in one self-describing document.

Output structure::

    {
      "metadata": {
        "tool": "spark-perf-lint",
        "version": "x.y.z",
        "generated_at": "2026-04-10T12:00:00.000000+00:00",
        "config_source": ".spark-perf-lint.yaml",   // or "defaults only"
        "severity_threshold": "INFO",
        "fail_on": ["CRITICAL"]
      },
      "scan": {
        "files_scanned": 5,
        "duration_seconds": 0.123,
        "passed": true,
        "critical_count": 0,
        "warning_count": 3,
        "info_count": 2
      },
      "summary": {
        "total": 5,
        "by_severity":  {"CRITICAL": 0, "WARNING": 3, "INFO": 2},
        "by_dimension": {"D03_JOINS": 2, ...},
        "by_effort":    {"MINOR_CODE_CHANGE": 4, ...}
      },
      "findings": [ ... ],      // one object per Finding
      "config": { ... }         // general + thresholds section; omitted when
                                // include_config=False
    }

Usage::

    from spark_perf_lint.reporters.json_reporter import JsonReporter, write_report

    # Write to stdout
    write_report(report, config)

    # Write to a file
    write_report(report, config, file=open("report.json", "w"))

    # Capture the dict for further processing
    reporter = JsonReporter(report, config)
    data = reporter.to_dict()
"""

from __future__ import annotations

import datetime
import json
import sys
from typing import IO, Any

from spark_perf_lint import __version__
from spark_perf_lint.config import LintConfig
from spark_perf_lint.types import AuditReport

# ---------------------------------------------------------------------------
# Public reporter class
# ---------------------------------------------------------------------------


class JsonReporter:
    """Serialise a ``spark-perf-lint`` ``AuditReport`` to JSON.

    Args:
        report: The completed scan result to serialise.
        config: Resolved ``LintConfig`` used during the scan.
        indent: JSON indentation level.  Use ``None`` for compact (single-line)
            output, or a positive integer for pretty-printed output.
            Defaults to ``2``.
        include_config: When ``True`` (default), embed a ``"config"`` block
            containing the ``general`` and ``thresholds`` sections of the
            resolved configuration.  Set to ``False`` to omit it (e.g. when
            the config is stored separately and you want a smaller payload).
    """

    def __init__(
        self,
        report: AuditReport,
        config: LintConfig,
        *,
        indent: int | None = 2,
        include_config: bool = True,
    ) -> None:
        self.report = report
        self.config = config
        self.indent = indent
        self.include_config = include_config

    # ------------------------------------------------------------------
    # Primary serialisation methods
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Return the full report as a JSON-serialisable dictionary.

        Returns:
            A nested dictionary with keys ``metadata``, ``scan``,
            ``summary``, ``findings``, and (when ``include_config=True``)
            ``config``.
        """
        doc: dict[str, Any] = {
            "metadata": self._build_metadata(),
            "scan": self._build_scan_block(),
            "summary": self.report.summary,
            "findings": [f.to_dict() for f in self.report.findings],
        }
        if self.include_config:
            doc["config"] = self._build_config_block()
        return doc

    def to_json(self) -> str:
        """Serialise the report to a JSON string.

        Returns:
            Formatted JSON string.  Uses ``self.indent`` for pretty-printing.
        """
        return json.dumps(self.to_dict(), indent=self.indent, ensure_ascii=False)

    def write(self, file: IO[str] | None = None) -> None:
        """Write the JSON report to *file* (or ``sys.stdout``).

        Args:
            file: An open, writable text-mode file object.  Pass ``None``
                (default) to write to ``sys.stdout``.
        """
        out = file if file is not None else sys.stdout
        out.write(self.to_json())
        out.write("\n")  # POSIX: text files end with a newline

    # ------------------------------------------------------------------
    # Private block builders
    # ------------------------------------------------------------------

    def _build_metadata(self) -> dict[str, Any]:
        """Build the ``metadata`` envelope block.

        Returns:
            Dictionary with tool name, version, timestamp, config source,
            severity threshold, and fail-on levels.
        """
        generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat(
            timespec="microseconds"
        )
        config_source = (
            str(self.config.config_file_path)
            if self.config.config_file_path is not None
            else "defaults only"
        )
        return {
            "tool": "spark-perf-lint",
            "version": __version__,
            "generated_at": generated_at,
            "config_source": config_source,
            "severity_threshold": self.config.severity_threshold.name,
            "fail_on": [s.name for s in self.config.fail_on],
        }

    def _build_scan_block(self) -> dict[str, Any]:
        """Build the ``scan`` block with counts and timing.

        Returns:
            Dictionary with ``files_scanned``, ``duration_seconds``,
            ``passed``, and per-severity counts.
        """
        return {
            "files_scanned": self.report.files_scanned,
            "duration_seconds": round(self.report.scan_duration_seconds, 6),
            "passed": self.report.passed,
            "critical_count": self.report.critical_count,
            "warning_count": self.report.warning_count,
            "info_count": self.report.info_count,
        }

    def _build_config_block(self) -> dict[str, Any]:
        """Build the ``config`` block from the resolved ``LintConfig``.

        Includes only ``general``, ``thresholds``, and ``severity_override``
        — the sections that directly affect scan results.  LLM, observability,
        and ignore sections are omitted to keep the payload concise.

        Returns:
            A plain dictionary of the relevant config sections.
        """
        raw = self.config.raw
        block: dict[str, Any] = {}
        for section in ("general", "thresholds", "severity_override"):
            if section in raw:
                block[section] = raw[section]
        return block


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------


def write_report(
    report: AuditReport,
    config: LintConfig,
    *,
    indent: int | None = 2,
    include_config: bool = True,
    file: IO[str] | None = None,
) -> None:
    """Serialise *report* to JSON and write to *file* (or ``sys.stdout``).

    This is a thin wrapper around :class:`JsonReporter` for callers that
    do not need to keep a reporter instance.

    Args:
        report: The completed scan result.
        config: Resolved configuration used during the scan.
        indent: JSON indentation spaces.  ``None`` = compact output.
        include_config: Embed ``general`` + ``thresholds`` config sections.
        file: Write to this open file handle instead of stdout.

    Example::

        from spark_perf_lint.reporters.json_reporter import write_report

        with open("report.json", "w", encoding="utf-8") as fh:
            write_report(report, config, file=fh)
    """
    JsonReporter(
        report,
        config,
        indent=indent,
        include_config=include_config,
    ).write(file=file)
