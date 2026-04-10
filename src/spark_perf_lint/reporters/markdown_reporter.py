"""Markdown reporter for spark-perf-lint.

Renders an ``AuditReport`` as GitHub-Flavored Markdown (GFM) suitable for:

* GitHub / GitLab PR comments
* Standalone ``report.md`` files committed to a repository
* Confluence / Notion pages that accept Markdown

Output structure:

1. **Header** — status badge (PASSED / FAILED), scan timestamp
2. **Summary table** — files, duration, threshold, per-severity counts
3. **Findings table** — one row per finding: severity icon, rule ID, file:line,
   short message (quick scan / dashboard view)
4. **Details section** — one ``<details>`` block per finding with collapsible
   full content: explanation, recommendation, before/after code blocks,
   config-suggestion table, impact, effort badge, references
5. **Dimension breakdown table** — finding count per dimension
6. **Footer** — version and generation timestamp

``compact=True`` skips the before/after code blocks and the per-finding
``<details>`` bodies; only the findings table and summary are emitted.  Use
this when character limits (GitHub PR comments top out at ~65 000 chars) are
a concern.

Usage::

    from spark_perf_lint.reporters.markdown_reporter import MarkdownReporter

    md = MarkdownReporter(report, config).to_str()

    # Write to a file
    MarkdownReporter(report, config).render(file=open("report.md", "w"))

    # Compact mode for PR comments
    MarkdownReporter(report, config, compact=True).render()
"""

from __future__ import annotations

import datetime
import sys
from typing import IO

from spark_perf_lint import __version__
from spark_perf_lint.config import LintConfig
from spark_perf_lint.types import AuditReport, Dimension, EffortLevel, Finding, Severity

# ---------------------------------------------------------------------------
# Severity / effort display constants
# ---------------------------------------------------------------------------

_SEV_ICON: dict[Severity, str] = {
    Severity.CRITICAL: "🔴",
    Severity.WARNING: "🟡",
    Severity.INFO: "🔵",
}

_EFFORT_LABEL: dict[EffortLevel, str] = {
    EffortLevel.CONFIG_ONLY: "⚙ config-only",
    EffortLevel.MINOR_CODE_CHANGE: "✎ minor change",
    EffortLevel.MAJOR_REFACTOR: "⚑ major refactor",
}


# ---------------------------------------------------------------------------
# Public reporter class
# ---------------------------------------------------------------------------


class MarkdownReporter:
    """Render a ``spark-perf-lint`` ``AuditReport`` as GitHub-Flavored Markdown.

    Args:
        report: The completed scan result to render.
        config: Resolved ``LintConfig`` used during the scan.
        show_fix: When ``True`` (default), include before / after code blocks
            inside each finding's ``<details>`` panel.
        compact: When ``True``, emit only the header, summary table, and
            findings table — no per-finding ``<details>`` bodies.  Useful
            when staying within GitHub PR comment character limits.
    """

    def __init__(
        self,
        report: AuditReport,
        config: LintConfig,
        *,
        show_fix: bool = True,
        compact: bool = False,
    ) -> None:
        self.report = report
        self.config = config
        self.show_fix = show_fix
        self.compact = compact

    # ------------------------------------------------------------------
    # Primary entry points
    # ------------------------------------------------------------------

    def to_str(self) -> str:
        """Return the full Markdown document as a string.

        Returns:
            A GitHub-Flavored Markdown string.
        """
        parts: list[str] = []
        threshold = self.config.severity_threshold
        visible = [f for f in self.report.findings if f.severity >= threshold]

        parts.append(self._render_header(visible))
        parts.append(self._render_summary(visible))

        if visible:
            parts.append(self._render_findings_table(visible))
            if not self.compact:
                parts.append(self._render_details_section(visible))
            parts.append(self._render_dimension_breakdown(visible))

        parts.append(self._render_footer())
        return "\n".join(parts)

    def render(self, file: IO[str] | None = None) -> None:
        """Write the Markdown report to *file* (or ``sys.stdout``).

        Args:
            file: An open, writable text-mode file object.  Pass ``None``
                (default) to write to ``sys.stdout``.
        """
        out = file if file is not None else sys.stdout
        out.write(self.to_str())
        out.write("\n")

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------

    def _render_header(self, visible: list[Finding]) -> str:
        fail_severities = set(self.config.fail_on)
        should_fail = any(f.severity in fail_severities for f in visible)

        if not self.report.findings:
            status = "✅ **PASSED**"
            subtitle = "No findings above threshold."
        elif should_fail:
            fail_count = sum(1 for f in visible if f.severity in fail_severities)
            status = "❌ **FAILED**"
            subtitle = f"{fail_count} finding(s) at or above the fail-on threshold."
        else:
            status = "✅ **PASSED**"
            subtitle = f"{len(visible)} finding(s) below fail-on threshold."

        return "# ⚡ spark-perf-lint Report\n" "\n" f"> {status} — {subtitle}"

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def _render_summary(self, visible: list[Finding]) -> str:
        crit = sum(1 for f in visible if f.severity == Severity.CRITICAL)
        warn = sum(1 for f in visible if f.severity == Severity.WARNING)
        info = sum(1 for f in visible if f.severity == Severity.INFO)
        elapsed = f"{self.report.scan_duration_seconds:.3f}s"

        lines = [
            "## Summary",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Files scanned | {self.report.files_scanned} |",
            f"| Scan duration | {elapsed} |",
            f"| Severity threshold | {self.config.severity_threshold.name} |",
            f"| Total findings | {len(visible)} |",
            "",
            "| Severity | Count |",
            "|----------|-------|",
            f"| {_SEV_ICON[Severity.CRITICAL]} CRITICAL | {crit} |",
            f"| {_SEV_ICON[Severity.WARNING]} WARNING | {warn} |",
            f"| {_SEV_ICON[Severity.INFO]} INFO | {info} |",
        ]
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Findings table
    # ------------------------------------------------------------------

    def _render_findings_table(self, visible: list[Finding]) -> str:
        lines = [
            "## Findings",
            "",
            "| # | Severity | Rule | Location | Message |",
            "|---|----------|------|----------|---------|",
        ]
        for i, finding in enumerate(visible, start=1):
            icon = _SEV_ICON[finding.severity]
            sev = finding.severity.name
            rule = finding.rule_id
            # Escape pipe characters that would break the table
            msg = _escape_table_cell(finding.message)
            loc = _escape_table_cell(f"{finding.file_path}:{finding.line_number}")
            lines.append(f"| {i} | {icon} {sev} | `{rule}` | `{loc}` | {msg} |")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Details section
    # ------------------------------------------------------------------

    def _render_details_section(self, visible: list[Finding]) -> str:
        parts = ["## Details", ""]
        for finding in visible:
            parts.append(self._render_finding_details(finding))
        return "\n".join(parts)

    def _render_finding_details(self, finding: Finding) -> str:
        icon = _SEV_ICON[finding.severity]
        col = f":{finding.column_offset}" if finding.column_offset is not None else ""
        summary_line = (
            f"{icon} `{finding.rule_id}` "
            f"`{finding.file_path}:{finding.line_number}{col}` "
            f"— {finding.message}"
        )

        body_parts: list[str] = []

        # ── Meta badges ─────────────────────────────────────────────────
        effort_label = _EFFORT_LABEL[finding.effort_level]
        body_parts.append(
            f"**Dimension:** {finding.dimension.display_name}  \n"
            f"**Severity:** {finding.severity.name}  \n"
            f"**Effort:** {effort_label}  "
            + (f"\n**Impact:** {finding.estimated_impact}" if finding.estimated_impact else "")
        )

        # ── Explanation ──────────────────────────────────────────────────
        if finding.explanation:
            body_parts.append("### Explanation\n\n" + finding.explanation.strip())

        # ── Recommendation ───────────────────────────────────────────────
        if finding.recommendation:
            body_parts.append("### Recommendation\n\n" + finding.recommendation.strip())

        # ── Before / after code ──────────────────────────────────────────
        if self.show_fix:
            if finding.before_code:
                body_parts.append(
                    "### Before\n\n" "```python\n" + finding.before_code.strip() + "\n```"
                )
            if finding.after_code:
                body_parts.append(
                    "### After\n\n" "```python\n" + finding.after_code.strip() + "\n```"
                )

        # ── Config suggestion ────────────────────────────────────────────
        if finding.config_suggestion:
            cfg_lines = [
                "### Config Changes",
                "",
                "| Config Key | Recommended Value |",
                "|------------|-------------------|",
            ]
            for key, val in finding.config_suggestion.items():
                cfg_lines.append(
                    f"| `{_escape_table_cell(str(key))}` " f"| `{_escape_table_cell(str(val))}` |"
                )
            body_parts.append("\n".join(cfg_lines))

        # ── References ───────────────────────────────────────────────────
        if finding.references:
            ref_lines = ["### References", ""]
            for ref in finding.references:
                ref_lines.append(f"- {ref}")
            body_parts.append("\n".join(ref_lines))

        body = "\n\n".join(body_parts)

        return "<details>\n" f"<summary>{summary_line}</summary>\n" "\n" + body + "\n\n</details>\n"

    # ------------------------------------------------------------------
    # Dimension breakdown
    # ------------------------------------------------------------------

    def _render_dimension_breakdown(self, visible: list[Finding]) -> str:
        dim_counts: dict[Dimension, int] = {}
        for f in visible:
            dim_counts[f.dimension] = dim_counts.get(f.dimension, 0) + 1

        if not dim_counts:
            return ""

        lines = [
            "## Dimension Breakdown",
            "",
            "| Dimension | Findings |",
            "|-----------|----------|",
        ]
        for dim in sorted(dim_counts, key=lambda d: d.value):
            lines.append(f"| {dim.display_name} | {dim_counts[dim]} |")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Footer
    # ------------------------------------------------------------------

    def _render_footer(self) -> str:
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        return f"---\n\n*Generated by spark-perf-lint v{__version__} · {ts}*"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _escape_table_cell(text: str) -> str:
    """Escape characters that break a GFM table cell.

    Replaces ``|`` with ``\\|`` and strips newlines so the cell stays
    on a single line.

    Args:
        text: Raw cell content.

    Returns:
        Escaped string safe for use inside a ``| … |`` table cell.
    """
    return text.replace("|", "\\|").replace("\n", " ").replace("\r", "")


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------


def write_report(
    report: AuditReport,
    config: LintConfig,
    *,
    show_fix: bool = True,
    compact: bool = False,
    file: IO[str] | None = None,
) -> None:
    """Render *report* as Markdown and write to *file* (or ``sys.stdout``).

    This is a thin wrapper around :class:`MarkdownReporter` for callers
    that do not need to keep a reporter instance.

    Args:
        report: The completed scan result.
        config: Resolved configuration used during the scan.
        show_fix: Include before / after code blocks.
        compact: Omit per-finding ``<details>`` bodies.
        file: Write to this open file handle instead of stdout.

    Example::

        from spark_perf_lint.reporters.markdown_reporter import write_report

        with open("report.md", "w", encoding="utf-8") as fh:
            write_report(report, config, file=fh)
    """
    MarkdownReporter(
        report,
        config,
        show_fix=show_fix,
        compact=compact,
    ).render(file=file)
