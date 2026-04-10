"""Rich-formatted terminal reporter for spark-perf-lint.

Renders an ``AuditReport`` to the terminal (or a plain-text file) using the
Rich library.  Output features:

* Colour-coded severity badges — CRITICAL red, WARNING yellow, INFO blue
* Findings grouped by severity with labelled section headers
* Syntax-highlighted before / after code panels (``--fix`` mode)
* Unified diff panel in ``--verbose`` mode
* Config-suggestion table when a finding carries ``config_suggestion``
* Summary footer with pass / fail status, counts, and timing
* ``--quiet`` mode: findings only, header and footer suppressed
* ``--verbose`` mode: full explanation, unified diff, references, per-dimension
  breakdown, and rule count
* Plain-text file output (``file=`` parameter): ANSI colour stripped
  automatically by Rich when ``no_color=True``
"""

from __future__ import annotations

import difflib
import sys
from typing import IO, Sequence

from rich.columns import Columns
from rich.console import Console, Group
from rich.padding import Padding
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

from spark_perf_lint import __version__
from spark_perf_lint.config import LintConfig
from spark_perf_lint.types import AuditReport, EffortLevel, Finding, Severity


# ---------------------------------------------------------------------------
# Theme and style constants
# ---------------------------------------------------------------------------

_THEME = Theme(
    {
        "spl.brand": "bold bright_cyan",
        "spl.critical": "bold bright_red",
        "spl.warning": "bold yellow",
        "spl.info": "bold bright_blue",
        "spl.dim": "bright_black",
        "spl.file": "bright_white underline",
        "spl.rule_id": "bright_black",
        "spl.label": "bold white",
        "spl.pass": "bold bright_green",
        "spl.fail": "bold bright_red",
        "spl.effort.config": "bright_green",
        "spl.effort.minor": "yellow",
        "spl.effort.major": "bright_red",
        "spl.section.critical": "bold bright_red",
        "spl.section.warning": "bold yellow",
        "spl.section.info": "bold bright_blue",
        "spl.recommendation": "italic",
        "spl.impact": "bright_magenta",
        "spl.dim_label": "cyan",
    }
)

# Severity → (theme_key, bullet_icon, section_rule_style)
_SEV_META: dict[Severity, tuple[str, str, str]] = {
    Severity.CRITICAL: ("spl.critical", "●", "bright_red"),
    Severity.WARNING: ("spl.warning", "◆", "yellow"),
    Severity.INFO: ("spl.info", "○", "bright_blue"),
}

# EffortLevel → (theme_key, display_label)
_EFFORT_META: dict[EffortLevel, tuple[str, str]] = {
    EffortLevel.CONFIG_ONLY: ("spl.effort.config", "⚙  config-only"),
    EffortLevel.MINOR_CODE_CHANGE: ("spl.effort.minor", "✎  minor change"),
    EffortLevel.MAJOR_REFACTOR: ("spl.effort.major", "⚑  major refactor"),
}

# Console width used when stdout is not a terminal (e.g. CI logs)
_FALLBACK_WIDTH = 120


# ---------------------------------------------------------------------------
# Public reporter class
# ---------------------------------------------------------------------------


class TerminalReporter:
    """Render a ``spark-perf-lint`` ``AuditReport`` as Rich terminal output.

    Args:
        report: The completed scan result to display.
        config: Resolved ``LintConfig`` used during the scan (needed for
            severity threshold and config-file path display).
        show_fix: When ``True``, include before / after code panels and the
            unified diff for each finding that carries them.
        quiet: When ``True``, suppress the header panel and summary footer;
            only emit finding cards.
        verbose: When ``True``, include full explanation text, unified diff
            panels, URL references, and a per-dimension breakdown in the footer.
    """

    def __init__(
        self,
        report: AuditReport,
        config: LintConfig,
        *,
        show_fix: bool = True,
        quiet: bool = False,
        verbose: bool = False,
    ) -> None:
        self.report = report
        self.config = config
        self.show_fix = show_fix
        self.quiet = quiet
        self.verbose = verbose

    # ------------------------------------------------------------------
    # Primary entry point
    # ------------------------------------------------------------------

    def render(self, file: IO[str] | None = None) -> None:
        """Write the formatted report to *file* (or ``sys.stdout``).

        When *file* is provided, ANSI escape codes are suppressed so the
        output can be safely stored or piped to downstream tools.

        Args:
            file: An open, writable text-mode file object.  Pass ``None``
                (default) to write to ``sys.stdout`` with full colour.
        """
        is_file_output = file is not None
        console = Console(
            theme=_THEME,
            highlight=False,
            markup=True,
            no_color=is_file_output,
            force_terminal=not is_file_output,
            file=file if is_file_output else sys.stdout,
            width=_FALLBACK_WIDTH if is_file_output else None,
        )

        threshold = self.config.severity_threshold
        visible: list[Finding] = [
            f for f in self.report.findings if f.severity >= threshold
        ]

        if not self.quiet:
            self._render_header(console, visible)

        if not visible:
            self._render_no_findings(console)
        else:
            self._render_all_findings(console, visible)

        if not self.quiet:
            self._render_footer(console, visible)

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------

    def _render_header(self, console: Console, visible: list[Finding]) -> None:
        """Render the branded scan-header panel."""
        # First line: brand + version
        header = Text()
        header.append("⚡ spark-perf-lint", style="spl.brand")
        header.append(f"  v{__version__}", style="spl.dim")

        # Second line: scan stats
        stats = Text()
        stats.append(f"  {self.report.files_scanned} file(s) scanned", style="spl.dim")

        # Optionally show rule count (lazy import avoids heavy load if not needed)
        if self.verbose:
            try:
                from spark_perf_lint.rules.registry import RuleRegistry  # noqa: PLC0415

                n_rules = len(RuleRegistry.instance().get_enabled_rules(self.config))
                stats.append(f"  ·  {n_rules} rules active", style="spl.dim")
            except Exception:  # noqa: BLE001
                pass

        stats.append(
            f"  ·  {self.report.scan_duration_seconds:.2f}s",
            style="spl.dim",
        )

        # Config source
        if self.verbose and self.config.config_file_path:
            stats.append(
                f"\n  Config: {self.config.config_file_path}",
                style="spl.dim",
            )

        panel_content = Group(header, stats)
        console.print(Panel(panel_content, border_style="bright_black", padding=(0, 1)))

    # ------------------------------------------------------------------
    # No findings
    # ------------------------------------------------------------------

    def _render_no_findings(self, console: Console) -> None:
        """Render a green 'all clear' message."""
        msg = Text()
        msg.append("  ✓  No findings", style="spl.pass")
        msg.append(
            f" at or above {self.config.severity_threshold.name} threshold.",
            style="default",
        )
        console.print(Padding(msg, (1, 0)))

    # ------------------------------------------------------------------
    # Findings
    # ------------------------------------------------------------------

    def _render_all_findings(self, console: Console, visible: list[Finding]) -> None:
        """Render all findings grouped by severity (CRITICAL → WARNING → INFO)."""
        for severity in (Severity.CRITICAL, Severity.WARNING, Severity.INFO):
            group = [f for f in visible if f.severity == severity]
            if not group:
                continue
            self._render_severity_section(console, severity, group)

    def _render_severity_section(
        self,
        console: Console,
        severity: Severity,
        findings: list[Finding],
    ) -> None:
        """Render a section header and all findings at *severity*."""
        _style_key, _icon, rule_style = _SEV_META[severity]
        label = f" {severity.name}  ({len(findings)}) "
        console.print()
        console.print(Rule(label, style=rule_style, align="left"))
        console.print()
        for finding in findings:
            self._render_finding(console, finding)

    def _render_finding(self, console: Console, finding: Finding) -> None:
        """Render a single finding card."""
        sev_style, icon, _ = _SEV_META[finding.severity]

        # ── Location line ───────────────────────────────────────────────
        location = Text()
        location.append(f" {icon} ", style=sev_style)
        location.append(f"[{finding.rule_id}]", style="spl.rule_id")
        location.append("  ")
        # Make file path relative if possible
        try:
            from pathlib import Path  # noqa: PLC0415
            rel = Path(finding.file_path)
            display_path = str(rel)
        except Exception:  # noqa: BLE001
            display_path = finding.file_path
        col = f":{finding.column_offset}" if finding.column_offset is not None else ""
        location.append(f"{display_path}:{finding.line_number}{col}", style="spl.file")
        location.append("  ·  ", style="spl.dim")
        location.append(finding.dimension.display_name, style="spl.dim_label")
        console.print(Padding(location, (0, 0, 0, 2)))

        # ── Message ─────────────────────────────────────────────────────
        msg = Text(f"    {finding.message}", style="bold")
        console.print(Padding(msg, (0, 0, 0, 2)))

        # ── Explanation (verbose only) ───────────────────────────────────
        if self.verbose and finding.explanation:
            exp_lines = finding.explanation.strip().splitlines()
            exp_text = Text()
            for line in exp_lines:
                exp_text.append(f"    {line}\n", style="spl.dim")
            console.print(Padding(exp_text, (0, 2, 0, 2)))

        # ── Recommendation ──────────────────────────────────────────────
        if finding.recommendation:
            rec_panel = Panel(
                Text(finding.recommendation, style="spl.recommendation"),
                title="[spl.label]Recommendation[/spl.label]",
                title_align="left",
                border_style="bright_black",
                padding=(0, 1),
            )
            console.print(Padding(rec_panel, (1, 2, 0, 4)))

        # ── Before / After code (show_fix) ───────────────────────────────
        if self.show_fix:
            self._render_code_panels(console, finding)

        # ── Config suggestion ───────────────────────────────────────────
        if finding.config_suggestion:
            self._render_config_suggestion(console, finding.config_suggestion)

        # ── Impact + effort badge ────────────────────────────────────────
        self._render_meta_line(console, finding)

        # ── References (verbose only) ────────────────────────────────────
        if self.verbose and finding.references:
            refs = Text()
            refs.append("    References: ", style="spl.dim")
            refs.append("  ".join(finding.references), style="spl.dim")
            console.print(Padding(refs, (0, 2, 0, 2)))

        # Blank line between findings
        console.print()

    # ------------------------------------------------------------------
    # Code panels
    # ------------------------------------------------------------------

    def _render_code_panels(self, console: Console, finding: Finding) -> None:
        """Render before / after syntax panels and (in verbose mode) a diff."""
        before = (finding.before_code or "").rstrip("\n")
        after = (finding.after_code or "").rstrip("\n")

        if not before and not after:
            return

        if before:
            before_syntax = Syntax(
                before,
                "python",
                theme="monokai",
                word_wrap=True,
                background_color="default",
            )
            before_panel = Panel(
                before_syntax,
                title="[bold red]Before[/bold red]",
                title_align="left",
                border_style="red",
                padding=(0, 1),
            )
            console.print(Padding(before_panel, (1, 2, 0, 4)))

        if after:
            after_syntax = Syntax(
                after,
                "python",
                theme="monokai",
                word_wrap=True,
                background_color="default",
            )
            after_panel = Panel(
                after_syntax,
                title="[bold green]After[/bold green]",
                title_align="left",
                border_style="green",
                padding=(0, 1),
            )
            console.print(Padding(after_panel, (1, 2, 0, 4)))

        # Unified diff in verbose mode
        if self.verbose and before and after:
            diff_lines = list(
                difflib.unified_diff(
                    before.splitlines(keepends=True),
                    after.splitlines(keepends=True),
                    fromfile="before",
                    tofile="after",
                )
            )
            if diff_lines:
                diff_text = "".join(diff_lines)
                diff_syntax = Syntax(
                    diff_text,
                    "diff",
                    theme="monokai",
                    word_wrap=True,
                    background_color="default",
                )
                diff_panel = Panel(
                    diff_syntax,
                    title="[spl.dim]Diff[/spl.dim]",
                    title_align="left",
                    border_style="bright_black",
                    padding=(0, 1),
                )
                console.print(Padding(diff_panel, (1, 2, 0, 4)))

    # ------------------------------------------------------------------
    # Config suggestion
    # ------------------------------------------------------------------

    def _render_config_suggestion(
        self,
        console: Console,
        config_suggestion: dict[str, object],
    ) -> None:
        """Render a Spark config key→value table."""
        tbl = Table(
            show_header=True,
            header_style="spl.label",
            border_style="bright_black",
            box=_ROUNDED_BOX,
            padding=(0, 1),
            expand=False,
        )
        tbl.add_column("Config Key", style="bright_cyan", no_wrap=True)
        tbl.add_column("Recommended Value", style="bright_green")

        for key, val in config_suggestion.items():
            tbl.add_row(str(key), str(val))

        console.print(
            Padding(
                Panel(
                    tbl,
                    title="[spl.label]Config Changes[/spl.label]",
                    title_align="left",
                    border_style="bright_black",
                    padding=(0, 0),
                ),
                (1, 2, 0, 4),
            )
        )

    # ------------------------------------------------------------------
    # Meta line (impact + effort)
    # ------------------------------------------------------------------

    def _render_meta_line(self, console: Console, finding: Finding) -> None:
        """Render the impact string and effort badge on one line."""
        meta = Text()
        meta.append("    ", style="default")

        if finding.estimated_impact:
            meta.append("Impact: ", style="spl.dim")
            meta.append(finding.estimated_impact, style="spl.impact")
            meta.append("  ·  ", style="spl.dim")

        effort_style, effort_label = _EFFORT_META[finding.effort_level]
        meta.append("Effort: ", style="spl.dim")
        meta.append(effort_label, style=effort_style)

        console.print(Padding(meta, (1, 2, 0, 2)))

    # ------------------------------------------------------------------
    # Footer
    # ------------------------------------------------------------------

    def _render_footer(self, console: Console, visible: list[Finding]) -> None:
        """Render the summary footer with counts, timing, and pass/fail."""
        console.print()
        console.print(Rule(style="bright_black"))

        # ── Count summary line ─────────────────────────────────────────
        counts = Text("  ")
        crit = sum(1 for f in visible if f.severity == Severity.CRITICAL)
        warn = sum(1 for f in visible if f.severity == Severity.WARNING)
        info = sum(1 for f in visible if f.severity == Severity.INFO)

        parts: list[tuple[str, str]] = []
        if crit:
            parts.append((f"{crit} critical", "spl.critical"))
        if warn:
            parts.append((f"{warn} warning", "spl.warning"))
        if info:
            parts.append((f"{info} info", "spl.info"))
        if not parts:
            parts.append(("0 findings", "spl.pass"))

        for i, (label, style) in enumerate(parts):
            counts.append(label, style=style)
            if i < len(parts) - 1:
                counts.append("  ·  ", style="spl.dim")

        counts.append(
            f"  ·  {self.report.files_scanned} file(s)"
            f"  ·  {self.report.scan_duration_seconds:.2f}s",
            style="spl.dim",
        )
        console.print(counts)

        # ── Verbose: per-dimension breakdown ───────────────────────────
        if self.verbose and visible:
            self._render_dimension_breakdown(console, visible)

        # ── Pass / fail status ─────────────────────────────────────────
        console.print()
        fail_severities = set(self.config.fail_on)
        should_fail = any(f.severity in fail_severities for f in visible)

        if not self.report.findings:
            status = Text()
            status.append(
                f"  ✓  PASSED",
                style="spl.pass",
            )
            status.append(
                f" — {self.report.files_scanned} file(s) scanned, "
                "no findings above threshold.",
                style="default",
            )
        elif should_fail:
            # Count findings that triggered failure
            fail_count = sum(1 for f in visible if f.severity in fail_severities)
            status = Text()
            status.append("  ✗  FAILED", style="spl.fail")
            status.append(
                f" — {fail_count} finding(s) at or above the fail-on threshold.",
                style="default",
            )
        else:
            status = Text()
            status.append("  ✓  PASSED", style="spl.pass")
            status.append(
                f" — {len(visible)} finding(s) below fail-on threshold.",
                style="default",
            )
        console.print(status)
        console.print()

    def _render_dimension_breakdown(
        self,
        console: Console,
        visible: list[Finding],
    ) -> None:
        """Render a per-dimension finding count table (verbose mode)."""
        from spark_perf_lint.types import Dimension  # noqa: PLC0415

        dim_counts: dict[Dimension, int] = {}
        for f in visible:
            dim_counts[f.dimension] = dim_counts.get(f.dimension, 0) + 1

        if not dim_counts:
            return

        tbl = Table(
            show_header=True,
            header_style="spl.label",
            border_style="bright_black",
            box=_SIMPLE_HEAD_BOX,
            padding=(0, 1),
        )
        tbl.add_column("Dimension", style="spl.dim_label", min_width=30)
        tbl.add_column("Findings", justify="right")

        for dim in sorted(dim_counts, key=lambda d: d.value):
            tbl.add_row(dim.display_name, str(dim_counts[dim]))

        console.print(Padding(tbl, (1, 0, 0, 2)))


# ---------------------------------------------------------------------------
# Minimal Rich box styles (avoid importing rich.box to reduce coupling)
# ---------------------------------------------------------------------------

from rich import box as _rich_box  # noqa: E402

_ROUNDED_BOX = _rich_box.ROUNDED
_SIMPLE_HEAD_BOX = _rich_box.SIMPLE_HEAD


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------


def render_report(
    report: AuditReport,
    config: LintConfig,
    *,
    show_fix: bool = True,
    quiet: bool = False,
    verbose: bool = False,
    file: IO[str] | None = None,
) -> None:
    """Render *report* to the terminal (or *file*) in one call.

    This is a thin wrapper around :class:`TerminalReporter` for callers
    that do not need to keep a reporter instance.

    Args:
        report: The completed scan result.
        config: Resolved configuration used during the scan.
        show_fix: Include before / after code panels.
        quiet: Suppress header and footer.
        verbose: Include explanations, diffs, references, and dimension table.
        file: Write plain-text output to this file instead of stdout.

    Example::

        from spark_perf_lint.reporters.terminal import render_report
        render_report(report, config, verbose=True)
    """
    TerminalReporter(
        report,
        config,
        show_fix=show_fix,
        quiet=quiet,
        verbose=verbose,
    ).render(file=file)
