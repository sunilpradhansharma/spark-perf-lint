"""CLI entry point for spark-perf-lint.

Exposes a Click command group with five sub-commands:

    spark-perf-lint scan     — run the linter on one or more paths
    spark-perf-lint rules    — list all available rules
    spark-perf-lint init     — scaffold a project config file
    spark-perf-lint version  — print the package version
    spark-perf-lint explain  — show detailed rule documentation

The scan command is wired to ``ScanOrchestrator`` for full engine execution
and ``TerminalReporter`` for Rich-formatted terminal output.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import click

from spark_perf_lint import __version__
from spark_perf_lint.config import CONFIG_FILENAME, ConfigError, LintConfig

# ---------------------------------------------------------------------------
# Shared option definitions (reused across commands via decorators)
# ---------------------------------------------------------------------------

_SEVERITY_CHOICES = click.Choice(["CRITICAL", "WARNING", "INFO"], case_sensitive=False)
_FORMAT_CHOICES = click.Choice(["terminal", "json", "markdown", "github-pr"], case_sensitive=False)


# =============================================================================
# Root group
# =============================================================================


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    invoke_without_command=True,
)
@click.pass_context
def main(ctx: click.Context) -> None:
    """spark-perf-lint — Apache Spark performance linter.

    Run 'spark-perf-lint COMMAND --help' for command-specific options.
    """
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# =============================================================================
# spark-perf-lint scan
# =============================================================================


@main.command("scan")
@click.argument(
    "paths",
    nargs=-1,
    type=click.Path(exists=True, file_okay=True, dir_okay=True, path_type=Path),
    metavar="PATH...",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help=f"Explicit path to a {CONFIG_FILENAME} config file.",
    metavar="PATH",
)
@click.option(
    "--severity-threshold",
    type=_SEVERITY_CHOICES,
    default=None,
    help="Minimum severity level to include in output (overrides config).",
    metavar="LEVEL",
)
@click.option(
    "--fail-on",
    type=_SEVERITY_CHOICES,
    multiple=True,
    help=(
        "Severity level(s) that cause a non-zero exit code. "
        "Can be repeated: --fail-on CRITICAL --fail-on WARNING. "
        "Overrides config."
    ),
    metavar="LEVEL",
)
@click.option(
    "--format",
    "output_format",
    type=_FORMAT_CHOICES,
    multiple=True,
    help=(
        "Output format(s). Can be repeated: --format terminal --format json. " "Overrides config."
    ),
    metavar="FORMAT",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    default=None,
    help="Write the report to this file instead of stdout.",
    metavar="PATH",
)
@click.option(
    "--dimension",
    "dimensions",
    default=None,
    help=(
        "Scan only these dimensions. Comma-separated dimension codes, "
        "e.g. 'D03,D08'. Prefix the code with '!' to exclude: '!D11'."
    ),
    metavar="CODES",
)
@click.option(
    "--rule",
    "rules",
    default=None,
    help=("Scan only these rule IDs. Comma-separated, " "e.g. 'SPL-D03-001,SPL-D08-002'."),
    metavar="RULE_IDS",
)
@click.option(
    "--fix/--no-fix",
    "show_fix",
    default=True,
    show_default=True,
    help="Show before/after code suggestions in the report.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show scan progress, rule counts, and timing details.",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    default=False,
    help="Only print findings; suppress header, footer, and progress output.",
)
@click.option(
    "--llm/--no-llm",
    "use_llm",
    default=False,
    help=(
        "Enable Tier 2 LLM analysis (requires 'anthropic' package and "
        "ANTHROPIC_API_KEY). Enriches findings with context-aware explanations "
        "and produces a cross-file summary. Overrides the 'llm.enabled' config key."
    ),
)
def scan(
    paths: tuple[Path, ...],
    config_path: Path | None,
    severity_threshold: str | None,
    fail_on: tuple[str, ...],
    output_format: tuple[str, ...],
    output_path: Path | None,
    dimensions: str | None,
    rules: str | None,
    show_fix: bool,
    verbose: bool,
    quiet: bool,
    use_llm: bool,
) -> None:
    """Scan PATH(s) for Spark performance anti-patterns.

    PATH can be one or more Python files or directories. Directories are
    scanned recursively; files not ending in '.py' are skipped.

    Exit codes:\n
      0  — no findings at or above --fail-on severity\n
      1  — one or more findings at or above --fail-on severity\n
      2  — configuration or I/O error

    Examples:\n
      spark-perf-lint scan .\n
      spark-perf-lint scan src/ --severity-threshold WARNING --format json\n
      spark-perf-lint scan jobs/etl.py --fail-on CRITICAL --fail-on WARNING\n
      spark-perf-lint scan . --dimension D03,D08 --quiet
    """
    # Build CLI override dict from options that were explicitly provided
    cli_overrides: dict[str, Any] = {}

    if severity_threshold:
        cli_overrides.setdefault("general", {})["severity_threshold"] = severity_threshold
    if fail_on:
        cli_overrides.setdefault("general", {})["fail_on"] = list(fail_on)
    if output_format:
        # Normalise github-pr → github_pr to match internal representation
        cli_overrides.setdefault("general", {})["report_format"] = [
            f.replace("-", "_") for f in output_format
        ]

    # Resolve config
    try:
        if config_path is not None:
            import yaml  # type: ignore[import-untyped]  # lazy import — only needed here

            raw_yaml = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            from spark_perf_lint.config import _DEFAULTS, _deep_merge

            merged = _deep_merge(_deep_merge({}, _DEFAULTS), raw_yaml)
            if cli_overrides:
                merged = _deep_merge(merged, cli_overrides)
            LintConfig._validate(merged)
            config = LintConfig(raw=merged, config_file_path=config_path)
        else:
            start_dir = (
                paths[0].parent
                if (paths and paths[0].is_file())
                else (paths[0] if paths else Path.cwd())
            )
            config = LintConfig.load(start_dir=start_dir, cli_overrides=cli_overrides or None)
    except ConfigError as exc:
        click.echo(f"Configuration error: {exc}", err=True)
        sys.exit(2)

    # --- Run the scan ---
    from spark_perf_lint.engine.orchestrator import ScanOrchestrator

    scan_paths: list[str | Path] = [str(p) for p in paths] if paths else [str(Path.cwd())]
    orchestrator = ScanOrchestrator(config)

    try:
        report = orchestrator.scan(scan_paths)
    except Exception as exc:  # noqa: BLE001
        click.echo(click.style(f"Scan error: {exc}", fg="red"), err=True)
        sys.exit(2)

    # --- Tier 2: LLM enrichment (optional) ---
    llm_result = None
    if use_llm or config.llm_enabled:
        llm_result = _run_llm_analysis(report, config, verbose=verbose, quiet=quiet)
        if llm_result is not None:
            # Replace findings list with the enriched copy
            import dataclasses

            report = dataclasses.replace(report, findings=llm_result.enriched_findings)

    # --- Render output ---
    _render_report(report, config, output_path, show_fix, quiet, verbose)

    # Print cross-file insights and executive summary below the main report
    if llm_result is not None and not quiet:
        _render_llm_extras(llm_result)

    # --- Exit code ---
    fail_severities = set(config.fail_on)
    should_fail = any(f.severity in fail_severities for f in report.findings)
    if should_fail:
        sys.exit(1)


def _render_report(
    report: Any,
    config: LintConfig,
    output_path: Path | None,
    show_fix: bool,
    quiet: bool,
    verbose: bool,
) -> None:
    """Render *report* using every format listed in ``config.report_formats``.

    When a single format is active, ``output_path`` (if supplied) redirects that
    reporter's output to a file.  When multiple formats are active, ``output_path``
    is ignored and each reporter writes to its natural destination (stdout for
    terminal/json/markdown; GitHub Actions streams for github_pr).

    Args:
        report: ``AuditReport`` produced by the orchestrator.
        config: Resolved configuration used during the scan.
        output_path: Write the report here instead of stdout (single-format only).
        show_fix: Whether to render before/after code panels.
        quiet: Suppress header and footer; emit only finding cards.
        verbose: Include explanations, diffs, references, and dimension breakdown.
    """
    formats = config.report_formats  # e.g. ["terminal"], ["json"], ["terminal", "json"]
    single_format = len(formats) == 1

    for fmt in formats:
        # output_path only applies when exactly one format is requested
        target_path: Path | None = output_path if single_format else None

        if fmt == "terminal":
            from spark_perf_lint.reporters.terminal import TerminalReporter

            reporter = TerminalReporter(
                report,
                config,
                show_fix=show_fix,
                quiet=quiet,
                verbose=verbose,
            )
            if target_path is not None:
                with target_path.open("w", encoding="utf-8") as fh:
                    reporter.render(file=fh)
            else:
                reporter.render()

        elif fmt == "json":
            from spark_perf_lint.reporters.json_reporter import JsonReporter

            jr = JsonReporter(report, config)
            if target_path is not None:
                with target_path.open("w", encoding="utf-8") as fh:
                    jr.write(file=fh)
            else:
                jr.write()

        elif fmt == "markdown":
            from spark_perf_lint.reporters.markdown_reporter import MarkdownReporter

            mr = MarkdownReporter(report, config, show_fix=show_fix)
            if target_path is not None:
                with target_path.open("w", encoding="utf-8") as fh:
                    mr.render(file=fh)
            else:
                mr.render()

        elif fmt == "github_pr":
            from spark_perf_lint.reporters.github_pr import GitHubPRReporter

            gpr = GitHubPRReporter(report, config, show_fix=show_fix)
            # GitHub PR reporter manages its own streams (stdout annotations,
            # $GITHUB_STEP_SUMMARY, and optional PR comment API call).
            gpr.run(annotations_file=None, post_comment=True, write_summary=True)


def _run_llm_analysis(
    report: Any,
    config: LintConfig,
    *,
    verbose: bool,
    quiet: bool,
) -> Any | None:
    """Run Tier 2 LLM analysis and return the ``LLMAnalysisResult``.

    All errors are caught and surfaced as stderr messages so that a missing
    API key or absent ``anthropic`` package never aborts the scan.

    Returns ``None`` on any failure.
    """
    if not quiet:
        click.echo(click.style("  Running Tier 2 LLM analysis…", fg="bright_black"))
    try:
        from spark_perf_lint.llm.analyzer import LLMAnalyzer

        analyzer = LLMAnalyzer.from_config(config)
        result = analyzer.analyze(report)

        if result.errors and verbose:
            for err in result.errors:
                click.echo(click.style(f"  [LLM warning] {err}", fg="yellow"), err=True)
        if not quiet:
            click.echo(
                click.style(
                    f"  LLM analysis done — {result.calls_made} call(s), " f"model: {result.model}",
                    fg="bright_black",
                )
            )
        return result
    except ImportError:
        click.echo(
            click.style(
                "  LLM analysis skipped: 'anthropic' package not installed. "
                "Run: pip install 'spark-perf-lint[llm]'",
                fg="yellow",
            ),
            err=True,
        )
    except ValueError as exc:
        click.echo(
            click.style(f"  LLM analysis skipped: {exc}", fg="yellow"),
            err=True,
        )
    except Exception as exc:  # noqa: BLE001
        click.echo(
            click.style(f"  LLM analysis failed: {exc}", fg="red"),
            err=True,
        )
    return None


def _render_llm_extras(llm_result: Any) -> None:
    """Print cross-file insights and executive summary to stdout."""
    if llm_result.cross_file_insights:
        click.echo("")
        click.echo(click.style("  ── Cross-File Insights (Tier 2) ──", bold=True, fg="bright_cyan"))
        click.echo("")
        for line in llm_result.cross_file_insights.splitlines():
            click.echo(f"  {line}")
        click.echo("")

    if llm_result.executive_summary:
        click.echo(click.style("  ── Executive Summary (Tier 2) ──", bold=True, fg="bright_cyan"))
        click.echo("")
        for line in llm_result.executive_summary.splitlines():
            click.echo(f"  {line}")
        click.echo("")


# =============================================================================
# spark-perf-lint rules
# =============================================================================


# Map of dimension code → (block_name, human label)
_DIMENSION_META: dict[str, tuple[str, str]] = {
    "D01": ("d01_cluster_config", "Cluster Configuration"),
    "D02": ("d02_shuffle", "Shuffle"),
    "D03": ("d03_joins", "Joins"),
    "D04": ("d04_partitioning", "Partitioning"),
    "D05": ("d05_skew", "Data Skew"),
    "D06": ("d06_caching", "Caching"),
    "D07": ("d07_io_format", "I/O Format"),
    "D08": ("d08_aqe", "Adaptive Query Execution"),
    "D09": ("d09_udf_code", "UDF Code Quality"),
    "D10": ("d10_catalyst", "Catalyst Optimizer"),
    "D11": ("d11_monitoring", "Monitoring & Observability"),
}


def _rule_catalogue() -> dict[str, tuple[str, str]]:
    """Return a live rule_id → (severity, description) dict from the registry.

    Replaces the old static ``_RULE_CATALOGUE`` dict so the ``rules`` and
    ``explain`` commands are always in sync with registered rules.
    """
    from spark_perf_lint.rules.registry import RuleRegistry

    return {
        r.rule_id: (r.default_severity.name, r.description)
        for r in RuleRegistry.instance().get_all_rules()
    }


# Back-compat alias used by ``explain`` and ``rules`` commands.
# Populated lazily on first use so import time stays fast.
_RULE_CATALOGUE: dict[str, tuple[str, str]] = {}


def _get_catalogue() -> dict[str, tuple[str, str]]:
    """Return (and cache) the rule catalogue from the live registry."""
    global _RULE_CATALOGUE
    if not _RULE_CATALOGUE:
        _RULE_CATALOGUE = _rule_catalogue()
    return _RULE_CATALOGUE


_SEVERITY_COLORS: dict[str, str] = {
    "CRITICAL": "red",
    "WARNING": "yellow",
    "INFO": "blue",
}


@main.command("rules")
@click.option(
    "--dimension",
    "dimension_filter",
    default=None,
    help="Show only rules for this dimension (e.g. 'D03').",
    metavar="CODE",
)
@click.option(
    "--severity",
    "severity_filter",
    type=_SEVERITY_CHOICES,
    default=None,
    help="Show only rules with this default severity.",
    metavar="LEVEL",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"], case_sensitive=False),
    default="table",
    show_default=True,
    help="Output format.",
)
def rules(
    dimension_filter: str | None,
    severity_filter: str | None,
    output_format: str,
) -> None:
    """List all available rules with IDs, dimensions, and default severities.

    Examples:\n
      spark-perf-lint rules\n
      spark-perf-lint rules --dimension D03\n
      spark-perf-lint rules --severity CRITICAL\n
      spark-perf-lint rules --format json
    """
    filtered = {
        rule_id: (sev, desc)
        for rule_id, (sev, desc) in _get_catalogue().items()
        if (dimension_filter is None or rule_id.startswith(f"SPL-{dimension_filter.upper()}"))
        and (severity_filter is None or sev == severity_filter.upper())
    }

    if output_format == "json":
        import json

        payload = [
            {
                "rule_id": rule_id,
                "dimension": rule_id.split("-")[1],
                "severity": sev,
                "description": desc,
            }
            for rule_id, (sev, desc) in filtered.items()
        ]
        click.echo(json.dumps(payload, indent=2))
        return

    # Table output
    if not filtered:
        click.echo("No rules match the given filters.")
        return

    current_dim = ""
    for rule_id, (sev, desc) in filtered.items():
        dim = rule_id.split("-")[1]  # e.g. "D03"
        if dim != current_dim:
            current_dim = dim
            _, label = _DIMENSION_META.get(dim, ("", dim))
            click.echo("")
            click.echo(click.style(f"  {dim} · {label}", bold=True, fg="bright_cyan"))
            click.echo(click.style("  " + "─" * 60, fg="bright_black"))

        sev_str = click.style(f"{sev:<8}", fg=_SEVERITY_COLORS.get(sev, "white"))
        click.echo(f"  {rule_id}  {sev_str}  {desc}")

    click.echo("")
    click.echo(
        click.style(
            f"  {len(filtered)} rule(s) listed. "
            "Use 'spark-perf-lint explain RULE_ID' for full documentation.",
            fg="bright_black",
        )
    )


# =============================================================================
# spark-perf-lint init
# =============================================================================


@main.command("init")
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help=f"Overwrite an existing {CONFIG_FILENAME} without prompting.",
)
@click.option(
    "--minimal",
    is_flag=True,
    default=False,
    help="Write only the most commonly customised keys instead of the full file.",
)
def init(force: bool, minimal: bool) -> None:
    """Create a default .spark-perf-lint.yaml in the current directory.

    The generated file contains all available options with inline comments
    explaining each setting. Edit it to tune thresholds, enable/disable
    rules, and configure reporters for your project.

    Examples:\n
      spark-perf-lint init\n
      spark-perf-lint init --force\n
      spark-perf-lint init --minimal
    """
    dest = Path.cwd() / CONFIG_FILENAME

    if dest.exists() and not force:
        click.confirm(
            f"{CONFIG_FILENAME} already exists. Overwrite?",
            abort=True,
        )

    # Locate the bundled default config shipped with the package
    bundled = Path(__file__).parent.parent.parent / CONFIG_FILENAME
    if not bundled.exists():
        # Fall back: the file may live at the repo root during development
        bundled = Path(__file__).resolve().parents[3] / CONFIG_FILENAME

    if bundled.exists() and not minimal:
        dest.write_text(bundled.read_text(encoding="utf-8"), encoding="utf-8")
        click.echo(
            click.style(f"Created {dest}", fg="green", bold=True)
            + "  (copied from bundled defaults)"
        )
    else:
        # Write a concise minimal stub
        _write_minimal_config(dest)
        click.echo(click.style(f"Created {dest}", fg="green", bold=True) + "  (minimal config)")

    click.echo(
        "\nEdit the file to customise thresholds, enable/disable rules, and "
        "configure reporters.\nRun 'spark-perf-lint scan .' to start linting."
    )


def _write_minimal_config(dest: Path) -> None:
    """Write a compact starter config to *dest*."""
    content = """\
# spark-perf-lint configuration
# Full reference: spark-perf-lint explain --help
# Generated by: spark-perf-lint init --minimal

general:
  severity_threshold: INFO   # INFO | WARNING | CRITICAL
  fail_on: [CRITICAL]
  report_format: [terminal]
  max_findings: 0            # 0 = unlimited

thresholds:
  broadcast_threshold_mb: 10
  max_shuffle_partitions: 2000
  min_shuffle_partitions: 10

severity_override: {}
  # SPL-D03-001: CRITICAL

ignore:
  files:
    - "**/*_test.py"
    - "**/conftest.py"
  rules: []
  directories:
    - ".venv"
    - "venv"
    - "build"
    - "dist"
"""
    dest.write_text(content, encoding="utf-8")


# =============================================================================
# spark-perf-lint version
# =============================================================================


@main.command("version")
@click.option(
    "--short",
    is_flag=True,
    default=False,
    help="Print only the version number (no package name).",
)
def version(short: bool) -> None:
    """Print the spark-perf-lint version and exit.

    Examples:\n
      spark-perf-lint version\n
      spark-perf-lint version --short
    """
    if short:
        click.echo(__version__)
    else:
        click.echo(f"spark-perf-lint {__version__}")


# =============================================================================
# spark-perf-lint explain
# =============================================================================


@main.command("explain")
@click.argument("rule_id", metavar="RULE_ID")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json", "markdown"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format.",
)
def explain(rule_id: str, output_format: str) -> None:
    """Show detailed documentation for a specific rule.

    Includes the rule description, default severity, which Spark
    internals are affected, before/after code examples, and the
    decision-matrix context used by the engine.

    RULE_ID follows the pattern SPL-D{dimension}-{number},
    e.g. SPL-D03-001.

    Examples:\n
      spark-perf-lint explain SPL-D03-001\n
      spark-perf-lint explain SPL-D08-001 --format markdown\n
      spark-perf-lint explain SPL-D02-002 --format json
    """
    from spark_perf_lint.rules.registry import RuleRegistry

    normalised = rule_id.strip().upper()

    registry = RuleRegistry.instance()
    rule = registry.get_rule_by_id(normalised)

    if rule is None:
        # Give a helpful suggestion if the dimension exists but the number doesn't
        try:
            dim = normalised.split("-")[1]
        except IndexError:
            dim = None

        catalogue = _get_catalogue()
        if dim and any(k.startswith(f"SPL-{dim}") for k in catalogue):
            matching = [k for k in catalogue if k.startswith(f"SPL-{dim}")]
            click.echo(
                click.style(f"Unknown rule: {rule_id!r}", fg="red"),
                err=True,
            )
            click.echo(
                f"Rules in dimension {dim}: {', '.join(matching)}",
                err=True,
            )
        else:
            click.echo(
                click.style(f"Unknown rule: {rule_id!r}", fg="red"),
                err=True,
            )
            click.echo(
                "Run 'spark-perf-lint rules' to see all available rule IDs.",
                err=True,
            )
        sys.exit(1)

    sev = rule.default_severity.name
    dim_code = normalised.split("-")[1]
    _, dim_label = _DIMENSION_META.get(dim_code, ("", dim_code))

    if output_format == "json":
        import json

        click.echo(
            json.dumps(
                {
                    "rule_id": normalised,
                    "dimension": dim_code,
                    "dimension_label": dim_label,
                    "default_severity": sev,
                    "description": rule.description,
                    "explanation": rule.explanation,
                    "recommendation": rule.recommendation_template,
                    "before_example": rule.before_example,
                    "after_example": rule.after_example,
                    "estimated_impact": rule.estimated_impact,
                    "effort_level": rule.effort_level.value,
                    "references": list(rule.references),
                },
                indent=2,
            )
        )
        return

    if output_format == "markdown":
        click.echo(f"## {normalised}\n")
        click.echo(f"**Dimension** : {dim_code} · {dim_label}  ")
        click.echo(f"**Severity**  : {sev}  ")
        click.echo(f"**Summary**   : {rule.description}\n")
        click.echo(f"### Why this matters\n\n{rule.explanation}\n")
        click.echo(f"### Recommendation\n\n{rule.recommendation_template}\n")
        if rule.before_example:
            click.echo(f"**Before:**\n```python\n{rule.before_example}\n```\n")
        if rule.after_example:
            click.echo(f"**After:**\n```python\n{rule.after_example}\n```\n")
        if rule.estimated_impact:
            click.echo(f"**Impact:** {rule.estimated_impact}  ")
        click.echo(f"**Effort:** {rule.effort_level.value}  ")
        if rule.references:
            click.echo("\n**References:**")
            for ref in rule.references:
                click.echo(f"- {ref}")
        return

    # Default: plain text
    W = 68  # content width inside the indent
    sep = click.style("  " + "─" * W, fg="bright_black")

    click.echo("")
    click.echo(
        "  "
        + click.style(normalised, bold=True, fg="bright_cyan")
        + "  "
        + click.style(f"[{sev}]", fg=_SEVERITY_COLORS.get(sev, "white"), bold=True)
    )
    click.echo(sep)
    click.echo(f"  {'Dimension':<14}: {dim_code} · {dim_label}")
    click.echo(f"  {'Summary':<14}: {rule.description}")
    click.echo(f"  {'Impact':<14}: {rule.estimated_impact or '—'}")
    click.echo(f"  {'Effort':<14}: {rule.effort_level.value}")
    click.echo("")

    click.echo(click.style("  Why this matters", bold=True))
    click.echo(sep)
    # Word-wrap explanation at 68 chars
    import textwrap

    for para in rule.explanation.split("\n\n"):
        for line in textwrap.wrap(para.strip(), width=W):
            click.echo(f"  {line}")
        click.echo("")

    click.echo(click.style("  Recommendation", bold=True))
    click.echo(sep)
    for line in textwrap.wrap(rule.recommendation_template, width=W):
        click.echo(f"  {line}")
    click.echo("")

    if rule.before_example or rule.after_example:
        click.echo(click.style("  Before / After", bold=True))
        click.echo(sep)
        if rule.before_example:
            click.echo(click.style("  Before:", fg="red"))
            for line in rule.before_example.splitlines():
                click.echo(f"    {line}")
        if rule.after_example:
            click.echo(click.style("  After:", fg="green"))
            for line in rule.after_example.splitlines():
                click.echo(f"    {line}")
        click.echo("")

    if rule.references:
        click.echo(click.style("  References", bold=True))
        click.echo(sep)
        for ref in rule.references:
            click.echo(f"  • {ref}")
        click.echo("")


# =============================================================================
# spark-perf-lint traces
# =============================================================================


@main.command("traces")
@click.option(
    "--dir",
    "trace_dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=".spark-perf-lint-traces",
    show_default=True,
    help="Directory containing spl-trace-*.json files.",
    metavar="DIR",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    default="spl-traces-report.html",
    show_default=True,
    help="Destination HTML file.",
    metavar="PATH",
)
@click.option(
    "--title",
    "title",
    default="spark-perf-lint Trace Report",
    show_default=True,
    help="Report title shown in the page header.",
    metavar="TEXT",
)
@click.option(
    "--open",
    "open_browser",
    is_flag=True,
    default=False,
    help="Open the generated report in the default browser after writing.",
)
def traces(
    trace_dir: Path,
    output_path: Path,
    title: str,
    open_browser: bool,
) -> None:
    """Generate a static HTML report from trace files.

    Reads all spl-trace-*.json files in DIR, produces a self-contained
    HTML report with a findings trend chart, run history table, dimension
    breakdown, and hotspot files.

    To produce trace files, enable observability in .spark-perf-lint.yaml:

    \b
      observability:
        enabled: true
        backend: file
        output_dir: .spark-perf-lint-traces
        trace_level: standard

    Examples:\n
      spark-perf-lint traces\n
      spark-perf-lint traces --dir ./traces --output report.html\n
      spark-perf-lint traces --open
    """
    from spark_perf_lint.observability.viewer import generate_report

    if not trace_dir.is_dir():
        click.echo(
            click.style(
                f"Trace directory not found: {trace_dir}\n"
                "Enable observability and run a scan first.",
                fg="yellow",
            ),
            err=True,
        )
        sys.exit(2)

    try:
        n = generate_report(trace_dir, output_path, title=title)
    except Exception as exc:  # noqa: BLE001
        click.echo(click.style(f"Failed to generate report: {exc}", fg="red"), err=True)
        sys.exit(2)

    if n == 0:
        click.echo(
            click.style(
                f"No trace files found in {trace_dir}. "
                "Enable observability and run a scan first.",
                fg="yellow",
            )
        )
    else:
        click.echo(
            click.style("Report generated: ", fg="bright_black")
            + click.style(str(output_path), fg="bright_cyan", bold=True)
            + click.style(f"  ({n} trace{'s' if n != 1 else ''} loaded)", fg="bright_black")
        )

    if open_browser and output_path.exists():
        import webbrowser

        webbrowser.open(output_path.resolve().as_uri())


if __name__ == "__main__":
    main()
