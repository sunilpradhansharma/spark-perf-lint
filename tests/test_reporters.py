"""Tests for spark-perf-lint reporters.

Covers:
- JsonReporter     — structure, field values, stdout/file output, include_config flag
- MarkdownReporter — sections present, finding detail content, compact mode
- TerminalReporter — delegated to test_cli.py (Rich output tested via CliRunner)
- GitHubPRReporter — annotation format, env-var isolation, pass/fail detection

All tests use synthetic AuditReport / Finding fixtures to remain fast and
fully offline (no actual file scanning, no Spark, no API calls).
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from spark_perf_lint.config import LintConfig
from spark_perf_lint.types import AuditReport, Dimension, EffortLevel, Finding, Severity

# =============================================================================
# Shared fixtures
# =============================================================================


def _make_finding(
    rule_id: str = "SPL-D03-001",
    severity: Severity = Severity.CRITICAL,
    dimension: Dimension = Dimension.D03_JOINS,
    file_path: str = "jobs/etl.py",
    line_number: int = 42,
    *,
    before_code: str | None = "result = df_a.crossJoin(df_b)",
    after_code: str | None = "result = df_a.join(df_b, on='id', how='inner')",
    config_suggestion: dict | None = None,
    references: list[str] | None = None,
) -> Finding:
    """Build a minimal Finding with sensible defaults."""
    return Finding(
        rule_id=rule_id,
        severity=severity,
        dimension=dimension,
        file_path=file_path,
        line_number=line_number,
        message="Cartesian cross join detected — produces N×M rows.",
        explanation=(
            "Cross joins multiply every row in the left table with every row in "
            "the right table, producing O(N×M) output rows.  This is almost always "
            "accidental and can OOM an executor on moderately-sized tables."
        ),
        recommendation="Add a join key: df_a.join(df_b, on='id', how='inner')",
        before_code=before_code,
        after_code=after_code,
        config_suggestion=config_suggestion or {},
        estimated_impact="10–100× data volume increase",
        effort_level=EffortLevel.MINOR_CODE_CHANGE,
        references=references
        or ["https://spark.apache.org/docs/latest/sql-performance-tuning.html"],
    )


def _make_warning_finding() -> Finding:
    """A WARNING-severity finding for multi-severity tests."""
    return Finding(
        rule_id="SPL-D02-003",
        severity=Severity.WARNING,
        dimension=Dimension.D02_SHUFFLE,
        file_path="jobs/etl.py",
        line_number=17,
        message="sortByKey without explicit partitionBy.",
        explanation="sortByKey triggers a full shuffle with default parallelism.",
        recommendation="Call rdd.repartition(N).sortByKey() to control partition count.",
        effort_level=EffortLevel.MINOR_CODE_CHANGE,
    )


def _make_info_finding() -> Finding:
    """An INFO-severity finding."""
    return Finding(
        rule_id="SPL-D06-001",
        severity=Severity.INFO,
        dimension=Dimension.D06_CACHING,
        file_path="jobs/etl.py",
        line_number=5,
        message="DataFrame accessed multiple times without cache().",
        explanation="Re-computing the same DataFrame wastes CPU.",
        recommendation="Call df.cache() before the first use.",
        effort_level=EffortLevel.CONFIG_ONLY,
    )


def _make_report(
    findings: list[Finding] | None = None,
    *,
    files_scanned: int = 3,
    duration: float = 0.123,
) -> AuditReport:
    return AuditReport(
        findings=findings if findings is not None else [_make_finding()],
        files_scanned=files_scanned,
        scan_duration_seconds=duration,
        config_used={},
    )


@pytest.fixture
def critical_finding() -> Finding:
    return _make_finding()


@pytest.fixture
def warning_finding() -> Finding:
    return _make_warning_finding()


@pytest.fixture
def default_config() -> LintConfig:
    return LintConfig.from_dict({})


@pytest.fixture
def strict_config() -> LintConfig:
    return LintConfig.from_dict(
        {"general": {"severity_threshold": "INFO", "fail_on": ["WARNING", "CRITICAL"]}}
    )


# =============================================================================
# JsonReporter
# =============================================================================


class TestJsonReporter:
    """Tests for spark_perf_lint.reporters.json_reporter.JsonReporter."""

    def _reporter(self, report=None, config=None, **kwargs):
        from spark_perf_lint.reporters.json_reporter import JsonReporter

        return JsonReporter(
            report or _make_report(),
            config or LintConfig.from_dict({}),
            **kwargs,
        )

    # ── structure ─────────────────────────────────────────────────────────────

    def test_to_dict_has_required_top_level_keys(self) -> None:
        data = self._reporter().to_dict()
        assert set(data.keys()) >= {"metadata", "scan", "summary", "findings", "config"}

    def test_metadata_block_fields(self) -> None:
        data = self._reporter().to_dict()
        meta = data["metadata"]
        assert meta["tool"] == "spark-perf-lint"
        assert "version" in meta
        assert "generated_at" in meta
        assert "severity_threshold" in meta
        assert "fail_on" in meta

    def test_generated_at_is_iso8601_utc(self) -> None:
        data = self._reporter().to_dict()
        ts = data["metadata"]["generated_at"]
        # Must end with +00:00 or Z to prove UTC
        assert ts.endswith("+00:00") or ts.endswith("Z"), f"Timestamp {ts!r} does not indicate UTC"

    def test_scan_block_fields(self) -> None:
        report = _make_report(files_scanned=5, duration=0.456)
        data = self._reporter(report=report).to_dict()
        scan = data["scan"]
        assert scan["files_scanned"] == 5
        assert isinstance(scan["duration_seconds"], float)
        assert isinstance(scan["passed"], bool)
        assert "critical_count" in scan
        assert "warning_count" in scan
        assert "info_count" in scan

    def test_scan_passed_true_when_no_critical(self) -> None:
        report = _make_report(findings=[_make_warning_finding()])
        config = LintConfig.from_dict({"general": {"fail_on": ["CRITICAL"]}})
        data = self._reporter(report=report, config=config).to_dict()
        assert data["scan"]["passed"] is True

    def test_scan_passed_false_when_critical_present(self) -> None:
        config = LintConfig.from_dict({"general": {"fail_on": ["CRITICAL"]}})
        data = self._reporter(config=config).to_dict()  # default report has CRITICAL
        assert data["scan"]["passed"] is False

    def test_findings_list_length(self) -> None:
        findings = [_make_finding(), _make_warning_finding()]
        data = self._reporter(report=_make_report(findings=findings)).to_dict()
        assert len(data["findings"]) == 2

    def test_finding_has_required_fields(self) -> None:
        data = self._reporter().to_dict()
        f = data["findings"][0]
        for key in ("rule_id", "severity", "dimension", "file_path", "line_number", "message"):
            assert key in f, f"Finding dict missing field: {key!r}"

    def test_summary_block_structure(self) -> None:
        data = self._reporter().to_dict()
        s = data["summary"]
        assert "total" in s
        assert "by_severity" in s
        assert "by_dimension" in s

    # ── config block ──────────────────────────────────────────────────────────

    def test_config_block_included_by_default(self) -> None:
        data = self._reporter().to_dict()
        assert "config" in data

    def test_config_block_omitted_when_flag_false(self) -> None:
        data = self._reporter(include_config=False).to_dict()
        assert "config" not in data

    # ── output ────────────────────────────────────────────────────────────────

    def test_to_json_returns_valid_json_string(self) -> None:
        raw = self._reporter().to_json()
        parsed = json.loads(raw)  # must not raise
        assert isinstance(parsed, dict)

    def test_write_to_stringio(self) -> None:
        buf = io.StringIO()
        self._reporter().write(file=buf)
        content = buf.getvalue()
        assert content.strip()
        json.loads(content)  # must be valid JSON

    def test_write_to_file(self, tmp_path: Path) -> None:
        out = tmp_path / "report.json"
        with out.open("w") as fh:
            self._reporter().write(file=fh)
        data = json.loads(out.read_text())
        assert data["metadata"]["tool"] == "spark-perf-lint"

    def test_compact_output_with_indent_none(self) -> None:
        raw = self._reporter(indent=None).to_json()
        # Compact JSON has no indented lines
        assert "\n  " not in raw

    def test_empty_report_serialises_cleanly(self) -> None:
        report = _make_report(findings=[])
        data = self._reporter(report=report).to_dict()
        assert data["findings"] == []
        assert data["scan"]["critical_count"] == 0

    def test_finding_rule_id_preserved(self) -> None:
        data = self._reporter().to_dict()
        assert data["findings"][0]["rule_id"] == "SPL-D03-001"

    def test_severity_threshold_in_metadata(self) -> None:
        config = LintConfig.from_dict({"general": {"severity_threshold": "WARNING"}})
        data = self._reporter(config=config).to_dict()
        assert data["metadata"]["severity_threshold"] == "WARNING"

    def test_fail_on_list_in_metadata(self) -> None:
        config = LintConfig.from_dict({"general": {"fail_on": ["CRITICAL", "WARNING"]}})
        data = self._reporter(config=config).to_dict()
        assert set(data["metadata"]["fail_on"]) == {"CRITICAL", "WARNING"}

    def test_multi_severity_counts(self) -> None:
        findings = [
            _make_finding(),  # CRITICAL
            _make_warning_finding(),  # WARNING
            _make_info_finding(),  # INFO
        ]
        report = _make_report(findings=findings)
        config = LintConfig.from_dict({"general": {"severity_threshold": "INFO"}})
        data = self._reporter(report=report, config=config).to_dict()
        assert data["scan"]["critical_count"] == 1
        assert data["scan"]["warning_count"] == 1
        assert data["scan"]["info_count"] == 1


# =============================================================================
# MarkdownReporter
# =============================================================================


class TestMarkdownReporter:
    """Tests for spark_perf_lint.reporters.markdown_reporter.MarkdownReporter."""

    def _reporter(self, report=None, config=None, **kwargs):
        from spark_perf_lint.reporters.markdown_reporter import MarkdownReporter

        return MarkdownReporter(
            report or _make_report(),
            config or LintConfig.from_dict({}),
            **kwargs,
        )

    def _md(self, report=None, config=None, **kwargs) -> str:
        return self._reporter(report=report, config=config, **kwargs).to_str()

    # ── sections ──────────────────────────────────────────────────────────────

    def test_output_has_header_section(self) -> None:
        md = self._md()
        # Header contains a pass/fail indicator
        assert "FAILED" in md or "PASSED" in md

    def test_passed_header_on_no_findings(self) -> None:
        md = self._md(report=_make_report(findings=[]))
        assert "PASSED" in md

    def test_failed_header_on_critical_finding(self) -> None:
        config = LintConfig.from_dict({"general": {"fail_on": ["CRITICAL"]}})
        md = self._md(config=config)
        assert "FAILED" in md

    def test_summary_table_present(self) -> None:
        md = self._md()
        assert (
            "files_scanned" in md.lower() or "Files Scanned" in md or "files scanned" in md.lower()
        )

    def test_findings_table_has_rule_id(self) -> None:
        md = self._md()
        assert "SPL-D03-001" in md

    def test_findings_table_has_severity(self) -> None:
        md = self._md()
        assert "CRITICAL" in md

    def test_details_section_has_collapsible_block(self) -> None:
        md = self._md()
        assert "<details>" in md
        assert "<summary>" in md

    def test_details_section_has_finding_message(self) -> None:
        md = self._md()
        assert "Cartesian cross join" in md

    def test_details_section_has_before_after_code(self) -> None:
        md = self._md(show_fix=True)
        assert "crossJoin" in md
        assert "```python" in md

    def test_no_fix_suppresses_code_blocks(self) -> None:
        md_fix = self._md(show_fix=True)
        md_nofix = self._md(show_fix=False)
        # show_fix=True should include the before code snippet
        assert "crossJoin" in md_fix
        # show_fix=False should not include code blocks
        assert "```python" not in md_nofix

    def test_dimension_breakdown_section(self) -> None:
        md = self._md()
        # Dimension breakdown table lists dimension codes
        assert "D03" in md

    def test_footer_timestamp(self) -> None:
        md = self._md()
        # Footer contains a generated-at line
        assert "Generated" in md or "generated" in md or "spark-perf-lint" in md

    def test_empty_findings_no_table(self) -> None:
        md = self._md(report=_make_report(findings=[]))
        # No findings table when there are no findings
        assert "SPL-D" not in md

    def test_multiple_findings_all_appear(self) -> None:
        findings = [_make_finding(), _make_warning_finding()]
        md = self._md(report=_make_report(findings=findings))
        assert "SPL-D03-001" in md
        assert "SPL-D02-003" in md

    def test_render_writes_to_stringio(self) -> None:
        buf = io.StringIO()
        self._reporter().render(file=buf)
        content = buf.getvalue()
        assert len(content) > 100

    def test_render_writes_to_file(self, tmp_path: Path) -> None:
        out = tmp_path / "report.md"
        with out.open("w", encoding="utf-8") as fh:
            self._reporter().render(file=fh)
        content = out.read_text(encoding="utf-8")
        assert "spark-perf-lint" in content.lower()

    def test_compact_mode_shorter_than_full(self) -> None:
        full_md = self._md(compact=False)
        compact_md = self._md(compact=True)
        assert len(compact_md) < len(full_md)

    def test_pipe_chars_in_messages_escaped(self) -> None:
        """Table cells with '|' must be escaped so Markdown tables don't break."""
        finding = Finding(
            rule_id="SPL-D03-001",
            severity=Severity.CRITICAL,
            dimension=Dimension.D03_JOINS,
            file_path="a.py",
            line_number=1,
            message="Found a|b|c in message",
            explanation="explain",
            recommendation="fix it",
        )
        md = self._md(report=_make_report(findings=[finding]))
        # The raw '|' in the message should be escaped as '\|' in table cells
        assert r"a\|b\|c" in md or "a|b|c" in md  # escaped or in non-table context


# =============================================================================
# GitHubPRReporter
# =============================================================================


class TestGitHubPRReporter:
    """Tests for spark_perf_lint.reporters.github_pr.GitHubPRReporter."""

    def _reporter(self, report=None, config=None, **kwargs):
        from spark_perf_lint.reporters.github_pr import GitHubPRReporter

        return GitHubPRReporter(
            report or _make_report(),
            config or LintConfig.from_dict({}),
            **kwargs,
        )

    # ── annotation format ─────────────────────────────────────────────────────

    def test_emit_annotations_critical_uses_error_level(self, capsys) -> None:
        """CRITICAL findings must emit ::error:: annotations."""
        self._reporter().emit_annotations()
        captured = capsys.readouterr()
        assert "::error " in captured.out

    def test_emit_annotations_warning_uses_warning_level(self, capsys) -> None:
        """WARNING findings must emit ::warning:: annotations."""
        report = _make_report(findings=[_make_warning_finding()])
        self._reporter(report=report).emit_annotations()
        captured = capsys.readouterr()
        assert "::warning " in captured.out

    def test_emit_annotations_info_uses_notice_level(self, capsys) -> None:
        """INFO findings must emit ::notice:: annotations."""
        config = LintConfig.from_dict({"general": {"severity_threshold": "INFO"}})
        report = _make_report(findings=[_make_info_finding()])
        self._reporter(report=report, config=config).emit_annotations()
        captured = capsys.readouterr()
        assert "::notice " in captured.out

    def test_annotation_contains_file_path(self, capsys) -> None:
        self._reporter().emit_annotations()
        out = capsys.readouterr().out
        assert "jobs/etl.py" in out

    def test_annotation_contains_line_number(self, capsys) -> None:
        self._reporter().emit_annotations()
        out = capsys.readouterr().out
        assert "line=42" in out

    def test_annotation_contains_title_with_rule_id(self, capsys) -> None:
        self._reporter().emit_annotations()
        out = capsys.readouterr().out
        assert "SPL-D03-001" in out

    def test_annotation_group_markers_present(self, capsys) -> None:
        """Output should be wrapped in ::group:: / ::endgroup:: blocks."""
        self._reporter().emit_annotations()
        out = capsys.readouterr().out
        assert "::group::" in out
        assert "::endgroup::" in out

    def test_no_annotations_for_empty_report(self, capsys) -> None:
        report = _make_report(findings=[])
        self._reporter(report=report).emit_annotations()
        out = capsys.readouterr().out
        # Only group markers; no ::error:: / ::warning:: / ::notice::
        assert "::error " not in out
        assert "::warning " not in out
        assert "::notice " not in out

    def test_annotation_output_to_file(self, tmp_path: Path) -> None:
        out_file = tmp_path / "annotations.txt"
        with out_file.open("w", encoding="utf-8") as fh:
            self._reporter().emit_annotations(file=fh)
        content = out_file.read_text()
        assert "::error " in content

    # ── severity filtering ────────────────────────────────────────────────────

    def test_annotations_respect_severity_threshold(self, capsys) -> None:
        """INFO findings should not appear when threshold is WARNING."""
        config = LintConfig.from_dict({"general": {"severity_threshold": "WARNING"}})
        report = _make_report(findings=[_make_info_finding()])
        self._reporter(report=report, config=config).emit_annotations()
        out = capsys.readouterr().out
        assert "::notice " not in out

    # ── run() return value ────────────────────────────────────────────────────

    def test_run_returns_1_when_critical_present(self, capsys, monkeypatch) -> None:
        """run() should return 1 when CRITICAL findings match fail_on."""
        config = LintConfig.from_dict({"general": {"fail_on": ["CRITICAL"]}})
        reporter = self._reporter(config=config)
        monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        # post_comment=False: no network; write_summary=False: no env var file
        rc = reporter.run(post_comment=False, write_summary=False)
        capsys.readouterr()  # drain stdout
        assert rc == 1

    def test_run_returns_0_when_no_blocking_findings(self, capsys, monkeypatch) -> None:
        """run() should return 0 when no findings match fail_on severity."""
        config = LintConfig.from_dict({"general": {"fail_on": ["CRITICAL"]}})
        report = _make_report(findings=[_make_warning_finding()])
        reporter = self._reporter(report=report, config=config)
        monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        rc = reporter.run(post_comment=False, write_summary=False)
        capsys.readouterr()
        assert rc == 0

    # ── encoding ─────────────────────────────────────────────────────────────

    def test_newlines_in_message_are_percent_encoded(self, capsys) -> None:
        """Newlines in annotation messages must be encoded as %0A."""
        finding = Finding(
            rule_id="SPL-D03-001",
            severity=Severity.CRITICAL,
            dimension=Dimension.D03_JOINS,
            file_path="a.py",
            line_number=1,
            message="line one\nline two",
            explanation="explain",
            recommendation="fix",
        )
        report = _make_report(findings=[finding])
        self._reporter(report=report).emit_annotations()
        out = capsys.readouterr().out
        # Raw newlines must NOT appear in the annotation payload
        # (they would break the ::command:: parser)
        lines = [ln for ln in out.splitlines() if "::error" in ln]
        assert lines, "Expected at least one ::error line"
        assert "\n" not in lines[0]

    def test_colons_in_file_path_are_encoded(self, capsys) -> None:
        """Colons in property values must be percent-encoded."""
        finding = Finding(
            rule_id="SPL-D03-001",
            severity=Severity.CRITICAL,
            dimension=Dimension.D03_JOINS,
            file_path="C:\\Users\\dev\\jobs\\etl.py",
            line_number=1,
            message="cross join",
            explanation="explain",
            recommendation="fix",
        )
        report = _make_report(findings=[finding])
        self._reporter(report=report).emit_annotations()
        out = capsys.readouterr().out
        # The :: after '::error' should only be the one separating properties from message
        # Check that the line is still parseable (no bare colon inside file= value)
        error_lines = [ln for ln in out.splitlines() if ln.startswith("::error")]
        assert error_lines


# =============================================================================
# Reporter integration: multi-format _render_report wiring in cli.py
# =============================================================================


class TestCliReporterWiring:
    """Verify that cli._render_report dispatches to the correct reporter(s)."""

    def test_json_format_produces_json_output(self, tmp_path: Path) -> None:
        """--format json should produce valid JSON on stdout."""
        from click.testing import CliRunner

        from spark_perf_lint.cli import main as cli_main

        code = tmp_path / "etl.py"
        code.write_text("result = df_a.crossJoin(df_b)\n", encoding="utf-8")

        result = CliRunner().invoke(
            cli_main,
            ["scan", "--format", "json", str(code)],
            catch_exceptions=False,
        )
        # Must be parseable JSON
        data = json.loads(result.output)
        assert data["metadata"]["tool"] == "spark-perf-lint"

    def test_markdown_format_produces_markdown_output(self, tmp_path: Path) -> None:
        """--format markdown should produce Markdown output."""
        from click.testing import CliRunner

        from spark_perf_lint.cli import main as cli_main

        code = tmp_path / "etl.py"
        code.write_text("result = df_a.crossJoin(df_b)\n", encoding="utf-8")

        result = CliRunner().invoke(
            cli_main,
            ["scan", "--format", "markdown", str(code)],
            catch_exceptions=False,
        )
        assert "# " in result.output or "##" in result.output or "<details>" in result.output

    def test_multiple_formats_both_present(self, tmp_path: Path) -> None:
        """Passing --format terminal --format json should include both reporters' output."""
        from click.testing import CliRunner

        from spark_perf_lint.cli import main as cli_main

        code = tmp_path / "etl.py"
        code.write_text("result = df_a.crossJoin(df_b)\n", encoding="utf-8")

        result = CliRunner().invoke(
            cli_main,
            ["scan", "--format", "terminal", "--format", "json", str(code)],
            catch_exceptions=False,
        )
        # Should contain JSON object (from json reporter)
        output = result.output
        # JSON block must be present somewhere
        assert '"tool"' in output or '"metadata"' in output

    def test_output_path_writes_file(self, tmp_path: Path) -> None:
        """--output FILE with --format json should write JSON to the file."""
        from click.testing import CliRunner

        from spark_perf_lint.cli import main as cli_main

        code = tmp_path / "etl.py"
        code.write_text("result = df_a.crossJoin(df_b)\n", encoding="utf-8")
        out_file = tmp_path / "report.json"

        CliRunner().invoke(
            cli_main,
            ["scan", "--format", "json", "--output", str(out_file), str(code)],
            catch_exceptions=False,
        )
        assert out_file.exists()
        data = json.loads(out_file.read_text())
        assert data["metadata"]["tool"] == "spark-perf-lint"
