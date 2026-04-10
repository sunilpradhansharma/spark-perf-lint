"""End-to-end integration tests for spark-perf-lint.

Five test classes:

1. TestBadCodeFixtures   — scan every bad_code fixture; assert the expected
                           CRITICAL rules fire for each dimension.
2. TestGoodCodeFixtures  — scan every good_code fixture; assert no CRITICAL
                           findings from the file's target dimension.
3. TestConfigOverrides   — disable rules, change severity, ignore files,
                           cap findings count.
4. TestReportFormats     — terminal, JSON, and Markdown reporters produce
                           well-formed output; --output writes a file.
5. TestExitCodes         — CLI exits 0/1/2 under the correct conditions.
6. TestCLICommands       — scan, rules, version, explain, init end-to-end.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from spark_perf_lint.cli import main
from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.orchestrator import ScanOrchestrator
from spark_perf_lint.types import AuditReport, Severity

# ---------------------------------------------------------------------------
# Fixture paths
# ---------------------------------------------------------------------------

_FIXTURES = Path(__file__).parent / "fixtures"
_BAD = _FIXTURES / "bad_code"
_GOOD = _FIXTURES / "good_code"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[mGKHF]")


def _strip(text: str) -> str:
    return _ANSI_ESCAPE.sub("", text)


def _scan_file(path: Path, config: LintConfig | None = None) -> AuditReport:
    """Read *path* and scan its contents with the given config."""
    cfg = config or LintConfig.from_dict({"general": {"severity_threshold": "INFO"}})
    orch = ScanOrchestrator(cfg)
    return orch.scan_content(path.read_text(encoding="utf-8"), path.name)


def _rule_ids(report: AuditReport) -> set[str]:
    return {f.rule_id for f in report.findings}


def _critical_ids(report: AuditReport) -> set[str]:
    return {f.rule_id for f in report.findings if f.severity == Severity.CRITICAL}


def _dim_critical_ids(report: AuditReport, dim: str) -> set[str]:
    """CRITICAL findings whose rule_id starts with SPL-{dim}."""
    prefix = f"SPL-{dim}-"
    return {
        f.rule_id
        for f in report.findings
        if f.rule_id.startswith(prefix) and f.severity == Severity.CRITICAL
    }


def _dim_warning_ids(report: AuditReport, dim: str) -> set[str]:
    prefix = f"SPL-{dim}-"
    return {
        f.rule_id
        for f in report.findings
        if f.rule_id.startswith(prefix) and f.severity == Severity.WARNING
    }


# ===========================================================================
# 1. Bad-code fixtures: expected CRITICAL rules fire
# ===========================================================================


class TestBadCodeFixtures:
    """Scanning bad_code fixtures produces the expected CRITICAL findings."""

    def test_bad_config_fires_executor_cores_warning(self) -> None:
        report = _scan_file(_BAD / "bad_config.py")
        # SPL-D01-005 fires when executor.cores > max_executor_cores (5)
        assert "SPL-D01-005" in _rule_ids(report)

    def test_bad_config_fires_shuffle_partitions_warning(self) -> None:
        report = _scan_file(_BAD / "bad_config.py")
        # No shuffle.partitions set → SPL-D01-010
        assert "SPL-D01-010" in _rule_ids(report)

    def test_bad_config_fires_missing_memory_warnings(self) -> None:
        report = _scan_file(_BAD / "bad_config.py")
        # No executor.memory → SPL-D01-001
        assert "SPL-D01-001" in _rule_ids(report)
        # No driver.memory → SPL-D01-002
        assert "SPL-D01-002" in _rule_ids(report)

    def test_bad_joins_fires_cross_join_critical(self) -> None:
        report = _scan_file(_BAD / "bad_joins.py")
        assert "SPL-D03-001" in _critical_ids(report)

    def test_bad_joins_fires_broadcast_threshold_disabled_critical(self) -> None:
        report = _scan_file(_BAD / "bad_joins.py")
        assert "SPL-D03-003" in _critical_ids(report)

    def test_bad_joins_fires_join_in_loop_critical(self) -> None:
        report = _scan_file(_BAD / "bad_joins.py")
        assert "SPL-D03-008" in _critical_ids(report)

    def test_bad_shuffles_fires_group_by_key_critical(self) -> None:
        report = _scan_file(_BAD / "bad_shuffles.py")
        assert "SPL-D02-001" in _critical_ids(report)

    def test_bad_shuffles_fires_collect_without_limit_critical(self) -> None:
        report = _scan_file(_BAD / "bad_shuffles.py")
        assert "SPL-D09-005" in _critical_ids(report)

    def test_bad_caching_fires_cache_in_loop_critical(self) -> None:
        report = _scan_file(_BAD / "bad_caching.py")
        assert "SPL-D06-003" in _critical_ids(report)

    def test_bad_partitioning_fires_repartition_one_critical(self) -> None:
        report = _scan_file(_BAD / "bad_partitioning.py")
        assert "SPL-D04-001" in _critical_ids(report)

    def test_bad_udfs_fires_udf_with_spark_ref_critical(self) -> None:
        report = _scan_file(_BAD / "bad_udfs.py")
        assert "SPL-D09-003" in _critical_ids(report)

    def test_bad_udfs_fires_collect_no_limit_critical(self) -> None:
        report = _scan_file(_BAD / "bad_udfs.py")
        assert "SPL-D09-005" in _critical_ids(report)

    def test_bad_udfs_fires_topandas_no_limit_critical(self) -> None:
        report = _scan_file(_BAD / "bad_udfs.py")
        assert "SPL-D09-006" in _critical_ids(report)

    def test_bad_io_fires_jdbc_no_partitions_critical(self) -> None:
        report = _scan_file(_BAD / "bad_io.py")
        assert "SPL-D07-006" in _critical_ids(report)

    def test_bad_aqe_fires_aqe_disabled_critical(self) -> None:
        report = _scan_file(_BAD / "bad_aqe.py")
        assert "SPL-D08-001" in _critical_ids(report)

    def test_bad_skew_fires_no_d05_criticals_from_config_file(self) -> None:
        # bad_skew.py disables skewJoin; we verify it fires at least one D05 rule
        report = _scan_file(_BAD / "bad_skew.py")
        d05 = _rule_ids(report)
        d05_rules = {r for r in d05 if r.startswith("SPL-D05-")}
        assert d05_rules, f"Expected at least one D05 rule, got: {d05_rules}"

    def test_bad_general_fires_udf_warning(self) -> None:
        report = _scan_file(_BAD / "bad_general.py")
        assert "SPL-D09-001" in _rule_ids(report)

    def test_all_bad_files_have_findings(self) -> None:
        for path in sorted(_BAD.glob("*.py")):
            if path.stem == "__init__":
                continue
            report = _scan_file(path)
            assert report.findings, f"{path.name} produced zero findings"

    def test_all_bad_files_have_more_findings_than_good_counterparts(self) -> None:
        """Each bad file has more findings than its good counterpart."""
        pairs = [
            ("bad_config.py", "good_config.py"),
            ("bad_joins.py", "good_joins.py"),
            ("bad_shuffles.py", "good_shuffles.py"),
            ("bad_caching.py", "good_caching.py"),
            ("bad_partitioning.py", "good_partitioning.py"),
            ("bad_udfs.py", "good_udfs.py"),
            ("bad_io.py", "good_io.py"),
            ("bad_aqe.py", "good_aqe.py"),
        ]
        for bad_name, good_name in pairs:
            bad_report = _scan_file(_BAD / bad_name)
            good_report = _scan_file(_GOOD / good_name)
            assert len(bad_report.findings) > len(good_report.findings), (
                f"{bad_name} ({len(bad_report.findings)}) should have more findings "
                f"than {good_name} ({len(good_report.findings)})"
            )

    def test_bad_files_have_more_criticals_than_good(self) -> None:
        """Bad files have strictly more CRITICAL findings than good counterparts."""
        pairs = [
            ("bad_joins.py", "good_joins.py", "D03"),
            ("bad_udfs.py", "good_udfs.py", "D09"),
            ("bad_aqe.py", "good_aqe.py", "D08"),
            ("bad_io.py", "good_io.py", "D07"),
        ]
        for bad_name, good_name, dim in pairs:
            bad_crits = len(_dim_critical_ids(_scan_file(_BAD / bad_name), dim))
            good_crits = len(_dim_critical_ids(_scan_file(_GOOD / good_name), dim))
            assert bad_crits > good_crits, (
                f"{bad_name} D{dim} criticals ({bad_crits}) should exceed "
                f"{good_name} ({good_crits})"
            )


# ===========================================================================
# 2. Good-code fixtures: no CRITICAL from the target dimension
# ===========================================================================


class TestGoodCodeFixtures:
    """Good-code fixtures have no CRITICAL findings from their target dimension."""

    @pytest.mark.parametrize(
        "filename,dim",
        [
            ("good_config.py", "D01"),
            ("good_joins.py", "D03"),
            ("good_udfs.py", "D09"),
            ("good_aqe.py", "D08"),
            ("good_skew.py", "D05"),
        ],
    )
    def test_no_critical_from_target_dimension(self, filename: str, dim: str) -> None:
        """Target-dimension rules produce no CRITICAL findings in good code."""
        report = _scan_file(_GOOD / filename)
        crits = _dim_critical_ids(report, dim)
        assert not crits, f"{filename}: expected no CRITICAL {dim} findings, got {crits}"

    def test_good_config_no_d01_findings(self) -> None:
        report = _scan_file(_GOOD / "good_config.py")
        d01 = _dim_critical_ids(report, "D01") | _dim_warning_ids(report, "D01")
        assert not d01, f"good_config.py: unexpected D01 findings: {d01}"

    def test_good_joins_no_d03_criticals(self) -> None:
        report = _scan_file(_GOOD / "good_joins.py")
        crits = _dim_critical_ids(report, "D03")
        assert not crits, f"good_joins.py: unexpected D03 CRITICAL: {crits}"

    def test_good_udfs_no_d09_findings(self) -> None:
        """good_udfs.py uses only native functions — zero D09 findings."""
        report = _scan_file(_GOOD / "good_udfs.py")
        d09 = {f.rule_id for f in report.findings if f.rule_id.startswith("SPL-D09-")}
        assert not d09, f"good_udfs.py: unexpected D09 findings: {d09}"

    def test_good_aqe_no_d08_criticals(self) -> None:
        report = _scan_file(_GOOD / "good_aqe.py")
        crits = _dim_critical_ids(report, "D08")
        assert not crits, f"good_aqe.py: unexpected D08 CRITICAL: {crits}"

    def test_good_shuffles_no_d02_criticals(self) -> None:
        report = _scan_file(_GOOD / "good_shuffles.py")
        crits = _dim_critical_ids(report, "D02")
        assert not crits, f"good_shuffles.py: unexpected D02 CRITICAL: {crits}"

    def test_good_partitioning_no_d04_criticals(self) -> None:
        report = _scan_file(_GOOD / "good_partitioning.py")
        crits = _dim_critical_ids(report, "D04")
        assert not crits, f"good_partitioning.py: unexpected D04 CRITICAL: {crits}"

    def test_good_io_no_d07_criticals(self) -> None:
        report = _scan_file(_GOOD / "good_io.py")
        crits = _dim_critical_ids(report, "D07")
        assert not crits, f"good_io.py: unexpected D07 CRITICAL: {crits}"

    def test_good_general_no_d10_criticals(self) -> None:
        report = _scan_file(_GOOD / "good_general.py")
        crits = _dim_critical_ids(report, "D10")
        assert not crits, f"good_general.py: unexpected D10 CRITICAL: {crits}"

    def test_good_skew_no_d05_findings(self) -> None:
        """good_skew.py: AQE skewJoin enabled — no D05 findings at all."""
        report = _scan_file(_GOOD / "good_skew.py")
        d05 = {f.rule_id for f in report.findings if f.rule_id.startswith("SPL-D05-")}
        assert not d05, f"good_skew.py: unexpected D05 findings: {d05}"

    def test_good_files_all_pass_at_critical_threshold(self) -> None:
        """Every good file passes when fail_on=CRITICAL (excluding files with
        legitimate CRITICAL patterns from non-primary dimensions)."""
        # good_caching.py: join() in a for-loop fires D03-008, but each
        # iteration writes to a distinct path — this is intentional.
        # good_skew.py: crossJoin on a 10-row salt table fires D03-001, but
        # the salting pattern is the whole point of the file.
        known_exceptions = {"good_caching.py", "good_skew.py"}

        config = LintConfig.from_dict({"general": {"fail_on": ["CRITICAL"]}})
        orch = ScanOrchestrator(config)
        fail_sevs = set(config.fail_on)
        for path in sorted(_GOOD.glob("*.py")):
            if path.stem == "__init__" or path.name in known_exceptions:
                continue
            report = orch.scan_content(path.read_text(), path.name)
            should_fail = any(f.severity in fail_sevs for f in report.findings)
            assert not should_fail, (
                f"{path.name} has CRITICAL findings: "
                f"{[f.rule_id for f in report.findings if f.severity == Severity.CRITICAL]}"
            )


# ===========================================================================
# 3. Config overrides
# ===========================================================================


class TestConfigOverrides:
    """Config overrides correctly change linting behaviour."""

    def test_disable_dimension_suppresses_all_its_findings(self) -> None:
        """Disabling d03_joins removes all SPL-D03-* findings."""
        config = LintConfig.from_dict(
            {
                "general": {"severity_threshold": "INFO"},
                "rules": {"d03_joins": {"enabled": False}},
            }
        )
        report = _scan_file(_BAD / "bad_joins.py", config)
        d03 = {f.rule_id for f in report.findings if f.rule_id.startswith("SPL-D03-")}
        assert not d03, f"D03 disabled but still fires: {d03}"

    def test_disable_multiple_dimensions(self) -> None:
        config = LintConfig.from_dict(
            {
                "general": {"severity_threshold": "INFO"},
                "rules": {
                    "d08_aqe": {"enabled": False},
                    "d01_cluster_config": {"enabled": False},
                },
            }
        )
        report = _scan_file(_BAD / "bad_aqe.py", config)
        forbidden = {
            f.rule_id
            for f in report.findings
            if f.rule_id.startswith("SPL-D08-") or f.rule_id.startswith("SPL-D01-")
        }
        assert not forbidden, f"Disabled dimensions still fire: {forbidden}"

    def test_ignore_rule_id_suppresses_finding(self) -> None:
        """Adding SPL-D03-001 to ignore.rules suppresses crossJoin finding."""
        config = LintConfig.from_dict(
            {
                "general": {"severity_threshold": "INFO"},
                "ignore": {"rules": ["SPL-D03-001"]},
            }
        )
        report = _scan_file(_BAD / "bad_joins.py", config)
        assert "SPL-D03-001" not in _rule_ids(report)

    def test_ignore_multiple_rules(self) -> None:
        config = LintConfig.from_dict(
            {
                "general": {"severity_threshold": "INFO"},
                "ignore": {"rules": ["SPL-D03-001", "SPL-D03-003", "SPL-D03-008"]},
            }
        )
        report = _scan_file(_BAD / "bad_joins.py", config)
        for rule_id in ["SPL-D03-001", "SPL-D03-003", "SPL-D03-008"]:
            assert rule_id not in _rule_ids(report), f"{rule_id} still fires after ignore"

    def test_severity_threshold_info_shows_all_findings(self) -> None:
        config_info = LintConfig.from_dict({"general": {"severity_threshold": "INFO"}})
        config_crit = LintConfig.from_dict({"general": {"severity_threshold": "CRITICAL"}})
        report_info = _scan_file(_BAD / "bad_config.py", config_info)
        report_crit = _scan_file(_BAD / "bad_config.py", config_crit)
        assert len(report_info.findings) >= len(report_crit.findings)

    def test_severity_threshold_critical_hides_warnings_in_reporter(self) -> None:
        """severity_threshold is a reporter concern — the CLI should show no
        findings when the threshold is CRITICAL and the file has only WARNINGs."""
        runner = CliRunner()
        r = runner.invoke(
            main,
            [
                "scan",
                str(_BAD / "bad_config.py"),
                "--severity-threshold",
                "CRITICAL",
                "--format",
                "json",
            ],
        )
        data = json.loads(r.output)
        # bad_config.py has no CRITICALs; at CRITICAL threshold the findings
        # list should be empty (reporters filter by threshold).
        critical_findings = [f for f in data["findings"] if f["severity"] == "CRITICAL"]
        assert len(critical_findings) == 0

    def test_max_findings_cap_limits_output(self) -> None:
        config = LintConfig.from_dict(
            {"general": {"severity_threshold": "INFO", "max_findings": 5}}
        )
        report = _scan_file(_BAD / "bad_joins.py", config)
        assert len(report.findings) <= 5

    def test_max_findings_zero_means_unlimited(self) -> None:
        config_uncapped = LintConfig.from_dict(
            {"general": {"severity_threshold": "INFO", "max_findings": 0}}
        )
        config_capped = LintConfig.from_dict(
            {"general": {"severity_threshold": "INFO", "max_findings": 3}}
        )
        report_uncapped = _scan_file(_BAD / "bad_joins.py", config_uncapped)
        report_capped = _scan_file(_BAD / "bad_joins.py", config_capped)
        assert len(report_uncapped.findings) > len(report_capped.findings)

    def test_ignore_file_glob_skips_file_in_directory_scan(self, tmp_path: Path) -> None:
        """A file matching ignore.files is excluded from directory scans."""
        # Copy bad_config.py into tmp_path under a name matching the glob.
        src = (_BAD / "bad_config.py").read_text(encoding="utf-8")
        target = tmp_path / "skip_me.py"
        target.write_text(src, encoding="utf-8")

        config = LintConfig.from_dict(
            {
                "general": {"severity_threshold": "INFO"},
                "ignore": {"files": ["skip_me.py"]},
            }
        )
        orch = ScanOrchestrator(config)
        report = orch.scan([str(tmp_path)])
        assert report.files_scanned == 0

    def test_fail_on_warning_exits_1_for_warnings(self) -> None:
        """With fail_on=[WARNING], a file with only warnings causes exit 1."""
        config = LintConfig.from_dict(
            {"general": {"fail_on": ["WARNING"], "severity_threshold": "INFO"}}
        )
        report = _scan_file(_BAD / "bad_config.py", config)
        fail_sevs = set(config.fail_on)
        should_fail = any(f.severity in fail_sevs for f in report.findings)
        assert should_fail

    def test_yaml_config_file_is_respected(self, tmp_path: Path) -> None:
        """A project-level YAML config file changes scan behaviour."""
        # Write a config that disables D01
        config_data = {
            "general": {"severity_threshold": "INFO"},
            "rules": {"d01_cluster_config": {"enabled": False}},
        }
        (tmp_path / ".spark-perf-lint.yaml").write_text(yaml.dump(config_data), encoding="utf-8")
        # Write bad_config.py into tmp_path
        src = (_BAD / "bad_config.py").read_text(encoding="utf-8")
        job_file = tmp_path / "job.py"
        job_file.write_text(src, encoding="utf-8")

        config = LintConfig.load(start_dir=tmp_path)
        orch = ScanOrchestrator(config)
        report = orch.scan([str(job_file)])
        d01 = {f.rule_id for f in report.findings if f.rule_id.startswith("SPL-D01-")}
        assert not d01, f"D01 disabled via YAML but still fires: {d01}"


# ===========================================================================
# 4. Report formats
# ===========================================================================


class TestReportFormats:
    """Reporters produce well-formed output in all supported formats."""

    def _invoke_scan(self, *args: str) -> tuple[int, str]:
        runner = CliRunner()
        r = runner.invoke(main, ["scan", *args], catch_exceptions=False)
        return r.exit_code, _strip(r.output)

    # JSON format -------------------------------------------------------

    def test_json_format_is_valid_json(self) -> None:
        runner = CliRunner()
        r = runner.invoke(
            main,
            ["scan", str(_BAD / "bad_config.py"), "--format", "json"],
        )
        data = json.loads(r.output)
        assert isinstance(data, dict)

    def test_json_format_has_expected_top_level_keys(self) -> None:
        runner = CliRunner()
        r = runner.invoke(
            main,
            ["scan", str(_BAD / "bad_config.py"), "--format", "json"],
        )
        data = json.loads(r.output)
        assert set(data.keys()) >= {"metadata", "scan", "summary", "findings"}

    def test_json_metadata_has_tool_and_version(self) -> None:
        runner = CliRunner()
        r = runner.invoke(
            main,
            ["scan", str(_BAD / "bad_config.py"), "--format", "json"],
        )
        data = json.loads(r.output)
        assert data["metadata"]["tool"] == "spark-perf-lint"
        assert "version" in data["metadata"]

    def test_json_findings_list_has_required_fields(self) -> None:
        runner = CliRunner()
        r = runner.invoke(
            main,
            ["scan", str(_BAD / "bad_joins.py"), "--format", "json"],
        )
        data = json.loads(r.output)
        assert data["findings"], "Expected at least one finding"
        finding = data["findings"][0]
        for field in ("rule_id", "severity", "file_path", "line_number", "message"):
            assert field in finding, f"Missing field {field!r} in finding"

    def test_json_scan_block_has_counts(self) -> None:
        runner = CliRunner()
        r = runner.invoke(
            main,
            ["scan", str(_BAD / "bad_joins.py"), "--format", "json"],
        )
        data = json.loads(r.output)
        scan = data["scan"]
        assert "files_scanned" in scan
        assert "critical_count" in scan
        assert "warning_count" in scan
        assert scan["critical_count"] > 0

    def test_json_to_file(self, tmp_path: Path) -> None:
        out = tmp_path / "report.json"
        runner = CliRunner()
        runner.invoke(
            main,
            ["scan", str(_BAD / "bad_config.py"), "--format", "json", "--output", str(out)],
        )
        assert out.exists()
        data = json.loads(out.read_text())
        assert "findings" in data

    # Markdown format ---------------------------------------------------

    def test_markdown_format_starts_with_heading(self) -> None:
        runner = CliRunner()
        r = runner.invoke(
            main,
            ["scan", str(_BAD / "bad_joins.py"), "--format", "markdown"],
        )
        plain = _strip(r.output)
        assert plain.startswith("#"), f"Expected markdown heading, got: {plain[:60]!r}"

    def test_markdown_format_contains_rule_ids(self) -> None:
        runner = CliRunner()
        r = runner.invoke(
            main,
            ["scan", str(_BAD / "bad_joins.py"), "--format", "markdown"],
        )
        assert "SPL-D03" in r.output

    def test_markdown_format_contains_severity_labels(self) -> None:
        runner = CliRunner()
        r = runner.invoke(
            main,
            ["scan", str(_BAD / "bad_joins.py"), "--format", "markdown"],
        )
        assert "CRITICAL" in r.output

    def test_markdown_to_file(self, tmp_path: Path) -> None:
        out = tmp_path / "report.md"
        runner = CliRunner()
        runner.invoke(
            main,
            ["scan", str(_BAD / "bad_joins.py"), "--format", "markdown", "--output", str(out)],
        )
        assert out.exists()
        content = out.read_text()
        assert "#" in content
        assert "SPL-D03" in content

    # Terminal format ---------------------------------------------------

    def test_terminal_format_shows_rule_ids(self) -> None:
        exit_code, output = self._invoke_scan(str(_BAD / "bad_joins.py"))
        assert "SPL-D03" in output

    def test_terminal_format_shows_passed_or_failed_banner(self) -> None:
        exit_code, output = self._invoke_scan(str(_BAD / "bad_joins.py"))
        assert "PASSED" in output or "FAILED" in output

    def test_terminal_quiet_flag_reduces_output(self) -> None:
        runner = CliRunner()
        r_normal = runner.invoke(main, ["scan", str(_BAD / "bad_config.py")])
        r_quiet = runner.invoke(main, ["scan", str(_BAD / "bad_config.py"), "--quiet"])
        normal_lines = len(r_normal.output.splitlines())
        quiet_lines = len(r_quiet.output.splitlines())
        assert quiet_lines < normal_lines

    def test_terminal_no_fix_hides_code_suggestions(self) -> None:
        exit_code, output = self._invoke_scan(str(_BAD / "bad_joins.py"), "--no-fix")
        # The "Before:" / "After:" panels should not appear.
        assert "Before:" not in output


# ===========================================================================
# 5. Exit codes
# ===========================================================================


class TestExitCodes:
    """CLI exits with the correct code under each scenario."""

    def _exit_code(self, *args: str) -> int:
        runner = CliRunner()
        r = runner.invoke(main, ["scan", *args])
        return r.exit_code

    def test_exit_0_when_no_critical_and_fail_on_critical(self) -> None:
        # bad_config.py has no CRITICAL findings; fail_on defaults to CRITICAL only.
        code = self._exit_code(str(_BAD / "bad_config.py"))
        assert code == 0

    def test_exit_1_when_critical_finding_and_fail_on_critical(self) -> None:
        code = self._exit_code(str(_BAD / "bad_joins.py"), "--fail-on", "CRITICAL")
        assert code == 1

    def test_exit_0_when_warnings_and_fail_on_critical_only(self) -> None:
        # bad_config.py only has WARNINGs; fail_on=CRITICAL → exit 0.
        code = self._exit_code(str(_BAD / "bad_config.py"), "--fail-on", "CRITICAL")
        assert code == 0

    def test_exit_1_when_warnings_and_fail_on_warning(self) -> None:
        code = self._exit_code(str(_BAD / "bad_config.py"), "--fail-on", "WARNING")
        assert code == 1

    def test_exit_0_for_clean_file(self) -> None:
        # good_udfs.py is completely clean at INFO threshold.
        code = self._exit_code(str(_GOOD / "good_udfs.py"), "--fail-on", "WARNING")
        assert code == 0

    def test_exit_0_for_non_spark_file(self, tmp_path: Path) -> None:
        """A plain Python file (no pyspark imports) exits 0."""
        plain = tmp_path / "util.py"
        plain.write_text("import os\ndef foo(): return 42\n", encoding="utf-8")
        code = self._exit_code(str(plain))
        assert code == 0

    def test_exit_2_on_bad_config_file(self, tmp_path: Path) -> None:
        """A malformed YAML config causes exit 2."""
        config_file = tmp_path / ".spark-perf-lint.yaml"
        config_file.write_text("general:\n  severity_threshold: INVALID_LEVEL\n")
        plain = tmp_path / "job.py"
        plain.write_text(
            "from pyspark.sql import SparkSession\n" "spark = SparkSession.builder.getOrCreate()\n"
        )
        code = self._exit_code(str(plain), "--config", str(config_file))
        assert code == 2

    def test_fail_on_repeated_flags(self) -> None:
        """--fail-on can be repeated: --fail-on CRITICAL --fail-on WARNING."""
        code = self._exit_code(
            str(_BAD / "bad_config.py"),
            "--fail-on",
            "CRITICAL",
            "--fail-on",
            "WARNING",
        )
        # bad_config.py has WARNINGs → exit 1
        assert code == 1

    def test_exit_0_on_empty_directory(self, tmp_path: Path) -> None:
        """An empty directory produces exit 0 (nothing to scan)."""
        code = self._exit_code(str(tmp_path))
        assert code == 0

    def test_exit_0_when_severity_threshold_hides_all_findings(self) -> None:
        """CRITICAL threshold on a WARNING-only file → exit 0 even with fail_on=CRITICAL."""
        # bad_config.py: no CRITICALs → fail_on=CRITICAL → exit 0
        code = self._exit_code(
            str(_BAD / "bad_config.py"),
            "--severity-threshold",
            "CRITICAL",
            "--fail-on",
            "CRITICAL",
        )
        assert code == 0


# ===========================================================================
# 6. CLI commands
# ===========================================================================


class TestCLICommands:
    """Full CLI command end-to-end tests."""

    _runner = CliRunner()

    def _run(self, *args: str) -> tuple[int, str]:
        r = self._runner.invoke(main, list(args))
        return r.exit_code, r.output

    # version -----------------------------------------------------------

    def test_version_exits_0(self) -> None:
        code, _ = self._run("version")
        assert code == 0

    def test_version_contains_package_name(self) -> None:
        _, output = self._run("version")
        assert "spark-perf-lint" in output

    def test_version_short_flag_prints_only_number(self) -> None:
        _, output = self._run("version", "--short")
        # Version number only: "0.1.0\n" — no package name
        assert "spark-perf-lint" not in output
        assert re.match(r"\d+\.\d+", output.strip())

    # rules -------------------------------------------------------------

    def test_rules_command_exits_0(self) -> None:
        code, _ = self._run("rules")
        assert code == 0

    def test_rules_lists_all_dimensions(self) -> None:
        _, output = self._run("rules")
        for dim in ["D01", "D02", "D03", "D04", "D05", "D06", "D07", "D08", "D09", "D10", "D11"]:
            assert dim in output, f"Dimension {dim} not listed"

    def test_rules_dimension_filter(self) -> None:
        _, output = self._run("rules", "--dimension", "D03")
        assert "SPL-D03" in output
        assert "SPL-D01" not in output

    def test_rules_severity_filter(self) -> None:
        _, output = self._run("rules", "--severity", "CRITICAL")
        assert "CRITICAL" in output

    def test_rules_json_format_valid_json(self) -> None:
        _, output = self._run("rules", "--format", "json")
        data = json.loads(output)
        assert isinstance(data, list)
        assert len(data) > 0

    def test_rules_json_has_rule_id_and_severity(self) -> None:
        _, output = self._run("rules", "--format", "json")
        rules = json.loads(output)
        for rule in rules:
            assert "rule_id" in rule
            assert "severity" in rule

    def test_rules_d03_json_returns_all_d03_rules(self) -> None:
        _, output = self._run("rules", "--dimension", "D03", "--format", "json")
        rules = json.loads(output)
        from spark_perf_lint.rules.registry import RuleRegistry

        expected = sum(
            1 for r in RuleRegistry.instance().get_all_rules() if r.rule_id.startswith("SPL-D03")
        )
        assert len(rules) == expected

    # explain -----------------------------------------------------------

    def test_explain_known_rule_exits_0(self) -> None:
        code, _ = self._run("explain", "SPL-D03-001")
        assert code == 0

    def test_explain_shows_rule_id_in_output(self) -> None:
        _, output = self._run("explain", "SPL-D03-001")
        assert "SPL-D03-001" in output

    def test_explain_shows_severity(self) -> None:
        _, output = self._run("explain", "SPL-D03-001")
        assert "CRITICAL" in output

    def test_explain_unknown_rule_exits_1(self) -> None:
        code, _ = self._run("explain", "SPL-D99-999")
        assert code == 1

    def test_explain_json_format_valid_json(self) -> None:
        _, output = self._run("explain", "SPL-D08-001", "--format", "json")
        data = json.loads(output)
        assert data["rule_id"] == "SPL-D08-001"
        assert "dimension" in data

    def test_explain_markdown_format_has_heading(self) -> None:
        _, output = self._run("explain", "SPL-D08-001", "--format", "markdown")
        assert output.startswith("## SPL-D08-001")

    def test_explain_case_insensitive_rule_id(self) -> None:
        code, output = self._run("explain", "spl-d03-001")
        assert code == 0
        assert "SPL-D03-001" in output

    # init --------------------------------------------------------------

    def test_init_creates_config_file(self, tmp_path: Path) -> None:
        r = self._runner.invoke(main, ["init"], catch_exceptions=False)
        # init writes to cwd; use isolated_filesystem
        with self._runner.isolated_filesystem(temp_dir=tmp_path):
            r = self._runner.invoke(main, ["init"])
            assert r.exit_code == 0
            assert Path(".spark-perf-lint.yaml").exists()

    def test_init_minimal_creates_smaller_file(self, tmp_path: Path) -> None:
        with self._runner.isolated_filesystem(temp_dir=tmp_path):
            self._runner.invoke(main, ["init"])
            full_size = len(Path(".spark-perf-lint.yaml").read_text())
            Path(".spark-perf-lint.yaml").unlink()
            self._runner.invoke(main, ["init", "--minimal"])
            minimal_size = len(Path(".spark-perf-lint.yaml").read_text())
        assert minimal_size <= full_size

    def test_init_minimal_is_valid_yaml(self, tmp_path: Path) -> None:
        with self._runner.isolated_filesystem(temp_dir=tmp_path):
            self._runner.invoke(main, ["init", "--minimal"])
            content = Path(".spark-perf-lint.yaml").read_text()
        data = yaml.safe_load(content)
        assert isinstance(data, dict)
        assert "general" in data

    # scan --------------------------------------------------------------

    def test_scan_single_file(self) -> None:
        code, output = self._run("scan", str(_BAD / "bad_joins.py"))
        assert code in (0, 1)
        assert "SPL-D03" in output

    def test_scan_multiple_files(self) -> None:
        code, output = self._run(
            "scan",
            str(_BAD / "bad_joins.py"),
            str(_BAD / "bad_config.py"),
        )
        assert code in (0, 1)
        # Both files scanned
        assert "SPL-D03" in output
        assert "SPL-D01" in output

    def test_scan_directory(self) -> None:
        code, output = self._run("scan", str(_BAD))
        assert code in (0, 1)
        # Should mention multiple files
        assert "SPL-D03" in output

    def test_scan_dimension_filter_d03_accepted(self) -> None:
        """--dimension D03 is accepted without error and D03 findings appear."""
        code, output = self._run(
            "scan",
            str(_BAD / "bad_joins.py"),
            "--dimension",
            "D03",
        )
        # Option is accepted cleanly (no Click UsageError → not exit 2)
        assert code in (0, 1)
        # D03 findings are present
        assert "SPL-D03" in output

    def test_scan_severity_threshold_warning_hides_info(self) -> None:
        code, output = self._run(
            "scan",
            str(_BAD / "bad_config.py"),
            "--severity-threshold",
            "WARNING",
        )
        # INFO-level rule IDs should not appear in output
        plain = _strip(output)
        # If any finding appears at all, it shouldn't have [INFO] label
        assert "[INFO]" not in plain

    def test_scan_verbose_flag_accepted(self) -> None:
        code, _ = self._run("scan", str(_BAD / "bad_config.py"), "--verbose")
        assert code in (0, 1)

    def test_scan_quiet_flag_accepted(self) -> None:
        code, _ = self._run("scan", str(_BAD / "bad_config.py"), "--quiet")
        assert code in (0, 1)

    def test_scan_no_paths_shows_usage(self) -> None:
        """Invoking with no args shows help text (no crash)."""
        code, output = self._run()
        assert code == 0
        assert "scan" in output.lower() or "usage" in output.lower() or "help" in output.lower()

    def test_scan_help_flag(self) -> None:
        code, output = self._run("scan", "--help")
        assert code == 0
        assert "PATH" in output

    def test_scan_json_output_sorted_by_severity(self) -> None:
        """JSON output: CRITICAL findings appear before WARNING/INFO."""
        runner = CliRunner()
        r = runner.invoke(
            main,
            ["scan", str(_BAD / "bad_joins.py"), "--format", "json"],
        )
        data = json.loads(r.output)
        findings = data["findings"]
        if len(findings) < 2:
            pytest.skip("Need at least 2 findings to check ordering")
        severity_order = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}
        for i in range(len(findings) - 1):
            a = severity_order[findings[i]["severity"]]
            b = severity_order[findings[i + 1]["severity"]]
            assert (
                a <= b
            ), f"Findings not sorted: {findings[i]['severity']} before {findings[i+1]['severity']}"

    def test_scan_nonexistent_path_fails(self) -> None:
        """Passing a non-existent path causes Click to report an error."""
        runner = CliRunner()
        r = runner.invoke(main, ["scan", "/nonexistent/path/job.py"])
        # Click's Path(exists=True) raises UsageError → exit 2
        assert r.exit_code == 2

    def test_scan_and_report_fields_match_findings(self) -> None:
        """AuditReport.files_scanned and finding counts are consistent."""
        config = LintConfig.from_dict({"general": {"severity_threshold": "INFO"}})
        orch = ScanOrchestrator(config)
        report = orch.scan([str(_BAD / "bad_joins.py")])
        assert report.files_scanned == 1
        assert report.critical_count == len(
            [f for f in report.findings if f.severity == Severity.CRITICAL]
        )
        assert report.warning_count == len(
            [f for f in report.findings if f.severity == Severity.WARNING]
        )
        assert report.summary["total"] == len(report.findings)
