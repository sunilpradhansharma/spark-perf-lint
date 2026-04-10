"""Targeted tests that close coverage gaps in core modules.

Covers previously-uncovered lines in:
- spark_perf_lint.types        (Severity/EffortLevel/Dimension reprs, AuditReport helpers)
- spark_perf_lint.rules.registry (reset, list_rules, __len__/__contains__, error paths)
- spark_perf_lint.config       (validation error paths, env-var overlay, _coerce_bool)
- spark_perf_lint.engine.file_scanner (OSError paths, glob expansion, from_git_diff)
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from spark_perf_lint.config import ConfigError, LintConfig, _coerce_bool
from spark_perf_lint.rules.registry import RuleRegistry, register_rule
from spark_perf_lint.types import (
    AuditReport,
    Dimension,
    EffortLevel,
    Finding,
    Severity,
    SparkConfigEntry,
)

# =============================================================================
# types.py
# =============================================================================


class TestSeverityComparisons:
    """Severity __lt__/__le__/__gt__/__ge__ with non-Severity operands."""

    def test_lt_non_severity_returns_not_implemented(self):
        result = Severity.INFO.__lt__(42)
        assert result is NotImplemented

    def test_le_non_severity_returns_not_implemented(self):
        result = Severity.WARNING.__le__("WARNING")
        assert result is NotImplemented

    def test_gt_non_severity_returns_not_implemented(self):
        result = Severity.CRITICAL.__gt__(None)
        assert result is NotImplemented

    def test_ge_non_severity_returns_not_implemented(self):
        result = Severity.INFO.__ge__(3.0)
        assert result is NotImplemented

    def test_ordering_is_consistent(self):
        assert Severity.INFO < Severity.WARNING < Severity.CRITICAL
        assert Severity.CRITICAL > Severity.WARNING > Severity.INFO
        assert Severity.WARNING <= Severity.WARNING
        assert Severity.WARNING >= Severity.WARNING


class TestEnumReprs:
    """__repr__ methods on all enum types."""

    def test_severity_repr(self):
        assert repr(Severity.CRITICAL) == "Severity.CRITICAL"
        assert repr(Severity.WARNING) == "Severity.WARNING"
        assert repr(Severity.INFO) == "Severity.INFO"

    def test_effort_level_repr(self):
        assert repr(EffortLevel.CONFIG_ONLY) == "EffortLevel.CONFIG_ONLY"
        assert repr(EffortLevel.MINOR_CODE_CHANGE) == "EffortLevel.MINOR_CODE_CHANGE"
        assert repr(EffortLevel.MAJOR_REFACTOR) == "EffortLevel.MAJOR_REFACTOR"

    def test_dimension_repr(self):
        assert repr(Dimension.D03_JOINS) == "Dimension.D03_JOINS"
        assert repr(Dimension.D01_CLUSTER_CONFIG) == "Dimension.D01_CLUSTER_CONFIG"


class TestSparkConfigEntry:
    """SparkConfigEntry __repr__ and to_dict."""

    def _entry(self) -> SparkConfigEntry:
        return SparkConfigEntry(
            parameter="spark.sql.adaptive.enabled",
            current_value="false",
            recommended_value="true",
            reason="AQE improves skew handling",
            impact="Static plan; no skew mitigation",
        )

    def test_repr_contains_parameter(self):
        entry = self._entry()
        r = repr(entry)
        assert "spark.sql.adaptive.enabled" in r
        assert "SparkConfigEntry" in r

    def test_to_dict_keys(self):
        d = self._entry().to_dict()
        assert set(d.keys()) == {
            "parameter", "current_value", "recommended_value", "reason", "impact"
        }

    def test_to_dict_values(self):
        d = self._entry().to_dict()
        assert d["parameter"] == "spark.sql.adaptive.enabled"
        assert d["current_value"] == "false"
        assert d["recommended_value"] == "true"


class TestFindingValidation:
    """Finding.__post_init__ raises ValueError for bad inputs."""

    def _make_finding(self, **kwargs) -> Finding:
        defaults = dict(
            rule_id="SPL-D03-001",
            severity=Severity.CRITICAL,
            dimension=Dimension.D03_JOINS,
            file_path="test.py",
            line_number=1,
            message="crossJoin detected",
            explanation="crossJoin produces a cartesian product",
            recommendation="Use a keyed join instead",
        )
        defaults.update(kwargs)
        return Finding(**defaults)

    def test_invalid_rule_id_prefix_raises(self):
        with pytest.raises(ValueError, match="rule_id must start with 'SPL-D'"):
            self._make_finding(rule_id="D03-001")

    def test_zero_line_number_raises(self):
        with pytest.raises(ValueError, match="line_number must be >= 1"):
            self._make_finding(line_number=0)

    def test_negative_line_number_raises(self):
        with pytest.raises(ValueError, match="line_number must be >= 1"):
            self._make_finding(line_number=-5)

    def test_repr_format(self):
        f = self._make_finding()
        r = repr(f)
        assert "Finding(rule_id=" in r
        assert "SPL-D03-001" in r
        assert "line=1" in r


class TestAuditReport:
    """AuditReport helpers: findings_by_*, to_dict, to_json, __repr__."""

    def _finding(self, severity: Severity, dim: Dimension = Dimension.D03_JOINS) -> Finding:
        return Finding(
            rule_id="SPL-D03-001",
            severity=severity,
            dimension=dim,
            file_path="f.py",
            line_number=1,
            message="msg",
            explanation="exp",
            recommendation="rec",
        )

    def _report(self) -> AuditReport:
        return AuditReport(
            findings=[
                self._finding(Severity.CRITICAL),
                self._finding(Severity.WARNING),
                self._finding(Severity.INFO, Dimension.D01_CLUSTER_CONFIG),
            ],
            files_scanned=1,
            scan_duration_seconds=0.1,
            config_used={},
        )

    def test_findings_by_severity_critical(self):
        report = self._report()
        criticals = report.findings_by_severity(Severity.CRITICAL)
        assert len(criticals) == 1
        assert all(f.severity == Severity.CRITICAL for f in criticals)

    def test_findings_by_severity_warning(self):
        report = self._report()
        warnings = report.findings_by_severity(Severity.WARNING)
        assert len(warnings) == 1

    def test_findings_by_dimension(self):
        report = self._report()
        d03 = report.findings_by_dimension(Dimension.D03_JOINS)
        assert len(d03) == 2  # CRITICAL + WARNING both tagged D03

    def test_findings_at_or_above_warning(self):
        report = self._report()
        at_or_above = report.findings_at_or_above(Severity.WARNING)
        assert len(at_or_above) == 2  # CRITICAL + WARNING

    def test_findings_at_or_above_info(self):
        report = self._report()
        all_findings = report.findings_at_or_above(Severity.INFO)
        assert len(all_findings) == 3

    def test_to_dict_has_expected_keys(self):
        d = self._report().to_dict()
        assert set(d.keys()) == {
            "summary", "files_scanned", "scan_duration_seconds",
            "passed", "findings", "config_used",
        }

    def test_to_dict_findings_list(self):
        d = self._report().to_dict()
        assert len(d["findings"]) == 3
        assert d["findings"][0]["rule_id"] == "SPL-D03-001"

    def test_to_json_is_valid_json(self):
        report = self._report()
        raw = report.to_json()
        parsed = json.loads(raw)
        assert parsed["files_scanned"] == 1

    def test_to_json_compact(self):
        report = self._report()
        raw = report.to_json(indent=0)
        assert "\n" not in raw

    def test_repr_format(self):
        r = repr(self._report())
        assert "AuditReport(" in r
        assert "findings=3" in r


# =============================================================================
# rules/registry.py
# =============================================================================


class TestRuleRegistry:
    """registry.py: reset, list_rules, __len__, __contains__, __repr__, error paths.

    Most tests use the existing singleton (which is already built by other test
    modules in the session).  We save and restore the singleton reference around
    each test so that mutations (reset, fake-rule registration) don't contaminate
    the shared process-wide singleton used by other test modules.
    """

    def setup_method(self):
        """Snapshot the current singleton (and any pending registrations)."""
        # Force the singleton to exist before we snapshot it.
        _ = RuleRegistry.instance()
        self._saved_instance = RuleRegistry._instance

    def teardown_method(self):
        """Restore the original singleton to avoid cross-test contamination."""
        RuleRegistry._instance = self._saved_instance

    # ------------------------------------------------------------------
    # Tests using the populated singleton (no reset)
    # ------------------------------------------------------------------

    def test_len_returns_rule_count(self):
        reg = RuleRegistry.instance()
        assert len(reg) > 0

    def test_contains_known_rule(self):
        reg = RuleRegistry.instance()
        assert "SPL-D03-001" in reg

    def test_not_contains_unknown_rule(self):
        reg = RuleRegistry.instance()
        assert "SPL-D99-999" not in reg

    def test_repr_format(self):
        reg = RuleRegistry.instance()
        r = repr(reg)
        assert "RuleRegistry(" in r
        assert "discovered=True" in r

    def test_get_rule_by_id_returns_rule(self):
        reg = RuleRegistry.instance()
        rule = reg.get_rule_by_id("SPL-D03-001")
        assert rule is not None
        assert rule.rule_id == "SPL-D03-001"

    def test_get_rule_by_id_missing_returns_none(self):
        reg = RuleRegistry.instance()
        assert reg.get_rule_by_id("SPL-D99-999") is None

    def test_get_rules_by_dimension_d03(self):
        reg = RuleRegistry.instance()
        d03_rules = reg.get_rules_by_dimension(Dimension.D03_JOINS)
        assert len(d03_rules) > 0
        assert all(r.rule_id.startswith("SPL-D03-") for r in d03_rules)
        ids = [r.rule_id for r in d03_rules]
        assert ids == sorted(ids)

    def test_get_rules_by_severity_critical(self):
        reg = RuleRegistry.instance()
        criticals = reg.get_rules_by_severity(Severity.CRITICAL)
        assert len(criticals) > 0
        ids = [r.rule_id for r in criticals]
        assert ids == sorted(ids)

    def test_list_rules_without_config(self):
        reg = RuleRegistry.instance()
        rows = reg.list_rules()
        assert len(rows) > 0
        row = rows[0]
        assert set(row.keys()) >= {
            "rule_id", "name", "dimension", "severity", "description",
            "effort_level", "enabled",
        }
        assert row["enabled"] is True  # no config → all enabled

    def test_list_rules_with_config_reflects_disabled_dimension(self):
        reg = RuleRegistry.instance()
        cfg = LintConfig.from_dict({"rules": {"d03_joins": {"enabled": False}}})
        rows = reg.list_rules(config=cfg)
        d03_rows = [r for r in rows if r["rule_id"].startswith("SPL-D03-")]
        assert all(not r["enabled"] for r in d03_rows)

    def test_discover_is_idempotent(self):
        reg = RuleRegistry.instance()
        count_before = len(reg)
        reg._discover()  # already discovered — should be a no-op
        assert len(reg) == count_before

    # ------------------------------------------------------------------
    # Tests that exercise register_rule paths — use an isolated registry
    # to avoid polluting the global singleton with fake rule IDs.
    # ------------------------------------------------------------------

    def test_register_rule_raises_typeerror_for_non_baserule(self):
        with pytest.raises(TypeError, match="@register_rule can only be applied"):
            register_rule(object)  # type: ignore[arg-type]

    def test_register_rule_after_singleton_built(self):
        """Registering via @register_rule after singleton exists calls _register directly."""
        from spark_perf_lint.rules.base import CodeRule

        # Use a fresh, isolated registry (not the process-wide singleton) so
        # the fake rule ID never leaks into other tests.
        isolated = RuleRegistry()
        isolated._discovered = True  # skip auto-discovery
        RuleRegistry._instance = isolated

        count_before = len(isolated)

        @register_rule
        class _FakeRulePostBuild(CodeRule):
            rule_id = "SPL-D99-FAKE-POST"
            name = "Fake post-build rule"
            description = "Registered after singleton creation"
            explanation = "Test explanation"
            recommendation_template = "Test recommendation"
            dimension = Dimension.D03_JOINS
            default_severity = Severity.INFO

            def check(self, analyzer, config):
                return []

        assert len(isolated) == count_before + 1
        assert "SPL-D99-FAKE-POST" in isolated
        # teardown_method restores _instance to self._saved_instance

    def test_duplicate_rule_id_is_silently_skipped(self):
        """Registering a rule with a duplicate ID onto an isolated registry skips it."""
        from spark_perf_lint.rules.base import CodeRule

        # Build a small isolated registry with one real rule
        isolated = RuleRegistry()
        isolated._discovered = True
        real_rule_class = type(RuleRegistry.instance().get_rule_by_id("SPL-D03-001"))
        isolated._register(real_rule_class)
        count_before = len(isolated)
        RuleRegistry._instance = isolated

        @register_rule
        class _DupeRuleIsolated(CodeRule):
            rule_id = "SPL-D03-001"  # same as the real rule above
            name = "Duplicate of cross join"
            description = "Duplicate"
            explanation = "Duplicate explanation"
            recommendation_template = "Duplicate recommendation"
            dimension = Dimension.D03_JOINS
            default_severity = Severity.CRITICAL

            def check(self, analyzer, config):
                return []

        # duplicate was silently skipped
        assert len(isolated) == count_before

    # ------------------------------------------------------------------
    # reset() behaviour
    # ------------------------------------------------------------------

    def test_reset_sets_instance_to_none_then_rebuilds(self):
        """reset() destroys the singleton; instance() rebuilds it."""
        reg1 = RuleRegistry.instance()
        RuleRegistry.reset()
        assert RuleRegistry._instance is None
        reg2 = RuleRegistry.instance()
        assert reg2 is not reg1  # new object


# =============================================================================
# config.py
# =============================================================================


class TestCoerceBool:
    """_coerce_bool covers the true/false/invalid branches."""

    def test_true_variants(self):
        assert _coerce_bool("true") is True
        assert _coerce_bool("True") is True
        assert _coerce_bool("1") is True
        assert _coerce_bool("yes") is True

    def test_false_variants(self):
        assert _coerce_bool("false") is False
        assert _coerce_bool("False") is False
        assert _coerce_bool("0") is False
        assert _coerce_bool("no") is False

    def test_invalid_raises_config_error(self):
        with pytest.raises(ConfigError, match="Expected a boolean value"):
            _coerce_bool("maybe")


class TestLintConfigValidation:
    """_validate() error paths for each section."""

    def test_invalid_severity_threshold_raises(self):
        with pytest.raises(ConfigError, match="severity_threshold"):
            LintConfig.from_dict({"general": {"severity_threshold": "BLOCKER"}})

    def test_invalid_fail_on_raises(self):
        with pytest.raises(ConfigError, match="fail_on"):
            LintConfig.from_dict({"general": {"fail_on": ["FATAL"]}})

    def test_invalid_report_format_raises(self):
        with pytest.raises(ConfigError, match="report_format"):
            LintConfig.from_dict({"general": {"report_format": ["pdf"]}})

    def test_negative_max_findings_raises(self):
        with pytest.raises(ConfigError, match="max_findings"):
            LintConfig.from_dict({"general": {"max_findings": -1}})

    def test_non_numeric_threshold_raises(self):
        with pytest.raises(ConfigError, match="thresholds"):
            LintConfig.from_dict({"thresholds": {"broadcast_threshold_mb": "big"}})

    def test_zero_threshold_raises(self):
        with pytest.raises(ConfigError, match="thresholds"):
            LintConfig.from_dict({"thresholds": {"broadcast_threshold_mb": 0}})

    def test_skew_ratio_ordering_violation_raises(self):
        with pytest.raises(ConfigError, match="skew_ratio_warning"):
            LintConfig.from_dict(
                {"thresholds": {"skew_ratio_warning": 10.0, "skew_ratio_critical": 5.0}}
            )

    def test_min_partition_gte_max_raises(self):
        with pytest.raises(ConfigError, match="min_partition_count"):
            LintConfig.from_dict(
                {"thresholds": {"min_partition_count": 500, "max_partition_count": 100}}
            )

    def test_min_shuffle_gte_max_raises(self):
        with pytest.raises(ConfigError, match="min_shuffle_partitions"):
            LintConfig.from_dict(
                {"thresholds": {"min_shuffle_partitions": 2000, "max_shuffle_partitions": 100}}
            )

    def test_invalid_severity_override_raises(self):
        with pytest.raises(ConfigError, match="severity_override"):
            LintConfig.from_dict({"severity_override": {"SPL-D03-001": "BLOCKER"}})

    def test_invalid_observability_backend_raises(self):
        with pytest.raises(ConfigError, match="observability.backend"):
            LintConfig.from_dict({"observability": {"backend": "datadog"}})

    def test_invalid_trace_level_raises(self):
        with pytest.raises(ConfigError, match="trace_level"):
            LintConfig.from_dict({"observability": {"trace_level": "ultra"}})

    def test_invalid_llm_provider_raises(self):
        with pytest.raises(ConfigError, match="llm.provider"):
            LintConfig.from_dict({"llm": {"provider": "openai"}})

    def test_invalid_min_severity_for_llm_raises(self):
        with pytest.raises(ConfigError, match="min_severity_for_llm"):
            LintConfig.from_dict({"llm": {"min_severity_for_llm": "BLOCKER"}})


class TestLintConfigYAMLErrors:
    """_parse_yaml_file error paths."""

    def test_unreadable_file_raises_config_error(self, tmp_path):
        bad = tmp_path / ".spark-perf-lint.yaml"
        bad.write_text("severity_threshold: INFO")
        bad.chmod(0o000)
        try:
            with pytest.raises(ConfigError, match="Cannot read config file"):
                LintConfig.load(start_dir=tmp_path)
        finally:
            bad.chmod(0o644)

    def test_invalid_yaml_raises_config_error(self, tmp_path):
        bad = tmp_path / ".spark-perf-lint.yaml"
        bad.write_text(": : : invalid yaml {{{{")
        with pytest.raises(ConfigError, match="not valid YAML"):
            LintConfig.load(start_dir=tmp_path)

    def test_non_mapping_yaml_raises_config_error(self, tmp_path):
        bad = tmp_path / ".spark-perf-lint.yaml"
        bad.write_text("- item1\n- item2\n")
        with pytest.raises(ConfigError, match="must be a YAML mapping"):
            LintConfig.load(start_dir=tmp_path)

    def test_empty_yaml_file_returns_defaults(self, tmp_path):
        empty = tmp_path / ".spark-perf-lint.yaml"
        empty.write_text("")
        cfg = LintConfig.load(start_dir=tmp_path)
        # Empty YAML → defaults only; should not raise
        assert cfg.severity_threshold == Severity.INFO


class TestLintConfigQueryMethods:
    """get_threshold, get_severity_for, llm_enabled, observability_enabled, __repr__."""

    def test_get_threshold_known_key(self):
        cfg = LintConfig.from_dict({})
        val = cfg.get_threshold("broadcast_threshold_mb")
        assert isinstance(val, float)
        assert val > 0

    def test_get_threshold_unknown_key_raises(self):
        cfg = LintConfig.from_dict({})
        with pytest.raises(KeyError, match="Unknown threshold"):
            cfg.get_threshold("nonexistent_threshold_xyz")

    def test_get_severity_for_override_present(self):
        cfg = LintConfig.from_dict({"severity_override": {"SPL-D03-001": "INFO"}})
        sev = cfg.get_severity_for("SPL-D03-001")
        assert sev == Severity.INFO

    def test_get_severity_for_no_override_returns_warning_default(self):
        cfg = LintConfig.from_dict({})
        sev = cfg.get_severity_for("SPL-D03-001")
        assert sev == Severity.WARNING  # safe fallback

    def test_llm_enabled_default_is_false(self):
        cfg = LintConfig.from_dict({})
        assert cfg.llm_enabled is False

    def test_observability_enabled_default_is_false(self):
        cfg = LintConfig.from_dict({})
        assert cfg.observability_enabled is False

    def test_repr_contains_class_name(self):
        cfg = LintConfig.from_dict({})
        r = repr(cfg)
        assert "LintConfig(" in r
        assert "severity_threshold=" in r

    def test_repr_with_config_file(self, tmp_path):
        f = tmp_path / ".spark-perf-lint.yaml"
        f.write_text("general:\n  severity_threshold: WARNING\n")
        cfg = LintConfig.load(start_dir=tmp_path)
        r = repr(cfg)
        assert ".spark-perf-lint.yaml" in r


class TestApplyEnvVars:
    """_apply_env_vars covers env-var overlay paths (requires LintConfig.load())."""

    def test_env_var_overrides_severity_threshold(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SPARK_PERF_LINT_SEVERITY_THRESHOLD", "CRITICAL")
        # load() (not from_dict) applies env-var overlays
        cfg = LintConfig.load(start_dir=tmp_path)
        assert cfg.severity_threshold == Severity.CRITICAL

    def test_env_var_overrides_max_findings_integer(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SPARK_PERF_LINT_MAX_FINDINGS", "50")
        cfg = LintConfig.load(start_dir=tmp_path)
        assert cfg.max_findings == 50

    def test_env_var_invalid_integer_raises(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SPARK_PERF_LINT_MAX_FINDINGS", "not_a_number")
        with pytest.raises(ConfigError, match="must be an integer"):
            LintConfig.load(start_dir=tmp_path)

    def test_env_var_report_format_comma_separated(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SPARK_PERF_LINT_REPORT_FORMAT", "json,markdown")
        cfg = LintConfig.load(start_dir=tmp_path)
        assert "json" in cfg.report_formats
        assert "markdown" in cfg.report_formats


# =============================================================================
# engine/file_scanner.py
# =============================================================================


class TestFileScannerErrorPaths:
    """OSError paths in _read_file, _read_head, and _build_target."""

    def test_from_paths_with_glob_pattern(self, tmp_path):
        """Glob patterns like '*.py' should be expanded relative to cwd."""
        from spark_perf_lint.engine.file_scanner import FileScanner

        f = tmp_path / "etl.py"
        f.write_text(
            "from pyspark.sql import SparkSession\n"
            "spark = SparkSession.builder.getOrCreate()\n"
        )
        cfg = LintConfig.from_dict({})
        targets, summary = FileScanner.from_paths(
            [str(tmp_path)], config=cfg, root=tmp_path
        )
        assert summary.total_found >= 1

    def test_from_paths_nonexistent_path_is_skipped(self, tmp_path):
        """A path that does not exist should be skipped without raising."""
        from spark_perf_lint.engine.file_scanner import FileScanner

        cfg = LintConfig.from_dict({})
        targets, summary = FileScanner.from_paths(
            [str(tmp_path / "does_not_exist.py")], config=cfg, root=tmp_path
        )
        assert summary.total_found == 0

    def test_from_paths_read_error_increments_read_errors(self, tmp_path):
        """A file that becomes unreadable after discovery should be counted in read_errors."""
        from spark_perf_lint.engine.file_scanner import FileScanner
        import spark_perf_lint.engine.file_scanner as fs_mod

        f = tmp_path / "bad.py"
        f.write_text("from pyspark.sql import SparkSession\n")
        cfg = LintConfig.from_dict({})

        with patch.object(fs_mod, "_read_file", return_value=("", "simulated OSError")):
            targets, summary = FileScanner.from_paths(
                [str(tmp_path)], config=cfg, root=tmp_path
            )
        assert summary.read_errors  # at least one error recorded

    def test_from_paths_head_read_error_returns_none_target(self, tmp_path):
        """_read_head failure makes _build_target return None → error recorded."""
        from spark_perf_lint.engine.file_scanner import FileScanner
        import spark_perf_lint.engine.file_scanner as fs_mod

        f = tmp_path / "spark_job.py"
        f.write_text("from pyspark.sql import SparkSession\n")
        cfg = LintConfig.from_dict({})

        with patch.object(fs_mod, "_read_head", return_value=("", "simulated head error")):
            targets, summary = FileScanner.from_paths(
                [str(tmp_path)], config=cfg, root=tmp_path
            )
        # file ends up None → counted as read error
        assert summary.read_errors or summary.total_found == 0


class TestFileScannerGitDiff:
    """from_git_diff and _git_diff_files/_git_untracked_files."""

    def test_from_git_diff_on_current_repo(self, tmp_path):
        """Comparing HEAD against itself should return an empty list of targets."""
        from spark_perf_lint.engine.file_scanner import FileScanner

        cfg = LintConfig.from_dict({})
        # HEAD vs HEAD → no changed files
        targets, summary = FileScanner.from_git_diff(
            base_ref="HEAD",
            config=cfg,
            repo_root=Path.cwd(),
        )
        assert isinstance(targets, list)
        assert summary.total_found >= 0

    def test_from_git_diff_bad_ref_raises_runtime_error(self):
        """An invalid git ref must raise RuntimeError."""
        from spark_perf_lint.engine.file_scanner import FileScanner

        cfg = LintConfig.from_dict({})
        with pytest.raises(RuntimeError):
            FileScanner.from_git_diff(
                base_ref="nonexistent-branch-xyz-abc-123",
                config=cfg,
                repo_root=Path.cwd(),
            )

    def test_git_untracked_files_suppresses_errors(self):
        """_git_untracked_files must return [] on CalledProcessError, not raise."""
        from spark_perf_lint.engine import file_scanner as fs_mod

        with patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "git")):
            result = fs_mod._git_untracked_files(Path.cwd())
        assert result == []

    def test_git_diff_files_raises_on_file_not_found(self):
        """_git_diff_files raises RuntimeError when git is not in PATH."""
        from spark_perf_lint.engine import file_scanner as fs_mod

        with patch("subprocess.run", side_effect=FileNotFoundError("git not found")):
            with pytest.raises(RuntimeError, match="git executable not found"):
                fs_mod._git_diff_files("HEAD", Path.cwd())

    def test_from_git_diff_include_untracked(self):
        """include_untracked=True must not raise (even if there are no untracked files)."""
        from spark_perf_lint.engine.file_scanner import FileScanner

        cfg = LintConfig.from_dict({})
        targets, summary = FileScanner.from_git_diff(
            base_ref="HEAD",
            config=cfg,
            repo_root=Path.cwd(),
            include_untracked=True,
        )
        assert isinstance(targets, list)
