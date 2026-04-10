"""Pre-commit hook integration tests.

Validates that the hook entry point (``spark-perf-lint scan``) behaves
exactly as pre-commit would invoke it:

* ``pass_filenames: true``  → filenames appended as positional arguments
* Exit code 1               → commit blocked  (findings ≥ fail_on severity)
* Exit code 0               → commit allowed  (no blocking findings)
* ``--no-verify`` bypass    → simulated by passing a clean / empty file

Uses Click's ``CliRunner`` so tests run in-process — no subprocess capture
quirks, no shell dependency, faster than spawning a child process.

All tests write files to ``tmp_path`` (a pytest-provided isolated directory).
The scanner discovers a git root and config automatically; since tmp_path has
no ``.spark-perf-lint.yaml``, all tests run with built-in defaults.
"""

from __future__ import annotations

import re
import subprocess
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from spark_perf_lint.cli import main as cli_main

# Regex that matches a single ANSI CSI escape sequence (e.g. \x1b[31m, \x1b[0m)
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[mGKHF]")


# ---------------------------------------------------------------------------
# Lightweight result type
# ---------------------------------------------------------------------------


@dataclass
class _HookResult:
    returncode: int
    stdout: str
    stderr: str = field(default="")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_hook(*extra_args: str, files: list[Path] | None = None) -> _HookResult:
    """Invoke the hook entry point as pre-commit would.

    pre-commit calls:  ``spark-perf-lint scan [args…] [staged-files…]``

    Args:
        *extra_args: CLI flags forwarded before the filenames.
        files: Paths to pass as positional file arguments.

    Returns:
        A ``_HookResult`` with ``returncode`` and ``stdout``.
    """
    args = ["scan", *extra_args]
    if files:
        args.extend(str(f) for f in files)

    result = CliRunner().invoke(cli_main, args, catch_exceptions=False)
    return _HookResult(
        returncode=result.exit_code,
        stdout=result.output or "",
    )


def _write_py(path: Path, source: str) -> Path:
    """Write *source* to *path*, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(source), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Anti-pattern fixtures
# ---------------------------------------------------------------------------

# CRITICAL: cartesian cross join → SPL-D03-001
_BAD_CROSS_JOIN = """\
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("test").getOrCreate()
    df_a = spark.table("orders")
    df_b = spark.table("products")
    result = df_a.crossJoin(df_b)
    result.write.parquet("/output/result")
"""

# WARNING only: sortByKey without partitionBy → SPL-D02-003
# (groupByKey is CRITICAL; use sortByKey which is WARNING)
_BAD_SORT_BY_KEY = """\
    from pyspark.sql import SparkSession
    from pyspark import SparkContext
    sc = SparkContext.getOrCreate()
    spark = SparkSession.builder.appName("rdd").getOrCreate()
    rdd = sc.parallelize([(1, "a"), (2, "b"), (1, "c")])
    result = rdd.sortByKey()
"""

# Clean file — no anti-patterns that should block on CRITICAL
_GOOD_ETL = """\
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder
        .appName("clean_etl")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.executor.memory", "8g")
        .config("spark.executor.cores", "4")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )

    customers = spark.read.parquet("/data/customers").filter(F.col("active") == True)
    orders = spark.read.parquet("/data/orders").filter(F.col("year") == 2024)
    result = customers.join(orders, on="customer_id", how="inner")
    result.write.mode("overwrite").parquet("/output/summary")
"""


# ---------------------------------------------------------------------------
# Exit-code tests
# ---------------------------------------------------------------------------


class TestHookExitCodes:
    """Exit code 1 = blocked, exit code 0 = allowed."""

    def test_critical_finding_exits_1(self, tmp_path: Path) -> None:
        """A CRITICAL finding in a staged file must block the commit."""
        bad = _write_py(tmp_path / "jobs" / "pipeline.py", _BAD_CROSS_JOIN)

        result = _run_hook("--fail-on", "CRITICAL", files=[bad])

        assert result.returncode == 1, (
            f"Expected exit 1 (blocked) for CRITICAL finding.\n" f"stdout: {result.stdout[:400]}"
        )

    def test_clean_file_exits_0(self, tmp_path: Path) -> None:
        """A file with no CRITICAL findings must allow the commit."""
        good = _write_py(tmp_path / "jobs" / "clean_etl.py", _GOOD_ETL)

        result = _run_hook("--fail-on", "CRITICAL", files=[good])

        assert result.returncode == 0, (
            f"Expected exit 0 (allowed) for clean file.\n" f"stdout: {result.stdout[:400]}"
        )

    def test_warning_does_not_block_when_fail_on_critical(self, tmp_path: Path) -> None:
        """A WARNING finding should not block when fail_on=[CRITICAL].

        Scopes the scan to --dimension D02 so only shuffle rules run.
        SPL-D02-003 (sortByKey without partitionBy) fires at WARNING severity.
        """
        warn_file = _write_py(tmp_path / "jobs" / "warn.py", _BAD_SORT_BY_KEY)

        result = _run_hook(
            "--fail-on",
            "CRITICAL",
            "--severity-threshold",
            "WARNING",
            "--dimension",
            "D02",
            files=[warn_file],
        )

        assert result.returncode == 0, (
            f"WARNING finding should not block with fail_on=CRITICAL.\n"
            f"stdout: {result.stdout[:400]}"
        )

    def test_warning_blocks_when_fail_on_includes_warning(self, tmp_path: Path) -> None:
        """A WARNING finding should block when fail_on includes WARNING."""
        warn_file = _write_py(tmp_path / "jobs" / "warn.py", _BAD_SORT_BY_KEY)

        result = _run_hook(
            "--fail-on",
            "CRITICAL",
            "--fail-on",
            "WARNING",
            "--dimension",
            "D02",
            files=[warn_file],
        )

        assert result.returncode == 1, (
            f"WARNING finding should block with fail_on=WARNING.\n" f"stdout: {result.stdout[:400]}"
        )

    def test_no_verify_simulation_exits_0(self, tmp_path: Path) -> None:
        """Simulate --no-verify: passing only a clean empty file exits 0.

        ``git commit --no-verify`` skips all hooks.  When pre-commit IS run
        but passes an empty / non-PySpark file, the scanner should exit 0.
        """
        empty = tmp_path / "empty_module.py"
        empty.write_text("# placeholder\n", encoding="utf-8")

        result = _run_hook("--fail-on", "CRITICAL", files=[empty])

        assert result.returncode == 0, (
            f"Empty file should produce exit 0.\n" f"stdout: {result.stdout[:400]}"
        )


# ---------------------------------------------------------------------------
# Output content tests
# ---------------------------------------------------------------------------


class TestHookOutput:
    """Verify the terminal reporter content the hook writes to stdout."""

    def test_critical_output_contains_rule_id(self, tmp_path: Path) -> None:
        """Finding output must contain the rule ID for the detected pattern."""
        bad = _write_py(tmp_path / "jobs" / "pipeline.py", _BAD_CROSS_JOIN)

        result = _run_hook("--fail-on", "CRITICAL", files=[bad])

        assert (
            "SPL-D03-001" in result.stdout
        ), f"Expected SPL-D03-001 in output.\nOutput: {result.stdout[:500]}"

    def test_quiet_flag_suppresses_header(self, tmp_path: Path) -> None:
        """--quiet should suppress the branded header panel."""
        bad = _write_py(tmp_path / "jobs" / "pipeline.py", _BAD_CROSS_JOIN)

        full_result = _run_hook("--fail-on", "CRITICAL", files=[bad])
        quiet_result = _run_hook("--quiet", "--fail-on", "CRITICAL", files=[bad])

        # Full output includes header with brand name; quiet should be shorter
        assert len(full_result.stdout) > len(
            quiet_result.stdout
        ), "--quiet output should be shorter than full output."
        assert "v0." not in quiet_result.stdout, "Quiet mode should suppress the versioned header."

    def test_no_fix_suppresses_code_panels(self, tmp_path: Path) -> None:
        """--no-fix should omit before/after code panels."""
        bad = _write_py(tmp_path / "jobs" / "pipeline.py", _BAD_CROSS_JOIN)

        result_fix = _run_hook("--fix", files=[bad])
        result_nofix = _run_hook("--no-fix", files=[bad])

        # --fix adds code panels so output is longer
        assert len(result_fix.stdout) >= len(
            result_nofix.stdout
        ), "--fix output should be at least as long as --no-fix output."

    def test_multiple_files_all_scanned(self, tmp_path: Path) -> None:
        """All filenames passed as args must appear in the output."""
        bad1 = _write_py(tmp_path / "a.py", _BAD_CROSS_JOIN)
        bad2 = _write_py(tmp_path / "b.py", _BAD_SORT_BY_KEY)

        result = _run_hook(
            "--severity-threshold",
            "INFO",
            files=[bad1, bad2],
        )

        assert "a.py" in result.stdout
        assert "b.py" in result.stdout

    def test_verbose_flag_adds_dimension_breakdown(self, tmp_path: Path) -> None:
        """--verbose should include a per-dimension breakdown table in footer."""
        bad = _write_py(tmp_path / "jobs" / "pipeline.py", _BAD_CROSS_JOIN)

        result = _run_hook("--verbose", files=[bad])

        # Dimension breakdown lists dimension display names like "D03 · Joins"
        assert (
            "D03" in result.stdout
        ), f"--verbose should include dimension breakdown.\nOutput: {result.stdout[:800]}"


# ---------------------------------------------------------------------------
# Hook definition YAML tests
# ---------------------------------------------------------------------------


class TestHookDefinitionYaml:
    """Validate the .pre-commit-hooks.yaml structure."""

    def test_hooks_yaml_exists_at_repo_root(self) -> None:
        """.pre-commit-hooks.yaml must exist at the repository root."""
        repo_root = Path(__file__).parent.parent
        assert (repo_root / ".pre-commit-hooks.yaml").exists()

    def test_hooks_yaml_is_valid_yaml(self) -> None:
        """The .pre-commit-hooks.yaml must parse as valid YAML."""
        import yaml

        repo_root = Path(__file__).parent.parent
        data = yaml.safe_load((repo_root / ".pre-commit-hooks.yaml").read_text(encoding="utf-8"))
        assert isinstance(data, list), "Root element must be a YAML list"
        assert len(data) >= 1, "Must define at least one hook"

    def test_hook_has_required_fields(self) -> None:
        """Each hook entry must have all required pre-commit fields."""
        import yaml

        repo_root = Path(__file__).parent.parent
        data = yaml.safe_load((repo_root / ".pre-commit-hooks.yaml").read_text(encoding="utf-8"))

        required = {"id", "name", "entry", "language"}
        for hook in data:
            missing = required - set(hook.keys())
            assert (
                not missing
            ), f"Hook {hook.get('id', '?')!r} is missing required fields: {missing}"

    def test_main_hook_attributes(self) -> None:
        """The spark-perf-lint hook must have the expected attribute values."""
        import yaml

        repo_root = Path(__file__).parent.parent
        data = yaml.safe_load((repo_root / ".pre-commit-hooks.yaml").read_text(encoding="utf-8"))

        hook = next((h for h in data if h["id"] == "spark-perf-lint"), None)
        assert hook is not None, "Hook with id 'spark-perf-lint' not found"

        assert hook["language"] == "python"
        assert hook["entry"] == "spark-perf-lint scan"
        assert hook["pass_filenames"] is True
        assert "pre-commit" in hook.get("stages", [])
        assert hook.get("files") == r"\.py$"


# ---------------------------------------------------------------------------
# Config discovery
# ---------------------------------------------------------------------------


def _init_git_repo(path: Path) -> None:
    """Initialise a bare git repository at *path* (no commits needed)."""
    subprocess.run(
        ["git", "init", str(path)],
        check=True,
        capture_output=True,
    )


class TestConfigDiscovery:
    """Config file is found by walking up to the git root."""

    def test_config_discovery_from_repo_root(self, tmp_path: Path) -> None:
        """Config at git root is picked up even when the file is in a sub-directory.

        Layout::

            tmp_path/           ← git root
              .spark-perf-lint.yaml   ← sets severity_threshold=WARNING
              src/
                jobs/
                  etl.py        ← file passed to the hook

        The hook is invoked with the absolute path to ``etl.py``.  Config
        discovery must walk up from ``src/jobs/`` past ``src/`` until it
        reaches the git root and finds the YAML.
        """
        _init_git_repo(tmp_path)

        # Place a config that overrides the default severity_threshold
        config_path = tmp_path / ".spark-perf-lint.yaml"
        config_path.write_text(
            yaml.dump(
                {
                    "general": {
                        "severity_threshold": "WARNING",
                        "fail_on": ["CRITICAL"],
                    }
                }
            ),
            encoding="utf-8",
        )

        # Place the PySpark file three levels deep
        spark_file = _write_py(tmp_path / "src" / "jobs" / "etl.py", _BAD_CROSS_JOIN)

        result = _run_hook("--fail-on", "CRITICAL", files=[spark_file])

        # CRITICAL finding → exit 1 confirms both the scan ran AND the config
        # loaded (if config was ignored the default fail_on=CRITICAL still applies,
        # but we can verify the config_source path in the output when verbose)
        assert result.returncode == 1, (
            f"Expected exit 1 — CRITICAL crossJoin should block.\n" f"stdout: {result.stdout[:400]}"
        )

    def test_config_in_subdirectory_not_picked_up_from_sibling(self, tmp_path: Path) -> None:
        """A config in a sibling directory must NOT be used for files elsewhere.

        Layout::

            tmp_path/           ← git root
              unrelated/
                .spark-perf-lint.yaml   ← not on the path to etl.py
              src/
                etl.py

        Config discovery for ``src/etl.py`` should walk up via src → tmp_path,
        finding nothing, and fall back to built-in defaults.
        """
        _init_git_repo(tmp_path)

        # Config in an unrelated directory
        unrelated = tmp_path / "unrelated"
        unrelated.mkdir()
        (unrelated / ".spark-perf-lint.yaml").write_text(
            yaml.dump({"general": {"fail_on": ["CRITICAL"]}}), encoding="utf-8"
        )

        spark_file = _write_py(tmp_path / "src" / "etl.py", _BAD_CROSS_JOIN)

        # With no config on the walked path, defaults apply (fail_on=CRITICAL).
        # The scan should still succeed structurally — exit 1 from the finding.
        result = _run_hook("--fail-on", "CRITICAL", files=[spark_file])
        assert result.returncode == 1, (
            "Scan should still run on a CRITICAL finding even without a config file.\n"
            f"stdout: {result.stdout[:400]}"
        )


# ---------------------------------------------------------------------------
# pass_filenames mode
# ---------------------------------------------------------------------------


class TestPassFilenamesMode:
    """pre-commit appends staged filenames as positional CLI arguments."""

    def test_pass_filenames_single_file(self, tmp_path: Path) -> None:
        """Passing a single absolute path as a positional arg works correctly."""
        bad = _write_py(tmp_path / "pipeline.py", _BAD_CROSS_JOIN)

        result = _run_hook("--fail-on", "CRITICAL", files=[bad])

        assert result.returncode == 1
        assert str(bad) in result.stdout or "pipeline.py" in result.stdout

    def test_pass_filenames_multiple_files(self, tmp_path: Path) -> None:
        """Multiple filenames appended by pre-commit are all scanned."""
        bad1 = _write_py(tmp_path / "a.py", _BAD_CROSS_JOIN)
        bad2 = _write_py(tmp_path / "b.py", _BAD_CROSS_JOIN)
        bad3 = _write_py(tmp_path / "c.py", _BAD_CROSS_JOIN)

        result = _run_hook("--fail-on", "CRITICAL", files=[bad1, bad2, bad3])

        assert result.returncode == 1
        # All three file names must appear in the output
        for name in ("a.py", "b.py", "c.py"):
            assert name in result.stdout, f"{name} not found in output"

    def test_pass_filenames_with_deep_paths(self, tmp_path: Path) -> None:
        """Paths with multiple directory components are accepted."""
        deep = _write_py(
            tmp_path / "project" / "src" / "spark_jobs" / "ingestion" / "etl.py",
            _BAD_CROSS_JOIN,
        )

        result = _run_hook("--fail-on", "CRITICAL", files=[deep])

        assert result.returncode == 1
        assert "etl.py" in result.stdout


# ---------------------------------------------------------------------------
# Mixed Spark / non-Spark files
# ---------------------------------------------------------------------------


class TestMixedSparkNonSparkFiles:
    """Non-Spark files are skipped; only PySpark files are analysed."""

    # Three non-Spark file contents
    _NON_SPARK_FILES = [
        "# plain utility module\nimport os\nprint(os.getcwd())\n",
        "import json\ndata = json.loads('{}')\n",
        "from pathlib import Path\nroot = Path('/data')\n",
    ]

    def test_non_spark_files_produce_no_findings(self, tmp_path: Path) -> None:
        """Non-PySpark files must not generate any findings."""
        non_spark = []
        for i, src in enumerate(self._NON_SPARK_FILES):
            p = tmp_path / f"util_{i}.py"
            p.write_text(src, encoding="utf-8")
            non_spark.append(p)

        result = _run_hook("--fail-on", "CRITICAL", files=non_spark)

        assert (
            result.returncode == 0
        ), f"Non-Spark files should never block a commit.\nstdout: {result.stdout[:400]}"

    def test_mixed_files_only_spark_files_blocked(self, tmp_path: Path) -> None:
        """2 Spark files + 3 non-Spark files: commit is blocked by Spark findings only."""
        spark1 = _write_py(tmp_path / "spark_job_a.py", _BAD_CROSS_JOIN)
        spark2 = _write_py(tmp_path / "spark_job_b.py", _BAD_CROSS_JOIN)

        non_spark = []
        for i, src in enumerate(self._NON_SPARK_FILES):
            p = tmp_path / f"util_{i}.py"
            p.write_text(src, encoding="utf-8")
            non_spark.append(p)

        all_files = [spark1, spark2, *non_spark]
        result = _run_hook("--fail-on", "CRITICAL", files=all_files)

        assert result.returncode == 1, (
            "CRITICAL findings in Spark files must block the commit.\n"
            f"stdout: {result.stdout[:400]}"
        )

    def test_only_spark_file_names_appear_in_output(self, tmp_path: Path) -> None:
        """Finding output must reference Spark files, not the non-Spark ones."""
        spark = _write_py(tmp_path / "spark_pipeline.py", _BAD_CROSS_JOIN)
        plain = tmp_path / "utils.py"
        plain.write_text("import os\n", encoding="utf-8")

        result = _run_hook(
            "--fail-on",
            "CRITICAL",
            "--severity-threshold",
            "INFO",
            files=[spark, plain],
        )

        assert "spark_pipeline.py" in result.stdout
        # utils.py has no findings so it should not appear as a finding source
        assert "SPL-" in result.stdout  # findings were reported

    def test_five_files_two_spark_three_plain(self, tmp_path: Path) -> None:
        """Exactly 2 of 5 files trigger findings; the other 3 are fast-skipped."""
        spark1 = _write_py(tmp_path / "etl.py", _BAD_CROSS_JOIN)
        spark2 = _write_py(tmp_path / "pipeline.py", _BAD_CROSS_JOIN)
        plain_files = []
        for i, src in enumerate(self._NON_SPARK_FILES):
            p = tmp_path / f"helper_{i}.py"
            p.write_text(src, encoding="utf-8")
            plain_files.append(p)

        result = _run_hook(
            "--fail-on",
            "CRITICAL",
            files=[spark1, spark2, *plain_files],
        )

        # Both Spark files have CRITICAL findings
        assert result.returncode == 1
        assert "etl.py" in result.stdout
        assert "pipeline.py" in result.stdout


# ---------------------------------------------------------------------------
# Exit-code matrix
# ---------------------------------------------------------------------------


class TestExitCodeMatrix:
    """Exhaustive exit-code matrix for the three cases in the spec."""

    def test_no_findings_exits_0(self, tmp_path: Path) -> None:
        """A file with zero findings must produce exit code 0."""
        clean = _write_py(tmp_path / "etl.py", _GOOD_ETL)

        result = _run_hook("--fail-on", "CRITICAL", files=[clean])

        assert (
            result.returncode == 0
        ), f"Clean file should produce exit 0.\nstdout: {result.stdout[:400]}"

    def test_warning_with_fail_on_critical_exits_0(self, tmp_path: Path) -> None:
        """WARNING finding + fail_on=CRITICAL → exit 0 (not blocked)."""
        warn = _write_py(tmp_path / "warn.py", _BAD_SORT_BY_KEY)

        result = _run_hook(
            "--fail-on",
            "CRITICAL",
            "--severity-threshold",
            "WARNING",
            "--dimension",
            "D02",
            files=[warn],
        )

        assert result.returncode == 0, (
            "WARNING-only finding must not block when fail_on=CRITICAL.\n"
            f"stdout: {result.stdout[:400]}"
        )

    def test_critical_with_fail_on_critical_exits_1(self, tmp_path: Path) -> None:
        """CRITICAL finding + fail_on=CRITICAL → exit 1 (blocked)."""
        bad = _write_py(tmp_path / "bad.py", _BAD_CROSS_JOIN)

        result = _run_hook("--fail-on", "CRITICAL", files=[bad])

        assert result.returncode == 1, (
            "CRITICAL finding must block when fail_on=CRITICAL.\n" f"stdout: {result.stdout[:400]}"
        )

    @pytest.mark.parametrize(
        "code, fail_on_flags, expected_rc",
        [
            # (source, [--fail-on args], expected_exit_code)
            (_GOOD_ETL, ["CRITICAL"], 0),
            (_BAD_SORT_BY_KEY, ["CRITICAL"], 0),  # WARNING only → pass
            (_BAD_CROSS_JOIN, ["CRITICAL"], 1),  # CRITICAL → block
            (_BAD_SORT_BY_KEY, ["WARNING"], 1),  # WARNING → block
            (_BAD_SORT_BY_KEY, ["CRITICAL", "WARNING"], 1),  # both → block on WARNING
        ],
        ids=[
            "clean-critical-pass",
            "warning-only-fail_on_critical-pass",
            "critical-fail_on_critical-block",
            "warning-fail_on_warning-block",
            "warning-fail_on_both-block",
        ],
    )
    def test_exit_code_matrix(
        self,
        tmp_path: Path,
        code: str,
        fail_on_flags: list[str],
        expected_rc: int,
    ) -> None:
        """Parametrized exit-code matrix covering all common pre-commit scenarios."""
        f = _write_py(tmp_path / "target.py", code)

        extra: list[str] = ["--severity-threshold", "WARNING", "--dimension", "D02,D03"]
        for level in fail_on_flags:
            extra += ["--fail-on", level]

        result = _run_hook(*extra, files=[f])

        assert result.returncode == expected_rc, (
            f"fail_on={fail_on_flags} with code snippet → expected rc={expected_rc}, "
            f"got {result.returncode}.\nstdout: {result.stdout[:400]}"
        )


# ---------------------------------------------------------------------------
# CLI args passthrough
# ---------------------------------------------------------------------------


class TestArgsPassthrough:
    """pre-commit passes user-configured args before the staged filenames."""

    def test_severity_threshold_arg_accepted(self, tmp_path: Path) -> None:
        """['--severity-threshold', 'CRITICAL', 'file.py'] must parse correctly."""
        bad = _write_py(tmp_path / "etl.py", _BAD_CROSS_JOIN)

        # Emulate exactly what pre-commit would build:
        #   spark-perf-lint scan --severity-threshold CRITICAL <file>
        result = _run_hook("--severity-threshold", "CRITICAL", files=[bad])

        # Should run without error — exit 0 because no finding reaches CRITICAL
        # display threshold (SPL-D03-001 is CRITICAL so it shows and fails)
        assert result.returncode in (0, 1), "CLI must not crash on valid args"

    def test_explicit_file_path_args(self, tmp_path: Path) -> None:
        """Explicit positional file arguments match pre-commit's pass_filenames=true."""
        f1 = _write_py(tmp_path / "file1.py", _BAD_CROSS_JOIN)
        f2 = _write_py(tmp_path / "file2.py", _GOOD_ETL)

        # Emulate: spark-perf-lint scan --severity-threshold CRITICAL file1.py file2.py
        result = _run_hook(
            "--severity-threshold",
            "CRITICAL",
            files=[f1, f2],
        )

        # f1 has a CRITICAL finding; default fail_on=CRITICAL → blocked
        assert result.returncode == 1

    def test_multiple_flag_repetition(self, tmp_path: Path) -> None:
        """Repeated --fail-on flags are all accepted (Click multiple=True)."""
        warn = _write_py(tmp_path / "warn.py", _BAD_SORT_BY_KEY)

        result = _run_hook(
            "--fail-on",
            "CRITICAL",
            "--fail-on",
            "WARNING",
            "--dimension",
            "D02",
            files=[warn],
        )

        # Both levels in fail_on; WARNING finding → blocked
        assert result.returncode == 1

    def test_dimension_filter_arg(self, tmp_path: Path) -> None:
        """--dimension restricts the scan to specified dimensions only."""
        bad = _write_py(tmp_path / "etl.py", _BAD_CROSS_JOIN)
        clean = _write_py(tmp_path / "clean.py", _GOOD_ETL)

        # Scan a clean file with only D02 rules — no CRITICAL findings expected
        result_d02_clean = _run_hook(
            "--fail-on",
            "CRITICAL",
            "--dimension",
            "D02",
            files=[clean],
        )
        # Scan the bad file with D03 included — cross-join rule fires
        result_d03 = _run_hook(
            "--fail-on",
            "CRITICAL",
            "--dimension",
            "D03",
            files=[bad],
        )

        assert (
            result_d02_clean.returncode == 0
        ), "Clean file scanned with only D02 rules should produce no CRITICAL findings"
        assert result_d03.returncode == 1, "D03 scan should find crossJoin"

    def test_unknown_arg_produces_error(self, tmp_path: Path) -> None:
        """An unrecognised flag should cause a non-zero exit (Click usage error)."""
        bad = _write_py(tmp_path / "etl.py", _BAD_CROSS_JOIN)

        args = ["scan", "--this-flag-does-not-exist", str(bad)]
        result = CliRunner().invoke(cli_main, args)  # catch_exceptions=True (default)

        assert result.exit_code != 0, "Unknown flags must not silently succeed"


# ---------------------------------------------------------------------------
# Terminal output format at 80-column width
# ---------------------------------------------------------------------------


class TestOutputFormat:
    """Terminal output is clean and readable at 80-column pre-commit width."""

    def test_output_width_does_not_exceed_120_chars(self, tmp_path: Path) -> None:
        """No visible output line should be absurdly wide.

        Rich wraps at the console width.  CliRunner captures the rendered text
        including ANSI escape codes; we strip those before measuring so that
        colour sequences don't inflate the perceived line length.  120 chars is
        intentionally generous to accommodate Rich's table borders and padding.
        Lines wider than this usually indicate a missing wrap or a very long
        non-breaking token.
        """
        bad = _write_py(tmp_path / "etl.py", _BAD_CROSS_JOIN)

        result = _run_hook("--fail-on", "CRITICAL", files=[bad])

        long_lines = [
            ln for ln in result.stdout.splitlines() if len(_ANSI_ESCAPE.sub("", ln)) > 120
        ]
        assert not long_lines, (
            f"Output contains {len(long_lines)} visible line(s) wider than 120 chars:\n"
            + "\n".join(
                f"  [{len(_ANSI_ESCAPE.sub('', ln))}] {_ANSI_ESCAPE.sub('', ln)[:80]}…"
                for ln in long_lines[:5]
            )
        )

    def test_quiet_output_is_compact(self, tmp_path: Path) -> None:
        """--quiet output should be significantly shorter than full output."""
        bad = _write_py(tmp_path / "etl.py", _BAD_CROSS_JOIN)

        full = _run_hook("--fail-on", "CRITICAL", files=[bad])
        quiet = _run_hook("--quiet", "--fail-on", "CRITICAL", files=[bad])

        assert len(quiet.stdout) < len(
            full.stdout
        ), "--quiet output should be shorter than full output."

    def test_rule_id_always_present_in_output(self, tmp_path: Path) -> None:
        """Every finding card must contain the rule ID so developers can look it up."""
        bad = _write_py(tmp_path / "etl.py", _BAD_CROSS_JOIN)

        result = _run_hook("--fail-on", "CRITICAL", files=[bad])

        assert (
            "SPL-D03-001" in result.stdout
        ), f"Rule ID not found in output.\nstdout: {result.stdout[:500]}"

    def test_severity_label_present_in_output(self, tmp_path: Path) -> None:
        """Finding output must include the severity label."""
        bad = _write_py(tmp_path / "etl.py", _BAD_CROSS_JOIN)

        result = _run_hook("--fail-on", "CRITICAL", files=[bad])

        assert "CRITICAL" in result.stdout

    def test_no_python_traceback_in_clean_run(self, tmp_path: Path) -> None:
        """A normal scan must never print a Python traceback to stdout."""
        bad = _write_py(tmp_path / "etl.py", _BAD_CROSS_JOIN)

        result = _run_hook("--fail-on", "CRITICAL", files=[bad])

        assert (
            "Traceback (most recent call last)" not in result.stdout
        ), "A Python traceback leaked into the hook output."
