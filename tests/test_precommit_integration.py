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

import textwrap
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from click.testing import CliRunner

from spark_perf_lint.cli import main as cli_main


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
            f"Expected exit 1 (blocked) for CRITICAL finding.\n"
            f"stdout: {result.stdout[:400]}"
        )

    def test_clean_file_exits_0(self, tmp_path: Path) -> None:
        """A file with no CRITICAL findings must allow the commit."""
        good = _write_py(tmp_path / "jobs" / "clean_etl.py", _GOOD_ETL)

        result = _run_hook("--fail-on", "CRITICAL", files=[good])

        assert result.returncode == 0, (
            f"Expected exit 0 (allowed) for clean file.\n"
            f"stdout: {result.stdout[:400]}"
        )

    def test_warning_does_not_block_when_fail_on_critical(
        self, tmp_path: Path
    ) -> None:
        """A WARNING finding should not block when fail_on=[CRITICAL].

        Scopes the scan to --dimension D02 so only shuffle rules run.
        SPL-D02-003 (sortByKey without partitionBy) fires at WARNING severity.
        """
        warn_file = _write_py(tmp_path / "jobs" / "warn.py", _BAD_SORT_BY_KEY)

        result = _run_hook(
            "--fail-on", "CRITICAL",
            "--severity-threshold", "WARNING",
            "--dimension", "D02",
            files=[warn_file],
        )

        assert result.returncode == 0, (
            f"WARNING finding should not block with fail_on=CRITICAL.\n"
            f"stdout: {result.stdout[:400]}"
        )

    def test_warning_blocks_when_fail_on_includes_warning(
        self, tmp_path: Path
    ) -> None:
        """A WARNING finding should block when fail_on includes WARNING."""
        warn_file = _write_py(tmp_path / "jobs" / "warn.py", _BAD_SORT_BY_KEY)

        result = _run_hook(
            "--fail-on", "CRITICAL",
            "--fail-on", "WARNING",
            "--dimension", "D02",
            files=[warn_file],
        )

        assert result.returncode == 1, (
            f"WARNING finding should block with fail_on=WARNING.\n"
            f"stdout: {result.stdout[:400]}"
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
            f"Empty file should produce exit 0.\n"
            f"stdout: {result.stdout[:400]}"
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

        assert "SPL-D03-001" in result.stdout, (
            f"Expected SPL-D03-001 in output.\nOutput: {result.stdout[:500]}"
        )

    def test_quiet_flag_suppresses_header(self, tmp_path: Path) -> None:
        """--quiet should suppress the branded header panel."""
        bad = _write_py(tmp_path / "jobs" / "pipeline.py", _BAD_CROSS_JOIN)

        full_result = _run_hook("--fail-on", "CRITICAL", files=[bad])
        quiet_result = _run_hook(
            "--quiet", "--fail-on", "CRITICAL", files=[bad]
        )

        # Full output includes header with brand name; quiet should be shorter
        assert len(full_result.stdout) > len(quiet_result.stdout), (
            "--quiet output should be shorter than full output."
        )
        assert "v0." not in quiet_result.stdout, (
            "Quiet mode should suppress the versioned header."
        )

    def test_no_fix_suppresses_code_panels(self, tmp_path: Path) -> None:
        """--no-fix should omit before/after code panels."""
        bad = _write_py(tmp_path / "jobs" / "pipeline.py", _BAD_CROSS_JOIN)

        result_fix = _run_hook("--fix", files=[bad])
        result_nofix = _run_hook("--no-fix", files=[bad])

        # --fix adds code panels so output is longer
        assert len(result_fix.stdout) >= len(result_nofix.stdout), (
            "--fix output should be at least as long as --no-fix output."
        )

    def test_multiple_files_all_scanned(self, tmp_path: Path) -> None:
        """All filenames passed as args must appear in the output."""
        bad1 = _write_py(tmp_path / "a.py", _BAD_CROSS_JOIN)
        bad2 = _write_py(tmp_path / "b.py", _BAD_SORT_BY_KEY)

        result = _run_hook(
            "--severity-threshold", "INFO",
            files=[bad1, bad2],
        )

        assert "a.py" in result.stdout
        assert "b.py" in result.stdout

    def test_verbose_flag_adds_dimension_breakdown(self, tmp_path: Path) -> None:
        """--verbose should include a per-dimension breakdown table in footer."""
        bad = _write_py(tmp_path / "jobs" / "pipeline.py", _BAD_CROSS_JOIN)

        result = _run_hook("--verbose", files=[bad])

        # Dimension breakdown lists dimension display names like "D03 · Joins"
        assert "D03" in result.stdout, (
            f"--verbose should include dimension breakdown.\nOutput: {result.stdout[:800]}"
        )


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
        data = yaml.safe_load(
            (repo_root / ".pre-commit-hooks.yaml").read_text(encoding="utf-8")
        )
        assert isinstance(data, list), "Root element must be a YAML list"
        assert len(data) >= 1, "Must define at least one hook"

    def test_hook_has_required_fields(self) -> None:
        """Each hook entry must have all required pre-commit fields."""
        import yaml

        repo_root = Path(__file__).parent.parent
        data = yaml.safe_load(
            (repo_root / ".pre-commit-hooks.yaml").read_text(encoding="utf-8")
        )

        required = {"id", "name", "entry", "language"}
        for hook in data:
            missing = required - set(hook.keys())
            assert not missing, (
                f"Hook {hook.get('id', '?')!r} is missing required fields: {missing}"
            )

    def test_main_hook_attributes(self) -> None:
        """The spark-perf-lint hook must have the expected attribute values."""
        import yaml

        repo_root = Path(__file__).parent.parent
        data = yaml.safe_load(
            (repo_root / ".pre-commit-hooks.yaml").read_text(encoding="utf-8")
        )

        hook = next((h for h in data if h["id"] == "spark-perf-lint"), None)
        assert hook is not None, "Hook with id 'spark-perf-lint' not found"

        assert hook["language"] == "python"
        assert hook["entry"] == "spark-perf-lint scan"
        assert hook["pass_filenames"] is True
        assert "pre-commit" in hook.get("stages", [])
        assert hook.get("files") == r"\.py$"
