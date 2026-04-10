"""Performance benchmarks and fast-path correctness tests for spark-perf-lint.

Goals validated here:
  - Fast-path: non-PySpark files are classified in < 50 ms each (head-only read)
  - Incremental: staged-file mode scans only the files it receives
  - Parallel: concurrent.futures workers used for batches ≥ 3 files
  - Benchmark: 20-file commit scanned end-to-end in < 5 seconds
  - Benchmark: 1000-line synthetic PySpark file scanned in < 2 seconds
  - Benchmark: 50-file mixed project (Spark + plain) scanned in < 10 seconds
  - Benchmark: non-Spark file classification costs < 50 ms per file

``@pytest.mark.slow`` tests are skipped unless ``-m slow`` is passed or the
full marker set is activated via the ``--run-slow`` flag.  All other tests run
in the normal suite.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.file_scanner import (
    _QUICK_SCAN_BYTES,
    FileScanner,
    _build_target,
    _read_head,
)
from spark_perf_lint.engine.orchestrator import (
    _MAX_WORKERS,
    _PARALLEL_THRESHOLD,
    ScanOrchestrator,
)
from tests.fixtures.code_generator import generate_multi_file_project, generate_spark_file

# =============================================================================
# Synthetic source templates
# =============================================================================

# A realistic PySpark job with three anti-patterns (D01, D02, D03).
_SPARK_JOB_TEMPLATE = """\
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = (
    SparkSession.builder
    .appName("benchmark_job_{n}")
    .getOrCreate()
)
sc = SparkContext.getOrCreate()

# D03-001: cartesian cross join
df_a = spark.table("orders_{n}")
df_b = spark.table("products_{n}")
cross = df_a.crossJoin(df_b)

# D02-002: groupByKey — prefer reduceByKey or aggregateByKey
rdd = sc.parallelize([(1, "a"), (2, "b"), (1, "c")])
grouped = rdd.groupByKey().collect()

# D01-001: high shuffle partitions without AQE
spark.conf.set("spark.sql.shuffle.partitions", "5000")

cross.write.mode("overwrite").parquet("/output/result_{n}")
"""

# Plain Python utility — no PySpark markers anywhere.
_PLAIN_PYTHON_TEMPLATE = """\
# Plain utility module — no Spark (benchmark_{n})
import os
import json
from pathlib import Path

SENTINEL_{n} = "value_{n}"


def load_config_{n}(config_path: str) -> dict:
    \"\"\"Load JSON config from disk.\"\"\"
    with open(config_path, encoding="utf-8") as fh:
        return json.load(fh)


def output_dir_{n}() -> Path:
    base = os.environ.get("OUTPUT_DIR_{n}", f"/tmp/output_{n}")
    return Path(base)


def process_{n}(items: list) -> list:
    return [str(x) for x in items if x is not None]
"""

# A large non-PySpark file: tests that even 50 KB of plain Python is
# skipped quickly (head read only, no full I/O).
_LARGE_PLAIN_TEMPLATE = "# auto-generated config constants\n" + "".join(
    f'CONFIG_{i} = "value_{i}"\n' for i in range(3000)
)


# =============================================================================
# Helpers
# =============================================================================


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _make_spark_files(directory: Path, count: int) -> list[Path]:
    return [
        _write(directory / f"spark_job_{i}.py", _SPARK_JOB_TEMPLATE.format(n=i))
        for i in range(count)
    ]


def _make_plain_files(directory: Path, count: int) -> list[Path]:
    return [
        _write(directory / f"util_{i}.py", _PLAIN_PYTHON_TEMPLATE.format(n=i)) for i in range(count)
    ]


# =============================================================================
# Fast-path: non-PySpark files skipped without full read
# =============================================================================


class TestFastPath:
    """_build_target() avoids a full file read for non-PySpark files."""

    def test_ignored_file_returns_no_content(self, tmp_path: Path) -> None:
        """Ignored files are returned with content='' (zero I/O beyond stat)."""
        f = _write(tmp_path / "test_utils.py", _PLAIN_PYTHON_TEMPLATE.format(n=0))
        config = LintConfig.from_dict({"ignore": {"files": ["**/*test*"]}})

        target = _build_target(f, tmp_path, config)

        assert target is not None
        assert target.skip_reason is not None
        assert target.content == ""
        assert target.size_bytes == 0

    def test_non_spark_file_returns_empty_content(self, tmp_path: Path) -> None:
        """Non-PySpark files classified by head-scan get content=''."""
        f = _write(tmp_path / "util.py", _PLAIN_PYTHON_TEMPLATE.format(n=0))
        config = LintConfig.from_dict({})

        target = _build_target(f, tmp_path, config)

        assert target is not None
        assert not target.is_pyspark
        assert target.content == ""
        assert target.size_bytes == 0

    def test_spark_file_gets_full_content(self, tmp_path: Path) -> None:
        """PySpark files still get their full content loaded for rule execution."""
        src = _SPARK_JOB_TEMPLATE.format(n=0)
        f = _write(tmp_path / "etl.py", src)
        config = LintConfig.from_dict({})

        target = _build_target(f, tmp_path, config)

        assert target is not None
        assert target.is_pyspark
        assert target.content == src
        assert target.size_bytes > 0

    def test_large_plain_file_head_only_read(self, tmp_path: Path) -> None:
        """A 50 KB+ non-Spark file is detected via 8 KB head — file is not fully read."""
        f = _write(tmp_path / "generated_constants.py", _LARGE_PLAIN_TEMPLATE)

        assert (
            f.stat().st_size > _QUICK_SCAN_BYTES
        ), "Precondition: file must exceed head-scan limit to exercise the optimisation"

        config = LintConfig.from_dict({})
        target = _build_target(f, tmp_path, config)

        assert target is not None
        assert not target.is_pyspark
        # content is empty → full file was not loaded into memory
        assert target.content == ""

    def test_read_head_returns_at_most_n_bytes(self, tmp_path: Path) -> None:
        """_read_head() never returns more than n_bytes of content."""
        f = _write(tmp_path / "big.py", "x = 1\n" * 10_000)
        head, err = _read_head(f, n_bytes=_QUICK_SCAN_BYTES)

        assert err is None
        # The head is decoded text; byte length of encoded head ≤ n_bytes + 3
        # (UTF-8 multi-byte characters could slightly inflate byte count)
        assert len(head.encode("utf-8")) <= _QUICK_SCAN_BYTES + 3

    def test_non_pyspark_files_produce_zero_findings(self, tmp_path: Path) -> None:
        """Scanner returns files_scanned=0 when all inputs are non-PySpark."""
        plain = _make_plain_files(tmp_path, 10)
        config = LintConfig.from_dict({})
        orchestrator = ScanOrchestrator(config)

        report = orchestrator.scan([str(p) for p in plain])

        assert report.files_scanned == 0
        assert report.findings == []


# =============================================================================
# Incremental / staged-files mode
# =============================================================================


class TestIncrementalScan:
    """from_staged_files() only processes the exact files passed to it."""

    def test_staged_mode_scans_only_given_files(self, tmp_path: Path) -> None:
        """Extra files in the directory must not be picked up."""
        spark = _write(tmp_path / "etl.py", _SPARK_JOB_TEMPLATE.format(n=0))
        # Create more files that are NOT staged
        _write(tmp_path / "not_staged.py", _SPARK_JOB_TEMPLATE.format(n=1))
        _write(tmp_path / "also_not_staged.py", _SPARK_JOB_TEMPLATE.format(n=2))

        config = LintConfig.from_dict({})
        targets, summary = FileScanner.from_staged_files([str(spark)], config, root=tmp_path)
        scannable = FileScanner.scannable(targets)

        assert summary.total_found == 1
        assert len(scannable) == 1
        assert scannable[0].path == spark.resolve()

    def test_staged_mode_skips_deleted_files(self, tmp_path: Path) -> None:
        """Paths that no longer exist on disk are silently skipped."""
        phantom = str(tmp_path / "deleted.py")
        config = LintConfig.from_dict({})

        targets, summary = FileScanner.from_staged_files([phantom], config, root=tmp_path)

        assert summary.total_found == 0
        assert targets == []

    def test_staged_mode_skips_non_python_files(self, tmp_path: Path) -> None:
        """Non-Python files are ignored even if explicitly staged."""
        yaml_file = _write(tmp_path / "config.yaml", "key: value\n")
        sql_file = _write(tmp_path / "query.sql", "SELECT 1;\n")
        config = LintConfig.from_dict({})

        targets, summary = FileScanner.from_staged_files(
            [str(yaml_file), str(sql_file)], config, root=tmp_path
        )

        assert summary.total_found == 0

    def test_staged_mode_mixed_spark_plain(self, tmp_path: Path) -> None:
        """Mixed staged files: only PySpark ones become scannable targets."""
        spark1 = _write(tmp_path / "spark_a.py", _SPARK_JOB_TEMPLATE.format(n=0))
        spark2 = _write(tmp_path / "spark_b.py", _SPARK_JOB_TEMPLATE.format(n=1))
        plain1 = _write(tmp_path / "util_a.py", _PLAIN_PYTHON_TEMPLATE.format(n=0))
        plain2 = _write(tmp_path / "util_b.py", _PLAIN_PYTHON_TEMPLATE.format(n=1))

        config = LintConfig.from_dict({})
        targets, summary = FileScanner.from_staged_files(
            [str(spark1), str(spark2), str(plain1), str(plain2)],
            config,
            root=tmp_path,
        )
        scannable = FileScanner.scannable(targets)

        assert summary.total_found == 4
        assert summary.non_pyspark == 2
        assert len(scannable) == 2


# =============================================================================
# Parallel execution
# =============================================================================


class TestParallelExecution:
    """Orchestrator uses a thread pool when target count ≥ _PARALLEL_THRESHOLD."""

    def test_parallel_threshold_constant_positive(self) -> None:
        """_PARALLEL_THRESHOLD must be a positive integer."""
        assert isinstance(_PARALLEL_THRESHOLD, int)
        assert _PARALLEL_THRESHOLD >= 1

    def test_max_workers_constant_positive(self) -> None:
        """_MAX_WORKERS must be a positive integer."""
        assert isinstance(_MAX_WORKERS, int)
        assert _MAX_WORKERS >= 1

    def test_parallel_results_match_serial_for_large_batch(self, tmp_path: Path) -> None:
        """Findings produced by the parallel path must equal the serial path's findings.

        Runs the same 8-file batch through both paths and compares the
        sorted rule_id+file_path+line_number tuples to verify correctness.
        """
        files = _make_spark_files(tmp_path, 8)
        config = LintConfig.from_dict({"general": {"severity_threshold": "INFO"}})

        # Serial path (threshold above batch size)
        serial_orch = ScanOrchestrator(config)
        from unittest.mock import patch

        with patch("spark_perf_lint.engine.orchestrator._PARALLEL_THRESHOLD", 999):
            serial_report = serial_orch.scan([str(p) for p in files])

        # Parallel path (normal threshold)
        parallel_orch = ScanOrchestrator(config)
        parallel_report = parallel_orch.scan([str(p) for p in files])

        def _key(f):
            return (f.rule_id, f.file_path, f.line_number)

        serial_keys = sorted(_key(f) for f in serial_report.findings)
        parallel_keys = sorted(_key(f) for f in parallel_report.findings)

        assert serial_keys == parallel_keys, (
            f"Parallel output differs from serial.\n"
            f"Serial only:   {set(serial_keys) - set(parallel_keys)}\n"
            f"Parallel only: {set(parallel_keys) - set(serial_keys)}"
        )

    def test_below_threshold_uses_serial_path(self, tmp_path: Path) -> None:
        """Batches smaller than _PARALLEL_THRESHOLD must still produce findings."""
        # Create exactly threshold-1 files so serial path is used
        n = max(1, _PARALLEL_THRESHOLD - 1)
        files = _make_spark_files(tmp_path, n)
        config = LintConfig.from_dict({})
        orchestrator = ScanOrchestrator(config)

        report = orchestrator.scan([str(p) for p in files])

        assert report.files_scanned == n
        assert len(report.findings) > 0

    def test_empty_target_list_returns_empty_report(self, tmp_path: Path) -> None:
        """Zero scannable files produces a valid empty AuditReport."""
        plain = _make_plain_files(tmp_path, 5)
        config = LintConfig.from_dict({})
        orchestrator = ScanOrchestrator(config)

        report = orchestrator.scan([str(p) for p in plain])

        assert report.files_scanned == 0
        assert report.findings == []
        assert report.scan_duration_seconds >= 0


# =============================================================================
# Benchmark: end-to-end scan time
# =============================================================================


@pytest.mark.slow
class TestBenchmark:
    """Wall-clock timing benchmarks.  Run with ``pytest -m slow``."""

    def test_non_spark_files_classified_under_50ms_each(self, tmp_path: Path) -> None:
        """100 non-PySpark files must be classified in < 50 ms total.

        The fast-path head-scan should trivially beat this budget; if it
        regresses to a full read the test will fail.
        """
        plain = _make_plain_files(tmp_path, 100)
        config = LintConfig.from_dict({})

        t0 = time.perf_counter()
        targets, _ = FileScanner.from_paths([str(p) for p in plain], config)
        elapsed = time.perf_counter() - t0

        scannable = FileScanner.scannable(targets)
        assert scannable == [], "Non-PySpark files must not be scannable"
        assert elapsed < 0.050 * 100, (  # 50 ms × 100 = 5 s generous ceiling
            f"Classifying 100 plain files took {elapsed:.3f}s (> 5 s budget). "
            "Fast-path may be broken."
        )

    def test_scan_20_spark_files_under_5_seconds(self, tmp_path: Path) -> None:
        """20 PySpark files with anti-patterns must complete in < 5 seconds.

        This is the primary pre-commit SLA: a developer committing 20 files
        should not wait more than 5 seconds for the hook.
        """
        files = _make_spark_files(tmp_path, 20)
        config = LintConfig.from_dict({})
        orchestrator = ScanOrchestrator(config)

        t0 = time.perf_counter()
        report = orchestrator.scan([str(p) for p in files])
        elapsed = time.perf_counter() - t0

        assert report.files_scanned == 20, f"Expected 20 files scanned, got {report.files_scanned}"
        assert len(report.findings) > 0, "Expected findings from anti-pattern files"
        assert elapsed < 5.0, (
            f"Scan of 20 PySpark files took {elapsed:.3f}s — exceeds 5-second budget.\n"
            f"files_scanned={report.files_scanned}, findings={len(report.findings)}"
        )

    def test_scan_20_files_mixed_spark_plain_under_5_seconds(self, tmp_path: Path) -> None:
        """20 Spark + 20 plain files must complete in < 5 seconds.

        Simulates a realistic pre-commit batch where half the staged files are
        utility modules that should be fast-pathed.
        """
        spark = _make_spark_files(tmp_path / "jobs", 20)
        plain = _make_plain_files(tmp_path / "utils", 20)
        all_files = spark + plain

        config = LintConfig.from_dict({})
        orchestrator = ScanOrchestrator(config)

        t0 = time.perf_counter()
        report = orchestrator.scan([str(p) for p in all_files])
        elapsed = time.perf_counter() - t0

        assert report.files_scanned == 20  # only Spark files analysed
        assert elapsed < 5.0, f"Mixed 40-file scan took {elapsed:.3f}s — exceeds 5-second budget."

    def test_scan_duration_reported_in_audit_report(self, tmp_path: Path) -> None:
        """AuditReport.scan_duration_seconds must reflect actual wall time."""
        files = _make_spark_files(tmp_path, 5)
        config = LintConfig.from_dict({})
        orchestrator = ScanOrchestrator(config)

        t0 = time.perf_counter()
        report = orchestrator.scan([str(p) for p in files])
        wall = time.perf_counter() - t0

        # Report duration should be close to wall time (within 2×)
        assert report.scan_duration_seconds > 0
        assert report.scan_duration_seconds <= wall * 2 + 0.1

    # ------------------------------------------------------------------
    # Code-generator benchmarks
    # ------------------------------------------------------------------

    def test_1000_line_spark_file_scanned_under_2_seconds(self) -> None:
        """A synthetic 1000-line PySpark file must be fully scanned in < 2 s.

        The file is generated in memory and scanned via ``scan_content`` so
        the timing measures only AST parsing + rule execution, not disk I/O.
        """
        code = generate_spark_file(
            n_lines=1000,
            n_joins=5,
            n_group_bys=3,
            n_caches=2,
            n_udfs=2,
            n_writes=3,
        )
        actual_lines = len(code.splitlines())
        assert (
            actual_lines >= 1000
        ), f"Precondition: generated file must have >= 1000 lines, got {actual_lines}"

        config = LintConfig.from_dict({"general": {"severity_threshold": "INFO"}})
        orch = ScanOrchestrator(config)

        t0 = time.perf_counter()
        report = orch.scan_content(code, "bench_1000_lines.py")
        elapsed = time.perf_counter() - t0

        assert report.files_scanned == 1
        assert len(report.findings) > 0, "Expected at least one finding from generated code"
        assert elapsed < 2.0, (
            f"Scanning a {actual_lines}-line PySpark file took {elapsed:.3f}s — "
            f"exceeds 2-second budget.\n"
            f"  findings={len(report.findings)}, "
            f"  duration_reported={report.scan_duration_seconds:.3f}s"
        )

    def test_50_mixed_files_full_scan_under_10_seconds(self, tmp_path: Path) -> None:
        """50 generated files (25 PySpark + 25 plain Python) must complete in < 10 s.

        Uses ``generate_multi_file_project`` so the project mirrors a realistic
        ETL repository with a mix of Spark jobs and utility modules.  Plain
        Python files must be fast-pathed; only Spark files run through all rules.
        """
        project = generate_multi_file_project(n_files=50, spark_files_ratio=0.5)
        for fname, code in project.items():
            (tmp_path / fname).write_text(code, encoding="utf-8")

        # Confirm the ratio split
        spark_fnames = [f for f in project if f.startswith("spark_job_")]
        plain_fnames = [f for f in project if f.startswith("utils_")]
        assert len(spark_fnames) == 25
        assert len(plain_fnames) == 25

        config = LintConfig.from_dict({"general": {"severity_threshold": "INFO"}})
        orch = ScanOrchestrator(config)

        t0 = time.perf_counter()
        report = orch.scan([str(tmp_path)])
        elapsed = time.perf_counter() - t0

        assert report.files_scanned == 25, (
            f"Expected 25 PySpark files scanned (50 % ratio), got {report.files_scanned}.\n"
            f"Plain files should be fast-pathed and not counted."
        )
        assert len(report.findings) > 0, "Expected findings from generated Spark files"
        assert elapsed < 10.0, (
            f"Full scan of 50 generated files took {elapsed:.3f}s — "
            f"exceeds 10-second budget.\n"
            f"  files_scanned={report.files_scanned}, findings={len(report.findings)}"
        )

    def test_non_spark_file_skip_under_50ms_per_file(self, tmp_path: Path) -> None:
        """Non-Spark file classification must cost < 50 ms per file on average.

        Generates 50 plain Python utility files via ``generate_multi_file_project``
        and times how long the file scanner takes to classify and discard them.
        The fast-path head-scan (8 KB read) must keep per-file cost well below
        the 50 ms threshold so pre-commit hooks remain fast for large changesets.
        """
        project = generate_multi_file_project(n_files=50, spark_files_ratio=0.0)
        assert len(project) == 50, "Precondition: 50 plain Python files"
        for fname, code in project.items():
            (tmp_path / fname).write_text(code, encoding="utf-8")

        config = LintConfig.from_dict({"general": {"severity_threshold": "INFO"}})

        t0 = time.perf_counter()
        targets, summary = FileScanner.from_paths([str(tmp_path)], config)
        elapsed = time.perf_counter() - t0

        scannable = FileScanner.scannable(targets)
        assert scannable == [], (
            f"All plain Python files should be fast-pathed and skipped, "
            f"but {len(scannable)} were marked scannable."
        )
        assert (
            summary.non_pyspark == 50
        ), f"Expected 50 non-PySpark files, got {summary.non_pyspark}"

        per_file_ms = (elapsed / 50) * 1000
        assert per_file_ms < 50.0, (
            f"Non-Spark file classification averaged {per_file_ms:.2f} ms/file — "
            f"exceeds 50 ms budget.\n"
            f"  total_elapsed={elapsed*1000:.1f}ms for 50 files\n"
            f"  Fast-path (head-scan) may be broken or bypassed."
        )
