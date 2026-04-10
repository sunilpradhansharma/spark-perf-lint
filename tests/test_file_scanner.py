"""Unit tests for spark_perf_lint.engine.file_scanner."""

from __future__ import annotations

from pathlib import Path

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.file_scanner import (
    FileScanner,
    ScanSummary,
    ScanTarget,
    is_pyspark_file,
    scan_paths,
    scan_staged,
)

# =============================================================================
# Helpers
# =============================================================================


def default_cfg(**overrides) -> LintConfig:
    """Return a LintConfig with no ignore rules unless overrides are given."""
    base = {
        "ignore": {
            "files": [],
            "directories": [],
            "rules": [],
        }
    }
    if overrides:
        base.update(overrides)
    return LintConfig.from_dict(base)


def write_py(directory: Path, name: str, content: str) -> Path:
    """Write a .py file and return its Path."""
    p = directory / name
    p.write_text(content, encoding="utf-8")
    return p


SPARK_CODE = "from pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()\n"
PLAIN_CODE = "import os\nprint(os.getcwd())\n"


# =============================================================================
# is_pyspark_file — fast detector
# =============================================================================


class TestIsPysparkFile:
    # --- True-positive cases ---

    def test_from_pyspark_import(self):
        assert is_pyspark_file("from pyspark.sql import SparkSession\n")

    def test_import_pyspark(self):
        assert is_pyspark_file("import pyspark\n")

    def test_sparksession_usage(self):
        assert is_pyspark_file("spark = SparkSession.builder.getOrCreate()\n")

    def test_sparkcontext_usage(self):
        assert is_pyspark_file("sc = SparkContext()\n")

    def test_spark_read_pattern(self):
        assert is_pyspark_file("df = spark.read.parquet('/data')\n")

    def test_spark_sql_pattern(self):
        assert is_pyspark_file("df = spark.sql('SELECT 1')\n")

    def test_repartition_pattern(self):
        assert is_pyspark_file("df = df.repartition(200)\n")

    def test_groupbykey_pattern(self):
        assert is_pyspark_file("rdd.groupByKey().collect()\n")

    def test_reducebykey_pattern(self):
        assert is_pyspark_file("rdd.reduceByKey(lambda a, b: a + b)\n")

    def test_cache_pattern(self):
        assert is_pyspark_file("df.cache()\n")

    def test_broadcast_pattern(self):
        assert is_pyspark_file("from pyspark.sql.functions import broadcast\n")

    def test_sqlcontext_usage(self):
        assert is_pyspark_file("sqlContext = SQLContext(sc)\n")

    # --- True-negative cases ---

    def test_plain_python(self):
        assert not is_pyspark_file("import os\nprint('hello')\n")

    def test_empty_file(self):
        assert not is_pyspark_file("")

    def test_docstring_only(self):
        assert not is_pyspark_file('"""Module docstring."""\n')

    def test_no_spark_in_comments(self):
        # "spark" in a comment should not trigger (not in our marker list)
        assert not is_pyspark_file("# This is not pyspark code\nprint('hello')\n")

    def test_pandas_only_not_spark(self):
        assert not is_pyspark_file("import pandas as pd\ndf = pd.DataFrame()\n")

    def test_variable_named_spark_no_import(self):
        # "spark" alone not in our marker list (we look for "SparkSession", not "spark")
        assert not is_pyspark_file("spark_value = 42\n")

    def test_large_file_head_scan(self):
        """Detection should work even if spark usage is in the first 8 KB."""
        prefix = "# " + "x" * 100 + "\n"
        content = prefix * 50 + "from pyspark.sql import SparkSession\n"
        # prefix is 104 bytes × 50 = 5200 bytes — within 8 KB head scan
        assert is_pyspark_file(content)

    def test_large_file_beyond_head_with_import(self):
        """Import marker found anywhere in the file (full scan)."""
        # 'from pyspark' triggers stage-1 scan of the full file
        huge = "x = 1\n" * 5000 + "from pyspark.sql import SparkSession\n"
        assert is_pyspark_file(huge)


# =============================================================================
# ScanTarget
# =============================================================================


class TestScanTarget:
    def test_repr_contains_path(self):
        t = ScanTarget(
            path=Path("/tmp/etl.py"),
            content="",
            is_pyspark=True,
            relative_path="etl.py",
            size_bytes=0,
        )
        assert "etl.py" in repr(t)

    def test_repr_spark_flag(self):
        t = ScanTarget(Path("/x.py"), "", True, "x.py", 0)
        assert "spark" in repr(t)

    def test_repr_plain_flag(self):
        t = ScanTarget(Path("/x.py"), "", False, "x.py", 0)
        assert "plain" in repr(t)


# =============================================================================
# FileScanner.from_paths
# =============================================================================


class TestFromPaths:
    def test_single_pyspark_file(self, tmp_path):
        f = write_py(tmp_path, "etl.py", SPARK_CODE)
        targets, summary = FileScanner.from_paths([f], default_cfg(), root=tmp_path)
        assert summary.total_found == 1
        assert summary.to_scan == 1
        assert len(FileScanner.scannable(targets)) == 1

    def test_single_plain_file(self, tmp_path):
        f = write_py(tmp_path, "util.py", PLAIN_CODE)
        targets, summary = FileScanner.from_paths([f], default_cfg(), root=tmp_path)
        assert summary.total_found == 1
        assert summary.non_pyspark == 1

    def test_directory_recursion(self, tmp_path):
        sub = tmp_path / "jobs"
        sub.mkdir()
        write_py(tmp_path, "a.py", SPARK_CODE)
        write_py(sub, "b.py", SPARK_CODE)
        targets, summary = FileScanner.from_paths([tmp_path], default_cfg(), root=tmp_path)
        assert summary.total_found == 2

    def test_non_py_files_skipped(self, tmp_path):
        (tmp_path / "data.csv").write_text("a,b,c\n")
        (tmp_path / "readme.md").write_text("# hi\n")
        write_py(tmp_path, "etl.py", SPARK_CODE)
        targets, summary = FileScanner.from_paths([tmp_path], default_cfg(), root=tmp_path)
        assert summary.total_found == 1

    def test_nonexistent_path_skipped(self, tmp_path):
        targets, summary = FileScanner.from_paths(
            [tmp_path / "nonexistent.py"], default_cfg(), root=tmp_path
        )
        assert summary.total_found == 0

    def test_duplicate_paths_deduplicated(self, tmp_path):
        f = write_py(tmp_path, "etl.py", SPARK_CODE)
        targets, summary = FileScanner.from_paths([f, f, f], default_cfg(), root=tmp_path)
        assert summary.total_found == 1

    def test_relative_path_computed(self, tmp_path):
        sub = tmp_path / "jobs"
        sub.mkdir()
        write_py(sub, "etl.py", SPARK_CODE)
        targets, _ = FileScanner.from_paths([tmp_path], default_cfg(), root=tmp_path)
        rel_paths = [t.relative_path for t in targets]
        assert any("jobs" in p for p in rel_paths)

    def test_content_populated(self, tmp_path):
        f = write_py(tmp_path, "etl.py", SPARK_CODE)
        targets, _ = FileScanner.from_paths([f], default_cfg(), root=tmp_path)
        assert SPARK_CODE in targets[0].content

    def test_size_bytes_populated(self, tmp_path):
        f = write_py(tmp_path, "etl.py", SPARK_CODE)
        targets, _ = FileScanner.from_paths([f], default_cfg(), root=tmp_path)
        assert targets[0].size_bytes > 0


# =============================================================================
# Ignore patterns
# =============================================================================


class TestIgnorePatterns:
    def test_ignore_directory(self, tmp_path):
        venv = tmp_path / ".venv" / "lib"
        venv.mkdir(parents=True)
        write_py(venv, "spark_module.py", SPARK_CODE)
        cfg = LintConfig.from_dict({"ignore": {"files": [], "directories": [".venv"], "rules": []}})
        targets, summary = FileScanner.from_paths([tmp_path], cfg, root=tmp_path)
        assert summary.ignored == 1
        assert all(t.skip_reason is not None for t in targets)

    def test_ignore_file_glob(self, tmp_path):
        write_py(tmp_path, "etl.py", SPARK_CODE)
        write_py(tmp_path, "etl_test.py", SPARK_CODE)
        cfg = LintConfig.from_dict(
            {"ignore": {"files": ["*_test.py"], "directories": [], "rules": []}}
        )
        targets, summary = FileScanner.from_paths([tmp_path], cfg, root=tmp_path)
        assert summary.ignored == 1
        scannable = FileScanner.scannable(targets)
        names = [t.relative_path for t in scannable]
        assert any("etl.py" in n for n in names)
        assert not any("etl_test.py" in n for n in names)

    def test_ignore_conftest(self, tmp_path):
        write_py(tmp_path, "conftest.py", SPARK_CODE)
        write_py(tmp_path, "etl.py", SPARK_CODE)
        cfg = LintConfig.from_dict(
            {"ignore": {"files": ["conftest.py"], "directories": [], "rules": []}}
        )
        _, summary = FileScanner.from_paths([tmp_path], cfg, root=tmp_path)
        assert summary.ignored == 1

    def test_multiple_ignore_patterns(self, tmp_path):
        write_py(tmp_path, "etl.py", SPARK_CODE)
        write_py(tmp_path, "etl_test.py", SPARK_CODE)
        write_py(tmp_path, "conftest.py", SPARK_CODE)
        cfg = LintConfig.from_dict(
            {"ignore": {"files": ["*_test.py", "conftest.py"], "directories": [], "rules": []}}
        )
        _, summary = FileScanner.from_paths([tmp_path], cfg, root=tmp_path)
        assert summary.ignored == 2

    def test_skip_reason_set_for_ignored(self, tmp_path):
        write_py(tmp_path, "etl_test.py", SPARK_CODE)
        cfg = LintConfig.from_dict(
            {"ignore": {"files": ["*_test.py"], "directories": [], "rules": []}}
        )
        targets, _ = FileScanner.from_paths([tmp_path], cfg, root=tmp_path)
        assert all(t.skip_reason is not None for t in FileScanner.skipped(targets))

    def test_non_ignored_file_has_no_skip_reason(self, tmp_path):
        write_py(tmp_path, "etl.py", SPARK_CODE)
        targets, _ = FileScanner.from_paths([tmp_path], default_cfg(), root=tmp_path)
        assert targets[0].skip_reason is None


# =============================================================================
# FileScanner.from_staged_files
# =============================================================================


class TestFromStagedFiles:
    def test_pyspark_staged_file_included(self, tmp_path):
        write_py(tmp_path, "etl.py", SPARK_CODE)
        targets, summary = FileScanner.from_staged_files(["etl.py"], default_cfg(), root=tmp_path)
        assert summary.total_found == 1
        assert summary.to_scan == 1

    def test_non_py_staged_file_skipped(self, tmp_path):
        (tmp_path / "data.csv").write_text("a,b\n")
        targets, summary = FileScanner.from_staged_files(["data.csv"], default_cfg(), root=tmp_path)
        assert summary.total_found == 0

    def test_deleted_staged_file_skipped(self, tmp_path):
        targets, summary = FileScanner.from_staged_files(
            ["deleted_file.py"], default_cfg(), root=tmp_path
        )
        assert summary.total_found == 0

    def test_relative_paths_resolved(self, tmp_path):
        sub = tmp_path / "jobs"
        sub.mkdir()
        write_py(sub, "etl.py", SPARK_CODE)
        targets, _ = FileScanner.from_staged_files(["jobs/etl.py"], default_cfg(), root=tmp_path)
        assert len(targets) == 1

    def test_absolute_paths_accepted(self, tmp_path):
        f = write_py(tmp_path, "etl.py", SPARK_CODE)
        targets, _ = FileScanner.from_staged_files([str(f)], default_cfg(), root=tmp_path)
        assert len(targets) == 1

    def test_ignore_applies_to_staged(self, tmp_path):
        write_py(tmp_path, "etl_test.py", SPARK_CODE)
        cfg = LintConfig.from_dict(
            {"ignore": {"files": ["*_test.py"], "directories": [], "rules": []}}
        )
        targets, summary = FileScanner.from_staged_files(["etl_test.py"], cfg, root=tmp_path)
        assert summary.ignored == 1

    def test_plain_python_staged_file(self, tmp_path):
        write_py(tmp_path, "util.py", PLAIN_CODE)
        targets, summary = FileScanner.from_staged_files(["util.py"], default_cfg(), root=tmp_path)
        assert summary.non_pyspark == 1

    def test_scan_staged_shorthand(self, tmp_path):
        write_py(tmp_path, "etl.py", SPARK_CODE)
        result = scan_staged(["etl.py"], default_cfg(), root=tmp_path)
        assert len(result) == 1
        assert result[0].is_pyspark is True


# =============================================================================
# FileScanner.from_glob_patterns
# =============================================================================


class TestFromGlobPatterns:
    def test_simple_glob(self, tmp_path):
        write_py(tmp_path, "etl.py", SPARK_CODE)
        write_py(tmp_path, "util.py", PLAIN_CODE)
        targets, summary = FileScanner.from_glob_patterns(["*.py"], default_cfg(), root=tmp_path)
        assert summary.total_found == 2

    def test_recursive_glob(self, tmp_path):
        sub = tmp_path / "jobs"
        sub.mkdir()
        write_py(sub, "batch.py", SPARK_CODE)
        targets, summary = FileScanner.from_glob_patterns(["**/*.py"], default_cfg(), root=tmp_path)
        assert summary.total_found >= 1

    def test_no_match_returns_empty(self, tmp_path):
        targets, summary = FileScanner.from_glob_patterns(
            ["nonexistent/*.py"], default_cfg(), root=tmp_path
        )
        assert summary.total_found == 0

    def test_multiple_patterns(self, tmp_path):
        jobs = tmp_path / "jobs"
        utils = tmp_path / "utils"
        jobs.mkdir()
        utils.mkdir()
        write_py(jobs, "etl.py", SPARK_CODE)
        write_py(utils, "helpers.py", PLAIN_CODE)
        targets, summary = FileScanner.from_glob_patterns(
            ["jobs/*.py", "utils/*.py"], default_cfg(), root=tmp_path
        )
        assert summary.total_found == 2


# =============================================================================
# FileScanner filter helpers
# =============================================================================


class TestFilterHelpers:
    def _make_targets(self, tmp_path) -> list[ScanTarget]:
        cfg = LintConfig.from_dict(
            {"ignore": {"files": ["*_test.py"], "directories": [], "rules": []}}
        )
        write_py(tmp_path, "etl.py", SPARK_CODE)
        write_py(tmp_path, "util.py", PLAIN_CODE)
        write_py(tmp_path, "etl_test.py", SPARK_CODE)
        targets, _ = FileScanner.from_paths([tmp_path], cfg, root=tmp_path)
        return targets

    def test_scannable_returns_only_pyspark_nonskipped(self, tmp_path):
        targets = self._make_targets(tmp_path)
        scannable = FileScanner.scannable(targets)
        assert all(t.is_pyspark and t.skip_reason is None for t in scannable)

    def test_skipped_returns_only_ignored(self, tmp_path):
        targets = self._make_targets(tmp_path)
        skipped = FileScanner.skipped(targets)
        assert all(t.skip_reason is not None for t in skipped)

    def test_non_pyspark_returns_plain_files(self, tmp_path):
        targets = self._make_targets(tmp_path)
        plain = FileScanner.non_pyspark(targets)
        assert all(not t.is_pyspark and t.skip_reason is None for t in plain)

    def test_scan_paths_shorthand(self, tmp_path):
        write_py(tmp_path, "etl.py", SPARK_CODE)
        result = scan_paths([tmp_path], default_cfg(), root=tmp_path)
        assert all(t.is_pyspark for t in result)


# =============================================================================
# Edge cases
# =============================================================================


class TestEdgeCases:
    def test_empty_directory(self, tmp_path):
        _, summary = FileScanner.from_paths([tmp_path], default_cfg(), root=tmp_path)
        assert summary.total_found == 0

    def test_empty_staged_list(self, tmp_path):
        _, summary = FileScanner.from_staged_files([], default_cfg(), root=tmp_path)
        assert summary.total_found == 0

    def test_file_with_encoding_issues_handled(self, tmp_path):
        f = tmp_path / "binary.py"
        f.write_bytes(b"from pyspark.sql import SparkSession\ndf = \xff\xfe\n")
        targets, summary = FileScanner.from_paths([f], default_cfg(), root=tmp_path)
        # Should not crash; file should be processed
        assert summary.total_found == 1

    def test_summary_repr_fields(self):
        s = ScanSummary(total_found=10, ignored=2, non_pyspark=3, to_scan=5)
        assert s.total_found == 10
        assert s.to_scan == 5
        assert s.read_errors == []
