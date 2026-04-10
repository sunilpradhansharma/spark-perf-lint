"""Tests for tests/fixtures/code_generator.py.

Coverage:
  TestGenerateSparkFile   — structural parameters, padding, config injection
  TestAntiPatternInjection — every anti-pattern produces valid code + the
                             expected code signature
  TestMultiFileProject    — file counts, Spark/plain ratio, content correctness
"""

from __future__ import annotations

import ast

import pytest

from tests.fixtures.code_generator import (
    ALL_ANTI_PATTERNS,
    generate_multi_file_project,
    generate_spark_file,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parses(code: str) -> bool:
    """Return True iff *code* is syntactically valid Python."""
    try:
        ast.parse(code)
        return True
    except SyntaxError:
        return False


def _is_spark_file(code: str) -> bool:
    """Return True iff *code* imports pyspark (minimal scanner heuristic)."""
    return "from pyspark" in code or "import pyspark" in code


# ---------------------------------------------------------------------------
# String signatures expected in generated code for each anti-pattern.
# If a list is given, ALL items must appear in the code.
# ---------------------------------------------------------------------------

_ANTI_PATTERN_SIGNATURES: dict[str, list[str]] = {
    "aqe_disabled": ['"spark.sql.adaptive.enabled"', '"false"'],
    "broadcast_disabled": ['"spark.sql.autoBroadcastJoinThreshold"', '"-1"'],
    "cache_no_unpersist": [".cache()"],
    "collect_no_filter": [".collect()"],
    "cross_join": [".crossJoin("],
    "csv_read": [".csv("],
    "groupByKey": [".groupByKey()"],
    "infer_schema": ['"inferSchema"', '"true"'],
    "jdbc_no_partitions": ['.format("jdbc")', ".load()"],
    "python_udf": ["@F.udf("],
    "repartition_one": [".repartition(1)"],
    "select_star": ['.select("*")'],
    "show_in_prod": [".show()"],
    "toPandas_no_limit": [".toPandas()"],
    "withColumn_loop": ["for _col in", ".withColumn("],
}


# ---------------------------------------------------------------------------
# TestGenerateSparkFile
# ---------------------------------------------------------------------------


class TestGenerateSparkFile:
    """Tests for the structural parameters of generate_spark_file()."""

    def test_default_call_produces_valid_python(self) -> None:
        code = generate_spark_file()
        assert _parses(code), "Default generate_spark_file() must produce valid Python"

    def test_default_is_detected_as_spark_file(self) -> None:
        code = generate_spark_file()
        assert _is_spark_file(code)

    def test_zero_joins(self) -> None:
        code = generate_spark_file(n_joins=0, n_group_bys=0, n_writes=1)
        assert _parses(code)

    def test_many_joins(self) -> None:
        code = generate_spark_file(n_joins=5, n_group_bys=2, n_writes=2)
        assert _parses(code)
        # Should contain at least 5 join calls
        assert code.count(".join(") >= 5

    def test_n_group_bys(self) -> None:
        code = generate_spark_file(n_joins=1, n_group_bys=3, n_writes=3)
        assert _parses(code)
        assert code.count(".groupBy(") >= 3

    def test_n_writes(self) -> None:
        code = generate_spark_file(n_writes=4, n_group_bys=4)
        assert _parses(code)
        assert code.count('.parquet("/output/out_') >= 4

    def test_zero_writes(self) -> None:
        code = generate_spark_file(n_writes=0)
        assert _parses(code)

    def test_n_caches_with_unpersist(self) -> None:
        code = generate_spark_file(n_caches=3)
        assert _parses(code)
        assert code.count(".cache()") >= 3
        assert code.count(".unpersist()") >= 3

    def test_n_udfs(self) -> None:
        code = generate_spark_file(n_udfs=3)
        assert _parses(code)
        assert code.count("@F.udf(") >= 3

    def test_n_lines_padding_adds_lines(self) -> None:
        code = generate_spark_file(n_lines=200, n_joins=1, n_group_bys=1, n_writes=1)
        assert _parses(code)
        assert len(code.splitlines()) >= 200

    def test_n_lines_small_no_error(self) -> None:
        # n_lines smaller than natural content — should still parse, no truncation.
        code = generate_spark_file(n_lines=5)
        assert _parses(code)
        assert _is_spark_file(code)

    def test_custom_spark_config_appears_in_output(self) -> None:
        cfg = {"spark.executor.cores": "4", "spark.driver.memory": "2g"}
        code = generate_spark_file(spark_config=cfg)
        assert _parses(code)
        assert '"spark.executor.cores"' in code
        assert '"spark.driver.memory"' in code

    def test_empty_spark_config_dict(self) -> None:
        code = generate_spark_file(spark_config={})
        assert _parses(code)
        # With empty config the session is still valid.
        assert "SparkSession.builder" in code

    def test_no_anti_patterns_produces_clean_code(self) -> None:
        code = generate_spark_file(inject_anti_patterns=[])
        assert _parses(code)
        # None of the anti-pattern signatures should appear.
        assert ".crossJoin(" not in code
        assert ".repartition(1)" not in code
        assert ".groupByKey()" not in code

    def test_unknown_anti_pattern_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown anti-pattern"):
            generate_spark_file(inject_anti_patterns=["not_a_real_pattern"])

    def test_multiple_unknown_anti_patterns_listed_in_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown anti-pattern"):
            generate_spark_file(inject_anti_patterns=["bad_one", "bad_two"])

    def test_returns_string(self) -> None:
        result = generate_spark_file()
        assert isinstance(result, str)

    def test_ends_with_newline(self) -> None:
        code = generate_spark_file()
        assert code.endswith("\n")

    def test_contains_sparksession_builder(self) -> None:
        code = generate_spark_file()
        assert "SparkSession.builder" in code
        assert ".getOrCreate()" in code

    def test_contains_logging(self) -> None:
        code = generate_spark_file()
        assert "import logging" in code
        assert "getLogger" in code


# ---------------------------------------------------------------------------
# TestAntiPatternInjection
# ---------------------------------------------------------------------------


class TestAntiPatternInjection:
    """Every anti-pattern injection must produce valid Python and include
    the expected code signature."""

    @pytest.mark.parametrize("pattern", sorted(ALL_ANTI_PATTERNS))
    def test_injection_produces_valid_python(self, pattern: str) -> None:
        """Injecting any single anti-pattern must not break Python syntax."""
        code = generate_spark_file(inject_anti_patterns=[pattern])
        assert _parses(code), (
            f"generate_spark_file(inject_anti_patterns=[{pattern!r}]) " f"produced invalid Python"
        )

    @pytest.mark.parametrize("pattern", sorted(ALL_ANTI_PATTERNS))
    def test_injection_file_still_detected_as_spark(self, pattern: str) -> None:
        code = generate_spark_file(inject_anti_patterns=[pattern])
        assert _is_spark_file(code), f"Pattern {pattern!r} broke pyspark import"

    @pytest.mark.parametrize("pattern,signatures", sorted(_ANTI_PATTERN_SIGNATURES.items()))
    def test_anti_pattern_signature_present(self, pattern: str, signatures: list[str]) -> None:
        """Each injected anti-pattern must produce its characteristic code."""
        code = generate_spark_file(
            inject_anti_patterns=[pattern],
            n_caches=1,  # needed for cache_no_unpersist
            n_udfs=0,  # UDFs are added by the anti-pattern itself
        )
        for sig in signatures:
            assert sig in code, (
                f"Pattern {pattern!r}: expected signature {sig!r} not found in "
                f"generated code.\n\n--- generated code ---\n{code}"
            )

    def test_aqe_disabled_sets_false_not_true(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["aqe_disabled"])
        assert '"spark.sql.adaptive.enabled", "false"' in code
        # Must not also set it to true.
        assert '"spark.sql.adaptive.enabled", "true"' not in code

    def test_broadcast_disabled_sets_minus_one(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["broadcast_disabled"])
        assert '"spark.sql.autoBroadcastJoinThreshold", "-1"' in code

    def test_cross_join_uses_crossjoin_method(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["cross_join"])
        assert ".crossJoin(" in code

    def test_csv_read_uses_csv_method(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["csv_read"])
        assert ".csv(" in code

    def test_infer_schema_option_present(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["infer_schema"])
        assert '"inferSchema"' in code
        assert '"true"' in code

    def test_csv_and_infer_schema_combined(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["csv_read", "infer_schema"])
        assert _parses(code)
        assert ".csv(" in code
        assert '"inferSchema"' in code

    def test_python_udf_uses_decorator(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["python_udf"])
        assert "@F.udf(" in code

    def test_withColumn_loop_uses_for_loop(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["withColumn_loop"])
        assert "for _col in" in code
        assert ".withColumn(" in code

    def test_repartition_one_uses_value_1(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["repartition_one"])
        assert ".repartition(1)" in code

    def test_groupby_key_uses_rdd(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["groupByKey"])
        assert ".groupByKey()" in code
        assert ".rdd" in code

    def test_jdbc_no_partitions_has_no_partition_column(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["jdbc_no_partitions"])
        assert '.format("jdbc")' in code
        assert ".load()" in code
        # Must NOT include partitionColumn option.
        assert "partitionColumn" not in code

    def test_collect_no_filter_calls_collect(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["collect_no_filter"])
        assert ".collect()" in code

    def test_to_pandas_no_limit_calls_topandas(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["toPandas_no_limit"])
        assert ".toPandas()" in code
        # toPandas must not be preceded by a .limit() on the same expression.
        # Check that the toPandas call is not chained off .limit().
        assert ".limit(" not in code.split(".toPandas()")[0].rsplit("\n", 1)[-1]

    def test_show_in_prod_calls_show(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["show_in_prod"])
        assert ".show()" in code

    def test_select_star_uses_star_string(self) -> None:
        code = generate_spark_file(inject_anti_patterns=["select_star"])
        assert '.select("*")' in code

    def test_cache_no_unpersist_has_cache_without_unpersist(self) -> None:
        code = generate_spark_file(
            inject_anti_patterns=["cache_no_unpersist"],
            n_caches=1,
        )
        assert _parses(code)
        assert ".cache()" in code
        # cached_0 (the anti-pattern target) should NOT be unpersisted.
        # cached_0 is the first cache var.
        lines = code.splitlines()
        cache_line = next(i for i, ln in enumerate(lines) if "cached_0.cache()" in ln)
        remaining = "\n".join(lines[cache_line:])
        assert "cached_0.unpersist()" not in remaining

    def test_all_anti_patterns_combined_valid_python(self) -> None:
        """Injecting all 15 patterns at once must still parse."""
        code = generate_spark_file(
            n_joins=2,
            n_group_bys=2,
            n_caches=2,
            n_udfs=1,
            n_writes=2,
            inject_anti_patterns=list(ALL_ANTI_PATTERNS),
        )
        assert _parses(code), "All anti-patterns combined produced invalid Python"
        assert _is_spark_file(code)

    def test_all_anti_patterns_complete_set(self) -> None:
        """Verify that _ANTI_PATTERN_SIGNATURES covers every anti-pattern."""
        missing = ALL_ANTI_PATTERNS - set(_ANTI_PATTERN_SIGNATURES)
        assert not missing, f"_ANTI_PATTERN_SIGNATURES is missing entries for: {missing!r}"


# ---------------------------------------------------------------------------
# TestMultiFileProject
# ---------------------------------------------------------------------------


class TestMultiFileProject:
    """Tests for generate_multi_file_project()."""

    def test_correct_total_file_count(self) -> None:
        project = generate_multi_file_project(n_files=10)
        assert len(project) == 10

    def test_returns_dict_of_strings(self) -> None:
        project = generate_multi_file_project(n_files=5)
        assert isinstance(project, dict)
        for fname, code in project.items():
            assert isinstance(fname, str)
            assert isinstance(code, str)

    def test_spark_ratio_60_percent(self) -> None:
        project = generate_multi_file_project(n_files=10, spark_files_ratio=0.6)
        spark_count = sum(1 for c in project.values() if _is_spark_file(c))
        # round(10 * 0.6) = 6
        assert spark_count == 6

    def test_spark_ratio_zero_no_spark_files(self) -> None:
        project = generate_multi_file_project(n_files=8, spark_files_ratio=0.0)
        spark_count = sum(1 for c in project.values() if _is_spark_file(c))
        assert spark_count == 0

    def test_spark_ratio_one_all_spark_files(self) -> None:
        project = generate_multi_file_project(n_files=8, spark_files_ratio=1.0)
        spark_count = sum(1 for c in project.values() if _is_spark_file(c))
        assert spark_count == 8

    def test_all_files_are_valid_python(self) -> None:
        project = generate_multi_file_project(n_files=12, spark_files_ratio=0.5)
        for fname, code in project.items():
            assert _parses(code), f"{fname} produced invalid Python"

    def test_non_spark_files_do_not_import_pyspark(self) -> None:
        project = generate_multi_file_project(n_files=10, spark_files_ratio=0.4)
        plain_files = [(fname, code) for fname, code in project.items() if not _is_spark_file(code)]
        assert plain_files, "Expected at least one non-Spark file"
        for fname, code in plain_files:
            assert (
                "pyspark" not in code
            ), f"{fname} has 'pyspark' but was supposed to be a plain Python file"

    def test_spark_filenames_prefixed_spark_job(self) -> None:
        project = generate_multi_file_project(n_files=6, spark_files_ratio=0.5)
        spark_fnames = [f for f, c in project.items() if _is_spark_file(c)]
        for fname in spark_fnames:
            assert fname.startswith("spark_job_"), fname

    def test_plain_filenames_prefixed_utils(self) -> None:
        project = generate_multi_file_project(n_files=6, spark_files_ratio=0.5)
        plain_fnames = [f for f, c in project.items() if not _is_spark_file(c)]
        for fname in plain_fnames:
            assert fname.startswith("utils_"), fname

    def test_all_filenames_unique(self) -> None:
        project = generate_multi_file_project(n_files=20, spark_files_ratio=0.5)
        assert len(set(project.keys())) == 20

    def test_anti_patterns_injected_into_spark_files(self) -> None:
        project = generate_multi_file_project(
            n_files=6,
            spark_files_ratio=1.0,
            anti_patterns_per_file=["cross_join", "aqe_disabled"],
        )
        for fname, code in project.items():
            assert _is_spark_file(code)
            assert ".crossJoin(" in code, f"{fname} missing crossJoin"
            assert '"spark.sql.adaptive.enabled", "false"' in code, f"{fname} missing aqe_disabled"

    def test_anti_patterns_not_in_plain_files(self) -> None:
        project = generate_multi_file_project(
            n_files=8,
            spark_files_ratio=0.5,
            anti_patterns_per_file=["cross_join"],
        )
        plain = [(f, c) for f, c in project.items() if not _is_spark_file(c)]
        for fname, code in plain:
            assert ".crossJoin(" not in code, f"{fname} plain file has crossJoin"

    def test_n_files_one(self) -> None:
        project = generate_multi_file_project(n_files=1, spark_files_ratio=1.0)
        assert len(project) == 1
        code = next(iter(project.values()))
        assert _parses(code)
        assert _is_spark_file(code)

    def test_invalid_n_files_raises(self) -> None:
        with pytest.raises(ValueError, match="n_files"):
            generate_multi_file_project(n_files=0)

    def test_invalid_ratio_raises(self) -> None:
        with pytest.raises(ValueError, match="spark_files_ratio"):
            generate_multi_file_project(n_files=5, spark_files_ratio=1.5)

    def test_ratio_rounding(self) -> None:
        # round(7 * 0.5) = round(3.5) = 4 (Python banker's rounding → 4)
        # round(5 * 0.5) = round(2.5) = 2 (banker's rounding → 2)
        # Just verify total is correct either way.
        project = generate_multi_file_project(n_files=7, spark_files_ratio=0.5)
        assert len(project) == 7

    def test_large_project_all_valid_python(self) -> None:
        project = generate_multi_file_project(n_files=50, spark_files_ratio=0.6)
        assert len(project) == 50
        bad = [f for f, c in project.items() if not _parses(c)]
        assert not bad, f"Invalid Python in: {bad}"
