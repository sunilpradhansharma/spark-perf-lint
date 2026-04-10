"""Unit tests for spark_perf_lint.engine.pattern_matcher."""

from __future__ import annotations

import pytest

from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.engine.pattern_matcher import (
    PatternMatch,
    PatternMatcher,
)

# =============================================================================
# Helpers
# =============================================================================


def make(code: str) -> PatternMatcher:
    """Build a PatternMatcher from an inline code string."""
    return PatternMatcher(ASTAnalyzer.from_source(code, "test.py"))


# =============================================================================
# PatternMatch container
# =============================================================================


class TestPatternMatch:
    def test_yes_is_truthy(self):
        assert PatternMatch.yes()

    def test_no_is_falsy(self):
        assert not PatternMatch.no()

    def test_yes_stores_context(self):
        m = PatternMatch.yes(foo="bar", count=42)
        assert m.context["foo"] == "bar"
        assert m.context["count"] == 42

    def test_first_line_with_calls(self):
        a = ASTAnalyzer.from_source("df.show()\n")
        calls = a.find_method_calls("show")
        m = PatternMatch.yes(calls=calls)
        assert m.first_line == 1

    def test_first_line_without_calls(self):
        assert PatternMatch.yes().first_line is None

    def test_matched_false_by_default_in_no(self):
        m = PatternMatch.no()
        assert m.matched is False
        assert m.calls == []
        assert m.configs == []


# =============================================================================
# 1. Chained method sequence patterns
# =============================================================================


class TestChainedSequence:
    def test_join_filter_chain(self):
        m = make("result = df1.join(df2, 'id').filter('x > 0')\n")
        matches = m.find_chained_sequence("join", "filter")
        assert len(matches) == 1
        assert matches[0].context["first_method"] == "join"

    def test_no_match_wrong_order(self):
        m = make("result = df.filter('x > 0').join(other, 'id')\n")
        # filter→join is NOT flagged by find_chained_sequence("join","filter")
        assert m.find_chained_sequence("join", "filter") == []

    def test_non_adjacent_not_matched(self):
        # repartition().cache().join() — repartition not directly before join
        m = make("df.repartition(10).cache().join(other, 'id')\n")
        assert m.find_chained_sequence("repartition", "join") == []

    def test_multiple_occurrences(self):
        code = "df1.join(df2, 'a').filter('x > 0')\n" "df3.join(df4, 'b').filter('y > 0')\n"
        matches = make(code).find_chained_sequence("join", "filter")
        assert len(matches) == 2

    def test_chain_context_preserved(self):
        m = make("df.join(other, 'id').filter('z > 1')\n")
        match = m.find_chained_sequence("join", "filter")[0]
        assert "join" in match.context["chain"]
        assert "filter" in match.context["chain"]


class TestMethodBeforeWithout:
    def test_join_without_prior_filter_flagged(self):
        m = make("result = df1.join(df2, 'id')\n")
        matches = m.find_method_before_without("join", "filter")
        assert len(matches) == 1

    def test_join_with_prior_filter_not_flagged(self):
        code = "df1 = df.filter('x > 0')\nresult = df1.join(df2, 'id')\n"
        matches = make(code).find_method_before_without("join", "filter")
        assert matches == []

    def test_window_respected(self):
        # filter is 100 lines before join — outside default window
        lines = ["pass"] * 100
        lines[0] = "df.filter('x > 0')"
        lines[-1] = "result = df1.join(df2, 'id')"
        matches = make("\n".join(lines)).find_method_before_without("join", "filter", window=10)
        assert len(matches) == 1  # prior is outside window → still flagged


class TestMethodAfter:
    def test_join_followed_by_filter(self):
        code = "df1.join(df2, 'id')\ndf_result.filter('x > 0')\n"
        matches = make(code).find_method_after("join", "filter", window=5)
        assert len(matches) == 1

    def test_no_following_method(self):
        matches = make("df1.join(df2, 'id')\n").find_method_after("join", "filter", window=5)
        assert matches == []

    def test_outside_window_not_matched(self):
        code = "df1.join(df2, 'id')\n" + "pass\n" * 20 + "df.filter('x')\n"
        matches = make(code).find_method_after("join", "filter", window=5)
        assert matches == []


class TestCacheWithoutUnpersist:
    def test_cache_without_unpersist_flagged(self):
        code = "df.cache()\ndf.show()\n"
        matches = make(code).find_cache_without_unpersist()
        assert len(matches) >= 1
        assert any(m.context.get("cached_receiver") == "df" for m in matches)

    def test_cache_with_unpersist_not_flagged(self):
        code = "df.cache()\ndf.show()\ndf.unpersist()\n"
        matches = make(code).find_cache_without_unpersist()
        # df has unpersist so should not appear
        assert not any(m.context.get("cached_receiver") == "df" for m in matches)

    def test_persist_without_unpersist(self):
        code = (
            "from pyspark import StorageLevel\ndf.persist(StorageLevel.MEMORY_ONLY)\ndf.count()\n"
        )
        matches = make(code).find_cache_without_unpersist()
        assert len(matches) >= 1


class TestCacheUsedOnce:
    def test_cache_used_once_flagged(self):
        code = "df.cache()\ndf.show()\n"
        matches = make(code).find_cache_used_once()
        assert len(matches) >= 1

    def test_cache_used_multiple_times_not_flagged(self):
        code = "df.cache()\ndf.show()\ndf.count()\n"
        matches = make(code).find_cache_used_once()
        # df is used twice after cache → should not appear as "used once"
        assert not any("df" == m.context.get("cached_receiver") for m in matches)


# =============================================================================
# 2. Config patterns
# =============================================================================


class TestMissingConfig:
    def test_missing_key_detected(self):
        m = make("x = 1\n")
        matches = m.find_missing_config("spark.sql.adaptive.enabled", "true")
        assert len(matches) == 1
        assert matches[0].context["reason"] == "missing"

    def test_correct_value_not_flagged(self):
        code = 'SparkSession.builder.config("spark.sql.adaptive.enabled", "true").getOrCreate()\n'
        assert make(code).find_missing_config("spark.sql.adaptive.enabled", "true") == []

    def test_wrong_value_flagged(self):
        code = 'SparkSession.builder.config("spark.sql.adaptive.enabled", "false").getOrCreate()\n'
        matches = make(code).find_missing_config("spark.sql.adaptive.enabled", "true")
        assert len(matches) == 1
        assert matches[0].context["reason"] == "wrong_value"
        assert matches[0].context["actual_value"] == "false"

    def test_case_insensitive_value_comparison(self):
        code = 'SparkSession.builder.config("spark.sql.adaptive.enabled", "TRUE").getOrCreate()\n'
        assert make(code).find_missing_config("spark.sql.adaptive.enabled", "true") == []

    def test_presence_only_check(self):
        code = 'SparkSession.builder.config("spark.executor.cores", "4").getOrCreate()\n'
        # expected_value=None → just check presence
        assert make(code).find_missing_config("spark.executor.cores") == []

    def test_presence_only_check_missing(self):
        code = "x = 1\n"
        matches = make(code).find_missing_config("spark.executor.cores")
        assert len(matches) == 1


class TestConflictingConfigs:
    def test_conflicting_detected(self):
        code = (
            "spark.conf.set('spark.sql.shuffle.partitions', '200')\n"
            "spark.conf.set('spark.sql.shuffle.partitions', '400')\n"
        )
        matches = make(code).find_conflicting_configs("spark.sql.shuffle.partitions")
        assert len(matches) == 1
        assert matches[0].context["occurrences"] == 2
        assert len(matches[0].context["distinct_values"]) == 2

    def test_same_value_twice_not_flagged(self):
        code = (
            "spark.conf.set('spark.sql.shuffle.partitions', '400')\n"
            "spark.conf.set('spark.sql.shuffle.partitions', '400')\n"
        )
        assert make(code).find_conflicting_configs("spark.sql.shuffle.partitions") == []

    def test_single_occurrence_not_flagged(self):
        code = "spark.conf.set('spark.sql.shuffle.partitions', '400')\n"
        assert make(code).find_conflicting_configs("spark.sql.shuffle.partitions") == []


class TestConfigBelowThreshold:
    _mult = {"g": 1024, "gb": 1024, "m": 1, "mb": 1}

    def test_below_threshold_flagged(self):
        code = 'SparkSession.builder.config("spark.executor.memory", "2g").getOrCreate()\n'
        matches = make(code).find_config_below_threshold("spark.executor.memory", 4096, self._mult)
        assert len(matches) == 1
        assert matches[0].context["parsed_value"] == pytest.approx(2048.0)

    def test_at_threshold_not_flagged(self):
        code = 'SparkSession.builder.config("spark.executor.memory", "4g").getOrCreate()\n'
        assert (
            make(code).find_config_below_threshold("spark.executor.memory", 4096, self._mult) == []
        )

    def test_above_threshold_not_flagged(self):
        code = 'SparkSession.builder.config("spark.executor.memory", "8g").getOrCreate()\n'
        assert (
            make(code).find_config_below_threshold("spark.executor.memory", 4096, self._mult) == []
        )

    def test_missing_key_returns_empty(self):
        assert make("x = 1\n").find_config_below_threshold("spark.executor.memory", 4096) == []

    def test_megabyte_suffix(self):
        code = 'SparkSession.builder.config("spark.executor.memory", "512m").getOrCreate()\n'
        matches = make(code).find_config_below_threshold("spark.executor.memory", 4096, self._mult)
        assert len(matches) == 1


# =============================================================================
# 3. Anti-pattern templates
# =============================================================================


class TestMethodInLoop:
    def test_withcolumn_in_loop_flagged(self):
        code = "for c in cols:\n    df = df.withColumn(c, df[c])\n"
        matches = make(code).find_method_in_loop("withColumn")
        assert len(matches) == 1
        assert matches[0].context["loop_type"] == "for"
        assert matches[0].context["occurrence_count"] == 1

    def test_no_loop_not_flagged(self):
        assert make("df = df.withColumn('x', df.x)\n").find_method_in_loop("withColumn") == []

    def test_while_loop_detected(self):
        code = "while cond:\n    df = df.cache()\n    break\n"
        matches = make(code).find_method_in_loop("cache")
        assert len(matches) == 1
        assert matches[0].context["loop_type"] == "while"

    def test_multiple_calls_in_one_loop(self):
        code = (
            "for c in cols:\n"
            "    df = df.withColumn(c, f(c))\n"
            "    df = df.withColumn(c+'_v2', g(c))\n"
        )
        matches = make(code).find_method_in_loop("withColumn")
        assert matches[0].context["occurrence_count"] == 2

    def test_loop_line_in_context(self):
        code = "\n\nfor c in cols:\n    df = df.withColumn(c, df[c])\n"
        matches = make(code).find_method_in_loop("withColumn")
        assert matches[0].context["loop_line"] == 3


class TestMethodOnResultOf:
    def test_collect_on_groupby(self):
        m = make("rows = df.groupBy('key').agg({'v': 'sum'}).collect()\n")
        matches = m.find_method_on_result_of("collect", "groupBy")
        assert len(matches) == 1
        assert "groupBy" in matches[0].context["chain"]

    def test_no_match_when_chain_absent(self):
        m = make("rows = df.collect()\n")
        assert m.find_method_on_result_of("collect", "groupBy") == []

    def test_topandas_on_unaggregated(self):
        m = make("pdf = large_df.toPandas()\n")
        assert m.find_method_on_result_of("toPandas", "groupBy") == []

    def test_nested_chain_detected(self):
        m = make("df.groupBy('k').agg({'v': 'sum'}).orderBy('v').collect()\n")
        matches = m.find_method_on_result_of("collect", "groupBy")
        assert len(matches) == 1


class TestRepeatedActionWithoutCache:
    def test_two_actions_flagged(self):
        code = "df.count()\ndf.show()\n"
        matches = make(code).find_repeated_action_without_cache()
        assert any(m.context.get("receiver") == "df" for m in matches)

    def test_cached_before_actions_not_flagged(self):
        code = "df.cache()\ndf.count()\ndf.show()\n"
        matches = make(code).find_repeated_action_without_cache()
        assert not any(m.context.get("receiver") == "df" for m in matches)

    def test_single_action_not_flagged(self):
        assert make("df.count()\n").find_repeated_action_without_cache() == []


class TestCollectWithoutLimit:
    def test_collect_without_limit_flagged(self):
        matches = make("rows = df.collect()\n").find_collect_without_limit()
        assert len(matches) == 1

    def test_topandas_without_limit_flagged(self):
        matches = make("pdf = df.toPandas()\n").find_collect_without_limit()
        assert len(matches) == 1

    def test_collect_with_limit_in_chain_not_flagged(self):
        assert make("rows = df.limit(100).collect()\n").find_collect_without_limit() == []

    def test_collect_with_nearby_limit_not_flagged(self):
        code = "df2 = df.limit(100)\nrows = df2.collect()\n"
        assert make(code).find_collect_without_limit() == []


# =============================================================================
# 4. Composite convenience patterns
# =============================================================================


class TestJoinBeforeFilter:
    def test_chained_join_filter_flagged(self):
        matches = make("df1.join(df2, 'id').filter('x > 0')\n").find_join_before_filter()
        assert len(matches) >= 1

    def test_filter_before_join_not_flagged(self):
        code = "df1 = df.filter('x > 0')\ndf1.join(df2, 'id')\n"
        # There's no join→filter chain here
        matches = make(code).find_join_before_filter()
        assert matches == []

    def test_chained_join_where_flagged(self):
        matches = make("df1.join(df2, 'id').where('x > 0')\n").find_join_before_filter()
        assert len(matches) >= 1


class TestRepartitionBeforeWrite:
    def test_chained_repartition_write_flagged(self):
        matches = make(
            "df.repartition(200).write.parquet('/out')\n"
        ).find_repartition_before_write()
        assert len(matches) >= 1
        assert any(m.context.get("write_terminal") == "parquet" for m in matches)

    def test_no_repartition_not_flagged(self):
        assert make("df.write.parquet('/out')\n").find_repartition_before_write() == []


class TestGroupByKeyPatterns:
    def test_groupbykey_flagged(self):
        matches = make("rdd.groupByKey().mapValues(list)\n").find_groupby_key_patterns()
        assert len(matches) == 1

    def test_groupby_dataframe_not_flagged(self):
        # DataFrame .groupBy() is fine — only RDD groupByKey is an issue
        matches = make("df.groupBy('key').count()\n").find_groupby_key_patterns()
        assert matches == []


class TestSelectStar:
    def test_select_star_flagged(self):
        matches = make("df.select('*').show()\n").find_select_star()
        assert len(matches) == 1

    def test_select_columns_not_flagged(self):
        assert make("df.select('a', 'b').show()\n").find_select_star() == []

    def test_select_no_args_not_flagged(self):
        assert make("df.select().show()\n").find_select_star() == []


class TestWithColumnInLoop:
    def test_withcolumn_in_loop_flagged(self):
        code = "for c in cols:\n    df = df.withColumn(c, df[c])\n"
        matches = make(code).find_withcolumn_in_loop()
        assert len(matches) == 1

    def test_withcolumn_outside_loop_not_flagged(self):
        assert make("df = df.withColumn('x', df.x)\n").find_withcolumn_in_loop() == []


class TestCrossJoin:
    def test_crossjoin_flagged(self):
        matches = make("result = df1.crossJoin(df2)\n").find_cross_join()
        assert len(matches) == 1

    def test_regular_join_not_flagged(self):
        assert make("result = df1.join(df2, 'id')\n").find_cross_join() == []


class TestCoalesceToOne:
    def test_coalesce_one_flagged(self):
        matches = make("df.coalesce(1).write.parquet('/out')\n").find_coalesce_to_one()
        assert len(matches) == 1
        assert matches[0].context["method"] == "coalesce"

    def test_repartition_one_flagged(self):
        matches = make("df.repartition(1).write.parquet('/out')\n").find_coalesce_to_one()
        assert len(matches) == 1
        assert matches[0].context["method"] == "repartition"

    def test_coalesce_many_not_flagged(self):
        assert make("df.coalesce(10).write.parquet('/out')\n").find_coalesce_to_one() == []


# =============================================================================
# 5. DataFrame lineage
# =============================================================================


class TestLineage:
    def test_simple_assignment_tracked(self):
        lineage = make("df2 = df1.join(other, 'id')\n").build_lineage()
        assert "df2" in lineage
        assert lineage["df2"].created_by == "join"

    def test_use_count_incremented(self):
        code = "df2 = df1.join(other, 'id')\ndf2.show()\ndf2.count()\n"
        lineage = make(code).build_lineage()
        assert lineage["df2"].use_count == 2

    def test_cache_marked(self):
        code = "df3 = df1.filter('x > 0')\ndf3.cache()\n"
        lineage = make(code).build_lineage()
        assert lineage["df3"].cached is True
        assert lineage["df3"].cache_line is not None

    def test_unpersist_marked(self):
        code = "df3 = df1.filter('x > 0')\ndf3.cache()\ndf3.show()\ndf3.unpersist()\n"
        lineage = make(code).build_lineage()
        assert lineage["df3"].unpersisted is True

    def test_cached_without_unpersist_via_lineage(self):
        code = "df3 = df1.filter('x > 0')\ndf3.cache()\ndf3.show()\ndf3.count()\n"
        matches = make(code).find_cached_without_unpersist()
        assert any(m.context.get("dataframe_name") == "df3" for m in matches)

    def test_cached_with_unpersist_not_flagged(self):
        code = "df3 = df1.filter('x > 0')\ndf3.cache()\ndf3.show()\ndf3.unpersist()\n"
        matches = make(code).find_cached_without_unpersist()
        assert not any(m.context.get("dataframe_name") == "df3" for m in matches)

    def test_cached_used_once_flagged(self):
        code = "df4 = df2.filter('y > 0')\ndf4.cache()\ndf4.show()\n"
        matches = make(code).find_cached_dataframes_used_once()
        assert any(m.context.get("dataframe_name") == "df4" for m in matches)

    def test_cached_used_multiple_times_not_flagged(self):
        code = "df4 = df2.filter('y > 0')\ndf4.cache()\ndf4.show()\ndf4.count()\n"
        matches = make(code).find_cached_dataframes_used_once()
        assert not any(m.context.get("dataframe_name") == "df4" for m in matches)

    def test_multi_use_without_cache_flagged(self):
        code = "df5 = df.filter('z > 0')\ndf5.show()\ndf5.count()\ndf5.write.parquet('/out')\n"
        matches = make(code).find_dataframes_used_multiple_times_without_cache(min_uses=2)
        assert any(m.context.get("dataframe_name") == "df5" for m in matches)

    def test_cached_multi_use_not_flagged(self):
        code = "df5 = df.filter('z > 0')\ndf5.cache()\ndf5.show()\ndf5.count()\n"
        matches = make(code).find_dataframes_used_multiple_times_without_cache(min_uses=2)
        assert not any(m.context.get("dataframe_name") == "df5" for m in matches)

    def test_lineage_cached_result(self):
        """build_lineage() returns same dict object on second call."""
        m = make("df2 = df.filter('x > 0')\n")
        first = m.build_lineage()
        second = m.build_lineage()
        assert first is second
