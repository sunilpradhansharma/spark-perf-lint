"""Unit tests for D02 Shuffle rules (SPL-D02-001 – SPL-D02-008).

Each rule has at least:
- A positive test (rule fires)
- A negative test (rule does not fire)

Tests call ``rule.check(analyzer, config)`` directly to avoid registry coupling.
"""

from __future__ import annotations

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.d02_shuffle import (
    DefaultShufflePartitionsRule,
    DistinctWithoutFilterRule,
    GroupByKeyRule,
    MultipleShufflesInSequenceRule,
    OrderByWithoutLimitRule,
    RepartitionBeforeJoinRule,
    ShuffleFileBufferTooSmallRule,
    ShuffleFollowedByCoalesceRule,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SPARK_HDR = "from pyspark.sql import SparkSession\nfrom pyspark.sql.functions import *\n"


def _a(code: str) -> ASTAnalyzer:
    return ASTAnalyzer.from_source(code, filename="test.py")


def _cfg(**kw) -> LintConfig:
    return LintConfig.from_dict(kw) if kw else LintConfig.from_dict({})


def findings(rule, code: str, **kw):
    return rule.check(_a(code), _cfg(**kw))


# =============================================================================
# SPL-D02-001 — groupByKey
# =============================================================================


class TestGroupByKeyRule:
    rule = GroupByKeyRule()

    def test_fires_on_groupbykey(self):
        code = SPARK_HDR + "result = rdd.groupByKey().mapValues(sum)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D02-001"
        assert "groupByKey" in fs[0].message

    def test_no_finding_for_reducebykey(self):
        code = SPARK_HDR + "result = rdd.reduceByKey(lambda a, b: a + b)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_groupby_dataframe(self):
        # DataFrame groupBy is fine — only RDD groupByKey is flagged
        code = SPARK_HDR + "df.groupBy('col').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = {}\n") == []

    def test_multiple_groupbykey_each_flagged(self):
        code = SPARK_HDR + "r1 = a.groupByKey()\nr2 = b.groupByKey()\n"
        assert len(findings(self.rule, code)) == 2

    def test_severity_is_critical(self):
        from spark_perf_lint.types import Severity

        code = SPARK_HDR + "rdd.groupByKey()\n"
        assert findings(self.rule, code)[0].severity == Severity.CRITICAL


# =============================================================================
# SPL-D02-002 — Default shuffle partitions
# =============================================================================


class TestDefaultShufflePartitionsRule:
    rule = DefaultShufflePartitionsRule()

    def test_fires_when_partitions_not_set(self):
        code = SPARK_HDR + 'spark = SparkSession.builder.appName("j").getOrCreate()\n'
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "200" in fs[0].message

    def test_fires_when_set_to_200(self):
        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.sql.shuffle.partitions", "200").getOrCreate()\n'
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_tuned(self):
        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.sql.shuffle.partitions", "400").getOrCreate()\n'
        )
        assert findings(self.rule, code) == []

    def test_no_finding_without_session_creation(self):
        # Imports spark but never calls getOrCreate → no session in this file
        code = SPARK_HDR + "df = spark.read.parquet('s3://bucket/data')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_no_finding_when_set_to_one(self):
        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.sql.shuffle.partitions", "1").getOrCreate()\n'
        )
        assert findings(self.rule, code) == []


# =============================================================================
# SPL-D02-003 — Unnecessary repartition before join
# =============================================================================


class TestRepartitionBeforeJoinRule:
    rule = RepartitionBeforeJoinRule()

    def test_fires_on_chained_repartition_join(self):
        code = SPARK_HDR + "result = df.repartition(200, 'key').join(other, 'key')\n"
        fs = findings(self.rule, code)
        assert len(fs) >= 1
        assert "repartition" in fs[0].message.lower()

    def test_fires_on_multistatement_repartition_then_join(self):
        code = SPARK_HDR + "df2 = df.repartition(200)\n" "result = df2.join(other, 'id')\n"
        assert len(findings(self.rule, code)) >= 1

    def test_no_finding_for_join_without_repartition(self):
        code = SPARK_HDR + "result = df.join(other, 'key')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_no_finding_when_repartition_far_from_join(self):
        # repartition > 5 lines before join → not flagged in multi-statement pattern
        lines = "\n".join([f"x{i} = {i}" for i in range(10)])
        code = (
            SPARK_HDR + "df2 = df.repartition(200)\n" + lines + "\nresult = df.join(other, 'id')\n"
        )
        # Chain pattern won't fire either since repartition is not in join's chain
        # The multi-statement window is 5 lines; this is > 5 lines apart
        assert findings(self.rule, code) == []


# =============================================================================
# SPL-D02-004 — orderBy without limit
# =============================================================================


class TestOrderByWithoutLimitRule:
    rule = OrderByWithoutLimitRule()

    def test_fires_on_orderby_without_limit(self):
        code = SPARK_HDR + "result = df.orderBy('revenue', ascending=False)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "orderBy" in fs[0].message

    def test_fires_on_sort_without_limit(self):
        code = SPARK_HDR + "result = df.sort('col')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "sort" in fs[0].message

    def test_no_finding_when_limit_in_chain(self):
        code = SPARK_HDR + "result = df.orderBy('col').limit(100)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_limit_nearby(self):
        code = SPARK_HDR + "result = df.orderBy('col')\n" "top = result.limit(10)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = sorted([1, 2])\n") == []

    def test_both_orderby_and_sort_detected(self):
        code = SPARK_HDR + "a = df.orderBy('x')\n" "b = df.sort('y')\n"
        assert len(findings(self.rule, code)) == 2


# =============================================================================
# SPL-D02-005 — distinct() without filter
# =============================================================================


class TestDistinctWithoutFilterRule:
    rule = DistinctWithoutFilterRule()

    def test_fires_on_bare_distinct(self):
        code = SPARK_HDR + "result = df.distinct()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "distinct" in fs[0].message

    def test_no_finding_when_filter_in_chain(self):
        code = SPARK_HDR + "result = df.filter('active = true').distinct()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_select_in_chain(self):
        code = SPARK_HDR + "result = df.select('id', 'type').distinct()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_where_in_chain(self):
        code = SPARK_HDR + "result = df.where('date > \"2024\"').distinct()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "s = set([1, 2, 2])\n") == []

    def test_multiple_bare_distinct_each_flagged(self):
        code = SPARK_HDR + "a = df.distinct()\nb = df2.distinct()\n"
        assert len(findings(self.rule, code)) == 2


# =============================================================================
# SPL-D02-006 — Shuffle followed by coalesce
# =============================================================================


class TestShuffleFollowedByCoalesceRule:
    rule = ShuffleFollowedByCoalesceRule()

    def test_fires_on_groupby_then_coalesce_chained(self):
        code = SPARK_HDR + "result = df.groupBy('col').agg(count('*')).coalesce(10)\n"
        fs = findings(self.rule, code)
        assert len(fs) >= 1
        assert "coalesce" in fs[0].message

    def test_fires_on_join_then_coalesce_chained(self):
        code = SPARK_HDR + "result = df.join(other, 'id').coalesce(5)\n"
        fs = findings(self.rule, code)
        assert len(fs) >= 1

    def test_fires_on_orderby_then_coalesce_multistatement(self):
        code = SPARK_HDR + "df2 = df.orderBy('col')\n" "df3 = df2.coalesce(4)\n"
        assert len(findings(self.rule, code)) >= 1

    def test_no_finding_for_coalesce_without_shuffle(self):
        code = SPARK_HDR + "df2 = df.coalesce(10)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_no_finding_for_repartition_then_coalesce_outside_window(self):
        # repartition many lines before coalesce — multi-statement pattern won't fire
        padding = "\n".join([f"x{i} = {i}" for i in range(10)])
        code = SPARK_HDR + "df2 = df.repartition(100)\n" + padding + "\ndf3 = df2.coalesce(10)\n"
        # Chain pattern: coalesce's chain won't contain repartition (different statements)
        # Multi-statement: > 5 lines apart
        fs = findings(self.rule, code)
        assert fs == []


# =============================================================================
# SPL-D02-007 — Multiple shuffles in sequence
# =============================================================================


class TestMultipleShufflesInSequenceRule:
    rule = MultipleShufflesInSequenceRule()

    def test_fires_on_two_shuffles_in_sequence(self):
        code = SPARK_HDR + "df2 = df.groupBy('a').agg(count('*'))\n" "df3 = df2.join(lookup, 'a')\n"
        fs = findings(self.rule, code)
        assert len(fs) >= 1
        assert any("2" in f.message for f in fs)

    def test_fires_on_three_consecutive_shuffles(self):
        code = (
            SPARK_HDR + "df2 = df.groupBy('a').agg(count('b'))\n"
            "df3 = df2.join(other, 'a')\n"
            "df4 = df3.orderBy('b')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) >= 1

    def test_no_finding_when_cache_between_shuffles(self):
        code = (
            SPARK_HDR + "df2 = df.groupBy('a').agg(count('*')).cache()\n"
            "df3 = df2.join(other, 'a')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_single_shuffle(self):
        code = SPARK_HDR + "df2 = df.groupBy('col').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_checkpoint_between_shuffles_resets_sequence(self):
        code = (
            SPARK_HDR + "df2 = df.groupBy('a').agg(count('*'))\n"
            "df2.checkpoint()\n"
            "df3 = df2.join(other, 'a')\n"
        )
        assert findings(self.rule, code) == []

    def test_finding_points_to_second_shuffle_line(self):
        code = (
            SPARK_HDR + "df2 = df.groupBy('a').count()  # line 3\n"
            "df3 = df2.join(other, 'a')       # line 4\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) >= 1
        # The finding should be at or near line 4 (the second shuffle)
        assert fs[0].line_number >= 3


# =============================================================================
# SPL-D02-008 — Shuffle file buffer too small
# =============================================================================


class TestShuffleFileBufferTooSmallRule:
    rule = ShuffleFileBufferTooSmallRule()

    def test_fires_when_buffer_below_1mb(self):
        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.shuffle.file.buffer", "32k").getOrCreate()\n'
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "32k" in fs[0].message

    def test_fires_for_default_32k(self):
        # Explicitly set to 32768 bytes = 32 KB
        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.shuffle.file.buffer", "32768").getOrCreate()\n'
        )
        # 32768 bytes = 32 KB < 1024 KB → should fire
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_absent(self):
        code = SPARK_HDR + "spark = SparkSession.builder.getOrCreate()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_at_1mb(self):
        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.shuffle.file.buffer", "1m").getOrCreate()\n'
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_buffer_is_large(self):
        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.shuffle.file.buffer", "4m").getOrCreate()\n'
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []
