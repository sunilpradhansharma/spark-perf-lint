"""Unit tests for D03 Join rules (SPL-D03-001 – SPL-D03-010).

Each rule has at least:
- A positive test (rule fires)
- A negative test (rule does not fire)

Tests call ``rule.check(analyzer, config)`` directly to avoid registry coupling.
"""

from __future__ import annotations

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.d03_joins import (
    BroadcastThresholdDisabledRule,
    CboNotEnabledRule,
    CrossJoinRule,
    JoinInsideLoopRule,
    JoinKeyTypeMismatchRule,
    JoinWithoutFilterRule,
    LeftJoinWithoutNullHandlingRule,
    MissingBroadcastHintRule,
    MultipleJoinsWithoutRepartitionRule,
    SelfJoinRule,
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
# SPL-D03-001 — Cross join / cartesian product
# =============================================================================


class TestCrossJoinRule:
    rule = CrossJoinRule()

    def test_fires_on_crossjoin_method(self):
        code = SPARK_HDR + "result = df.crossJoin(other)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D03-001"
        assert "cartesian" in fs[0].message.lower() or "O(n²)" in fs[0].message

    def test_fires_on_join_how_cross(self):
        code = SPARK_HDR + "result = df.join(other, how='cross')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "cross" in fs[0].message.lower() or "cartesian" in fs[0].message.lower()

    def test_fires_on_join_how_cross_positional(self):
        code = SPARK_HDR + "result = df.join(other, None, 'cross')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_inner_join(self):
        code = SPARK_HDR + "result = df.join(other, 'id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_left_join(self):
        code = SPARK_HDR + "result = df.join(other, 'id', 'left')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_severity_is_critical(self):
        from spark_perf_lint.types import Severity

        code = SPARK_HDR + "df.crossJoin(other)\n"
        assert findings(self.rule, code)[0].severity == Severity.CRITICAL

    def test_multiple_cross_joins_each_flagged(self):
        code = SPARK_HDR + "a = df.crossJoin(x)\nb = df.crossJoin(y)\n"
        assert len(findings(self.rule, code)) == 2


# =============================================================================
# SPL-D03-002 — Missing broadcast hint
# =============================================================================


class TestMissingBroadcastHintRule:
    rule = MissingBroadcastHintRule()

    def test_fires_when_no_broadcast_hint(self):
        code = SPARK_HDR + "result = df.join(other, 'id')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D03-002"
        assert "broadcast" in fs[0].message.lower()

    def test_no_finding_when_broadcast_function_used(self):
        code = SPARK_HDR + "result = df.join(broadcast(other), 'id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_hint_broadcast(self):
        code = SPARK_HDR + "result = df.join(other.hint('broadcast'), 'id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_threshold_configured_positive(self):
        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.sql.autoBroadcastJoinThreshold", "10485760").getOrCreate()\n'
            "result = df.join(other, 'id')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_cross_join(self):
        # crossJoin is covered by SPL-D03-001, not SPL-D03-002
        code = SPARK_HDR + "result = df.crossJoin(other)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_fires_when_threshold_is_minus_one(self):
        # threshold=-1 (disabled) still lacks a broadcast hint → Rule 002 fires
        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()\n'
            "result = df.join(other, 'id')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) >= 1


# =============================================================================
# SPL-D03-003 — Broadcast threshold disabled (-1)
# =============================================================================


class TestBroadcastThresholdDisabledRule:
    rule = BroadcastThresholdDisabledRule()

    def test_fires_when_threshold_minus_one(self):
        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()\n'
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D03-003"
        assert "-1" in fs[0].message

    def test_no_finding_when_threshold_positive(self):
        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.sql.autoBroadcastJoinThreshold", "10485760").getOrCreate()\n'
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_threshold_absent(self):
        code = SPARK_HDR + "spark = SparkSession.builder.getOrCreate()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_severity_is_critical(self):
        from spark_perf_lint.types import Severity

        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()\n'
        )
        assert findings(self.rule, code)[0].severity == Severity.CRITICAL


# =============================================================================
# SPL-D03-004 — Join without prior filter/select
# =============================================================================


class TestJoinWithoutFilterRule:
    rule = JoinWithoutFilterRule()

    def test_fires_on_read_directly_into_join(self):
        code = (
            SPARK_HDR
            + "result = spark.read.parquet('s3://bucket/events').join(users, 'id')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "filter" in fs[0].message.lower() or "unfiltered" in fs[0].message.lower()

    def test_no_finding_when_filter_precedes_join(self):
        code = (
            SPARK_HDR
            + "result = (spark.read.parquet('s3://bucket/events')"
            ".filter('date > \"2024\"').join(users, 'id'))\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_select_precedes_join(self):
        code = (
            SPARK_HDR
            + "result = (spark.read.parquet('s3://bucket/events')"
            ".select('id', 'type').join(users, 'id'))\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_join_without_read_in_chain(self):
        # df was already filtered/assigned elsewhere — can't determine statically
        code = SPARK_HDR + "result = df.join(other, 'id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_fires_on_csv_read_into_join(self):
        code = (
            SPARK_HDR
            + "result = spark.read.csv('data.csv').join(lookup, 'key')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1


# =============================================================================
# SPL-D03-005 — Join key type mismatch risk
# =============================================================================


class TestJoinKeyTypeMismatchRule:
    rule = JoinKeyTypeMismatchRule()

    def test_fires_on_different_column_names_in_condition(self):
        code = SPARK_HDR + "result = df1.join(df2, df1.order_id == df2.id)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "order_id" in fs[0].message or "id" in fs[0].message

    def test_no_finding_for_string_key(self):
        code = SPARK_HDR + "result = df1.join(df2, 'id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_list_keys(self):
        code = SPARK_HDR + "result = df1.join(df2, ['id', 'name'])\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_same_column_name(self):
        # df1.id == df2.id — same name, likely same type
        code = SPARK_HDR + "result = df1.join(df2, df1.id == df2.id)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_fires_on_timestamp_vs_date_mismatch(self):
        code = SPARK_HDR + "result = df1.join(df2, df1.created_at == df2.event_date)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1


# =============================================================================
# SPL-D03-006 — Multiple joins without repartition
# =============================================================================


class TestMultipleJoinsWithoutRepartitionRule:
    rule = MultipleJoinsWithoutRepartitionRule()

    def test_fires_on_three_consecutive_joins(self):
        code = (
            SPARK_HDR
            + "df2 = df.join(a, 'k')\n"
            "df3 = df2.join(b, 'k')\n"
            "df4 = df3.join(c, 'k')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) >= 1
        assert "3" in fs[0].message

    def test_no_finding_for_two_joins(self):
        code = SPARK_HDR + "df2 = df.join(a, 'k')\ndf3 = df2.join(b, 'k')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_cache_between_joins(self):
        code = (
            SPARK_HDR
            + "df2 = df.join(a, 'k').cache()\n"
            "df3 = df2.join(b, 'k')\n"
            "df4 = df3.join(c, 'k')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_joins_far_apart(self):
        padding = "\n".join([f"x{i} = {i}" for i in range(20)])
        code = SPARK_HDR + "df2 = df.join(a, 'k')\n" + padding + "\ndf3 = df2.join(b, 'k')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_finding_points_to_third_join(self):
        code = (
            SPARK_HDR
            + "df2 = df.join(a, 'k')    # line 3\n"
            "df3 = df2.join(b, 'k')   # line 4\n"
            "df4 = df3.join(c, 'k')   # line 5\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) >= 1
        assert fs[0].line_number >= 3


# =============================================================================
# SPL-D03-007 — Self-join
# =============================================================================


class TestSelfJoinRule:
    rule = SelfJoinRule()

    def test_fires_on_self_join(self):
        code = SPARK_HDR + "result = df.join(df, 'id')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "self-join" in fs[0].message.lower() or "df" in fs[0].message

    def test_no_finding_for_different_dataframes(self):
        code = SPARK_HDR + "result = df1.join(df2, 'id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_no_finding_when_one_side_is_transformed(self):
        # df.join(df.withColumnRenamed(...)) — receiver is "df",
        # other is "df.withColumnRenamed(...)" → different src strings → no finding
        code = SPARK_HDR + "result = df.join(df.withColumnRenamed('a', 'b'), 'id')\n"
        assert findings(self.rule, code) == []

    def test_fires_on_aliased_self_join(self):
        code = SPARK_HDR + "result = employees.join(employees, 'manager_id')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1


# =============================================================================
# SPL-D03-008 — Join inside loop
# =============================================================================


class TestJoinInsideLoopRule:
    rule = JoinInsideLoopRule()

    def test_fires_on_join_in_for_loop(self):
        code = (
            SPARK_HDR
            + "for date in dates:\n"
            "    result = result.join(df.filter(f'date = \"{date}\"'), 'id')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "loop" in fs[0].message.lower()
        assert fs[0].rule_id == "SPL-D03-008"

    def test_fires_on_join_in_while_loop(self):
        code = (
            SPARK_HDR
            + "while i < 10:\n"
            "    df2 = df2.join(partitions[i], 'id')\n"
            "    i += 1\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "while" in fs[0].message.lower()

    def test_no_finding_for_join_outside_loop(self):
        code = SPARK_HDR + "result = df.join(other, 'id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "for x in []: pass\n") == []

    def test_severity_is_critical(self):
        from spark_perf_lint.types import Severity

        code = (
            SPARK_HDR
            + "for x in items:\n"
            "    df = df.join(lookup, 'id')\n"
        )
        assert findings(self.rule, code)[0].severity == Severity.CRITICAL

    def test_finding_points_to_loop_line(self):
        code = (
            SPARK_HDR
            + "for date in dates:\n"
            "    result = result.join(daily, 'id')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].line_number == 3  # "for" line is line 3 after SPARK_HDR (2 lines)


# =============================================================================
# SPL-D03-009 — Left join without null handling
# =============================================================================


class TestLeftJoinWithoutNullHandlingRule:
    rule = LeftJoinWithoutNullHandlingRule()

    def test_fires_on_left_join_without_null_handling(self):
        code = SPARK_HDR + "result = df.join(other, 'id', 'left')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "null" in fs[0].message.lower()
        assert fs[0].rule_id == "SPL-D03-009"

    def test_fires_on_left_outer_join(self):
        code = SPARK_HDR + "result = df.join(other, 'id', 'left_outer')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_left_outer_kwarg(self):
        code = SPARK_HDR + "result = df.join(other, 'id', how='left')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_fillna_in_chain(self):
        code = SPARK_HDR + "result = df.join(other, 'id', 'left').fillna(0)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_dropna_nearby(self):
        code = (
            SPARK_HDR
            + "result = df.join(other, 'id', 'left')\n"
            "clean = result.dropna(subset=['required_col'])\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_inner_join(self):
        code = SPARK_HDR + "result = df.join(other, 'id', 'inner')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D03-010 — CBO/statistics not enabled
# =============================================================================


class TestCboNotEnabledRule:
    rule = CboNotEnabledRule()

    def test_fires_when_three_joins_and_no_cbo(self):
        code = (
            SPARK_HDR
            + "spark = SparkSession.builder.getOrCreate()\n"
            "df2 = df.join(a, 'k')\n"
            "df3 = df2.join(b, 'k')\n"
            "df4 = df3.join(c, 'k')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "cbo" in fs[0].message.lower() or "spark.sql.cbo" in fs[0].message
        assert fs[0].rule_id == "SPL-D03-010"

    def test_no_finding_when_cbo_enabled(self):
        code = (
            SPARK_HDR + "spark = SparkSession.builder"
            '.config("spark.sql.cbo.enabled", "true").getOrCreate()\n'
            "df2 = df.join(a, 'k')\n"
            "df3 = df2.join(b, 'k')\n"
            "df4 = df3.join(c, 'k')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_only_two_joins(self):
        code = (
            SPARK_HDR
            + "spark = SparkSession.builder.getOrCreate()\n"
            "df2 = df.join(a, 'k')\n"
            "df3 = df2.join(b, 'k')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_no_session_creation(self):
        # File uses joins but doesn't create a session — config set elsewhere
        code = (
            SPARK_HDR
            + "df2 = df.join(a, 'k')\n"
            "df3 = df2.join(b, 'k')\n"
            "df4 = df3.join(c, 'k')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_finding_includes_join_count(self):
        code = (
            SPARK_HDR
            + "spark = SparkSession.builder.getOrCreate()\n"
            "df2 = df.join(a, 'k')\n"
            "df3 = df2.join(b, 'k')\n"
            "df4 = df3.join(c, 'k')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "3" in fs[0].message
