"""Unit tests for D04 Partitioning rules (SPL-D04-001 – SPL-D04-010).

Each rule has at least:
- A positive test (rule fires)
- A negative test (rule does not fire)

Tests call ``rule.check(analyzer, config)`` directly to avoid registry coupling.
"""

from __future__ import annotations

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.d04_partitioning import (
    CoalesceBeforeWriteRule,
    CoalesceToOneRule,
    HighCardinalityPartitionRule,
    HighPartitionCountRule,
    LowPartitionCountRule,
    MissingBucketByRule,
    MissingPartitionByOnWriteRule,
    MissingPartitionFilterRule,
    RepartitionJoinKeyMismatchRule,
    RepartitionToOneRule,
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
# SPL-D04-001 — repartition(1) bottleneck
# =============================================================================


class TestRepartitionToOneRule:
    rule = RepartitionToOneRule()

    def test_fires_on_repartition_one(self):
        code = SPARK_HDR + "df2 = df.repartition(1)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D04-001"
        assert "1" in fs[0].message

    def test_fires_on_repartition_one_in_chain(self):
        code = SPARK_HDR + "df2 = df.repartition(1).write.parquet('path')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_repartition_200(self):
        code = SPARK_HDR + "df2 = df.repartition(200)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_repartition_by_column(self):
        code = SPARK_HDR + "df2 = df.repartition('id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_coalesce_one(self):
        # coalesce(1) is SPL-D04-002, not SPL-D04-001
        code = SPARK_HDR + "df2 = df.coalesce(1)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_severity_is_critical(self):
        from spark_perf_lint.types import Severity

        code = SPARK_HDR + "df.repartition(1)\n"
        assert findings(self.rule, code)[0].severity == Severity.CRITICAL

    def test_multiple_repartition_ones_each_flagged(self):
        code = SPARK_HDR + "a = df.repartition(1)\nb = df2.repartition(1)\n"
        assert len(findings(self.rule, code)) == 2


# =============================================================================
# SPL-D04-002 — coalesce(1) bottleneck
# =============================================================================


class TestCoalesceToOneRule:
    rule = CoalesceToOneRule()

    def test_fires_on_coalesce_one(self):
        code = SPARK_HDR + "df2 = df.coalesce(1)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D04-002"

    def test_fires_on_coalesce_one_before_groupby(self):
        code = SPARK_HDR + "df2 = df.coalesce(1).groupBy('key').count()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_coalesce_ten(self):
        code = SPARK_HDR + "df2 = df.coalesce(10)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_repartition_one(self):
        # repartition(1) is SPL-D04-001, not SPL-D04-002
        code = SPARK_HDR + "df2 = df.repartition(1)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_severity_is_warning(self):
        from spark_perf_lint.types import Severity

        code = SPARK_HDR + "df.coalesce(1)\n"
        assert findings(self.rule, code)[0].severity == Severity.WARNING


# =============================================================================
# SPL-D04-003 — Repartition with very high partition count
# =============================================================================


class TestHighPartitionCountRule:
    rule = HighPartitionCountRule()

    def test_fires_on_very_high_count(self):
        code = SPARK_HDR + "df2 = df.repartition(50000)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D04-003"
        assert "50" in fs[0].message or "50,000" in fs[0].message

    def test_fires_on_count_just_above_threshold(self):
        code = SPARK_HDR + "df2 = df.repartition(10001)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_reasonable_count(self):
        code = SPARK_HDR + "df2 = df.repartition(400)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_at_threshold(self):
        code = SPARK_HDR + "df2 = df.repartition(10000)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_column_repartition(self):
        code = SPARK_HDR + "df2 = df.repartition('id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D04-004 — Repartition with very low partition count
# =============================================================================


class TestLowPartitionCountRule:
    rule = LowPartitionCountRule()

    def test_fires_on_low_count(self):
        code = SPARK_HDR + "df2 = df.repartition(3)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D04-004"
        assert "3" in fs[0].message

    def test_fires_on_count_just_below_threshold(self):
        code = SPARK_HDR + "df2 = df.repartition(9)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_repartition_one(self):
        # SPL-D04-001 handles this; SPL-D04-004 excludes n == 1
        code = SPARK_HDR + "df2 = df.repartition(1)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_at_threshold(self):
        code = SPARK_HDR + "df2 = df.repartition(10)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_reasonable_count(self):
        code = SPARK_HDR + "df2 = df.repartition(200)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_column_repartition(self):
        code = SPARK_HDR + "df2 = df.repartition('id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D04-005 — Coalesce before write
# =============================================================================


class TestCoalesceBeforeWriteRule:
    rule = CoalesceBeforeWriteRule()

    def test_fires_on_coalesce_before_parquet_write(self):
        code = SPARK_HDR + "df.coalesce(20).write.parquet('s3://bucket/out')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D04-005"
        assert "coalesce" in fs[0].message.lower()

    def test_fires_on_coalesce_before_save(self):
        code = SPARK_HDR + "df.coalesce(5).write.format('orc').save('path')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_repartition_before_write(self):
        code = SPARK_HDR + "df.repartition(20).write.parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_write_without_coalesce(self):
        code = SPARK_HDR + "df.write.parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_coalesce_one(self):
        # coalesce(1) is covered by SPL-D04-002; SPL-D04-005 skips it
        code = SPARK_HDR + "df.coalesce(1).write.parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_coalesce_without_write(self):
        # coalesce followed by further transformations — not a write context
        code = SPARK_HDR + "df2 = df.coalesce(10).groupBy('key').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D04-006 — Missing partitionBy on write
# =============================================================================


class TestMissingPartitionByOnWriteRule:
    rule = MissingPartitionByOnWriteRule()

    def test_fires_on_parquet_write_without_partition(self):
        code = SPARK_HDR + "df.write.mode('overwrite').parquet('s3://bucket/events')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D04-006"
        assert "parquet" in fs[0].message.lower() or "partitionby" in fs[0].message.lower()

    def test_fires_on_orc_write_without_partition(self):
        code = SPARK_HDR + "df.write.orc('hdfs://path/data')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_partitionby_present(self):
        code = SPARK_HDR + "df.write.partitionBy('date').parquet('s3://bucket/events')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_csv_write(self):
        # CSV writes don't commonly use partitionBy
        code = SPARK_HDR + "df.write.csv('output.csv')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_no_finding_for_read(self):
        # Reads should not be flagged — only writes
        code = SPARK_HDR + "df = spark.read.parquet('s3://bucket/events')\n"
        assert findings(self.rule, code) == []


# =============================================================================
# SPL-D04-007 — Over-partitioning (high-cardinality column)
# =============================================================================


class TestHighCardinalityPartitionRule:
    rule = HighCardinalityPartitionRule()

    def test_fires_on_user_id_partition(self):
        code = SPARK_HDR + "df.write.partitionBy('user_id').parquet('s3://bucket/events')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D04-007"
        assert "user_id" in fs[0].message

    def test_fires_on_created_at_partition(self):
        code = SPARK_HDR + "df.write.partitionBy('created_at').parquet('path')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "created_at" in fs[0].message

    def test_fires_on_uuid_column(self):
        code = SPARK_HDR + "df.write.partitionBy('event_uuid').parquet('path')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_date_partition(self):
        code = SPARK_HDR + "df.write.partitionBy('date').parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_region_partition(self):
        code = SPARK_HDR + "df.write.partitionBy('region', 'status').parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_year_month_partition(self):
        code = SPARK_HDR + "df.write.partitionBy('year', 'month').parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D04-008 — Missing bucketBy for repeatedly joined tables
# =============================================================================


class TestMissingBucketByRule:
    rule = MissingBucketByRule()

    def test_fires_on_save_as_table_without_bucket_when_joins_present(self):
        code = (
            SPARK_HDR
            + "result = df.join(other, 'id')\n"
            "df.write.mode('overwrite').saveAsTable('events')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D04-008"

    def test_no_finding_when_bucket_by_present(self):
        code = (
            SPARK_HDR
            + "result = df.join(other, 'id')\n"
            "df.write.bucketBy(256, 'id').sortBy('id').saveAsTable('events')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_no_joins_in_file(self):
        code = (
            SPARK_HDR
            + "df.write.mode('overwrite').saveAsTable('events')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_parquet_write(self):
        # Only saveAsTable is flagged; parquet writes are handled by Rule 006
        code = (
            SPARK_HDR
            + "result = df.join(other, 'id')\n"
            "df.write.parquet('path')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D04-009 — Partition column not used in query filters
# =============================================================================


class TestMissingPartitionFilterRule:
    rule = MissingPartitionFilterRule()

    def test_fires_on_read_into_groupby_without_filter(self):
        code = (
            SPARK_HDR
            + "result = spark.read.parquet('s3://bucket/events').groupBy('user_id').count()\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D04-009"
        assert "filter" in fs[0].message.lower() or "partition" in fs[0].message.lower()

    def test_fires_on_read_into_distinct_without_filter(self):
        code = SPARK_HDR + "result = spark.read.parquet('s3://events').distinct()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_filter_precedes_groupby(self):
        code = (
            SPARK_HDR
            + "result = (spark.read.parquet('s3://bucket/events')"
            ".filter('date = \"2024-01-01\"').groupBy('user_id').count())\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_where_precedes_expensive_op(self):
        code = (
            SPARK_HDR
            + "result = (spark.read.parquet('s3://bucket/events')"
            ".where('date = \"2024\"').distinct())\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_groupby_without_read_in_chain(self):
        # df was assigned earlier — can't determine scan scope statically
        code = SPARK_HDR + "result = df.groupBy('key').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_fires_on_read_into_orderby(self):
        code = SPARK_HDR + "result = spark.read.orc('hdfs://path').orderBy('ts')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1


# =============================================================================
# SPL-D04-010 — Repartition by column different from join key
# =============================================================================


class TestRepartitionJoinKeyMismatchRule:
    rule = RepartitionJoinKeyMismatchRule()

    def test_fires_on_mismatched_repartition_and_join_key(self):
        code = SPARK_HDR + "result = df.repartition('user_id').join(other, 'order_id')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D04-010"
        assert "user_id" in fs[0].message
        assert "order_id" in fs[0].message

    def test_no_finding_when_columns_match(self):
        code = SPARK_HDR + "result = df.repartition('user_id').join(other, 'user_id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_repartition_by_count_only(self):
        # repartition(200) has no column — no mismatch to detect
        code = SPARK_HDR + "result = df.repartition(200).join(other, 'id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_join_without_repartition_in_chain(self):
        code = SPARK_HDR + "result = df.join(other, 'id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_no_finding_when_key_in_list_of_keys(self):
        # repartition('id') then join on ['id', 'name'] — 'id' is in join keys → no fire
        code = SPARK_HDR + "result = df.repartition('id').join(other, ['id', 'name'])\n"
        assert findings(self.rule, code) == []

    def test_fires_on_repartition_with_count_and_column(self):
        # repartition(200, 'user_id') then join on 'order_id' — column mismatch
        code = SPARK_HDR + "result = df.repartition(200, 'user_id').join(other, 'order_id')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
