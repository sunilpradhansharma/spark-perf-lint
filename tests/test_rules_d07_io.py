"""Unit tests for D07 I/O and file format rules (SPL-D07-001 – SPL-D07-010).

Each rule has at least:
- A positive test (rule fires)
- A negative test (rule does not fire)

Tests call ``rule.check(analyzer, config)`` directly to avoid registry coupling.
"""

from __future__ import annotations

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.d07_io_format import (
    CsvJsonAnalyticalRule,
    JdbcWithoutPartitionsRule,
    MergeSchemaRule,
    NoFormatSpecifiedRule,
    ParquetCompressionNotSetRule,
    PostJoinFilterRule,
    SchemaInferenceRule,
    SelectStarRule,
    SmallFileWriteRule,
    WriteModeNotSpecifiedRule,
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
# SPL-D07-001 — CSV/JSON used for analytical workload
# =============================================================================


class TestCsvJsonAnalyticalRule:
    rule = CsvJsonAnalyticalRule()

    def test_fires_on_csv_read(self):
        code = SPARK_HDR + "df = spark.read.csv('data/')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D07-001"
        assert "csv" in fs[0].message

    def test_fires_on_json_read(self):
        code = SPARK_HDR + "df = spark.read.json('data/')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "json" in fs[0].message

    def test_fires_on_csv_write(self):
        code = SPARK_HDR + "df.write.mode('overwrite').csv('output/')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_json_write(self):
        code = SPARK_HDR + "df.write.mode('overwrite').json('output/')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_parquet_read(self):
        code = SPARK_HDR + "df = spark.read.parquet('data.parquet')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_orc_read(self):
        code = SPARK_HDR + "df = spark.read.orc('data.orc')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_parquet_write(self):
        code = SPARK_HDR + "df.write.mode('overwrite').parquet('output/')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D07-002 — Schema inference enabled
# =============================================================================


class TestSchemaInferenceRule:
    rule = SchemaInferenceRule()

    def test_fires_on_infer_schema_true_lowercase(self):
        code = SPARK_HDR + "df = spark.read.option('inferSchema', 'true').csv('path')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D07-002"
        assert "inferSchema" in fs[0].message

    def test_fires_on_infer_schema_true_uppercase(self):
        code = SPARK_HDR + "df = spark.read.option('inferSchema', 'True').csv('path')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_schema_explicitly_set(self):
        code = SPARK_HDR + (
            "schema = 'id LONG, name STRING'\n" "df = spark.read.schema(schema).csv('path')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_infer_schema_false(self):
        code = SPARK_HDR + "df = spark.read.option('inferSchema', 'false').csv('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_parquet_no_infer(self):
        code = SPARK_HDR + "df = spark.read.parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D07-003 — select("*")
# =============================================================================


class TestSelectStarRule:
    rule = SelectStarRule()

    def test_fires_on_select_star(self):
        code = SPARK_HDR + "df.select('*').groupBy('status').count()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D07-003"
        assert "select" in fs[0].message.lower()

    def test_no_finding_for_explicit_columns(self):
        code = SPARK_HDR + "df.select('id', 'name', 'status').groupBy('status').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_select_without_star(self):
        code = SPARK_HDR + "df.select('id').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_col_star(self):
        # col('*') is an ast.Call, not ast.Constant — should not fire
        code = SPARK_HDR + "df.select(col('*'))\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D07-004 — Filter after join (missing predicate pushdown)
# =============================================================================


class TestPostJoinFilterRule:
    rule = PostJoinFilterRule()

    def test_fires_on_filter_after_join(self):
        code = SPARK_HDR + "df.join(other, 'id').filter('status = \"active\"').count()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D07-004"
        assert "filter" in fs[0].message

    def test_fires_on_where_after_join(self):
        code = SPARK_HDR + "df.join(other, 'id').where('val > 0').count()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "where" in fs[0].message

    def test_no_finding_when_filter_before_join(self):
        code = SPARK_HDR + "df.filter('status = \"active\"').join(other, 'id').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_filter_without_join(self):
        code = SPARK_HDR + "df.filter('val > 0').groupBy('x').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D07-005 — Small file problem on write
# =============================================================================


class TestSmallFileWriteRule:
    rule = SmallFileWriteRule()

    def test_fires_on_partition_by_without_coalesce(self):
        code = SPARK_HDR + "df.write.partitionBy('date').parquet('s3://bucket/data')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D07-005"
        assert "'date'" in fs[0].message

    def test_fires_on_partition_by_orc_without_coalesce(self):
        code = SPARK_HDR + "df.write.mode('overwrite').partitionBy('year', 'month').orc('path')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_coalesce_in_chain(self):
        code = SPARK_HDR + "df.coalesce(5).write.partitionBy('date').parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_repartition_in_chain(self):
        code = SPARK_HDR + "df.repartition(10).write.partitionBy('date').parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_no_partition_by(self):
        code = SPARK_HDR + "df.write.mode('overwrite').parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D07-006 — JDBC read without partition parameters
# =============================================================================


class TestJdbcWithoutPartitionsRule:
    rule = JdbcWithoutPartitionsRule()

    def test_fires_on_jdbc_without_params(self):
        code = SPARK_HDR + "df = spark.read.jdbc(url, 'orders')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D07-006"
        assert "jdbc" in fs[0].message.lower()

    def test_no_finding_with_column_kwarg(self):
        code = SPARK_HDR + (
            "df = spark.read.jdbc(url, 'orders',"
            " column='id', lowerBound=0, upperBound=1000000, numPartitions=20)\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_with_num_partitions_kwarg(self):
        code = SPARK_HDR + "df = spark.read.jdbc(url, 'orders', numPartitions=10)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_with_partition_column_option(self):
        # format("jdbc").option("partitionColumn", ...).load() pattern
        code = SPARK_HDR + (
            "df = spark.read.format('jdbc')"
            ".option('url', url)"
            ".option('dbtable', 'orders')"
            ".option('partitionColumn', 'order_id')"
            ".option('lowerBound', '0')"
            ".option('upperBound', '1000000')"
            ".option('numPartitions', '20')"
            ".load()\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D07-007 — Parquet compression not set
# =============================================================================


class TestParquetCompressionNotSetRule:
    rule = ParquetCompressionNotSetRule()

    def test_fires_on_parquet_write_without_compression(self):
        code = SPARK_HDR + "df.write.mode('overwrite').parquet('s3://bucket/data')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D07-007"
        assert "compression" in fs[0].message

    def test_no_finding_when_compression_option_set(self):
        code = SPARK_HDR + (
            "df.write.mode('overwrite').option('compression', 'snappy').parquet('path')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_codec_option_set(self):
        code = SPARK_HDR + ("df.write.mode('overwrite').option('codec', 'zstd').parquet('path')\n")
        assert findings(self.rule, code) == []

    def test_no_finding_when_global_config_set(self):
        code = (
            "from pyspark.sql import SparkSession\n"
            "spark = SparkSession.builder"
            '.config("spark.sql.parquet.compression.codec", "snappy")'
            ".getOrCreate()\n"
            "df.write.mode('overwrite').parquet('path')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_csv_write(self):
        code = SPARK_HDR + "df.write.mode('overwrite').csv('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D07-008 — Write mode not specified
# =============================================================================


class TestWriteModeNotSpecifiedRule:
    rule = WriteModeNotSpecifiedRule()

    def test_fires_on_parquet_write_no_mode(self):
        code = SPARK_HDR + "df.write.parquet('s3://bucket/data')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D07-008"
        assert "mode" in fs[0].message

    def test_fires_on_csv_write_no_mode(self):
        code = SPARK_HDR + "df.write.csv('output/')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_save_no_mode(self):
        code = SPARK_HDR + "df.write.format('parquet').save('path')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_mode_overwrite(self):
        code = SPARK_HDR + "df.write.mode('overwrite').parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_mode_append(self):
        code = SPARK_HDR + "df.write.mode('append').parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D07-009 — No format specified on read/write
# =============================================================================


class TestNoFormatSpecifiedRule:
    rule = NoFormatSpecifiedRule()

    def test_fires_on_bare_load(self):
        code = SPARK_HDR + "df = spark.read.load('path')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D07-009"
        assert "format" in fs[0].message

    def test_fires_on_bare_save(self):
        code = SPARK_HDR + "df.write.mode('overwrite').save('path')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_parquet_read(self):
        code = SPARK_HDR + "df = spark.read.parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_format_then_load(self):
        code = SPARK_HDR + "df = spark.read.format('parquet').load('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_format_then_save(self):
        code = SPARK_HDR + "df.write.mode('overwrite').format('parquet').save('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_csv_read(self):
        code = SPARK_HDR + "df = spark.read.csv('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D07-010 — mergeSchema enabled
# =============================================================================


class TestMergeSchemaRule:
    rule = MergeSchemaRule()

    def test_fires_on_option_merge_schema_true(self):
        code = SPARK_HDR + "df = spark.read.option('mergeSchema', 'true').parquet('path')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D07-010"
        assert "mergeSchema" in fs[0].message

    def test_fires_on_global_config_merge_schema_true(self):
        code = (
            "from pyspark.sql import SparkSession\n"
            "spark = SparkSession.builder"
            '.config("spark.sql.parquet.mergeSchema", "true")'
            ".getOrCreate()\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "mergeSchema" in fs[0].message

    def test_fires_for_both_option_and_config(self):
        code = (
            "from pyspark.sql import SparkSession\n"
            "spark = SparkSession.builder"
            '.config("spark.sql.parquet.mergeSchema", "true")'
            ".getOrCreate()\n"
            "df = spark.read.option('mergeSchema', 'true').parquet('path')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 2

    def test_no_finding_when_merge_schema_false(self):
        code = SPARK_HDR + "df = spark.read.option('mergeSchema', 'false').parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_parquet_without_option(self):
        code = SPARK_HDR + "df = spark.read.parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []
