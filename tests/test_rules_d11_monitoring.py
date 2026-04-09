"""Unit tests for D11 monitoring and observability rules (SPL-D11-001 – SPL-D11-005).

Each rule has at least:
- A positive test (rule fires)
- A negative test (rule does not fire)

Tests call ``rule.check(analyzer, config)`` directly to avoid registry coupling.
"""

from __future__ import annotations

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.d11_monitoring import (
    HardcodedPathRule,
    MissingErrorHandlingRule,
    NoExplainInTestsRule,
    NoMetricsLoggingRule,
    NoSparkListenerRule,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SPARK_HDR = "from pyspark.sql import SparkSession\nfrom pyspark.sql.functions import *\n"

_SS = "from pyspark.sql import SparkSession\nspark = SparkSession.builder"


def _ss_cfg(*kvpairs: tuple[str, str]) -> str:
    chain = "".join(f'.config("{k}", "{v}")' for k, v in kvpairs)
    return f"{_SS}{chain}.getOrCreate()\n"


def _a(code: str, filename: str = "test.py") -> ASTAnalyzer:
    return ASTAnalyzer.from_source(code, filename=filename)


def _cfg(**kw) -> LintConfig:
    return LintConfig.from_dict(kw) if kw else LintConfig.from_dict({})


def findings(rule, code: str, filename: str = "test.py", **kw):
    return rule.check(_a(code, filename=filename), _cfg(**kw))


# =============================================================================
# SPL-D11-001 — No explain() in test files
# =============================================================================


class TestNoExplainInTestsRule:
    rule = NoExplainInTestsRule()

    def test_fires_for_test_file_with_join_no_explain(self):
        code = (
            SPARK_HDR
            + "def test_join():\n"
            "    result = users.join(events, 'user_id')\n"
            "    assert result.count() == 10\n"
        )
        fs = findings(self.rule, code, filename="test_etl.py")
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D11-001"
        assert "explain" in fs[0].message

    def test_fires_for_test_file_with_groupby_no_explain(self):
        code = (
            SPARK_HDR
            + "def test_agg():\n"
            "    result = df.groupBy('k').agg(count('*'))\n"
            "    assert result.count() > 0\n"
        )
        assert len(findings(self.rule, code, filename="test_agg.py")) == 1

    def test_no_finding_when_explain_present(self):
        code = (
            SPARK_HDR
            + "def test_join():\n"
            "    result = users.join(events, 'user_id')\n"
            "    result.explain()\n"
            "    assert result.count() == 10\n"
        )
        assert findings(self.rule, code, filename="test_etl.py") == []

    def test_no_finding_for_non_test_file(self):
        # Same code but filename is not a test module
        code = (
            SPARK_HDR
            + "result = users.join(events, 'user_id')\n"
        )
        assert findings(self.rule, code, filename="etl.py") == []

    def test_no_finding_for_test_file_without_heavy_ops(self):
        # Test file but only simple filter — no join/groupBy/agg
        code = (
            SPARK_HDR
            + "def test_filter():\n"
            "    result = df.filter('x > 0')\n"
            "    assert result.count() > 0\n"
        )
        assert findings(self.rule, code, filename="test_filter.py") == []

    def test_fires_for_test_file_with_underscore_suffix(self):
        code = (
            SPARK_HDR
            + "def test_join():\n"
            "    result = a.join(b, 'id')\n"
            "    assert result.count() > 0\n"
        )
        assert len(findings(self.rule, code, filename="etl_test.py")) == 1

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n", filename="test_x.py") == []


# =============================================================================
# SPL-D11-002 — No Spark listener configured
# =============================================================================


class TestNoSparkListenerRule:
    rule = NoSparkListenerRule()

    def test_fires_when_session_has_no_listener(self):
        code = _ss_cfg(("spark.app.name", "my-job"))
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D11-002"
        assert "listener" in fs[0].message.lower()

    def test_fires_for_bare_get_or_create(self):
        code = f"{_SS}.getOrCreate()\n"
        assert len(findings(self.rule, code)) == 1

    def test_no_finding_when_extra_listeners_set(self):
        code = _ss_cfg(("spark.extraListeners", "com.example.Listener"))
        assert findings(self.rule, code) == []

    def test_no_finding_when_query_execution_listeners_set(self):
        code = _ss_cfg(
            ("spark.sql.queryExecutionListeners", "com.example.QEListener")
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_add_spark_listener_called(self):
        code = (
            f"{_SS}.getOrCreate()\n"
            "spark.sparkContext.addSparkListener(my_listener)\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_without_get_or_create(self):
        # File that uses spark but doesn't create a session
        code = SPARK_HDR + "df.join(other, 'id').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D11-003 — No metrics logging in long-running jobs
# =============================================================================


class TestNoMetricsLoggingRule:
    rule = NoMetricsLoggingRule()

    def test_fires_when_two_writes_no_logging(self):
        code = (
            SPARK_HDR
            + "df.write.mode('overwrite').parquet('s3://b/a')\n"
            "df2.write.mode('overwrite').parquet('s3://b/b')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D11-003"
        assert "logging" in fs[0].message.lower()

    def test_no_finding_when_logging_imported(self):
        code = (
            "import logging\n"
            + SPARK_HDR
            + "logger = logging.getLogger(__name__)\n"
            "df.write.mode('overwrite').parquet('s3://b/a')\n"
            "df2.write.mode('overwrite').parquet('s3://b/b')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_logger_used(self):
        code = (
            SPARK_HDR
            + "logger.info('starting job')\n"
            "df.write.mode('overwrite').parquet('s3://b/a')\n"
            "df2.write.mode('overwrite').parquet('s3://b/b')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_single_write(self):
        code = SPARK_HDR + "df.write.mode('overwrite').parquet('s3://b/a')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_no_writes(self):
        code = SPARK_HDR + "df.filter('x > 0').groupBy('k').count()\n"
        assert findings(self.rule, code) == []

    def test_fires_when_three_writes_no_logging(self):
        code = (
            SPARK_HDR
            + "a.write.parquet('s3://b/a')\n"
            "b.write.parquet('s3://b/b')\n"
            "c.write.parquet('s3://b/c')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "3" in fs[0].message

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D11-004 — Missing error handling around Spark actions
# =============================================================================


class TestMissingErrorHandlingRule:
    rule = MissingErrorHandlingRule()

    def test_fires_when_count_outside_try(self):
        code = SPARK_HDR + "n = df.count()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D11-004"
        assert "try/except" in fs[0].message

    def test_fires_when_collect_outside_try(self):
        code = SPARK_HDR + "rows = df.collect()\n"
        assert len(findings(self.rule, code)) == 1

    def test_fires_when_write_outside_try(self):
        code = SPARK_HDR + "df.write.mode('overwrite').parquet('s3://b/out')\n"
        assert len(findings(self.rule, code)) == 1

    def test_no_finding_when_count_inside_try(self):
        code = (
            SPARK_HDR
            + "try:\n"
            "    n = df.count()\n"
            "except Exception as e:\n"
            "    raise\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_write_inside_try(self):
        code = (
            SPARK_HDR
            + "try:\n"
            "    df.write.mode('overwrite').parquet('s3://b/out')\n"
            "except Exception as e:\n"
            "    raise\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_all_actions_in_try(self):
        code = (
            SPARK_HDR
            + "try:\n"
            "    n = df.count()\n"
            "    rows = df.collect()\n"
            "except Exception:\n"
            "    pass\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_file_with_no_actions(self):
        code = (
            SPARK_HDR
            + "df2 = df.filter('x > 0').join(other, 'id')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D11-005 — Hardcoded paths
# =============================================================================


class TestHardcodedPathRule:
    rule = HardcodedPathRule()

    def test_fires_on_s3_read_path(self):
        code = SPARK_HDR + "df = spark.read.parquet('s3://my-bucket/data/')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D11-005"
        assert "s3://my-bucket/data/" in fs[0].message

    def test_fires_on_s3a_write_path(self):
        code = SPARK_HDR + "df.write.parquet('s3a://bucket/output')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "s3a://bucket/output" in fs[0].message

    def test_fires_on_hdfs_path(self):
        code = SPARK_HDR + "df = spark.read.csv('hdfs:///user/data/events.csv')\n"
        assert len(findings(self.rule, code)) == 1

    def test_fires_on_absolute_posix_path(self):
        code = SPARK_HDR + "df = spark.read.parquet('/data/input/events')\n"
        assert len(findings(self.rule, code)) == 1

    def test_fires_on_relative_path_with_slash(self):
        code = SPARK_HDR + "df = spark.read.parquet('data/input/events.parquet')\n"
        assert len(findings(self.rule, code)) == 1

    def test_fires_for_both_read_and_write_paths(self):
        code = (
            SPARK_HDR
            + "df = spark.read.parquet('s3://bucket/input')\n"
            "df.write.parquet('s3://bucket/output')\n"
        )
        assert len(findings(self.rule, code)) == 2

    def test_no_finding_when_path_is_variable(self):
        # Variable path — not captured as a literal in SparkIOInfo
        code = SPARK_HDR + "df = spark.read.parquet(input_path)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_table_name_without_slash(self):
        # Format "delta" with a bare table name (no slash) — not a storage path
        code = SPARK_HDR + "df = spark.read.format('delta').load('my_table')\n"
        assert findings(self.rule, code) == []

    def test_fires_on_gs_path(self):
        code = SPARK_HDR + "df = spark.read.parquet('gs://gcs-bucket/data')\n"
        assert len(findings(self.rule, code)) == 1

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []
