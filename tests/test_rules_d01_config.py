"""Unit tests for D01 Cluster Configuration rules (SPL-D01-001 – SPL-D01-010).

Each rule has at least:
- A positive test (rule fires)
- A negative test (rule does not fire)

Tests instantiate each rule class directly and call ``rule.check(analyzer, config)``
to stay independent of the registry singleton.
"""

from __future__ import annotations

import pytest

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.d01_cluster_config import (
    DefaultShufflePartitionsRule,
    DriverMemoryNotConfiguredRule,
    DynamicAllocationNotEnabledRule,
    ExecutorCoresTooHighRule,
    ExecutorMemoryNotConfiguredRule,
    MemoryOverheadTooLowRule,
    MissingKryoSerializerRule,
    MissingPySparkWorkerMemoryRule,
    NetworkTimeoutTooLowRule,
    SpeculationNotEnabledRule,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _analyze(code: str) -> ASTAnalyzer:
    return ASTAnalyzer.from_source(code, filename="test.py")


def _config(**overrides) -> LintConfig:
    return LintConfig.from_dict(overrides) if overrides else LintConfig.from_dict({})


def _findings(rule, code: str, **cfg_overrides):
    return rule.check(_analyze(code), _config(**cfg_overrides))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BASE_SPARK = "from pyspark.sql import SparkSession\n"


@pytest.fixture
def default_config():
    return _config()


# =============================================================================
# SPL-D01-001 — Missing Kryo serializer
# =============================================================================


class TestMissingKryoSerializer:
    rule = MissingKryoSerializerRule()

    def test_fires_when_serializer_absent(self):
        code = BASE_SPARK + 'spark = SparkSession.builder.appName("j").getOrCreate()\n'
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1
        assert findings[0].rule_id == "SPL-D01-001"
        assert "KryoSerializer" in findings[0].message

    def test_fires_when_wrong_serializer(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.serializer", "java.io.ObjectOutputStream")\\\n'
            "    .getOrCreate()\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1

    def test_no_finding_when_kryo_set(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_for_non_spark_file(self):
        assert self.rule.check(_analyze("x = 1\n"), _config()) == []

    def test_finding_has_correct_severity(self):
        code = BASE_SPARK + "spark = SparkSession.builder.getOrCreate()\n"
        f = self.rule.check(_analyze(code), _config())[0]
        from spark_perf_lint.types import Severity

        assert f.severity == Severity.WARNING

    def test_severity_override_respected(self):
        code = BASE_SPARK + "spark = SparkSession.builder.getOrCreate()\n"
        cfg = LintConfig.from_dict({"severity_override": {"SPL-D01-001": "CRITICAL"}})
        f = self.rule.check(_analyze(code), cfg)[0]
        from spark_perf_lint.types import Severity

        assert f.severity == Severity.CRITICAL

    def test_conf_set_pattern_detected(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder.getOrCreate()\n"
            'spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\n'
        )
        # kryo IS set via conf.set → should not fire
        assert self.rule.check(_analyze(code), _config()) == []


# =============================================================================
# SPL-D01-002 — Executor memory not configured
# =============================================================================


class TestExecutorMemoryNotConfigured:
    rule = ExecutorMemoryNotConfiguredRule()

    def test_fires_when_absent(self):
        code = BASE_SPARK + 'spark = SparkSession.builder.appName("j").getOrCreate()\n'
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1
        assert "executor.memory" in findings[0].message

    def test_no_finding_when_set(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.executor.memory", "8g")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_when_set_via_conf_set(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder.getOrCreate()\n"
            'spark.conf.set("spark.executor.memory", "4g")\n'
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_for_non_spark_file(self):
        assert self.rule.check(_analyze("import pandas as pd\n"), _config()) == []


# =============================================================================
# SPL-D01-003 — Driver memory not configured
# =============================================================================


class TestDriverMemoryNotConfigured:
    rule = DriverMemoryNotConfiguredRule()

    def test_fires_when_absent(self):
        code = BASE_SPARK + 'spark = SparkSession.builder.appName("j").getOrCreate()\n'
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1
        assert "driver.memory" in findings[0].message

    def test_no_finding_when_set(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.driver.memory", "4g")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_for_non_spark_file(self):
        assert self.rule.check(_analyze("x = 1\n"), _config()) == []

    def test_fires_even_when_executor_memory_is_set(self):
        # Executor memory is set but driver memory is missing — still fires
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.executor.memory", "4g")\\\n'
            "    .getOrCreate()\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1


# =============================================================================
# SPL-D01-004 — Dynamic allocation not enabled
# =============================================================================


class TestDynamicAllocationNotEnabled:
    rule = DynamicAllocationNotEnabledRule()

    def test_fires_when_absent(self):
        code = BASE_SPARK + "spark = SparkSession.builder.getOrCreate()\n"
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1
        assert "dynamicAllocation" in findings[0].message

    def test_fires_when_set_to_false(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.dynamicAllocation.enabled", "false")\\\n'
            "    .getOrCreate()\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1

    def test_no_finding_when_enabled(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.dynamicAllocation.enabled", "true")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_for_non_spark_file(self):
        assert self.rule.check(_analyze("print('hello')\n"), _config()) == []


# =============================================================================
# SPL-D01-005 — Executor cores set too high
# =============================================================================


class TestExecutorCoresTooHigh:
    rule = ExecutorCoresTooHighRule()

    def test_fires_when_cores_above_max(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.executor.cores", "8")\\\n'
            "    .getOrCreate()\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1
        assert "8" in findings[0].message

    def test_no_finding_at_default_max(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.executor.cores", "5")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_when_cores_not_set(self):
        code = BASE_SPARK + "spark = SparkSession.builder.getOrCreate()\n"
        assert self.rule.check(_analyze(code), _config()) == []

    def test_custom_threshold_respected(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.executor.cores", "3")\\\n'
            "    .getOrCreate()\n"
        )
        # Custom max = 2 → 3 should fire
        cfg = LintConfig.from_dict({"thresholds": {"max_executor_cores": 2}})
        findings = self.rule.check(_analyze(code), cfg)
        assert len(findings) == 1

    def test_no_finding_non_integer_value(self):
        # Non-parseable value — rule skips gracefully
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.executor.cores", "auto")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []


# =============================================================================
# SPL-D01-006 — Memory overhead too low
# =============================================================================


class TestMemoryOverheadTooLow:
    rule = MemoryOverheadTooLowRule()

    def test_fires_when_below_384mb(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.executor.memoryOverhead", "128m")\\\n'
            "    .getOrCreate()\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1
        assert "128m" in findings[0].message

    def test_fires_for_small_value_in_gb_units(self):
        # 0.1g = ~102 MB — below 384 MB threshold
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.executor.memoryOverhead", "0.1g")\\\n'
            "    .getOrCreate()\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1

    def test_no_finding_when_absent(self):
        # Not set → Spark uses its own default (max(384,10%)); not our concern
        code = BASE_SPARK + "spark = SparkSession.builder.getOrCreate()\n"
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_at_384mb(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.executor.memoryOverhead", "384m")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_when_overhead_is_1g(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.executor.memoryOverhead", "1g")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []


# =============================================================================
# SPL-D01-007 — Missing PySpark worker memory
# =============================================================================


class TestMissingPySparkWorkerMemory:
    rule = MissingPySparkWorkerMemoryRule()

    def test_fires_when_udf_present_and_no_config(self):
        code = (
            BASE_SPARK + "from pyspark.sql.functions import udf\n"
            "from pyspark.sql.types import StringType\n"
            "@udf(returnType=StringType())\n"
            "def upper(x): return x.upper()\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1
        assert "UDF" in findings[0].message

    def test_no_finding_when_worker_memory_set(self):
        code = (
            BASE_SPARK + "from pyspark.sql.functions import udf\n"
            "from pyspark.sql.types import StringType\n"
            "@udf(returnType=StringType())\n"
            "def upper(x): return x.upper()\n"
            "spark = SparkSession.builder\\\n"
            '    .config("spark.python.worker.memory", "1g")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_when_no_udfs(self):
        code = BASE_SPARK + "spark = SparkSession.builder.getOrCreate()\n"
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_for_non_spark_file(self):
        code = "def plain_func(x): return x * 2\n"
        assert self.rule.check(_analyze(code), _config()) == []

    def test_udf_count_in_message(self):
        code = (
            BASE_SPARK + "from pyspark.sql.functions import udf\n"
            "from pyspark.sql.types import StringType, IntegerType\n"
            "@udf(returnType=StringType())\n"
            "def u1(x): return x\n"
            "@udf(returnType=IntegerType())\n"
            "def u2(x): return len(x)\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1
        assert "2" in findings[0].message


# =============================================================================
# SPL-D01-008 — Network timeout too low
# =============================================================================


class TestNetworkTimeoutTooLow:
    rule = NetworkTimeoutTooLowRule()

    def test_fires_when_timeout_below_120s(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.network.timeout", "30s")\\\n'
            "    .getOrCreate()\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1
        assert "30s" in findings[0].message

    def test_fires_for_milliseconds_below_threshold(self):
        # 60000 ms = 60 s < 120 s
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.network.timeout", "60000ms")\\\n'
            "    .getOrCreate()\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1

    def test_no_finding_when_absent(self):
        code = BASE_SPARK + "spark = SparkSession.builder.getOrCreate()\n"
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_at_exactly_120s(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.network.timeout", "120s")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_when_above_threshold(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.network.timeout", "300s")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_bare_integer_treated_as_milliseconds(self):
        # 60000 bare = 60 s < 120 s → should fire
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.network.timeout", "60000")\\\n'
            "    .getOrCreate()\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1


# =============================================================================
# SPL-D01-009 — Speculation not enabled
# =============================================================================


class TestSpeculationNotEnabled:
    rule = SpeculationNotEnabledRule()

    def test_fires_when_absent(self):
        code = BASE_SPARK + "spark = SparkSession.builder.getOrCreate()\n"
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1
        assert "speculation" in findings[0].message

    def test_fires_when_set_to_false(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.speculation", "false")\\\n'
            "    .getOrCreate()\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1

    def test_no_finding_when_enabled(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.speculation", "true")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_for_non_spark_file(self):
        assert self.rule.check(_analyze("x = 1\n"), _config()) == []


# =============================================================================
# SPL-D01-010 — Default shuffle partitions unchanged
# =============================================================================


class TestDefaultShufflePartitions:
    rule = DefaultShufflePartitionsRule()

    def test_fires_when_absent_with_getsession(self):
        code = BASE_SPARK + 'spark = SparkSession.builder.appName("j").getOrCreate()\n'
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1
        assert "shuffle.partitions" in findings[0].message

    def test_fires_when_explicitly_set_to_200(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.sql.shuffle.partitions", "200")\\\n'
            "    .getOrCreate()\n"
        )
        findings = self.rule.check(_analyze(code), _config())
        assert len(findings) == 1
        assert "200" in findings[0].message

    def test_no_finding_when_tuned_value_set(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.sql.shuffle.partitions", "400")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_when_set_to_one(self):
        # Any non-200 value is considered intentional
        code = (
            BASE_SPARK + "spark = SparkSession.builder\\\n"
            '    .config("spark.sql.shuffle.partitions", "1")\\\n'
            "    .getOrCreate()\n"
        )
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_when_no_spark_session(self):
        # Spark is imported but no builder call → nothing to flag
        code = BASE_SPARK + 'df = spark.read.parquet("s3://bucket/data")\n'
        assert self.rule.check(_analyze(code), _config()) == []

    def test_no_finding_for_non_spark_file(self):
        assert self.rule.check(_analyze("x = 1\n"), _config()) == []

    def test_conf_set_to_non_default_not_flagged(self):
        code = (
            BASE_SPARK + "spark = SparkSession.builder.getOrCreate()\n"
            'spark.conf.set("spark.sql.shuffle.partitions", "800")\n'
        )
        assert self.rule.check(_analyze(code), _config()) == []
