"""Unit tests for D08 AQE rules (SPL-D08-001 – SPL-D08-007).

Each rule has at least:
- A positive test (rule fires)
- A negative test (rule does not fire)

Tests call ``rule.check(analyzer, config)`` directly to avoid registry coupling.
"""

from __future__ import annotations

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.d08_aqe import (
    AqeAdvisoryPartitionSizeTooSmallRule,
    AqeCoalesceDisabledRule,
    AqeDisabledRule,
    AqeLocalShuffleReaderDisabledRule,
    AqeSkewFactorTooAggressiveRule,
    AqeSkewJoinDisabledWithJoinsRule,
    ManualShufflePartitionsWithAqeRule,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SS = "from pyspark.sql import SparkSession\nspark = SparkSession.builder"


def _ss_cfg(*kvpairs: tuple[str, str]) -> str:
    """Build a one-line SparkSession with the given config pairs + getOrCreate."""
    chain = "".join(f'.config("{k}", "{v}")' for k, v in kvpairs)
    return f"{_SS}{chain}.getOrCreate()\n"


SPARK_HDR = "from pyspark.sql import SparkSession\nfrom pyspark.sql.functions import *\n"


def _a(code: str) -> ASTAnalyzer:
    return ASTAnalyzer.from_source(code, filename="test.py")


def _cfg(**kw) -> LintConfig:
    return LintConfig.from_dict(kw) if kw else LintConfig.from_dict({})


def findings(rule, code: str, **kw):
    return rule.check(_a(code), _cfg(**kw))


# =============================================================================
# SPL-D08-001 — AQE disabled
# =============================================================================


class TestAqeDisabledRule:
    rule = AqeDisabledRule()

    def test_fires_when_aqe_false(self):
        code = _ss_cfg(("spark.sql.adaptive.enabled", "false"))
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D08-001"
        assert "adaptive.enabled" in fs[0].message

    def test_no_finding_when_aqe_true(self):
        code = _ss_cfg(("spark.sql.adaptive.enabled", "true"))
        assert findings(self.rule, code) == []

    def test_no_finding_when_aqe_not_set(self):
        code = f"{_SS}.getOrCreate()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_session(self):
        code = "from pyspark.sql import SparkSession\ndf.join(other, 'id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D08-002 — AQE coalesce disabled
# =============================================================================


class TestAqeCoalesceDisabledRule:
    rule = AqeCoalesceDisabledRule()

    def test_fires_when_coalesce_false(self):
        code = _ss_cfg(("spark.sql.adaptive.coalescePartitions.enabled", "false"))
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D08-002"
        assert "coalescePartitions" in fs[0].message

    def test_no_finding_when_coalesce_true(self):
        code = _ss_cfg(("spark.sql.adaptive.coalescePartitions.enabled", "true"))
        assert findings(self.rule, code) == []

    def test_no_finding_when_not_set(self):
        code = f"{_SS}.getOrCreate()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_session(self):
        code = "from pyspark.sql import SparkSession\ndf.count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D08-003 — AQE advisory partition size too small
# =============================================================================


class TestAqeAdvisoryPartitionSizeTooSmallRule:
    rule = AqeAdvisoryPartitionSizeTooSmallRule()

    def test_fires_on_4mb_advisory_size(self):
        code = _ss_cfg(("spark.sql.adaptive.advisoryPartitionSizeInBytes", "4m"))
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D08-003"
        assert "4" in fs[0].message

    def test_fires_on_1mb_advisory_size(self):
        code = _ss_cfg(("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1048576"))
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_64mb_default(self):
        code = _ss_cfg(("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m"))
        assert findings(self.rule, code) == []

    def test_no_finding_for_exactly_16mb(self):
        # 16 MB = boundary: not strictly less than min → no finding
        code = _ss_cfg(("spark.sql.adaptive.advisoryPartitionSizeInBytes", "16m"))
        assert findings(self.rule, code) == []

    def test_no_finding_when_not_set(self):
        code = f"{_SS}.getOrCreate()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_session(self):
        code = "from pyspark.sql import SparkSession\ndf.count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D08-004 — AQE skew join disabled with skew-prone joins
# =============================================================================


class TestAqeSkewJoinDisabledWithJoinsRule:
    rule = AqeSkewJoinDisabledWithJoinsRule()

    def test_fires_when_skew_disabled_and_skew_id_join(self):
        code = (
            _ss_cfg(("spark.sql.adaptive.skewJoin.enabled", "false"))
            + "df.join(events, 'user_id')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D08-004"
        assert "skewJoin" in fs[0].message

    def test_fires_when_skew_disabled_and_low_cardinality_join(self):
        code = (
            _ss_cfg(("spark.sql.adaptive.skewJoin.enabled", "false"))
            + "df.join(other, 'status')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_when_skew_disabled_and_is_prefix_join(self):
        code = (
            _ss_cfg(("spark.sql.adaptive.skewJoin.enabled", "false"))
            + "df.join(other, 'is_active')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_skew_disabled_but_no_skew_prone_key(self):
        # order_id is not in skew-prone sets
        code = (
            _ss_cfg(("spark.sql.adaptive.skewJoin.enabled", "false"))
            + "df.join(other, 'order_id')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_skew_disabled_but_no_joins(self):
        code = _ss_cfg(("spark.sql.adaptive.skewJoin.enabled", "false"))
        assert findings(self.rule, code) == []

    def test_no_finding_when_skew_enabled_with_skew_join(self):
        # Config not set → skewJoin enabled by default → no finding
        code = f"{_SS}.getOrCreate()\ndf.join(events, 'user_id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_session(self):
        code = "from pyspark.sql import SparkSession\ndf.join(other, 'user_id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D08-005 — AQE skew factor too aggressive
# =============================================================================


class TestAqeSkewFactorTooAggressiveRule:
    rule = AqeSkewFactorTooAggressiveRule()

    def test_fires_on_factor_1(self):
        code = _ss_cfg(("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "1"))
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D08-005"
        assert "1.0" in fs[0].message

    def test_fires_on_factor_1_point_5(self):
        code = _ss_cfg(("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "1.5"))
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_factor_2_exactly(self):
        # Boundary: 2 is the minimum allowed → no finding
        code = _ss_cfg(("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2"))
        assert findings(self.rule, code) == []

    def test_no_finding_for_default_factor_5(self):
        code = _ss_cfg(("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5"))
        assert findings(self.rule, code) == []

    def test_no_finding_when_not_set(self):
        code = f"{_SS}.getOrCreate()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_session(self):
        code = "from pyspark.sql import SparkSession\ndf.count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D08-006 — AQE local shuffle reader disabled
# =============================================================================


class TestAqeLocalShuffleReaderDisabledRule:
    rule = AqeLocalShuffleReaderDisabledRule()

    def test_fires_when_local_reader_false(self):
        code = _ss_cfg(("spark.sql.adaptive.localShuffleReader.enabled", "false"))
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D08-006"
        assert "localShuffleReader" in fs[0].message

    def test_no_finding_when_local_reader_true(self):
        code = _ss_cfg(("spark.sql.adaptive.localShuffleReader.enabled", "true"))
        assert findings(self.rule, code) == []

    def test_no_finding_when_not_set(self):
        code = f"{_SS}.getOrCreate()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_session(self):
        code = "from pyspark.sql import SparkSession\ndf.count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D08-007 — Manual shuffle partition count high with AQE
# =============================================================================


class TestManualShufflePartitionsWithAqeRule:
    rule = ManualShufflePartitionsWithAqeRule()

    def test_fires_on_shuffle_partitions_1000_with_aqe(self):
        code = _ss_cfg(("spark.sql.shuffle.partitions", "1000"))
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D08-007"
        assert "1000" in fs[0].message

    def test_fires_on_shuffle_partitions_2000(self):
        code = _ss_cfg(("spark.sql.shuffle.partitions", "2000"))
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_shuffle_partitions_200(self):
        # Default value — not flagged
        code = _ss_cfg(("spark.sql.shuffle.partitions", "200"))
        assert findings(self.rule, code) == []

    def test_no_finding_when_shuffle_partitions_400(self):
        # Exactly at threshold (not strictly greater) → no finding
        code = _ss_cfg(("spark.sql.shuffle.partitions", "400"))
        assert findings(self.rule, code) == []

    def test_no_finding_when_aqe_explicitly_disabled(self):
        # High partitions with AQE off is intentional — suppress
        code = _ss_cfg(
            ("spark.sql.shuffle.partitions", "1000"),
            ("spark.sql.adaptive.enabled", "false"),
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_not_set(self):
        code = f"{_SS}.getOrCreate()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_session(self):
        code = "from pyspark.sql import SparkSession\ndf.count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []
