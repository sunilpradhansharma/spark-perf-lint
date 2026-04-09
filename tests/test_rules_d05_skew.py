"""Unit tests for D05 Data Skew rules (SPL-D05-001 – SPL-D05-007).

Each rule has at least:
- A positive test (rule fires)
- A negative test (rule does not fire)

Tests call ``rule.check(analyzer, config)`` directly to avoid registry coupling.
"""

from __future__ import annotations

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.d05_skew import (
    AqeSkewJoinDisabledRule,
    AqeSkewThresholdTooHighRule,
    LowCardinalityGroupByRule,
    LowCardinalityJoinRule,
    MissingSaltingPatternRule,
    NullHeavyJoinKeyRule,
    WindowPartitionSkewRule,
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
# SPL-D05-001 — Join on low-cardinality column
# =============================================================================


class TestLowCardinalityJoinRule:
    rule = LowCardinalityJoinRule()

    def test_fires_on_status_key(self):
        code = SPARK_HDR + "df.join(other, 'status')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D05-001"
        assert "'status'" in fs[0].message

    def test_fires_on_type_key(self):
        code = SPARK_HDR + "df.join(other, 'type')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_is_prefix_key(self):
        code = SPARK_HDR + "df.join(other, 'is_active')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "'is_active'" in fs[0].message

    def test_fires_on_has_prefix_key(self):
        code = SPARK_HDR + "df.join(other, 'has_premium')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_flag_prefix_key(self):
        code = SPARK_HDR + "df.join(other, 'flag_deleted')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_gender_key(self):
        code = SPARK_HDR + "df.join(other, 'gender')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_list_containing_skew_key(self):
        code = SPARK_HDR + "df.join(other, ['status', 'user_id'])\n"
        # status is skew-prone even though user_id is also present
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_kwarg_on(self):
        code = SPARK_HDR + "df.join(other, on='status')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_user_id_key(self):
        code = SPARK_HDR + "df.join(other, 'user_id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_date_key(self):
        code = SPARK_HDR + "df.join(other, 'date')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_column_expression(self):
        # col() objects — can't statically determine cardinality
        code = SPARK_HDR + "df.join(other, col('status') == other.status)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D05-002 — GroupBy on low-cardinality column without secondary key
# =============================================================================


class TestLowCardinalityGroupByRule:
    rule = LowCardinalityGroupByRule()

    def test_fires_on_status_only(self):
        code = SPARK_HDR + "df.groupBy('status').count()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D05-002"
        assert "'status'" in fs[0].message

    def test_fires_on_type_only(self):
        code = SPARK_HDR + "df.groupBy('type').agg(count('*'))\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_is_active_only(self):
        code = SPARK_HDR + "df.groupBy('is_active').count()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_multiple_all_skew_prone(self):
        code = SPARK_HDR + "df.groupBy('status', 'flag').count()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_secondary_high_cardinality_key(self):
        # status + user_id → has a non-skew-prone key → no finding
        code = SPARK_HDR + "df.groupBy('status', 'user_id').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_when_only_high_cardinality_key(self):
        code = SPARK_HDR + "df.groupBy('user_id').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_no_string_keys(self):
        # column objects → can't determine cardinality
        code = SPARK_HDR + "df.groupBy(col('status')).count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_fires_on_list_syntax(self):
        code = SPARK_HDR + "df.groupBy(['status']).count()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1


# =============================================================================
# SPL-D05-003 — AQE skew join disabled
# =============================================================================

_SS = "from pyspark.sql import SparkSession\nspark = SparkSession.builder"


def _ss_cfg(*kvpairs: tuple[str, str]) -> str:
    """Build a valid one-line SparkSession builder with the given config pairs."""
    chain = "".join(f'.config("{k}", "{v}")' for k, v in kvpairs)
    return f"{_SS}{chain}.getOrCreate()\n"


class TestAqeSkewJoinDisabledRule:
    rule = AqeSkewJoinDisabledRule()

    def test_fires_when_adaptive_enabled_false(self):
        code = _ss_cfg(("spark.sql.adaptive.enabled", "false"))
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D05-003"
        assert "adaptive.enabled" in fs[0].message

    def test_fires_when_skew_join_enabled_false(self):
        code = _ss_cfg(("spark.sql.adaptive.skewJoin.enabled", "false"))
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "skewJoin.enabled" in fs[0].message

    def test_fires_for_both_keys_disabled(self):
        code = _ss_cfg(
            ("spark.sql.adaptive.enabled", "false"),
            ("spark.sql.adaptive.skewJoin.enabled", "false"),
        )
        fs = findings(self.rule, code)
        assert len(fs) == 2

    def test_no_finding_when_adaptive_enabled_true(self):
        code = _ss_cfg(("spark.sql.adaptive.enabled", "true"))
        assert findings(self.rule, code) == []

    def test_no_finding_when_no_config_set(self):
        code = f"{_SS}.getOrCreate()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_session(self):
        # No getOrCreate → suppressed
        code = "from pyspark.sql import SparkSession\ndf.join(other, 'status')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D05-004 — AQE skew threshold too high
# =============================================================================

_THRESHOLD_KEY = "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"
_FACTOR_KEY = "spark.sql.adaptive.skewJoin.skewedPartitionFactor"


class TestAqeSkewThresholdTooHighRule:
    rule = AqeSkewThresholdTooHighRule()

    def test_fires_on_threshold_2gb(self):
        code = _ss_cfg((_THRESHOLD_KEY, "2g"))
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D05-004"
        assert "2.0 GB" in fs[0].message

    def test_fires_on_threshold_2gb_uppercase(self):
        code = _ss_cfg((_THRESHOLD_KEY, "2G"))
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_factor_greater_than_10(self):
        code = _ss_cfg((_FACTOR_KEY, "20"))
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "20" in fs[0].message

    def test_no_finding_for_default_threshold_256mb(self):
        code = _ss_cfg((_THRESHOLD_KEY, "256m"))
        assert findings(self.rule, code) == []

    def test_no_finding_for_threshold_exactly_1gb(self):
        # Exactly 1 GB → not strictly greater → no finding
        code = _ss_cfg((_THRESHOLD_KEY, "1g"))
        assert findings(self.rule, code) == []

    def test_no_finding_for_default_factor_5(self):
        code = _ss_cfg((_FACTOR_KEY, "5"))
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D05-005 — Missing salting pattern for known skewed keys
# =============================================================================


class TestMissingSaltingPatternRule:
    rule = MissingSaltingPatternRule()

    def test_fires_on_user_id_without_salting(self):
        code = SPARK_HDR + "df.join(events, 'user_id')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D05-005"
        assert "'user_id'" in fs[0].message

    def test_fires_on_customer_id(self):
        code = SPARK_HDR + "df.join(events, 'customer_id')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_product_id(self):
        code = SPARK_HDR + "df.join(events, 'product_id')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_rand_used(self):
        code = SPARK_HDR + (
            "df_salted = df.withColumn('salt', (rand()*10).cast('int'))\n"
            "df_salted.join(events, 'user_id')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_explode_used(self):
        code = SPARK_HDR + (
            "events_rep = events.withColumn('salt', explode(array([lit(i) for i in range(10)])))\n"
            "df.join(events_rep, 'user_id')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_skew_id_key(self):
        # 'order_id' is not in _SKEW_ID_NAMES
        code = SPARK_HDR + "df.join(events, 'order_id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_status_key(self):
        # status is low-cardinality (D05-001 territory), not a power-law ID
        code = SPARK_HDR + "df.join(other, 'status')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_multiple_skew_id_joins_all_flagged(self):
        code = SPARK_HDR + (
            "df.join(a, 'user_id')\n"
            "df.join(b, 'product_id')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 2


# =============================================================================
# SPL-D05-006 — Window function partitioned by skew-prone column
# =============================================================================


class TestWindowPartitionSkewRule:
    rule = WindowPartitionSkewRule()

    def test_fires_on_window_partition_by_status(self):
        code = SPARK_HDR + "w = Window.partitionBy('status').orderBy('ts')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D05-006"
        assert "'status'" in fs[0].message

    def test_fires_on_window_partition_by_type(self):
        code = SPARK_HDR + "w = Window.partitionBy('type')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_is_active(self):
        code = SPARK_HDR + "w = Window.partitionBy('is_active').orderBy('ts')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_secondary_high_cardinality_key(self):
        # status + user_id → user_id is high-cardinality → no finding
        code = SPARK_HDR + "w = Window.partitionBy('status', 'user_id').orderBy('ts')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_high_cardinality_only(self):
        code = SPARK_HDR + "w = Window.partitionBy('user_id').orderBy('ts')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_write_partition_by(self):
        # write.partitionBy is D04 territory — must not fire here
        code = SPARK_HDR + "df.write.partitionBy('status').parquet('path')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_no_string_keys(self):
        code = SPARK_HDR + "w = Window.partitionBy(col('status'))\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D05-007 — Null-heavy join key
# =============================================================================


class TestNullHeavyJoinKeyRule:
    rule = NullHeavyJoinKeyRule()

    def test_fires_on_parent_id(self):
        code = SPARK_HDR + "df.join(other, 'parent_id')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D05-007"
        assert "'parent_id'" in fs[0].message

    def test_fires_on_manager_id(self):
        code = SPARK_HDR + "df.join(other, 'manager_id')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_referred_by(self):
        code = SPARK_HDR + "df.join(other, 'referred_by')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_parent_prefix_column(self):
        code = SPARK_HDR + "df.join(other, 'parent_category_id')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_manager_prefix_column(self):
        code = SPARK_HDR + "df.join(other, 'manager_user_id')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_dropna_within_5_lines_before(self):
        code = SPARK_HDR + (
            "df2 = df.dropna(subset=['parent_id'])\n"
            "df2.join(other, 'parent_id')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_fillna_within_5_lines_before(self):
        code = SPARK_HDR + (
            "df2 = df.fillna({'parent_id': 0})\n"
            "df2.join(other, 'parent_id')\n"
        )
        assert findings(self.rule, code) == []

    def test_fires_when_dropna_more_than_5_lines_before(self):
        # 6 blank lines between dropna and join → outside suppression window
        null_line = "df2 = df.dropna(subset=['parent_id'])"
        join_line = "df2.join(other, 'parent_id')"
        lines = [null_line] + ["x = 1"] * 6 + [join_line]
        code = SPARK_HDR + "\n".join(lines) + "\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_non_null_prone_key(self):
        code = SPARK_HDR + "df.join(other, 'user_id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []
