"""Unit tests for D10 Catalyst optimizer rules (SPL-D10-001 – SPL-D10-006).

Each rule has at least:
- A positive test (rule fires)
- A negative test (rule does not fire)

Tests call ``rule.check(analyzer, config)`` directly to avoid registry coupling.
"""

from __future__ import annotations

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.d10_catalyst import (
    CboNotEnabledRule,
    DeepMethodChainRule,
    JoinReorderDisabledRule,
    NondeterministicInFilterRule,
    TableStatsNotCollectedRule,
    UdfBlocksPredicatePushdownRule,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SPARK_HDR = "from pyspark.sql import SparkSession\nfrom pyspark.sql.functions import *\n"
UDF_HDR = (
    "from pyspark.sql import SparkSession\n"
    "from pyspark.sql.functions import udf, col\n"
    "from pyspark.sql.types import StringType\n"
)

_SS = "from pyspark.sql import SparkSession\nspark = SparkSession.builder"


def _ss_cfg(*kvpairs: tuple[str, str]) -> str:
    chain = "".join(f'.config("{k}", "{v}")' for k, v in kvpairs)
    return f"{_SS}{chain}.getOrCreate()\n"


def _a(code: str) -> ASTAnalyzer:
    return ASTAnalyzer.from_source(code, filename="test.py")


def _cfg(**kw) -> LintConfig:
    return LintConfig.from_dict(kw) if kw else LintConfig.from_dict({})


def findings(rule, code: str, **kw):
    return rule.check(_a(code), _cfg(**kw))


# =============================================================================
# SPL-D10-001 — UDF blocks predicate pushdown
# =============================================================================


class TestUdfBlocksPredicatePushdownRule:
    rule = UdfBlocksPredicatePushdownRule()

    def test_fires_when_udf_and_filter_present(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def norm(s):\n"
            "    return s.lower()\n\n"
            "df.withColumn('n', norm(col('s'))).filter(col('n') == 'x')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D10-001"
        assert "norm" in fs[0].message

    def test_fires_when_udf_and_where_present(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def clean(s):\n"
            "    return s.strip()\n\n"
            "df.where(col('status') == 'active')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "clean" in fs[0].message

    def test_fires_once_per_udf_when_multiple_udfs(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def f1(s):\n"
            "    return s.lower()\n\n"
            "@udf(returnType=StringType())\n"
            "def f2(s):\n"
            "    return s.upper()\n\n"
            "df.filter(col('x') > 0)\n"
        )
        assert len(findings(self.rule, code)) == 2

    def test_no_finding_when_udf_but_no_filter(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def norm(s):\n"
            "    return s.lower()\n\n"
            "df.withColumn('n', norm(col('s'))).write.parquet('out')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_pandas_udf_with_filter(self):
        # D10-001 specifically targets @udf (row-at-a-time), not pandas_udf
        code = (
            "from pyspark.sql import SparkSession\n"
            "from pyspark.sql.functions import pandas_udf, col\n"
            "from pyspark.sql.types import StringType\n"
            "@pandas_udf(returnType=StringType())\n"
            "def clean(s):\n"
            "    return s.str.strip()\n\n"
            "df.filter(col('x') > 0)\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D10-002 — CBO not enabled for complex queries
# =============================================================================


class TestCboNotEnabledRule:
    rule = CboNotEnabledRule()

    def test_fires_on_two_joins_without_cbo(self):
        code = (
            SPARK_HDR
            + "result = a.join(b, 'id').join(c, 'key')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D10-002"
        assert "cbo.enabled" in fs[0].message

    def test_fires_on_three_joins_without_cbo(self):
        code = (
            SPARK_HDR
            + "r = a.join(b, 'id').join(c, 'k').join(d, 'ref')\n"
        )
        assert len(findings(self.rule, code)) == 1

    def test_no_finding_when_cbo_enabled(self):
        code = (
            _ss_cfg(("spark.sql.cbo.enabled", "true"))
            + "result = a.join(b, 'id').join(c, 'key')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_single_join(self):
        code = SPARK_HDR + "r = a.join(b, 'id')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_no_joins(self):
        code = SPARK_HDR + "df.filter('x > 0').groupBy('k').count()\n"
        assert findings(self.rule, code) == []

    def test_fires_when_cbo_explicitly_false(self):
        code = (
            _ss_cfg(("spark.sql.cbo.enabled", "false"))
            + "result = a.join(b, 'id').join(c, 'key')\n"
        )
        assert len(findings(self.rule, code)) == 1

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D10-003 — Join reordering disabled for multi-table joins
# =============================================================================


class TestJoinReorderDisabledRule:
    rule = JoinReorderDisabledRule()

    def test_fires_on_three_joins_without_reorder(self):
        code = (
            SPARK_HDR
            + "r = a.join(b, 'id').join(c, 'k').join(d, 'ref')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D10-003"
        assert "joinReorder" in fs[0].message

    def test_no_finding_when_join_reorder_enabled(self):
        code = (
            _ss_cfg(("spark.sql.cbo.joinReorder.enabled", "true"))
            + "r = a.join(b, 'id').join(c, 'k').join(d, 'ref')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_two_joins(self):
        # Threshold is 3; two joins should not fire
        code = SPARK_HDR + "r = a.join(b, 'id').join(c, 'key')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_one_join(self):
        code = SPARK_HDR + "r = a.join(b, 'id')\n"
        assert findings(self.rule, code) == []

    def test_fires_with_four_joins(self):
        code = (
            SPARK_HDR
            + "r = a.join(b, 'id').join(c, 'k').join(d, 'ref').join(e, 'z')\n"
        )
        assert len(findings(self.rule, code)) == 1

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D10-004 — Table statistics not collected
# =============================================================================


class TestTableStatsNotCollectedRule:
    rule = TableStatsNotCollectedRule()

    def test_fires_when_join_without_analyze(self):
        code = (
            SPARK_HDR
            + "result = spark.table('events').join(spark.table('users'), 'id')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D10-004"
        assert "ANALYZE TABLE" in fs[0].message

    def test_no_finding_when_analyze_table_present(self):
        code = (
            SPARK_HDR
            + "spark.sql('ANALYZE TABLE events COMPUTE STATISTICS FOR ALL COLUMNS')\n"
            "result = spark.table('events').join(spark.table('users'), 'id')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_analyze_table_uppercase(self):
        code = (
            SPARK_HDR
            + "spark.sql('ANALYZE TABLE users COMPUTE STATISTICS')\n"
            "result = a.join(b, 'id')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_without_joins(self):
        code = SPARK_HDR + "df.filter('x > 0').groupBy('k').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_fires_on_multiple_joins_without_analyze(self):
        code = (
            SPARK_HDR
            + "r = a.join(b, 'id').join(c, 'k').join(d, 'ref')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "3" in fs[0].message


# =============================================================================
# SPL-D10-005 — Non-deterministic function in filter
# =============================================================================


class TestNondeterministicInFilterRule:
    rule = NondeterministicInFilterRule()

    def test_fires_on_rand_in_filter(self):
        code = SPARK_HDR + "sample = df.filter(rand() < 0.1)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D10-005"
        assert "rand" in fs[0].message

    def test_fires_on_randn_in_filter(self):
        code = SPARK_HDR + "sample = df.filter(randn() > 0)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "randn" in fs[0].message

    def test_fires_on_uuid_in_where(self):
        code = SPARK_HDR + "sample = df.where(uuid() != 'x')\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "uuid" in fs[0].message

    def test_fires_on_monotonically_increasing_id_in_filter(self):
        code = SPARK_HDR + "sample = df.filter(monotonically_increasing_id() < 100)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_deterministic_filter(self):
        code = SPARK_HDR + "filtered = df.filter(col('x') > 0)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_rand_outside_filter(self):
        code = SPARK_HDR + "df2 = df.withColumn('r', rand())\n"
        assert findings(self.rule, code) == []

    def test_fires_once_per_filter_with_nondeterministic(self):
        code = (
            SPARK_HDR
            + "s1 = df.filter(rand() < 0.1)\n"
            "s2 = df.filter(randn() > 0)\n"
        )
        assert len(findings(self.rule, code)) == 2

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D10-006 — Complex nested query plan risk (>20 chained ops)
# =============================================================================

# Build a helper to construct a deeply chained expression
def _chain(n: int) -> str:
    """Return a Python expression that chains n filter() calls on df."""
    return "df" + ".filter('x > 0')" * n + "\n"


class TestDeepMethodChainRule:
    rule = DeepMethodChainRule()

    def test_fires_on_chain_of_21(self):
        code = SPARK_HDR + "result = " + _chain(21)
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D10-006"
        assert "21" in fs[0].message

    def test_fires_on_chain_of_30(self):
        code = SPARK_HDR + "result = " + _chain(30)
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_chain_of_20(self):
        # Exactly at threshold — not strictly greater → no finding
        code = SPARK_HDR + "result = " + _chain(20)
        assert findings(self.rule, code) == []

    def test_no_finding_for_short_chain(self):
        code = (
            SPARK_HDR
            + "result = df.filter('x > 0').dropDuplicates(['id']).join(other, 'id')\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_standalone_expression_under_threshold(self):
        code = SPARK_HDR + _chain(5)
        assert findings(self.rule, code) == []

    def test_fires_on_standalone_expression_over_threshold(self):
        # Expression statement (no assignment) with deep chain
        code = SPARK_HDR + _chain(25)
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []
