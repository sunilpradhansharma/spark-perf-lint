"""Unit tests for D09 UDF and code pattern rules (SPL-D09-001 – SPL-D09-012).

Each rule has at least:
- A positive test (rule fires)
- A negative test (rule does not fire)

Tests call ``rule.check(analyzer, config)`` directly to avoid registry coupling.
"""

from __future__ import annotations

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.d09_udf_code import (
    CollectWithoutLimitRule,
    CountForEmptinessRule,
    ExplainInProductionRule,
    NestedUdfCallRule,
    PandasUdfWithoutTypeHintsRule,
    PythonUdfDetectedRule,
    RddConversionRule,
    RowByRowIterationRule,
    ShowInProductionRule,
    ToPandasWithoutLimitRule,
    UdfReplaceableRule,
    WithColumnInLoopRule,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SPARK_HDR = "from pyspark.sql import SparkSession\nfrom pyspark.sql.functions import *\n"
UDF_HDR = (
    "from pyspark.sql import SparkSession\n"
    "from pyspark.sql.functions import udf, pandas_udf, col\n"
    "from pyspark.sql.types import StringType, DoubleType\n"
)


def _a(code: str) -> ASTAnalyzer:
    return ASTAnalyzer.from_source(code, filename="test.py")


def _cfg(**kw) -> LintConfig:
    return LintConfig.from_dict(kw) if kw else LintConfig.from_dict({})


def findings(rule, code: str, **kw):
    return rule.check(_a(code), _cfg(**kw))


# =============================================================================
# SPL-D09-001 — Python UDF (row-at-a-time) detected
# =============================================================================


class TestPythonUdfDetectedRule:
    rule = PythonUdfDetectedRule()

    def test_fires_on_udf_decorator(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def my_func(s):\n"
            "    return s.lower().strip()\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D09-001"
        assert "my_func" in fs[0].message

    def test_fires_on_multiple_udfs(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def func_a(s):\n"
            "    return s.lower()\n\n"
            "@udf(returnType=StringType())\n"
            "def func_b(s):\n"
            "    return s.upper()\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 2

    def test_no_finding_for_pandas_udf(self):
        code = (
            UDF_HDR
            + "@pandas_udf(returnType=DoubleType())\n"
            "def my_pudf(s):\n"
            "    return s * 2\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_plain_function(self):
        code = (
            SPARK_HDR
            + "def my_func(s):\n"
            "    return s.lower()\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        # No pyspark import at all — has_spark_imports() returns False
        code = (
            "@udf\n"
            "def f(s):\n"
            "    return s\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D09-002 — Python UDF replaceable with native function
# =============================================================================


class TestUdfReplaceableRule:
    rule = UdfReplaceableRule()

    def test_fires_on_lower_udf(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def my_lower(s):\n"
            "    return s.lower()\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D09-002"
        assert "lower" in fs[0].message

    def test_fires_on_upper_udf(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def my_upper(s):\n"
            "    return s.upper()\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "upper" in fs[0].message

    def test_fires_on_strip_udf(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def my_strip(s):\n"
            "    return s.strip()\n"
        )
        assert len(findings(self.rule, code)) == 1

    def test_no_finding_for_complex_udf_body(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def my_func(s):\n"
            "    return s.lower().strip()\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_multi_statement_udf(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def my_func(s):\n"
            "    x = s.lower()\n"
            "    return x\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_pandas_udf(self):
        code = (
            UDF_HDR
            + "@pandas_udf(returnType=StringType())\n"
            "def my_pudf(s):\n"
            "    return s.str.lower()\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D09-003 — withColumn() inside loop
# =============================================================================


class TestWithColumnInLoopRule:
    rule = WithColumnInLoopRule()

    def test_fires_on_with_column_in_for_loop(self):
        code = (
            SPARK_HDR
            + "for c in ['a', 'b', 'c']:\n"
            "    df = df.withColumn(c, col(c) * 2)\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D09-003"
        assert "withColumn" in fs[0].message

    def test_fires_on_with_column_in_while_loop(self):
        code = (
            SPARK_HDR
            + "i = 0\n"
            "while i < 5:\n"
            "    df = df.withColumn(f'col_{i}', col(f'x_{i}'))\n"
            "    i += 1\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_with_column_outside_loop(self):
        code = (
            SPARK_HDR
            + "df = df.withColumn('new_col', col('x') * 2)\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        code = (
            "for c in cols:\n"
            "    df = df.withColumn(c, c)\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D09-004 — Row-by-row iteration over DataFrame
# =============================================================================


class TestRowByRowIterationRule:
    rule = RowByRowIterationRule()

    def test_fires_on_for_collect(self):
        code = (
            SPARK_HDR
            + "for row in df.collect():\n"
            "    process(row)\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D09-004"
        assert "collect" in fs[0].message

    def test_fires_on_for_to_local_iterator(self):
        code = (
            SPARK_HDR
            + "for row in df.toLocalIterator():\n"
            "    process(row)\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "toLocalIterator" in fs[0].message

    def test_no_finding_for_plain_list_iteration(self):
        code = (
            SPARK_HDR
            + "for item in my_list:\n"
            "    process(item)\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_range_loop(self):
        code = (
            SPARK_HDR
            + "for i in range(10):\n"
            "    do_something(i)\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        code = (
            "for row in df.collect():\n"
            "    process(row)\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D09-005 — .collect() without prior filter or limit
# =============================================================================


class TestCollectWithoutLimitRule:
    rule = CollectWithoutLimitRule()

    def test_fires_on_bare_collect(self):
        code = SPARK_HDR + "rows = df.join(other, 'id').collect()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D09-005"
        assert "collect" in fs[0].message

    def test_no_finding_with_limit_before_collect(self):
        code = SPARK_HDR + "rows = df.limit(100).collect()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_with_filter_before_collect(self):
        code = SPARK_HDR + "rows = df.filter('x > 0').collect()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_with_where_before_collect(self):
        code = SPARK_HDR + "rows = df.where('status = \"active\"').collect()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_with_head(self):
        code = SPARK_HDR + "rows = df.head(10)\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        code = "rows = df.collect()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D09-006 — .toPandas() without prior filter or limit
# =============================================================================


class TestToPandasWithoutLimitRule:
    rule = ToPandasWithoutLimitRule()

    def test_fires_on_bare_to_pandas(self):
        code = SPARK_HDR + "pdf = df.join(other, 'id').toPandas()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D09-006"
        assert "toPandas" in fs[0].message

    def test_no_finding_with_limit_before_to_pandas(self):
        code = SPARK_HDR + "pdf = df.limit(1000).toPandas()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_with_filter_before_to_pandas(self):
        code = SPARK_HDR + "pdf = df.filter('x > 0').toPandas()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_with_sample_before_to_pandas(self):
        code = SPARK_HDR + "pdf = df.sample(0.01).toPandas()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        code = "pdf = df.toPandas()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D09-007 — .count() for emptiness check
# =============================================================================


class TestCountForEmptinessRule:
    rule = CountForEmptinessRule()

    def test_fires_on_count_eq_zero(self):
        code = (
            SPARK_HDR
            + "if df.count() == 0:\n"
            "    raise ValueError('empty')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D09-007"
        assert "count" in fs[0].message

    def test_fires_on_count_gt_zero(self):
        code = (
            SPARK_HDR
            + "if df.count() > 0:\n"
            "    print('has data')\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_count_in_comparison_with_variable(self):
        code = (
            SPARK_HDR
            + "n = 100\n"
            "if df.count() < n:\n"
            "    raise ValueError('too few rows')\n"
        )
        assert len(findings(self.rule, code)) == 1

    def test_no_finding_for_count_assigned_to_variable(self):
        code = (
            SPARK_HDR
            + "row_count = df.count()\n"
            "print(row_count)\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        code = "if df.count() == 0:\n    pass\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D09-008 — .show() in production code
# =============================================================================


class TestShowInProductionRule:
    rule = ShowInProductionRule()

    def test_fires_on_show(self):
        code = SPARK_HDR + "df.groupBy('status').count().show()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D09-008"
        assert "show" in fs[0].message

    def test_fires_on_show_with_args(self):
        code = SPARK_HDR + "df.show(20, truncate=False)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_multiple_show_calls(self):
        code = (
            SPARK_HDR
            + "df.show()\n"
            "df.filter('x > 0').show(10)\n"
        )
        assert len(findings(self.rule, code)) == 2

    def test_no_finding_without_show(self):
        code = SPARK_HDR + "df.write.parquet('out')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        code = "df.show()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D09-009 — .explain() or .printSchema() in production code
# =============================================================================


class TestExplainInProductionRule:
    rule = ExplainInProductionRule()

    def test_fires_on_explain(self):
        code = SPARK_HDR + "df.join(other, 'id').explain(True)\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D09-009"
        assert "explain" in fs[0].message

    def test_fires_on_print_schema(self):
        code = SPARK_HDR + "df.printSchema()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "printSchema" in fs[0].message

    def test_fires_on_both_explain_and_print_schema(self):
        code = (
            SPARK_HDR
            + "df.explain()\n"
            "df.printSchema()\n"
        )
        assert len(findings(self.rule, code)) == 2

    def test_no_finding_without_explain_or_print_schema(self):
        code = SPARK_HDR + "df.write.parquet('out')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        code = "df.explain()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D09-010 — .rdd conversion
# =============================================================================


class TestRddConversionRule:
    rule = RddConversionRule()

    def test_fires_on_rdd_access(self):
        code = SPARK_HDR + "result = df.rdd.map(lambda r: r['id']).collect()\n"
        fs = findings(self.rule, code)
        assert len(fs) >= 1
        assert any(f.rule_id == "SPL-D09-010" for f in fs)
        assert any("rdd" in f.message for f in fs)

    def test_fires_on_rdd_without_method(self):
        code = SPARK_HDR + "rdd = df.rdd\n"
        fs = findings(self.rule, code)
        assert len(fs) >= 1

    def test_no_finding_without_rdd_access(self):
        code = SPARK_HDR + "df.select('id').write.parquet('out')\n"
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        code = "rdd = df.rdd\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D09-011 — pandas_udf without type hints
# =============================================================================


class TestPandasUdfWithoutTypeHintsRule:
    rule = PandasUdfWithoutTypeHintsRule()

    def test_fires_on_pandas_udf_without_hints(self):
        code = (
            UDF_HDR
            + "@pandas_udf(returnType=DoubleType())\n"
            "def my_udf(col):\n"
            "    return col * 2\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D09-011"
        assert "my_udf" in fs[0].message

    def test_no_finding_with_return_type_hint(self):
        code = (
            UDF_HDR
            + "import pandas as pd\n"
            "@pandas_udf('double')\n"
            "def my_udf(col: pd.Series) -> pd.Series:\n"
            "    return col * 2\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_with_param_annotation_only(self):
        code = (
            UDF_HDR
            + "import pandas as pd\n"
            "@pandas_udf('double')\n"
            "def my_udf(col: pd.Series):\n"
            "    return col * 2\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_regular_udf(self):
        # @udf without type hints is caught by D09-001, not D09-011
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def my_udf(s):\n"
            "    return s.lower()\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D09-012 — Nested UDF calls
# =============================================================================


class TestNestedUdfCallRule:
    rule = NestedUdfCallRule()

    def test_fires_when_outer_udf_calls_inner_udf(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def inner(s):\n"
            "    return s.strip()\n\n"
            "@udf(returnType=StringType())\n"
            "def outer(s):\n"
            "    return inner(s).lower()\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D09-012"
        assert "outer" in fs[0].message
        assert "inner" in fs[0].message

    def test_fires_when_pandas_udf_calls_regular_udf(self):
        # inner is called directly (not passed as argument) inside the pandas_udf body
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def inner(s):\n"
            "    return s.strip()\n\n"
            "@pandas_udf(returnType=StringType())\n"
            "def main_udf(col):\n"
            "    return inner(col)\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_udf_calls_plain_function(self):
        code = (
            UDF_HDR
            + "def helper(s):\n"
            "    return s.strip()\n\n"
            "@udf(returnType=StringType())\n"
            "def my_udf(s):\n"
            "    return helper(s).lower()\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_with_single_udf(self):
        code = (
            UDF_HDR
            + "@udf(returnType=StringType())\n"
            "def my_udf(s):\n"
            "    return s.lower()\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_without_spark_imports(self):
        assert findings(self.rule, "x = 1\n") == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []
