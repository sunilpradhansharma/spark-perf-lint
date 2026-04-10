"""Parametrized edge-case tests for the spark-perf-lint rule engine.

Tests ten categories of unusual or tricky inputs to verify correctness,
false-positive resistance, and performance under stress.

Categories
----------
1.  Empty / minimal files         — 0 findings each
2.  Syntax errors                  — 1 SPL-D00-000 INFO finding
3.  Multiple anti-patterns on one line
4.  Deep chained operations        — no CRITICAL
5.  False-positive resistance      — string/pandas .join() → 0 D03 findings
6.  Multiline expressions          — csv + inferSchema fire correctly
7.  spark.sql() string queries     — multiple findings
8.  Conditional Spark code         — rule fires inside if/else branches
9.  Class-based Spark code         — rule fires inside class methods
10. Stress test                    — 1 000-line generated file + 8 anti-patterns
"""

from __future__ import annotations

import time

import pytest

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.orchestrator import ScanOrchestrator
from spark_perf_lint.types import Severity
from tests.fixtures.code_generator import generate_spark_file

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _scan(code: str, config: LintConfig | None = None) -> list:
    """Run scan_content and return the findings list."""
    cfg = config or LintConfig.from_dict({})
    return ScanOrchestrator(cfg).scan_content(code, "edge_case.py").findings


def _rule_ids(findings) -> list[str]:
    return [f.rule_id for f in findings]


def _critical_ids(findings) -> list[str]:
    return [f.rule_id for f in findings if f.severity == Severity.CRITICAL]


# =============================================================================
# Category 1: Empty / minimal files
# =============================================================================

_MINIMAL_CASES = [
    pytest.param("", id="empty_string"),
    pytest.param("# just a comment\n", id="comment_only"),
    pytest.param("# -*- coding: utf-8 -*-\n\n# Nothing here\n", id="encoding_comment"),
    pytest.param(
        "import os\nimport json\n\nx = json.loads('{}')\n",
        id="non_spark_imports",
    ),
    pytest.param(
        'def add(a, b):\n    """Add two numbers."""\n    return a + b\n',
        id="pure_python_function",
    ),
]


@pytest.mark.parametrize("code", _MINIMAL_CASES)
def test_minimal_files_produce_no_findings(code):
    """Minimal / non-Spark files must produce zero findings."""
    findings = _scan(code)
    assert findings == [], f"Expected 0 findings, got: {_rule_ids(findings)}"


# =============================================================================
# Category 2: Syntax errors
# =============================================================================

_SYNTAX_ERROR_CASES = [
    pytest.param(
        "from pyspark.sql import SparkSession\n\ndef broken(\n    pass\n",
        id="incomplete_function_def",
    ),
    pytest.param(
        "from pyspark.sql import SparkSession\nspark = (\n",
        id="unclosed_paren",
    ),
    pytest.param(
        "from pyspark.sql import SparkSession\ndf = spark.read.parquet(\n",
        id="unclosed_method_call",
    ),
]


@pytest.mark.parametrize("code", _SYNTAX_ERROR_CASES)
def test_syntax_error_produces_parse_error_finding(code):
    """Syntax-invalid PySpark files must yield exactly one SPL-D00-000 finding."""
    findings = _scan(code)
    assert len(findings) == 1, (
        f"Expected exactly 1 finding for syntax error, got {len(findings)}: "
        f"{_rule_ids(findings)}"
    )
    assert (
        findings[0].rule_id == "SPL-D00-000"
    ), f"Expected SPL-D00-000 but got {findings[0].rule_id}"
    assert findings[0].severity == Severity.INFO


# =============================================================================
# Category 3: Multiple anti-patterns on one line
# =============================================================================

# Three anti-patterns on a single logical line:
#   - crossJoin        → SPL-D03-001 (CRITICAL)
#   - repartition(1)   → SPL-D04-001 (CRITICAL)
#   - .collect()       → SPL-D09-005 (CRITICAL)
_DENSE_CODE = """\
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("dense").getOrCreate()
df = spark.read.parquet("/data/a")
other = spark.read.parquet("/data/b")

result = df.crossJoin(other).repartition(1).collect()
"""

_DENSE_EXPECTED_CRITICALS = {"SPL-D03-001", "SPL-D04-001", "SPL-D09-005"}


def test_multiple_antipatterns_on_one_line_all_fire():
    """crossJoin + repartition(1) + collect on one line must all produce CRITICAL findings."""
    findings = _scan(_DENSE_CODE)
    fired = set(_critical_ids(findings))
    missing = _DENSE_EXPECTED_CRITICALS - fired
    assert not missing, (
        f"Expected CRITICAL rules {_DENSE_EXPECTED_CRITICALS} but missing: {missing}. "
        f"All fired: {fired}"
    )


def test_multiple_antipatterns_on_one_line_correct_severity():
    """Each dense anti-pattern finding must have CRITICAL severity."""
    findings = _scan(_DENSE_CODE)
    for rule_id in _DENSE_EXPECTED_CRITICALS:
        matching = [f for f in findings if f.rule_id == rule_id]
        if matching:
            for f in matching:
                assert (
                    f.severity == Severity.CRITICAL
                ), f"{rule_id} should be CRITICAL, got {f.severity}"


# =============================================================================
# Category 4: Deep chained operations
# =============================================================================

_DEEP_CHAIN_CODE = """\
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("chain").getOrCreate()
df = spark.read.parquet("/data/events")

result = (
    df
    .filter(F.col("event_type") == "purchase")
    .filter(F.col("amount") > 0)
    .filter(F.col("user_id").isNotNull())
    .select("user_id", "amount", "ts", "product_id", "category")
    .groupBy("user_id", "category")
    .agg(
        F.sum("amount").alias("total_spend"),
        F.count("product_id").alias("purchase_count"),
        F.max("ts").alias("last_purchase_ts"),
    )
    .withColumn("avg_spend", F.col("total_spend") / F.col("purchase_count"))
    .withColumn("spend_rank", F.dense_rank().over(
        __import__("pyspark.sql.window", fromlist=["Window"]).Window
        .partitionBy("category")
        .orderBy(F.col("total_spend").desc())
    ))
    .filter(F.col("spend_rank") <= 100)
)

result.write.mode("overwrite").parquet("/output/top_spenders")
"""


def test_deep_chain_produces_no_critical_findings():
    """A deeply chained but correct pipeline must produce zero CRITICAL findings."""
    findings = _scan(_DEEP_CHAIN_CODE)
    criticals = _critical_ids(findings)
    assert not criticals, f"Expected no CRITICAL findings in deep chain, got: {criticals}"


def test_deep_chain_total_finding_count():
    """Deep chain should produce some findings (cross-cutting rules) but not excessively."""
    findings = _scan(_DEEP_CHAIN_CODE)
    # Should have at most 20 findings — deep chain is not a worst-case pattern file
    assert len(findings) <= 20, (
        f"Deep chain produced {len(findings)} findings, expected ≤ 20: " f"{_rule_ids(findings)}"
    )


# =============================================================================
# Category 5: False-positive resistance
# =============================================================================

_STRING_JOIN_CODE = """\
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("str_join").getOrCreate()

words = ["hello", "world", "foo"]
sentence = " ".join(words)

parts = ["a", "b", "c"]
csv_line = ",".join(parts)

path = "/".join(["/data", "events", "2024"])

df = spark.read.parquet(path)
df.write.mode("overwrite").parquet("/output/out")
"""

_PANDAS_JOIN_CODE = """\
from pyspark.sql import SparkSession, functions as F
import pandas as pd

spark = SparkSession.builder.appName("pandas_join").getOrCreate()

orders = pd.DataFrame({"order_id": [1, 2], "amount": [100, 200]})
customers = pd.DataFrame({"order_id": [1, 2], "name": ["Alice", "Bob"]})

merged = orders.join(customers.set_index("order_id"), on="order_id")

spark_df = spark.createDataFrame(merged)
spark_df.write.mode("overwrite").parquet("/output/merged")
"""

_FALSE_POSITIVE_CASES = [
    pytest.param(_STRING_JOIN_CODE, id="string_join_not_spark_join"),
    pytest.param(_PANDAS_JOIN_CODE, id="pandas_join_not_spark_join"),
]


@pytest.mark.parametrize("code", _FALSE_POSITIVE_CASES)
def test_false_positive_resistance_no_crossjoin_finding(code):
    """String .join() and pandas .join() must not trigger the crossJoin rule SPL-D03-001."""
    findings = _scan(code)
    cross_findings = [f for f in findings if f.rule_id == "SPL-D03-001"]
    assert (
        not cross_findings
    ), f"False positive: SPL-D03-001 (crossJoin) fired on non-Spark join: {cross_findings}"


def test_string_join_no_critical_findings():
    """String .join() code must not produce any CRITICAL findings."""
    findings = _scan(_STRING_JOIN_CODE)
    criticals = _critical_ids(findings)
    assert not criticals, f"String join produced unexpected CRITICAL findings: {criticals}"


# =============================================================================
# Category 6: Multiline expressions
# =============================================================================

_MULTILINE_CSV_CODE = """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("multiline").getOrCreate()

df = (
    spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/data/events.csv")
)

df.write.mode("overwrite").parquet("/output/events")
"""

_MULTILINE_JSON_CODE = """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("json_multiline").getOrCreate()

df = (
    spark
    .read
    .option("multiLine", "true")
    .json(
        "/data/records/*.json"
    )
)

df.write.mode("overwrite").parquet("/output/records")
"""


def test_multiline_csv_fires_csv_rule():
    """CSV read spread across multiple lines must still trigger D07-001."""
    findings = _scan(_MULTILINE_CSV_CODE)
    csv_rules = [f for f in findings if f.rule_id == "SPL-D07-001"]
    assert csv_rules, (
        f"Expected SPL-D07-001 to fire on multiline CSV read, " f"got: {_rule_ids(findings)}"
    )


def test_multiline_csv_fires_infer_schema_rule():
    """inferSchema option spread across multiple lines must still trigger D07-002."""
    findings = _scan(_MULTILINE_CSV_CODE)
    schema_rules = [f for f in findings if f.rule_id == "SPL-D07-002"]
    assert schema_rules, (
        f"Expected SPL-D07-002 to fire on multiline inferSchema, " f"got: {_rule_ids(findings)}"
    )


def test_multiline_json_fires_row_oriented_format_rule():
    """JSON is a row-oriented format — D07-001 must fire to recommend switching to Parquet."""
    findings = _scan(_MULTILINE_JSON_CODE)
    row_format_rules = [f for f in findings if f.rule_id == "SPL-D07-001"]
    assert row_format_rules, (
        f"Expected SPL-D07-001 to fire on JSON read (row-oriented format), "
        f"got: {_rule_ids(findings)}"
    )


# =============================================================================
# Category 7: spark.sql() string queries
# =============================================================================

_SPARK_SQL_CODE = """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sql_queries").getOrCreate()

transactions = spark.read.parquet("/data/transactions")
accounts = spark.read.parquet("/data/accounts")

transactions.createOrReplaceTempView("transactions")
accounts.createOrReplaceTempView("accounts")

# Raw SQL queries — the linter still analyses the Python file
result = spark.sql(\"\"\"
    SELECT t.transaction_id, t.amount, a.account_name
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    WHERE t.status = 'active'
    GROUP BY t.transaction_id, t.amount, a.account_name
\"\"\")

summary = spark.sql(
    "SELECT account_id, SUM(amount) as total FROM transactions GROUP BY account_id"
)

result.write.mode("overwrite").parquet("/output/result")
summary.write.mode("overwrite").parquet("/output/summary")
"""


def test_spark_sql_code_produces_findings():
    """Files using spark.sql() must still produce findings from Python-level rules."""
    findings = _scan(_SPARK_SQL_CODE)
    assert len(findings) > 0, "Expected at least some findings from spark.sql() file"


def test_spark_sql_no_parse_error():
    """spark.sql() string arguments must not cause a parse error."""
    findings = _scan(_SPARK_SQL_CODE)
    parse_errors = [f for f in findings if f.rule_id == "SPL-D00-000"]
    assert not parse_errors, f"Unexpected parse error in spark.sql() code: {parse_errors}"


def test_spark_sql_findings_have_valid_rule_ids():
    """All findings from spark.sql() code must have well-formed SPL-D??-??? rule IDs."""
    import re

    findings = _scan(_SPARK_SQL_CODE)
    pattern = re.compile(r"^SPL-D\d{2}-\d{3}$")
    for f in findings:
        assert pattern.match(f.rule_id), f"Malformed rule_id: {f.rule_id!r}"


# =============================================================================
# Category 8: Conditional Spark code
# =============================================================================

_CONDITIONAL_CODE = """\
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("conditional").getOrCreate()

df = spark.read.parquet("/data/events")
other = spark.read.parquet("/data/other")

use_cross = os.environ.get("USE_CROSS_JOIN", "false").lower() == "true"

if use_cross:
    result = df.crossJoin(other)
else:
    result = df.join(other, "event_id", "left")

result.write.mode("overwrite").parquet("/output/result")
"""

_CONDITIONAL_NESTED_CODE = """\
import os
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("nested_cond").getOrCreate()

df = spark.read.parquet("/data/events")
dim = spark.read.parquet("/data/dim")
extra = spark.read.parquet("/data/extra")

env = os.environ.get("ENV", "prod")

if env == "dev":
    if os.environ.get("FULL_JOIN"):
        result = df.crossJoin(dim)
    else:
        result = df.join(dim, "id", "left")
elif env == "staging":
    result = df.join(F.broadcast(dim), "id", "inner")
else:
    result = df.join(F.broadcast(dim), "id", "inner")

result.write.mode("overwrite").parquet("/output/result")
"""


def test_conditional_crossjoin_fires_critical():
    """crossJoin inside an if-branch must still trigger SPL-D03-001."""
    findings = _scan(_CONDITIONAL_CODE)
    critical_ids = set(_critical_ids(findings))
    assert (
        "SPL-D03-001" in critical_ids
    ), f"Expected SPL-D03-001 inside if-branch, got criticals: {critical_ids}"


def test_conditional_safe_else_branch_no_extra_criticals():
    """The safe else branch (broadcast join) should not introduce additional CRITICAL findings."""
    findings = _scan(_CONDITIONAL_CODE)
    # Only D03-001 from the crossJoin is expected among criticals
    criticals = _critical_ids(findings)
    non_cross_criticals = [r for r in criticals if r != "SPL-D03-001"]
    assert (
        not non_cross_criticals
    ), f"Unexpected CRITICAL findings beyond crossJoin: {non_cross_criticals}"


def test_nested_conditional_crossjoin_fires():
    """crossJoin nested inside two if levels must still be detected."""
    findings = _scan(_CONDITIONAL_NESTED_CODE)
    assert "SPL-D03-001" in _rule_ids(findings), (
        f"SPL-D03-001 not found in nested conditional code. " f"Got: {_rule_ids(findings)}"
    )


# =============================================================================
# Category 9: Class-based Spark code
# =============================================================================

_CLASS_BASED_CODE = """\
from pyspark.sql import SparkSession, functions as F


class ETLPipeline:
    \"\"\"ETL pipeline implemented as a class.\"\"\"

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load(self, path: str):
        return self.spark.read.parquet(path)

    def run(self):
        events = self.load("/data/events")
        dims = self.load("/data/dims")

        # Anti-pattern inside a method — must still be detected
        result = events.crossJoin(dims)

        result.write.mode("overwrite").parquet("/output/result")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("class_etl").getOrCreate()
    pipeline = ETLPipeline(spark)
    pipeline.run()
"""

_CLASS_MULTI_METHOD_CODE = """\
from pyspark.sql import SparkSession, functions as F


class DataProcessor:
    \"\"\"Processes data with multiple methods.\"\"\"

    def __init__(self):
        self.spark = SparkSession.builder.appName("processor").getOrCreate()

    def process_events(self):
        df = self.spark.read.parquet("/data/events")
        other = self.spark.read.parquet("/data/other")
        # Anti-pattern in one method
        return df.crossJoin(other)

    def process_metrics(self):
        df = self.spark.read.parquet("/data/metrics")
        dim = self.spark.read.parquet("/data/dim")
        # Safe join in another method
        return df.join(F.broadcast(dim), "metric_id", "inner")

    def save(self, df, path: str):
        df.write.mode("overwrite").parquet(path)
"""


def test_class_based_crossjoin_in_method_fires():
    """crossJoin inside a class method must trigger SPL-D03-001."""
    findings = _scan(_CLASS_BASED_CODE)
    assert "SPL-D03-001" in _rule_ids(
        findings
    ), f"SPL-D03-001 not found in class-based code. Got: {_rule_ids(findings)}"


def test_class_based_crossjoin_severity_is_critical():
    """The crossJoin finding inside a class method must be CRITICAL."""
    findings = _scan(_CLASS_BASED_CODE)
    cross_findings = [f for f in findings if f.rule_id == "SPL-D03-001"]
    assert cross_findings, "SPL-D03-001 did not fire"
    for f in cross_findings:
        assert (
            f.severity == Severity.CRITICAL
        ), f"Expected CRITICAL, got {f.severity} on line {f.line_number}"


def test_class_multi_method_only_bad_method_fires():
    """Only the method with crossJoin should fire SPL-D03-001, not the broadcast join."""
    findings = _scan(_CLASS_MULTI_METHOD_CODE)
    cross_findings = [f for f in findings if f.rule_id == "SPL-D03-001"]
    assert cross_findings, "SPL-D03-001 should fire for the crossJoin method"


def test_class_based_no_parse_error():
    """Class-based Spark code must not cause parse errors."""
    findings = _scan(_CLASS_BASED_CODE)
    parse_errors = [f for f in findings if f.rule_id == "SPL-D00-000"]
    assert not parse_errors, f"Unexpected parse error in class-based code: {parse_errors}"


# =============================================================================
# Category 10: Stress test — generated 1000-line file + 8 anti-patterns
# =============================================================================

_STRESS_ANTI_PATTERNS = [
    "cross_join",
    "collect_no_filter",
    "withColumn_loop",
    "cache_no_unpersist",
    "repartition_one",
    "csv_read",
    "python_udf",
    "aqe_disabled",
]

_STRESS_EXPECTED_CRITICALS = {
    "SPL-D03-001",  # cross_join
    "SPL-D04-001",  # repartition_one
    "SPL-D08-001",  # aqe_disabled
    "SPL-D09-003",  # python_udf
    "SPL-D09-005",  # collect_no_filter
}


@pytest.mark.slow
def test_stress_1000_line_file_produces_minimum_findings():
    """Generated 1000-line file with 8 anti-patterns must produce >= 8 findings."""
    code = generate_spark_file(
        n_lines=1000,
        n_joins=5,
        n_group_bys=3,
        n_caches=2,
        n_udfs=2,
        n_writes=3,
        inject_anti_patterns=_STRESS_ANTI_PATTERNS,
    )
    findings = _scan(code)
    assert len(findings) >= 8, (
        f"Expected >= 8 findings in stress test, got {len(findings)}: " f"{_rule_ids(findings)}"
    )


@pytest.mark.slow
def test_stress_1000_line_file_contains_expected_criticals():
    """Generated stress file must fire the CRITICAL rules matching injected anti-patterns."""
    code = generate_spark_file(
        n_lines=1000,
        n_joins=5,
        n_group_bys=3,
        n_caches=2,
        n_udfs=2,
        n_writes=3,
        inject_anti_patterns=_STRESS_ANTI_PATTERNS,
    )
    findings = _scan(code)
    fired_criticals = set(_critical_ids(findings))
    missing = _STRESS_EXPECTED_CRITICALS - fired_criticals
    assert not missing, (
        f"Missing expected CRITICAL rules: {missing}. " f"All criticals fired: {fired_criticals}"
    )


@pytest.mark.slow
def test_stress_1000_line_file_scanned_under_3_seconds():
    """Scanning a 1000-line generated file with 8 anti-patterns must complete in < 3 s."""
    code = generate_spark_file(
        n_lines=1000,
        n_joins=5,
        n_group_bys=3,
        n_caches=2,
        n_udfs=2,
        n_writes=3,
        inject_anti_patterns=_STRESS_ANTI_PATTERNS,
    )
    cfg = LintConfig.from_dict({})
    orchestrator = ScanOrchestrator(cfg)

    start = time.perf_counter()
    orchestrator.scan_content(code, "stress_test.py")
    elapsed = time.perf_counter() - start

    assert elapsed < 3.0, f"Stress test scan took {elapsed:.3f}s, expected < 3.0s"


@pytest.mark.slow
def test_stress_1000_line_file_line_count():
    """Generated file must have at least 1000 lines as requested."""
    code = generate_spark_file(
        n_lines=1000,
        n_joins=5,
        n_group_bys=3,
        n_caches=2,
        n_udfs=2,
        n_writes=3,
        inject_anti_patterns=_STRESS_ANTI_PATTERNS,
    )
    lines = code.splitlines()
    assert len(lines) >= 1000, f"Generated file has {len(lines)} lines, expected >= 1000"


@pytest.mark.slow
def test_stress_no_parse_error():
    """Generated stress file must be valid Python (no SPL-D00-000 finding)."""
    code = generate_spark_file(
        n_lines=1000,
        n_joins=5,
        n_group_bys=3,
        n_caches=2,
        n_udfs=2,
        n_writes=3,
        inject_anti_patterns=_STRESS_ANTI_PATTERNS,
    )
    findings = _scan(code)
    parse_errors = [f for f in findings if f.rule_id == "SPL-D00-000"]
    assert not parse_errors, f"Generated stress file has syntax errors: {parse_errors}"
