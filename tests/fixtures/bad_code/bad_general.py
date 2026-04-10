"""Fraud-detection scoring pipeline — bad Catalyst and observability patterns.

Scores transactions against a rule-based fraud model.  The pipeline
blocks Catalyst optimisations (D10) and has no production observability
(D11): no metrics logging, hardcoded paths, and missing error handling.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

spark = (
    SparkSession.builder
    .appName("fraud_detection_scoring")
    .config("spark.sql.shuffle.partitions", "400")
    # CBO and join-reorder are both off — Catalyst uses heuristic join
    # ordering only, which is suboptimal for a 4-table star schema.
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# UDFs that block predicate pushdown
# ---------------------------------------------------------------------------

# SPL-D10-001 / SPL-D09-001: Python UDFs are black boxes to Catalyst.
# Any filter on a UDF-derived column cannot be pushed down to the data
# source — Spark must deserialise every row before evaluating it.    [WARNING]
@F.udf(IntegerType())
def risk_band(score: float) -> int:
    """Map a raw fraud score to a 1-5 risk band."""
    if score is None:
        return 0
    if score >= 0.9:
        return 5
    if score >= 0.7:
        return 4
    if score >= 0.5:
        return 3
    if score >= 0.3:
        return 2
    return 1


# ---------------------------------------------------------------------------
# Load tables — hardcoded paths, no abstraction layer
# ---------------------------------------------------------------------------

# SPL-D11-005 (×4): every path is a hardcoded string literal.  In CI
# and staging environments these paths do not exist, and the job silently
# reads zero rows.  Externalise paths to environment variables or a
# config file.                                                         [INFO]
transactions = spark.read.parquet("/data/transactions/current")
accounts     = spark.read.parquet("/data/accounts/v2")
rules        = spark.read.parquet("/data/fraud_rules/latest")
watchlist    = spark.read.parquet("/data/watchlist")

# ---------------------------------------------------------------------------
# Multi-join scoring pipeline — CBO absent
# ---------------------------------------------------------------------------

# SPL-D10-002: this file contains 3 joins and spark.sql.cbo.enabled is
# not set.  Without CBO, Catalyst cannot estimate cardinalities and may
# choose the wrong join order (e.g. start with the largest table).   [WARNING]
#
# SPL-D10-003: spark.sql.cbo.joinReorder.enabled is also absent, so
# the three-way join will execute in the order written.              [INFO]
scored = (
    transactions
    .join(accounts,  on="account_id", how="left")
    .join(rules,     on="rule_set_id", how="inner")
    .join(watchlist, on="entity_id",  how="left")
    .withColumn("fraud_score",
                F.col("base_score") * F.col("rule_weight") * F.col("account_risk_factor"))
    .withColumn("risk_band_label", risk_band(F.col("fraud_score")))
)

# Filter on a UDF column — Catalyst cannot push this down to storage
# because it doesn't understand risk_band_label's derivation.
high_risk = scored.filter(F.col("risk_band_label") >= 4)

# ---------------------------------------------------------------------------
# Actions with no error handling
# ---------------------------------------------------------------------------

# SPL-D11-004: write() and count() are Spark actions that can fail due
# to network partitions, S3 throttling, or executor OOM.  Neither is
# wrapped in try/except, so the job will crash without a useful error
# message or cleanup step.                                             [INFO]
high_risk.write.mode("overwrite").parquet("/output/fraud_high_risk")

flagged_count = high_risk.count()
print(f"High-risk transactions flagged: {flagged_count}")

# SPL-D11-003: no Python logging imports or calls anywhere in this
# file.  A production fraud pipeline should log record counts, rule
# hit rates, and timing at each stage for SLA monitoring.             [INFO]
scored.write.mode("overwrite").parquet("/output/fraud_all_scores")
