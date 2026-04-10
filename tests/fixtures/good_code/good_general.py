"""Fraud-detection scoring pipeline — correct Catalyst and observability patterns.

Uses native Spark SQL functions instead of Python UDFs, enables CBO,
externalises paths to environment variables, uses Python logging, and
wraps all actions in try/except.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

DATA_ROOT   = os.environ.get("DATA_ROOT",   "/data")
OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", "/output")

spark = (
    SparkSession.builder
    .appName("fraud_detection_scoring")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory",   "4g")
    .config("spark.executor.cores",  "4")
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    # CBO: Catalyst uses statistics-based join ordering for the 4-table star schema.
    .config("spark.sql.cbo.enabled", "true")
    .config("spark.sql.cbo.joinReorder.enabled", "true")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.speculation", "true")
    .config("spark.extraListeners",
            "org.apache.spark.scheduler.StatsReportListener")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Load tables — paths from environment variables, not hardcoded literals.
# ---------------------------------------------------------------------------

transactions = spark.read.parquet(f"{DATA_ROOT}/transactions/current")
accounts     = spark.read.parquet(f"{DATA_ROOT}/accounts/v2")
rules        = spark.read.parquet(f"{DATA_ROOT}/fraud_rules/latest")
watchlist    = spark.read.parquet(f"{DATA_ROOT}/watchlist")

logger.info(
    "Loaded source tables: transactions, accounts, rules, watchlist"
)

# ---------------------------------------------------------------------------
# Native Spark SQL function instead of a Python UDF.
#
# A Python UDF is a black box to Catalyst — no predicate pushdown,
# no code generation, row-by-row Python serialisation overhead.
# The equivalent F.when() chain is fully transparent to the optimiser.
# ---------------------------------------------------------------------------

def risk_band_expr(score_col: str) -> F.Column:
    """Return a Column expression mapping a fraud score to a 1-5 risk band."""
    return (
        F.when(F.col(score_col).isNull(), F.lit(0))
         .when(F.col(score_col) >= 0.9, F.lit(5))
         .when(F.col(score_col) >= 0.7, F.lit(4))
         .when(F.col(score_col) >= 0.5, F.lit(3))
         .when(F.col(score_col) >= 0.3, F.lit(2))
         .otherwise(F.lit(1))
    )


# ---------------------------------------------------------------------------
# Multi-join scoring pipeline — CBO + joinReorder choose optimal order.
# ---------------------------------------------------------------------------

scored = (
    transactions
    .join(accounts,               on="account_id",  how="left")
    .join(F.broadcast(rules),     on="rule_set_id", how="inner")
    .join(F.broadcast(watchlist), on="entity_id",   how="left")
    .withColumn(
        "fraud_score",
        F.col("base_score") * F.col("rule_weight") * F.col("account_risk_factor"),
    )
    .withColumn("risk_band_label", risk_band_expr("fraud_score"))
)

# Filter on a native column expression — Catalyst CAN push this down.
high_risk = scored.filter(F.col("risk_band_label") >= 4)

logger.info("Scoring complete; writing outputs")

# ---------------------------------------------------------------------------
# Actions wrapped in try/except with logging — production-grade error handling.
# ---------------------------------------------------------------------------

try:
    high_risk.write.mode("overwrite").parquet(f"{OUTPUT_ROOT}/fraud_high_risk")
    flagged_count = high_risk.count()
    logger.info("High-risk transactions flagged: %d", flagged_count)
except Exception as exc:
    logger.error("High-risk write failed: %s", exc)
    raise

try:
    scored.write.mode("overwrite").partitionBy("risk_band_label").parquet(
        f"{OUTPUT_ROOT}/fraud_all_scores"
    )
    logger.info("All-scores write complete")
except Exception as exc:
    logger.error("All-scores write failed: %s", exc)
    raise
