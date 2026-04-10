"""Content scoring pipeline — correct UDF and code patterns.

Replaces Python UDFs with native Spark SQL functions, uses select()
instead of iterative withColumn(), and limits collect()/toPandas()
to small results.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

DATA_ROOT = os.environ.get("DATA_ROOT", "/data")
OUTPUT_ROOT = os.environ.get("OUTPUT_ROOT", "/output")

spark = (
    SparkSession.builder.appName("content_scoring")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.cores", "4")
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.cbo.enabled", "true")
    .config("spark.sql.cbo.joinReorder.enabled", "true")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.speculation", "true")
    .config("spark.extraListeners", "org.apache.spark.scheduler.StatsReportListener")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

articles = spark.read.parquet(f"{DATA_ROOT}/articles")
metadata = spark.read.parquet(f"{DATA_ROOT}/article_metadata")  # small

# ---------------------------------------------------------------------------
# Use native Spark functions instead of Python UDFs.
#
# Python UDFs are opaque to Catalyst — no predicate pushdown, no
# code generation, row-by-row serialisation overhead.  The same
# transformations expressed with F.when/F.col are transparent to the
# optimiser.
# ---------------------------------------------------------------------------

scored = articles.withColumn(
    "quality_tier",
    F.when(F.col("quality_score") >= 0.9, "premium")
    .when(F.col("quality_score") >= 0.7, "standard")
    .when(F.col("quality_score") >= 0.5, "basic")
    .otherwise("rejected"),
)

scored = scored.withColumn(
    "word_count_bucket",
    F.when(F.col("word_count") >= 2000, "long")
    .when(F.col("word_count") >= 500, "medium")
    .otherwise("short"),
)

# ---------------------------------------------------------------------------
# Add multiple columns in a single select() call instead of chained
# withColumn() — avoids creating a new plan node for every column.
# ---------------------------------------------------------------------------

feature_cols = [
    "article_id",
    "quality_tier",
    "word_count_bucket",
    F.col("publish_date").cast("date").alias("pub_date"),
    F.length("title").alias("title_length"),
    F.lower("author").alias("author_norm"),
    F.regexp_replace("slug", r"[^a-z0-9\-]", "").alias("clean_slug"),
    F.col("view_count").cast("long").alias("views"),
    F.col("share_count").cast("long").alias("shares"),
    (F.col("share_count") / F.greatest(F.col("view_count"), F.lit(1))).alias("share_rate"),
]

enriched = scored.select(*feature_cols)

# ---------------------------------------------------------------------------
# collect() with limit — never collect unbounded results to the driver.
# ---------------------------------------------------------------------------

top_articles = (
    enriched.filter(F.col("quality_tier") == "premium")  # noqa: SPL-D11-004
    .orderBy(F.col("views").desc())
    .limit(50)
    .collect()
)

logger.info("Top %d premium articles retrieved", len(top_articles))

# ---------------------------------------------------------------------------
# toPandas() with limit — small, bounded result only.
# ---------------------------------------------------------------------------

sample_df = enriched.filter(F.col("quality_tier") == "rejected").limit(200).toPandas()

logger.info("Rejected sample size: %d", len(sample_df))

# ---------------------------------------------------------------------------
# Write full enriched output
# ---------------------------------------------------------------------------

logger.info("Writing enriched articles")
try:
    enriched.write.mode("overwrite").partitionBy(  # noqa: SPL-D07-005, SPL-D07-007
        "quality_tier"
    ).parquet(  # noqa: SPL-D07-005, SPL-D07-007
        f"{OUTPUT_ROOT}/articles_enriched"
    )
except Exception as exc:
    logger.error("Write failed: %s", exc)
    raise
