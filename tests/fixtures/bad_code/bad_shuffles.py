"""Sales aggregation pipeline — bad shuffle patterns.

Computes daily and weekly sales KPIs from raw transaction logs.  The file
contains several shuffle anti-patterns that balloon network I/O on any
cluster with more than one executor.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("sales_kpi_aggregation").getOrCreate()
sc = spark.sparkContext

# ---------------------------------------------------------------------------
# Load raw events
# ---------------------------------------------------------------------------

transactions = spark.read.parquet("/data/transactions")
products = spark.read.parquet("/data/products")

# ---------------------------------------------------------------------------
# RDD word-count style aggregation — groupByKey anti-pattern
# ---------------------------------------------------------------------------

raw_rdd = transactions.select("store_id", "amount").rdd.map(
    lambda row: (row["store_id"], row["amount"])
)

# SPL-D02-001: groupByKey() shuffles ALL values to each reducer before
# summing.  Replace with reduceByKey(lambda a, b: a + b) to combine
# partial sums on the map side — dramatically less shuffle data.     [CRITICAL]
store_totals = raw_rdd.groupByKey().mapValues(sum).collect()

# ---------------------------------------------------------------------------
# DataFrame pipeline — multiple shuffles without a checkpoint break
# ---------------------------------------------------------------------------

# Shuffle 1: groupBy + agg
daily_sales = transactions.groupBy("sale_date", "store_id").agg(
    F.sum("amount").alias("daily_total")
)

# Shuffle 2: join (sort-merge)
daily_enriched = daily_sales.join(products, on="store_id", how="left")

# Shuffle 3: another groupBy on the already-shuffled frame
# SPL-D02-007: three consecutive shuffle operations with no cache(),
# persist(), or checkpoint() in between.  The plan re-reads and
# re-shuffles the upstream stages on every action.                   [WARNING]
weekly_rollup = daily_enriched.groupBy("week_number", "category").agg(
    F.sum("daily_total").alias("weekly_total")
)

# ---------------------------------------------------------------------------
# Deduplication without prior narrow filter
# ---------------------------------------------------------------------------

# SPL-D02-005: distinct() on the full table before any filter or select.
# Applying a date-range filter first would reduce the cardinality that
# the shuffle engine has to handle.                                    [INFO]
unique_transactions = transactions.distinct()

# ---------------------------------------------------------------------------
# Full sort without a limit guard
# ---------------------------------------------------------------------------

# SPL-D02-004: orderBy() forces a total sort across the entire dataset
# with a full shuffle.  Add .limit(1000) before this call if you only
# need the top-N rows, or drop the sort entirely for downstream writes. [WARNING]
sorted_transactions = transactions.orderBy(F.col("amount").desc())

sorted_transactions.write.mode("overwrite").parquet("/output/sorted_transactions")
weekly_rollup.write.mode("overwrite").parquet("/output/weekly_sales")
