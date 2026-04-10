"""Customer data transformation pipeline — bad UDF and code patterns.

Cleans and enriches a customer profile dataset for a CRM system.  The
file contains Python UDF misuse and driver-side collection patterns that
will fail or stall on any production-scale dataset.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

spark = (
    SparkSession.builder.appName("customer_profile_transform")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Python UDF — row-at-a-time serialisation overhead
# ---------------------------------------------------------------------------


# SPL-D09-001: Python UDF invokes the Python worker for every row.
# The equivalent F.lower(F.trim(F.col("email"))) runs entirely in the
# JVM with no serialisation cost.                                     [WARNING]
@F.udf(StringType())
def normalise_email(email: str) -> str:
    if email is None:
        return None
    return email.strip().lower()


# SPL-D09-001 (second UDF): another row-at-a-time UDF for a calculation
# that F.when / F.col arithmetic handles natively.                    [WARNING]
@F.udf(DoubleType())
def lifetime_value_score(purchases: int, avg_order: float) -> float:
    if purchases is None or avg_order is None:
        return 0.0
    return float(purchases) * avg_order * 0.85


# ---------------------------------------------------------------------------
# Load and clean customer data
# ---------------------------------------------------------------------------

customers = spark.read.parquet("/data/customers")
orders = spark.read.parquet("/data/orders")

customer_stats = orders.groupBy("customer_id").agg(
    F.count("*").alias("purchase_count"),
    F.avg("order_total").alias("avg_order_value"),
)

enriched = (
    customers.join(customer_stats, on="customer_id", how="left")
    .withColumn("email_clean", normalise_email(F.col("email")))
    .withColumn(
        "ltv_score", lifetime_value_score(F.col("purchase_count"), F.col("avg_order_value"))
    )
)

# ---------------------------------------------------------------------------
# withColumn inside a loop — plan explosion
# ---------------------------------------------------------------------------

score_columns = [
    "recency_score",
    "frequency_score",
    "monetary_score",
    "engagement_score",
    "loyalty_score",
]

df = enriched
for col_name in score_columns:
    # SPL-D09-003: withColumn() inside a loop appends one logical plan
    # node per iteration.  With 5 columns this is mild, but at 50+
    # columns Catalyst's plan optimiser times out.  Use a single
    # .select([...]) with all new columns instead.                    [CRITICAL]
    df = df.withColumn(col_name, F.col("ltv_score") * F.rand())

# ---------------------------------------------------------------------------
# Driver-side collection without a limit guard
# ---------------------------------------------------------------------------

high_value = enriched.filter(F.col("ltv_score") > 500.0)

# SPL-D09-005: collect() with no preceding .limit().  On a 50 M-row
# customers table this will attempt to pull the entire filtered result
# set into driver memory, likely causing an OOM or a very long stall.
#                                                                     [CRITICAL]
high_value_list = high_value.collect()
print(f"High-value customers found: {len(high_value_list)}")

# ---------------------------------------------------------------------------
# toPandas() without a limit — same OOM risk
# ---------------------------------------------------------------------------

segment_summary = df.groupBy("country", "customer_segment").agg(
    F.avg("ltv_score").alias("avg_ltv"), F.count("*").alias("n")
)

# SPL-D09-006: toPandas() materialises the entire DataFrame in driver
# memory.  Add .limit(10_000) before this call if the result is only
# used for plotting or local inspection.                              [CRITICAL]
pdf = segment_summary.toPandas()
pdf.to_csv("/tmp/segment_summary.csv", index=False)

df.write.mode("overwrite").parquet("/output/enriched_customers")
