"""ML feature engineering pipeline — bad caching patterns.

Builds user-level features for a recommendation model.  The file
contains caching mistakes that either waste memory or cause silent
recomputation of expensive upstream stages.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("recommendation_feature_engineering")
    .config("spark.sql.shuffle.partitions", "400")
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Load base tables
# ---------------------------------------------------------------------------

events = spark.read.parquet("/data/user_events")
users = spark.read.parquet("/data/users")
items = spark.read.parquet("/data/items")

# ---------------------------------------------------------------------------
# Expensive join recomputed multiple times — should be cached
# ---------------------------------------------------------------------------

# Build a user-item interaction frame with several expensive joins.
interactions = (
    events.filter(F.col("event_type").isin("view", "click", "purchase"))
    .join(users, on="user_id", how="inner")
    .join(items, on="item_id", how="inner")
)

# SPL-D06-006: `interactions` is used in 3 separate aggregations below
# without cache() / persist().  Spark will re-execute the full join
# pipeline from scratch for every action.                             [WARNING]
view_counts = interactions.filter(F.col("event_type") == "view").groupBy("user_id").count()
click_counts = interactions.filter(F.col("event_type") == "click").groupBy("user_id").count()
purchase_counts = interactions.filter(F.col("event_type") == "purchase").groupBy("user_id").count()

# ---------------------------------------------------------------------------
# Cache after filtering — but the filter should come FIRST
# ---------------------------------------------------------------------------

raw_user_features = (
    users.join(view_counts, on="user_id", how="left")
    .join(click_counts, on="user_id", how="left")
    .join(purchase_counts, on="user_id", how="left")
)

# SPL-D06-004: cache() is called before the narrow WHERE clause.  The
# cached RDD will hold ALL users including inactive ones.  Move the
# .filter() before .cache() to cache a smaller, more useful dataset.  [WARNING]
user_features = raw_user_features.cache().filter(F.col("is_active") == True)  # noqa: E712

# ---------------------------------------------------------------------------
# Cache inside a loop — memory leak
# ---------------------------------------------------------------------------

model_versions = ["v1", "v2", "v3", "v4"]

for version in model_versions:
    # SPL-D06-003: cache() called on every loop iteration.  Each call
    # pins a new copy of the filtered frame in executor memory.  Old
    # copies are never unpersisted, so the cluster runs OOM after a few
    # iterations.                                                      [CRITICAL]
    versioned = user_features.filter(F.col("model_version") == version).cache()
    versioned.write.mode("overwrite").parquet(f"/output/features_{version}")

# ---------------------------------------------------------------------------
# cache() but no unpersist() anywhere in the file
# ---------------------------------------------------------------------------

lookup_table = items.filter(F.col("category") == "electronics")

# SPL-D06-001: lookup_table is cached but never unpersist()-ed.  The
# dataset occupies executor memory for the rest of the Spark session
# even after this job's actions are complete.                         [WARNING]
lookup_table.cache()

lookup_table.count()  # trigger materialisation
lookup_table.write.mode("overwrite").parquet("/output/electronics_lookup")

# Missing:  lookup_table.unpersist()
