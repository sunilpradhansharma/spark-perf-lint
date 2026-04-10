# Spark Performance Knowledge Base

> **Audience:** Senior Spark engineers, tech leads, and platform teams who want the
> *why* behind every lint rule — not just a list of what to avoid.  
> **Scope:** All 11 performance dimensions covered by `spark-perf-lint`, with decision
> matrices, config reference, and annotated code examples drawn from the tool's
> knowledge YAML files.

---

## Table of Contents

1. [D01 · Cluster Configuration](#d01--cluster-configuration)
2. [D02 · Shuffle](#d02--shuffle)
3. [D03 · Joins](#d03--joins)
4. [D04 · Partitioning](#d04--partitioning)
5. [D05 · Data Skew](#d05--data-skew)
6. [D06 · Caching](#d06--caching)
7. [D07 · I/O Format](#d07--io-format)
8. [D08 · Adaptive Query Execution](#d08--adaptive-query-execution)
9. [D09 · UDF Code Quality](#d09--udf-code-quality)
10. [D10 · Catalyst Optimizer](#d10--catalyst-optimizer)
11. [D11 · Monitoring and Observability](#d11--monitoring-and-observability)
12. [Appendix A · Spark Config Quick Reference](#appendix-a--spark-config-quick-reference)
13. [Appendix B · Decision Matrices](#appendix-b--decision-matrices)
14. [Appendix C · Memory Sizing Formula](#appendix-c--memory-sizing-formula)

---

## D01 · Cluster Configuration

### Why it matters

The physical plan Spark emits is only as good as the executor memory and CPU that execute
it. A beautifully optimised query plan will still OOM, spill to disk, or run 10× slower
than necessary if the cluster is misconfigured. Cluster configuration is the highest-ROI
dimension because each fix is a config-only change with zero code modifications.

### Executor sizing

The fundamental trade-off is **JVM overhead vs task-level memory**. Each executor is one
JVM process. With only 1 core per executor (the Spark default on YARN) you pay the full
JVM startup and GC cost for each core you allocate — this is almost always wrong.

The practical sweet spot is **4–5 cores per executor**:

```python
spark = (
    SparkSession.builder
    .appName("etl_daily_positions")
    .config("spark.executor.memory",         "16g")   # JVM heap per executor
    .config("spark.executor.memoryOverhead", "2g")    # off-JVM: Python workers, Arrow buffers
    .config("spark.executor.cores",          "4")     # concurrent tasks per executor
    .config("spark.driver.memory",           "4g")
    .config("spark.driver.maxResultSize",    "2g")    # prevents OOM from large collect()
    .getOrCreate()
)
```

**Memory allocation inside the executor JVM:**

```
spark.executor.memory  (e.g. 16 GB)
│
├── Spark managed memory  (spark.memory.fraction = 0.8 → 12.8 GB)
│   ├── Execution memory   (spark.memory.storageFraction = 0.5 → 6.4 GB)
│   │   Sort buffers, hash tables, shuffle read buffers
│   └── Storage memory     (6.4 GB)
│       Cached RDD/DataFrame partitions, broadcast variables
│
└── User memory  (0.2 × 16 GB = 3.2 GB)
    Application objects, UDF data structures
```

The unified memory pool means execution and storage steal from each other when one side
is idle. This is generally good — but if broadcast variables consume most of storage
memory, shuffle aggregations will spill to disk even with abundant total memory.

### Driver sizing

The driver is single-threaded for task scheduling and single-JVM for `collect()` results.
Undersized drivers are the most common cause of `OutOfMemoryError: Java heap space` in
production pipelines:

| Operation | Driver Memory Required |
|---|---|
| `df.collect()` on 10 M rows × 10 cols | ~2–4 GB |
| Broadcast variable (10 MB table) | 10 MB (negligible) |
| `df.toPandas()` on 100 M rows | 40+ GB — **never do this** |
| `spark.sparkContext.addFile()` on 1 GB file | 1 GB + overhead |
| SQL query plan with 500 `withColumn` calls | Several GB of plan serialization |

### Serialization: Java vs Kryo

Spark defaults to Java serialization. Kryo is faster in every measurable way:

| Metric | Java Serializer | Kryo Serializer | Gain |
|---|---|---|---|
| Serialization speed | 1× (baseline) | ~10× faster | 10× |
| Serialized object size | 1× (baseline) | ~3–5× smaller | 3–5× |
| Shuffle data written | 1× (baseline) | ~3–5× less | 3–5× |
| GC pressure | High | Low | Significant |

```python
spark = (
    SparkSession.builder
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrationRequired", "false")     # safe during migration
    .config("spark.kryoserializer.buffer.max", "512m")      # raise if serialization errors
    .getOrCreate()
)
```

Set `registrationRequired = false` during migration to avoid `ClassNotRegistered`
exceptions on unregistered types. Once all classes are registered, flip to `true` for
maximum efficiency.

### Dynamic allocation

For variable-load clusters (common in data platforms), dynamic allocation scales the
executor pool automatically:

```python
.config("spark.dynamicAllocation.enabled",                "true")
.config("spark.shuffle.service.enabled",                  "true")   # REQUIRED for dynamic alloc
.config("spark.dynamicAllocation.minExecutors",           "2")
.config("spark.dynamicAllocation.maxExecutors",           "200")
.config("spark.dynamicAllocation.executorIdleTimeout",    "60s")
.config("spark.dynamicAllocation.schedulerBacklogTimeout","1s")
```

Without `spark.shuffle.service.enabled = true`, removing an executor loses its shuffle
files and the reduce tasks reading them will fail. The external shuffle service holds the
shuffle data independently of the executor process lifetime.

### Network timeouts

The default `spark.network.timeout = 120s` is too short for large shuffles on slow
storage or congested networks. Executors that take longer than 120 seconds to read shuffle
data will be declared dead and the stage will retry:

```python
.config("spark.network.timeout",        "800s")
.config("spark.rpc.askTimeout",         "600s")
.config("spark.shuffle.io.maxRetries",  "10")
.config("spark.shuffle.io.retryWait",   "60s")
```

---

## D02 · Shuffle

### What a shuffle is

A shuffle is a full redistribution of data across all executors triggered by a wide
transformation (`groupBy`, `join`, `distinct`, `sort`, `repartition`). Every row is
written to disk as a shuffle map output, transferred over the network to the reducer
executor, and read from disk by the reducer. On a 1 TB dataset with 200 partitions, this
means 1 TB of disk writes and 1 TB of network transfers **per shuffle stage**.

Minimising the number of shuffles and their size is the single most impactful
optimisation in most production Spark jobs.

### `groupByKey` vs `reduceByKey`

`groupByKey` collects **all values** for each key on the reducer before any aggregation.
On a key with 10 million rows, those 10 million values are all shipped over the network
and held in memory simultaneously. `reduceByKey` and `aggregateByKey` perform a
**map-side pre-aggregation** (combiner) that reduces data volume before the shuffle:

```python
# BEFORE — groupByKey: sends all raw values over network
word_counts = (
    rdd
    .map(lambda word: (word, 1))
    .groupByKey()               # network: 1 entry per word occurrence
    .mapValues(sum)
)

# AFTER — reduceByKey: combines locally first, sends totals
word_counts = (
    rdd
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)   # network: 1 entry per unique word per partition
)
```

For non-associative aggregations (e.g. computing exact medians), `groupByKey` may be
unavoidable — but `countByKey`, `combineByKey`, or a custom `aggregateByKey` can often
replace it. In the DataFrame API, `groupBy` + `agg()` automatically performs map-side
pre-aggregation for all built-in aggregate functions.

### The default 200 partitions problem

`spark.sql.shuffle.partitions` defaults to 200. This number was chosen for a cluster
with ~20 executors × 10 cores = 200 cores in 2012. Today, pipelines run on clusters an
order of magnitude larger or on 10 GB of data where 200 partitions create overhead.

**The formula:**

```
recommended_partitions = max(
    executor_count × cores_per_executor × 2,    # keep all cores busy with headroom
    ceil(data_size_bytes / 128_MB)              # target ~128 MB per partition
)
```

With AQE enabled, set a generous upper bound and let Spark coalesce down:

```python
# AQE approach: generous upper bound, AQE coalesces to advisory size
spark.conf.set("spark.sql.shuffle.partitions",                   "4000")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",  "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes","134217728")  # 128 MB
```

Without AQE, set the value explicitly based on the dataset size. A 10 TB dataset with
128 MB target → 80,000 partitions. A 5 GB dataset → 40 partitions, not 200.

### Unnecessary shuffles before writes

A common anti-pattern is calling `repartition()` immediately before a write to control
output file count. This introduces an extra shuffle stage with zero data transformation
benefit:

```python
# BEFORE — extra shuffle just to control file count
df.repartition(100).write.parquet(output_path)

# AFTER — use coalesce() for small reductions (no shuffle)
df.coalesce(100).write.parquet(output_path)

# Or let the write's task count control parallelism directly
spark.conf.set("spark.sql.shuffle.partitions", "100")
df.groupBy("date").agg(...).write.parquet(output_path)
```

`coalesce(n)` is a narrow transformation — it only moves data from excess partitions into
neighbouring partitions without a full shuffle. It can only reduce partition count, never
increase it. For increasing partitions, `repartition(n)` is correct but costs a full
shuffle.

### Multiple wide transformations and checkpointing

When a query plan contains many `groupBy`, `join`, and `sort` operations chained
together, the logical plan grows deep and DAG recomputation in case of failure becomes
expensive. Long lineage chains also stress the driver's plan serialization:

```python
# Checkpoint to truncate lineage after a complex multi-stage pipeline
spark.sparkContext.setCheckpointDir("/tmp/checkpoints")

# After a heavy join + aggregation stage
intermediate = (
    transactions
    .join(accounts, "account_id")
    .groupBy("product_type", "region")
    .agg(F.sum("amount").alias("total"))
)
intermediate.checkpoint()                 # writes to HDFS/S3; breaks lineage
intermediate = spark.read.parquet("/tmp/checkpoints/...")  # re-read
```

Checkpoint after any stage that is expensive to recompute and whose result is used
multiple times downstream.

---

## D03 · Joins

### The six join strategies

Spark has six physical join algorithms. Catalyst picks the strategy automatically, but
knowing the selection rules lets you write hints that prevent costly mis-selections.

| Strategy | When Catalyst Selects | Shuffle Required | Sort Required |
|---|---|---|---|
| **Broadcast Hash Join (BHJ)** | Smaller side ≤ `autoBroadcastJoinThreshold` | None | None |
| **Shuffle Hash Join (SHJ)** | One side ≤ 3× smaller; AQE may promote from SMJ | Both sides | None |
| **Sort-Merge Join (SMJ)** | Both sides large; equi-join | Both sides | Both sides |
| **Bucketed Sort-Merge Join** | Both tables pre-bucketed on same key+count | None (pre-shuffled) | None |
| **Broadcast Nested Loop Join** | Non-equi / cross join with small build side | None | None |
| **Cartesian Product** | Cross join without equi condition, no broadcast possible | None | None |

### Broadcast Hash Join

BHJ is the fastest strategy because it eliminates the shuffle entirely. Spark serialises
the smaller table on the driver, broadcasts it to every executor, and builds an in-memory
hash table per executor. Probe-side rows are matched against this local hash table with
O(1) lookup.

```python
from pyspark.sql.functions import broadcast

# Explicit hint — always use this when the dimension table is known-small
result = large_fact.join(broadcast(small_dim), "product_id", "left")

# Or raise the auto-threshold (Catalyst selects BHJ automatically)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))  # 100 MB

# Disable auto-broadcast to force SMJ (when you need deterministic plans)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

**When NOT to broadcast:** The broadcast table is serialised on the driver first — if
driver heap is < 2 GB and the broadcast table is 1 GB, the driver OOMs before executors
receive anything. Also, every executor holds a full copy: with 50 executors and a 512 MB
broadcast table, you consume 25 GB of aggregate executor storage memory.

### Sort-Merge Join

SMJ is Spark's default for large-large equi-joins. Both sides are sorted by join key
after shuffling, then merged in a linear scan. The sort is the dominant cost: O(N log N)
per side. However, SMJ spills gracefully to disk, making it robust for datasets that
exceed aggregate executor memory — unlike Shuffle Hash Join, which OOMs if a partition
does not fit in memory.

```python
# Verify the plan — look for SortMergeJoin in the physical plan
result = fact_df.join(dim_df, "order_id")
result.explain()
# ...
# +- SortMergeJoin [order_id#1L], [order_id#2L], Inner
#    :- Sort [order_id#1L ASC NULLS FIRST]
#       +- Exchange hashpartitioning(order_id#1L, 200)

# Force SMJ with a hint (useful when AQE might otherwise choose SHJ):
result = fact_df.hint("MERGE").join(dim_df, "order_id")
```

### Bucketed Join: eliminating shuffle entirely

When the same tables are joined repeatedly on the same key, write them bucketed once and
save the shuffle on every subsequent read:

```python
# Write once — this costs a full shuffle
orders.write \
    .bucketBy(64, "customer_id") \
    .sortBy("customer_id") \
    .mode("overwrite") \
    .saveAsTable("orders_bucketed")

customers.write \
    .bucketBy(64, "customer_id") \
    .sortBy("customer_id") \
    .mode("overwrite") \
    .saveAsTable("customers_bucketed")

# All subsequent reads: zero shuffle stages
orders_r    = spark.table("orders_bucketed")
customers_r = spark.table("customers_bucketed")
result = orders_r.join(customers_r, "customer_id")
# plan: BucketedTableScan → SortMergeJoin (no Exchange)
```

**Constraints:** Both tables must use the same bucket count and bucket key. If one has
64 buckets and the other 128, Spark cannot skip the shuffle. Choose the bucket count
as a power of 2 that gives ~128 MB files: `ceil(total_size_gb * 1024 / 128)`.

### Skew join patterns

A skewed join is one where a small number of join-key values account for a
disproportionate fraction of rows. All rows for that key land on one reduce task, creating
a straggler while every other task completes. There are three remediation strategies in
order of preference:

**1. AQE Skew Join (Spark 3.0+, zero code change)**

```python
spark.conf.set("spark.sql.adaptive.enabled",                              "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                    "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor",      "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", str(256 * 1024 * 1024))

# AQE automatically splits oversized partitions at runtime — no code change
result = events.join(user_profiles, "user_id")
```

Verify skew detection fired: look for `SkewJoin = true` in `df.explain()` or the Spark
UI stage details after execution.

**2. Raise the broadcast threshold to include the skewed side**

If the skewed side is the *smaller* table (e.g., a 2 GB product catalog joined to a
500 GB fact table where product_id is skewed), raising the broadcast threshold eliminates
the shuffle entirely — and therefore eliminates the skew:

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(2 * 1024 * 1024 * 1024))
result = large_fact.join(product_catalog, "product_id")
```

**3. Manual salting (Spark < 3.0 or extreme skew)**

```python
from pyspark.sql.functions import col, expr, lit, array, explode

SALT_FACTOR = 10  # choose ceil(skew_ratio / 5)

# Probe side: append random salt suffix
salted_fact = large_fact.withColumn(
    "salted_key",
    expr(f"concat(cast(user_id AS STRING), '_', cast(floor(rand() * {SALT_FACTOR}) AS INT))")
)

# Build side: replicate N times with matching suffixes
salted_dim = (
    small_dim
    .withColumn("salt", explode(array([lit(i) for i in range(SALT_FACTOR)])))
    .withColumn("salted_key", expr("concat(cast(user_id AS STRING), '_', cast(salt AS INT))"))
)

result = salted_fact.join(salted_dim, "salted_key").drop("salted_key", "salt")
```

Salting costs: the build side is read `SALT_FACTOR` times. For a 100 GB dimension table
with SALT_FACTOR=10, that is 1 TB of scan + network. Use sparingly.

### Join decision matrix

```
join_type == non_equi OR cross?
  └─ YES → BROADCAST_NESTED_LOOP  (both sides must be small)
  └─ NO  →
      smaller_side ≤ broadcast_threshold?
        └─ YES → BROADCAST_HASH_JOIN  (fastest; no shuffle)
        └─ NO  →
            both_bucketed on same key and count?
              └─ YES → BUCKETED_SORT_MERGE_JOIN  (no shuffle)
              └─ NO  →
                  skew_ratio ≥ 5 AND AQE + Spark 3.0?
                    └─ YES → AQE_SKEW_JOIN  (transparent)
                    └─ NO  →
                        skew_ratio ≥ 10 AND AQE unavailable?
                          └─ YES → SALTED_JOIN  (manual)
                          └─ NO  →
                              size_ratio ≥ 3 AND smaller ≤ 10 GB?
                                └─ YES → SHUFFLE_HASH_JOIN  (no sort cost)
                                └─ NO  → SORT_MERGE_JOIN   (default)
```

---

## D04 · Partitioning

### Partitions, tasks, and parallelism

In Spark, a **partition** is the unit of work: one task processes one partition on one
executor core. The number of partitions is therefore the maximum parallelism of a stage.
A job with 100 GB of data in 200 partitions on a 1000-core cluster runs at 20%
utilisation for most of the job.

The partition count should generally satisfy:

```
partition_count ≥ total_core_count × 2      (keep all cores busy with some headroom)
partition_size  ≈ 64 MB – 256 MB            (balance scheduling overhead vs task cost)
```

### `repartition` vs `coalesce`

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Shuffle triggered | **Always** (full shuffle) | **Never** (narrow transformation) |
| Can increase partitions | Yes | No |
| Output distribution | Uniform | May be uneven (combines adjacent partitions) |
| Use case | Need uniform distribution or increased parallelism | Reduce partition count cheaply before write |

```python
# Increasing partitions after a filter that reduced data volume
small_result = large_df.filter(col("date") == "2026-01-15")
# Now has 1000 partitions but only 1% of data — over-partitioned
small_result = small_result.coalesce(20)  # cheap; no shuffle

# Redistributing before a join on a new key
df_for_join = df.repartition(200, "customer_id")  # full shuffle; ensures uniform distribution
```

### `coalesce(1)` and `repartition(1)`: the silent serialiser

Writing `coalesce(1)` concentrates all data in a single partition on a single executor,
then writes one output file. This means:

- **100% of the data** must fit in one executor's memory
- The write is **fully sequential** — one task, one file
- The entire shuffle or re-read of prior stages feeds into a **single bottleneck**

On 100 GB of data, `coalesce(1)` turns what could be a 100-way parallel write into a
single-threaded write that takes 100× longer. The fix depends on intent:

```python
# Intent: produce one output file for downstream systems that require it
# Use a post-job S3/HDFS merge, not coalesce(1):
df.write.mode("overwrite").parquet("s3://bucket/output/")
# Then merge with: aws s3 cp --recursive + compaction job, or use Delta OPTIMIZE

# Intent: write a small dataset (< 100 MB) efficiently
df.coalesce(1).write.csv("output/small_result.csv")  # acceptable when data is genuinely small

# Intent: write N files instead of thousands of small files
df.repartition(N).write.parquet("output/")           # correct; costs a shuffle
```

### Partition key selection for writes

`partitionBy` on a DataFrame write creates a directory tree keyed by the partition columns:
`output/year=2026/month=04/day=10/*.parquet`. This enables partition pruning on reads —
Spark skips entire directories without opening files. However, partition key cardinality
must be bounded:

```python
# GOOD — low-cardinality partition key; ~365 directories per year
df.write \
    .partitionBy("date")             # 365 values/year
    .mode("overwrite") \
    .parquet("s3://warehouse/events/")

# BAD — high-cardinality partition key; millions of tiny files
df.write \
    .partitionBy("user_id")          # 50 million unique users → 50M directories
    .mode("overwrite") \
    .parquet("s3://warehouse/events/")
# Results in: millions of tiny files, massive S3 LIST overhead, OOM in driver
```

**Rule of thumb:** A partition key is acceptable if its cardinality produces output
partitions of ≥ 64 MB each. Measure the data volume per partition value before choosing.

### The small-files problem

Too many small files creates three types of overhead:

1. **Driver overhead:** Every file is an entry in the Spark task plan. 1 million files =
   1 million tasks = gigabytes of plan serialisation on the driver.
2. **S3/HDFS overhead:** Each file requires a metadata request. S3 charges per LIST and
   GET operation; HDFS has NameNode memory limits.
3. **Parquet overhead:** Each Parquet file has a footer with row group statistics.
   Millions of footers = gigabytes of footer reads before any data is processed.

Fix small files with periodic compaction:

```python
# Compact a partitioned table (run on a schedule, e.g. daily)
spark.read.parquet("s3://warehouse/events/date=2026-01-15/") \
    .coalesce(target_file_count) \
    .write \
    .mode("overwrite") \
    .parquet("s3://warehouse/events/date=2026-01-15/")
```

With Delta Lake: `OPTIMIZE events WHERE date = '2026-01-15'` compacts and bin-packs files
to the target size automatically.

---

## D05 · Data Skew

### What skew is and why it matters

Data skew occurs when the distribution of data across partitions is non-uniform. In a
shuffle, rows are assigned to partitions by `hash(key) % numPartitions`. If one key value
appears in 50% of all rows, that partition holds 50% of all data while every other
partition holds a fraction. The job cannot complete until the straggler task finishes,
regardless of how many other tasks have completed.

Skew manifests in the Spark UI as: **one task with 100× longer duration and 10–100×
more data than the median task** in the same stage.

### Measuring skew

```python
from pyspark.sql import functions as F

# After a groupBy + shuffle, check partition size distribution
partition_stats = (
    df
    .withColumn("pid", F.spark_partition_id())
    .groupBy("pid")
    .agg(F.count("*").alias("rows"))
    .agg(
        F.max("rows").alias("max_rows"),
        F.avg("rows").alias("avg_rows"),
        F.min("rows").alias("min_rows"),
        (F.max("rows") / F.avg("rows")).alias("skew_ratio"),
    )
    .collect()[0]
)
print(f"Skew ratio: {partition_stats['skew_ratio']:.1f}x")
# skew_ratio > 5.0 → WARNING (SPL-D05-001)
# skew_ratio > 10.0 → CRITICAL
```

### Null-key skew

Null values are all hashed to the same partition in a join. If 10% of `customer_id`
values are null in a 10 billion row fact table, one partition holds 1 billion rows:

```python
# BEFORE — null keys all go to one partition
result = orders.join(customers, orders.customer_id == customers.id, "left")

# AFTER — filter out nulls before join; handle separately
orders_with_key = orders.filter(col("customer_id").isNotNull())
orders_null_key = orders.filter(col("customer_id").isNull())

result_matched = orders_with_key.join(customers, "customer_id", "left")
result = result_matched.unionAll(orders_null_key.withColumn("customer_name", lit(None)))
```

### Window functions and skew

Unbounded window functions (`Window.partitionBy("user_id")` without `rowsBetween` or
`rangeBetween`) bring all rows for a partition key into a single task:

```python
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# BEFORE — unbounded window: collects all rows for each user_id in one task
w_unbounded = Window.partitionBy("user_id").orderBy("event_ts")
df = df.withColumn("running_total", F.sum("amount").over(w_unbounded))

# AFTER — bounded window limits data per task
w_bounded = (
    Window.partitionBy("user_id")
          .orderBy("event_ts")
          .rowsBetween(-6, 0)          # last 7 events only
)
df = df.withColumn("rolling_7_total", F.sum("amount").over(w_bounded))
```

### `collect()` and `toPandas()` on unaggregated data

`collect()` and `toPandas()` pull all data from all executors to the driver. On an
unaggregated DataFrame with hundreds of millions of rows, this is an OOM waiting to happen:

```python
# CRITICAL — pulls 100M rows to driver
all_data = df.collect()         # SPL-D05-005

# CORRECT patterns:
# 1. Sample before collecting
sample = df.sample(fraction=0.001).collect()

# 2. Aggregate first, then collect the small result
totals = df.groupBy("region").agg(F.sum("revenue")).collect()

# 3. Write to storage; read back with a different tool
df.write.parquet("s3://output/")
```

---

## D06 · Caching

### When to cache

A `DataFrame` should be cached when it is:
1. **Used more than once** in the same job — otherwise the cache overhead is pure waste
2. **Expensive to recompute** — e.g., the result of a multi-stage join + aggregation
3. **Small enough to fit in executor storage memory** — or acceptable to spill to disk

```
Is the DataFrame used more than once in this job?
  └─ NO  → Do NOT cache (SPL-D06-002: single-use cache)
  └─ YES →
      Is it expensive to recompute (multi-stage pipeline)?
        └─ NO  → Probably fine without cache
        └─ YES →
            Fits in memory?
              └─ YES → df.cache() or df.persist(MEMORY_ONLY)
              └─ NO  →
                  Is recompute cheaper than disk I/O?
                    └─ YES → Do NOT cache (let Spark recompute)
                    └─ NO  → df.persist(MEMORY_AND_DISK)
```

### Storage levels

| Level | Memory | Disk | Serialized | Replicated | Use Case |
|---|:---:|:---:|:---:|:---:|---|
| `MEMORY_ONLY` | ✅ | ❌ | ❌ | ❌ | Fast access; evicted partitions recomputed |
| `MEMORY_AND_DISK` | ✅ | ✅ | ❌ | ❌ | Large DataFrames; disk fallback on eviction |
| `MEMORY_ONLY_SER` | ✅ | ❌ | ✅ | ❌ | Reduce memory footprint; slower access |
| `DISK_ONLY` | ❌ | ✅ | ✅ | ❌ | Avoid: write Parquet directly instead |
| `OFF_HEAP` | ✅ (off) | ❌ | ✅ | ❌ | Reduce JVM GC pressure for large caches |

```python
from pyspark import StorageLevel

# Default cache() = MEMORY_AND_DISK
df.cache()

# Explicit level for large DataFrames
df.persist(StorageLevel.MEMORY_AND_DISK)

# Serialized in memory — 2–5× smaller, 2–3× slower access
df.persist(StorageLevel.MEMORY_ONLY_SER)

# ALWAYS unpersist when done — Spark does not GC cached DataFrames automatically
df.unpersist()
```

### The leaked cache problem

Spark's cache storage is LRU-evicted, but only when storage memory pressure forces
eviction. In long-running notebooks or streaming jobs, cached DataFrames accumulate and
gradually consume all storage memory, forcing execution memory into the unified pool
and causing shuffle spills:

```python
# PATTERN: cache → use → unpersist in a try/finally block
def process_batch(df):
    df.cache()
    try:
        count_1 = df.count()
        count_2 = df.filter(col("status") == "ACTIVE").count()
        return count_1, count_2
    finally:
        df.unpersist()     # always runs, even on exception
```

### `cache()` after a write is pointless

`df.write.parquet(path)` materialises the DataFrame and disconnects it from the cache.
The cached copy is never read again:

```python
# BEFORE — cache after write: wastes storage memory
df.cache()
df.write.parquet(output_path)    # materialises to storage; cache is never hit
df.count()                       # reads from cache BUT df was already fully computed

# AFTER — cache before the first action if you need multiple reads
df.cache()
print(df.count())                # fills cache
df.write.parquet(output_path)    # reads from cache
df.unpersist()
```

---

## D07 · I/O Format

### Why columnar formats dominate analytics workloads

Row-oriented formats (CSV, JSON, Avro) store all fields of a row contiguously. An
analytics query that reads 3 of 100 columns must still scan every byte. Columnar formats
(Parquet, ORC) store each column's values contiguously, enabling:

1. **Column pruning:** Skip unneeded column chunks entirely at the storage layer.
2. **Predicate pushdown:** Row groups whose statistics (min/max) don't match a filter
   are skipped without decompression or deserialization.
3. **Dictionary encoding + run-length encoding:** Low-cardinality columns compress 10–100×.
4. **Vectorized reading:** Arrow-based batch reading processes 1024 rows at once without
   per-row deserialization overhead.

**Storage format comparison:**

| Format | Compression | Column Pruning | Predicate Pushdown | Schema Evolution | Streaming |
|---|---|---|---|---|---|
| **Parquet** | Best | ✅ | ✅ | Limited | Read-only |
| **ORC** | Excellent | ✅ | ✅ | Limited | Read-only |
| **Delta Lake** | Best | ✅ | ✅ (+ Z-ORDER) | Full | ✅ |
| **Avro** | Good | ❌ | ❌ | Excellent | ✅ |
| **CSV** | Poor | ❌ | ❌ | None | ✅ |
| **JSON** | Poor | ❌ | ❌ | None | ✅ |

### Never write CSV or JSON in production

CSV/JSON writes from Spark are anti-patterns for nearly every production use case:

```python
# BEFORE — writing JSON: large, slow, no pushdown, schema loss
df.write.json("s3://output/transactions/")

# AFTER — Parquet: 5–10× smaller, column pruning, predicate pushdown
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("s3://output/transactions/")
```

For the rare case where a downstream system requires CSV/JSON (e.g., a legacy API or
human-readable report), write Parquet internally and convert at the boundary.

### Always specify schema on read

`inferSchema=True` forces Spark to scan the entire file to determine column types. On a
1 TB dataset, this is 1 TB of wasted I/O before your job begins:

```python
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# BEFORE — inferSchema: full scan on every read
df = spark.read.option("inferSchema", "true").csv("s3://data/transactions/")

# AFTER — explicit schema: zero overhead; correct types guaranteed
schema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("amount",         DoubleType(), nullable=True),
    StructField("timestamp",      LongType(),   nullable=False),
    StructField("customer_id",    StringType(), nullable=True),
])
df = spark.read.schema(schema).csv("s3://data/transactions/")
```

### Predicate pushdown with Parquet

Parquet's row group statistics (min/max per column per row group) allow Spark to skip
reading entire row groups that cannot satisfy a filter. However, pushdown only works if
the filter is on the same column that the data is **physically ordered or sorted by**:

```python
# READ — predicate is pushed down to Parquet row group statistics
df = (
    spark.read.parquet("s3://data/events/")
    .filter(col("event_date") == "2026-01-15")  # pushed down: row groups outside date range skipped
    .filter(col("user_id").isNotNull())          # pushed down: null statistics used
)

# Verify pushdown is active:
df.explain()
# Should show: PushedFilters: [IsNotNull(event_date), EqualTo(event_date,2026-01-15)]
```

If the filter is on a column that is **randomly distributed** across row groups (e.g.,
`user_id` in append-only logs), pushdown reads the statistics but cannot skip row groups
— achieving nothing. In this case, Z-ORDER or sorting at write time improves selectivity.

### Compression codec selection

| Codec | Ratio | Speed | Splittable | Use Case |
|---|---|---|---|---|
| `snappy` | Medium | Fast | No | Default; good balance |
| `lz4` | Low-Medium | Fastest | No | Low-latency pipelines |
| `zstd` | Best | Medium | No | Storage-cost-sensitive workloads |
| `gzip` | Best | Slow | No | Archive / cold storage |
| `uncompressed` | None | Fastest | Yes | When CPU is bottleneck |

```python
df.write \
    .option("compression", "zstd") \     # best ratio for cold storage
    .parquet("s3://archive/")

df.write \
    .option("compression", "snappy") \   # balanced default for hot data
    .parquet("s3://warehouse/")
```

### Reading partitioned data efficiently

```python
# BEFORE — reads ALL partitions, then filters in memory
df = spark.read.parquet("s3://events/").filter(col("date") == "2026-01-15")
# Catalyst should push this down, but verify with explain()

# AFTER — filter the path explicitly to guarantee partition pruning
df = spark.read.parquet("s3://events/date=2026-01-15/")

# Or use partition discovery with an explicit predicate:
df = (
    spark.read
    .parquet("s3://events/")
    .where("date = '2026-01-15'")   # partition filter: only opens that directory
)
```

---

## D08 · Adaptive Query Execution

### What AQE does

Adaptive Query Execution (Spark 3.0+) replaces Spark's static planning model with a
runtime-adaptive one. At each shuffle boundary, AQE evaluates actual shuffle output
statistics before scheduling the next stage. This lets it:

1. **Coalesce shuffle partitions:** Merge many small post-shuffle partitions into fewer
   larger ones, eliminating task scheduling overhead for tiny tasks.
2. **Convert SMJ to BHJ:** If AQE discovers a shuffle output is smaller than the
   broadcast threshold, it promotes the join from Sort-Merge to Broadcast Hash at runtime.
3. **Skew join optimization:** Split oversized shuffle partitions into smaller sub-tasks
   and read the matching portion of the other side multiple times.

### Enabling AQE

```python
spark = (
    SparkSession.builder
    # Core AQE toggle — single highest-ROI Spark 3 config
    .config("spark.sql.adaptive.enabled",                               "true")
    # Partition coalescing — merges small post-shuffle partitions
    .config("spark.sql.adaptive.coalescePartitions.enabled",            "true")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes",          str(128 * 1024 * 1024))
    .config("spark.sql.adaptive.coalescePartitions.minPartitionNum",    "1")
    # Skew join — splits oversized partitions
    .config("spark.sql.adaptive.skewJoin.enabled",                      "true")
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor",        "5")
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", str(256 * 1024 * 1024))
    # Local shuffle reader — avoids remote fetch when reducer is on same node
    .config("spark.sql.adaptive.localShuffleReader.enabled",            "true")
    .getOrCreate()
)
```

### AQE with partition coalescing: the 200-becomes-10 problem

Without AQE, `spark.sql.shuffle.partitions = 200` on a 500 MB dataset produces 200
tasks each processing 2.5 MB — extreme over-partitioning. With AQE partition coalescing:

```
Dataset: 500 MB
shuffle.partitions: 4000       (generous upper bound)
advisoryPartitionSizeInBytes: 128 MB
AQE coalesces: 4000 → ceil(500/128) = 4 partitions
```

Set `shuffle.partitions` to a generous upper bound (typically `max_cores × 4` or `20000`
for very large clusters) and let AQE coalesce. Never set it to 200 on a large cluster.

### Verifying AQE is active

```python
# Check the physical plan for AdaptiveSparkPlan
df.explain()
# Should show: AdaptiveSparkPlan isFinalPlan=false
# After execution: isFinalPlan=true, with AQEShuffleRead nodes where coalescing occurred

# Check runtime skew handling:
df.explain()
# Should show: SkewJoin = true  (in the SortMergeJoin or ShuffledHashJoin node)
```

### When AQE doesn't help

- **Bucketed joins:** AQE has no effect on Exchange-free bucketed join plans.
- **Determinism requirements:** AQE makes the physical plan runtime-dependent. If plan
  reproducibility is required for debugging, disable per-query with
  `spark.conf.set("spark.sql.adaptive.enabled", "false")`.
- **Streaming:** AQE is disabled for streaming queries (micro-batch plans are fixed).
- **Very small datasets:** On 100 MB datasets the AQE planning overhead exceeds the
  benefit. Tolerable but not necessary.

---

## D09 · UDF Code Quality

### The row-at-a-time UDF tax

A standard Python UDF (`@udf`) is called once per row. Each call crosses the JVM↔Python
boundary via Py4J, serializes the row's input columns into a Python object, invokes the
function, and deserializes the result back into a JVM value. On 100 million rows, this
produces 100 million serialization round-trips.

A pandas UDF (`@pandas_udf`) batches rows into Arrow columnar buffers (typically 10,000
rows per batch) and transfers them in a single boundary crossing. The function operates
on `pandas.Series` or `pandas.DataFrame` objects using vectorized NumPy operations:

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

# BEFORE — row-at-a-time: 100M boundary crossings on 100M rows
@udf(returnType=DoubleType())
def risk_score_slow(exposure, volatility):
    return exposure * volatility * 1.5

# AFTER — pandas UDF: ~10,000 boundary crossings on 100M rows (10,000-row batches)
@pandas_udf(DoubleType())
def risk_score_fast(exposure: pd.Series, volatility: pd.Series) -> pd.Series:
    return exposure * volatility * 1.5

# Required config for Arrow-based pandas UDFs:
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

Speedup is typically 3–10× for numeric operations and 2–5× for string operations.

### UDF anti-patterns

**UDF containing DataFrame or SparkContext reference (critical)**

UDFs are serialised by pickle and sent to workers. A UDF that captures a DataFrame
or SparkContext in its closure will either fail to serialise (PicklingError) or, worse,
attempt to create a nested SparkContext on the worker:

```python
# CRITICAL — captures spark (SparkSession) in closure
lookup_table = spark.table("product_codes").collect()

@udf(returnType=StringType())
def lookup_code(product_id):
    # This creates a new SparkContext on the worker — forbidden
    spark.sql(f"SELECT code FROM product_codes WHERE id = {product_id}")

# CORRECT — broadcast the lookup table, access it as a plain dict
lookup_dict = {row["id"]: row["code"] for row in spark.table("product_codes").collect()}
broadcast_lookup = spark.sparkContext.broadcast(lookup_dict)

@udf(returnType=StringType())
def lookup_code(product_id):
    return broadcast_lookup.value.get(product_id)   # accesses broadcast variable safely
```

**UDF in a loop**

Applying a UDF inside a Python loop creates a new DataFrame plan node per iteration
rather than vectorising the operation:

```python
# BEFORE — loop over columns: N UDF applications, N plan nodes
result = df
for col_name in ["col1", "col2", "col3", "col4", "col5"]:
    result = result.withColumn(col_name + "_clean", clean_udf(col(col_name)))

# AFTER — vectorise with a single pandas UDF operating on all columns
# Or use built-in functions that avoid the JVM boundary entirely:
from pyspark.sql.functions import regexp_replace

for col_name in ["col1", "col2", "col3", "col4", "col5"]:
    result = result.withColumn(col_name + "_clean",
                               regexp_replace(col(col_name), r"[^a-zA-Z0-9]", ""))
```

**Lambda UDFs**

Lambda UDFs are anonymous, cannot be introspected or tested in isolation, and produce
opaque `<lambda>` labels in physical plans:

```python
# BEFORE — lambda: not debuggable, not reusable
result = df.withColumn("score", udf(lambda x: x * 1.5, DoubleType())(col("value")))

# AFTER — named function: testable, visible in plan
@udf(returnType=DoubleType())
def compute_score(value):
    return value * 1.5

result = df.withColumn("score", compute_score(col("value")))
```

### UDF output as join key

UDFs applied to derive a join key prevent Catalyst from pushing the join's sort/hash
requirement into the scan, and break predicate pushdown:

```python
# BEFORE — UDF on join key: opaque to Catalyst
normalize_id_udf = udf(lambda x: x.lower().strip(), StringType())
result = df1.join(df2, normalize_id_udf(col("id")) == normalize_id_udf(col("id")))

# AFTER — use built-in functions: Catalyst can optimise these
from pyspark.sql.functions import lower, trim
result = (
    df1.withColumn("id_norm", lower(trim(col("id"))))
       .join(df2.withColumn("id_norm", lower(trim(col("id")))), "id_norm")
)
```

---

## D10 · Catalyst Optimizer

### How Catalyst works

Catalyst is Spark's query optimizer. It transforms a logical plan through four phases:

```
SQL / DataFrame API
        │
        ▼
1. Unresolved Logical Plan    (column names not resolved)
        │  Analysis
        ▼
2. Analyzed Logical Plan      (all names resolved against catalog)
        │  Optimization (rule-based + cost-based)
        ▼
3. Optimized Logical Plan     (predicates pushed, columns pruned, filters merged)
        │  Physical Planning
        ▼
4. Physical Plan              (Exchange, SortMergeJoin, FileScan, ...)
        │  Code Generation
        ▼
5. Generated Java bytecode    (WholeStageCodegen)
```

Anti-patterns in this dimension prevent Catalyst from completing its optimisation passes
correctly, producing physically inefficient plans.

### `select("*")` prevents column pruning

Catalyst's column pruning rule eliminates columns from `FileScan` nodes that are not
used by downstream operations. `select("*")` tells Catalyst that all columns are needed,
defeating this optimisation:

```python
# BEFORE — reads all 500 columns from Parquet; only 3 are used
result = (
    spark.read.parquet("s3://wide_table/")
    .select("*")                      # SPL-D10-001: prevents pruning
    .filter(col("status") == "ACTIVE")
    .groupBy("region")
    .agg(F.sum("revenue"))
)

# AFTER — Catalyst prunes to 3 columns at the FileScan node
result = (
    spark.read.parquet("s3://wide_table/")
    .select("status", "region", "revenue")    # explicit; 497 columns skipped at read
    .filter(col("status") == "ACTIVE")
    .groupBy("region")
    .agg(F.sum("revenue"))
)
```

### Filter placement: push down, not up

Filters reduce data volume as early as possible. A filter applied *after* a join or
aggregation is processed on the full dataset; the same filter applied *before* is
processed on the raw scan, reducing the data that flows into the join:

```python
# BEFORE — filter after join: join processes all 1B rows
result = (
    fact_1B_rows
    .join(dim_df, "product_id")
    .filter(col("region") == "EMEA")    # late filter: runs after 1B-row join
)

# AFTER — filter before join: join processes only EMEA rows (~200M)
result = (
    fact_1B_rows
    .filter(col("region") == "EMEA")    # early filter: reduces input to join
    .join(dim_df, "product_id")
)
```

Catalyst often pushes filters through joins automatically — but not always. Verify with
`explain()` that `Filter` nodes appear *below* `Exchange` nodes in the physical plan.

### `withColumn` in a loop

Each `withColumn` call creates a new plan node. A loop of 100 `withColumn` calls
produces a plan with 100 nested `Project` nodes. Catalyst must process this 100-node
plan, and at execution time, 100 independent closures are evaluated per row:

```python
# BEFORE — 100 withColumn calls: deeply nested plan, slow planning
result = df
for i in range(100):
    result = result.withColumn(f"col_{i}", F.lit(i) * col("base_value"))

# AFTER — single select() with all expressions: flat plan, fast execution
exprs = [F.col("*")] + [
    (F.lit(i) * col("base_value")).alias(f"col_{i}")
    for i in range(100)
]
result = df.select(*exprs)
```

The timing difference on 100 columns: `withColumn` loop takes O(n²) planning time (each
`withColumn` re-analyses the entire preceding plan); `select()` takes O(n).

### `explode()` without a filter

`explode()` expands one row into N rows — one per array element. On a column with a
maximum array size of 10,000, a 100 million row DataFrame becomes 1 trillion rows:

```python
# BEFORE — explode without filter: potentially catastrophic expansion
from pyspark.sql.functions import explode

result = (
    events_df
    .withColumn("event", explode(col("events_array")))   # array of up to 10,000 items
)
# With 100M rows and avg 1,000 items: 100B output rows

# AFTER — filter + limit the array before exploding
from pyspark.sql.functions import slice, array_distinct

result = (
    events_df
    .withColumn("events_clean", array_distinct(slice(col("events_array"), 1, 100)))
    .withColumn("event", explode(col("events_clean")))   # bounded expansion
    .filter(col("event.type").isin(["click", "purchase"]))
)
```

### Non-deterministic functions in join conditions

`rand()`, `randn()`, `uuid()`, and `now()` are evaluated once per row, but in a join
condition they may be evaluated multiple times (during plan optimisation, re-evaluation).
This produces incorrect join results:

```python
# BEFORE — rand() in join condition: non-deterministic; may produce duplicates or mismatches
result = df1.join(df2, col("df1.salt") == F.floor(F.rand() * 10))   # rand re-evaluates

# AFTER — materialise the non-deterministic value before the join
df1_salted = df1.withColumn("salt", F.floor(F.rand() * 10).cast(IntegerType()))
result = df1_salted.join(df2, "salt")
```

---

## D11 · Monitoring and Observability

### Why observability matters in Spark

Spark jobs fail silently in ways that are invisible without instrumentation. A job that
"completes successfully" may have:
- Processed 90% of expected records (missing data, not an error)
- Spilled 500 GB to disk (degraded performance, no exception thrown)
- Retried 50 tasks due to executor evictions (wasted resources, logs only)
- Written partial output due to a non-retriable task failure mid-stage

Good observability turns these silent failures into visible, actionable signals.

### Event logging

```python
spark = (
    SparkSession.builder
    .config("spark.eventLog.enabled",  "true")
    .config("spark.eventLog.dir",      "s3://spark-logs/history/")
    .config("spark.eventLog.compress", "true")
    .getOrCreate()
)
```

Event logs enable the Spark History Server, post-mortem DAG visualisation, and
third-party tools (Datadog, Grafana, Databricks Ganglia). Without them, a failed job
from yesterday is a black box.

### SparkListener for real-time metrics

```python
from pyspark import SparkContext
from pyspark.sql import SparkSession

class ETLJobListener:
    """Custom listener that tracks stage metrics."""

    def __init__(self):
        self.stage_metrics = {}

    def onStageCompleted(self, stage_completed):
        info = stage_completed.stageInfo()
        task_metrics = info.taskMetrics()
        self.stage_metrics[info.stageId()] = {
            "name":              info.name(),
            "records_read":      task_metrics.inputMetrics().recordsRead(),
            "bytes_read":        task_metrics.inputMetrics().bytesRead(),
            "shuffle_bytes":     task_metrics.shuffleReadMetrics().totalBytesRead(),
            "disk_spill_bytes":  task_metrics.diskBytesSpilled(),
            "gc_time_ms":        task_metrics.jvmGCTime(),
        }

listener = ETLJobListener()
sc._jvm.org.apache.spark.SparkContext.getOrCreate().addSparkListener(listener)
```

### Accumulators for business metrics

Accumulators are distributed counters that aggregate across tasks back to the driver.
Use them to track business-level metrics that indicate data quality:

```python
null_counter     = spark.sparkContext.accumulator(0, "null_customer_ids")
negative_counter = spark.sparkContext.accumulator(0, "negative_amounts")

def validate_and_flag(row):
    if row["customer_id"] is None:
        null_counter.add(1)
    if row["amount"] < 0:
        negative_counter.add(1)
    return row

validated = transactions.rdd.map(validate_and_flag).toDF(transactions.schema)
validated.write.parquet("s3://output/")   # trigger accumulator evaluation

print(f"Null customer IDs: {null_counter.value}")
print(f"Negative amounts:  {negative_counter.value}")

if null_counter.value > 0.01 * total_rows:
    raise ValueError(f"Data quality failure: {null_counter.value} null customer IDs")
```

### Error handling around Spark actions

Spark actions (`count`, `collect`, `write`, `save`) can fail with transient errors. Without
try/except, a failure silently marks the job as failed with no context in the
application logs:

```python
import logging
logger = logging.getLogger(__name__)

# BEFORE — no error handling: silent failure, no context
result_df.write.mode("overwrite").parquet(output_path)

# AFTER — structured error handling with retry logic
MAX_RETRIES = 3
for attempt in range(1, MAX_RETRIES + 1):
    try:
        result_df.write.mode("overwrite").parquet(output_path)
        logger.info("Write succeeded on attempt %d: %s", attempt, output_path)
        break
    except Exception as exc:
        logger.error(
            "Write failed on attempt %d/%d: %s — %s",
            attempt, MAX_RETRIES, output_path, exc,
            exc_info=True,
        )
        if attempt == MAX_RETRIES:
            raise RuntimeError(
                f"Write to {output_path} failed after {MAX_RETRIES} attempts"
            ) from exc
        time.sleep(2 ** attempt)  # exponential backoff
```

### Logging best practices

Every Spark job should configure Python's logging framework before creating the
SparkSession:

```python
import logging

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),                          # captured by Spark driver logs
        logging.FileHandler(f"/tmp/etl_{job_date}.log"), # local file for debugging
    ]
)
logger = logging.getLogger("etl_daily_positions")

def main():
    logger.info("Job starting: etl_daily_positions date=%s", job_date)
    spark = SparkSession.builder.appName("etl_daily_positions").getOrCreate()
    logger.info("SparkSession created. appId=%s", spark.sparkContext.applicationId)
    # ...
    logger.info("Job complete: %d records written in %.1fs", record_count, elapsed)
```

---

## Appendix A · Spark Config Quick Reference

### Executor and memory

| Config | Default | Recommended | Notes |
|---|---|---|---|
| `spark.executor.memory` | `1g` | `4g – 64g` | Scale with task data size; add 20% headroom |
| `spark.executor.memoryOverhead` | `max(384m, 10%)` | `512m – 8g` | Add 2–4g for pandas UDF workloads |
| `spark.executor.cores` | `1` | `4` | Sweet spot: 3–5 cores per executor |
| `spark.memory.fraction` | `0.6` | `0.8` | Fraction of JVM heap for Spark managed memory |
| `spark.memory.storageFraction` | `0.5` | `0.3` | Reduce if execution pressure > storage reuse |
| `spark.driver.memory` | `1g` | `4g` | Increase for large `collect()` or broadcasts |
| `spark.driver.maxResultSize` | `1g` | `2g` | Prevents OOM from large `collect()` results |

### Serialization and compression

| Config | Default | Recommended | Notes |
|---|---|---|---|
| `spark.serializer` | `JavaSerializer` | `KryoSerializer` | 10× faster; 3–5× smaller payloads |
| `spark.kryo.registrationRequired` | `false` | `false` | Set `true` only after registering all classes |
| `spark.kryoserializer.buffer.max` | `64m` | `512m` | Raise if seeing buffer overflow errors |
| `spark.io.compression.codec` | `lz4` | `lz4` / `zstd` | lz4 for speed; zstd for storage savings |
| `spark.shuffle.compress` | `true` | `true` | Always keep enabled |
| `spark.broadcast.compress` | `true` | `true` | Always keep enabled |

### Shuffle and partitioning

| Config | Default | Recommended | Notes |
|---|---|---|---|
| `spark.sql.shuffle.partitions` | `200` | Formula-based | Set high; AQE coalesces down |
| `spark.shuffle.file.buffer` | `32k` | `1m` | Larger buffer → fewer disk flushes |
| `spark.reducer.maxSizeInFlight` | `48m` | `96m` | Increase on high-bandwidth networks |
| `spark.shuffle.io.maxRetries` | `3` | `10` | Increase for flaky network environments |
| `spark.shuffle.io.retryWait` | `5s` | `60s` | Back off longer between retries |

### Joins

| Config | Default | Recommended | Notes |
|---|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` | `10m` | `100m – 512m` | Tune based on executor memory |
| `spark.sql.broadcastTimeout` | `300s` | `600s` | Increase for large broadcasts on slow networks |
| `spark.sql.join.preferSortMergeJoin` | `true` | `true` | Disable only when testing SHJ explicitly |

### Adaptive Query Execution

| Config | Default | Recommended | Notes |
|---|---|---|---|
| `spark.sql.adaptive.enabled` | `false` (2.x) / `true` (3.3+) | `true` | Highest-ROI single config; always enable |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | `true` | Merges small post-shuffle partitions |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64m` | `128m` | Target post-coalesce partition size |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | `true` | Automatically splits skewed partitions |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` | `5` | Trigger when partition is 5× median |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `256m` | `256m` | Minimum size before skew treatment |
| `spark.sql.adaptive.localShuffleReader.enabled` | `true` | `true` | Reads shuffle data locally when possible |

### Network and timeouts

| Config | Default | Recommended | Notes |
|---|---|---|---|
| `spark.network.timeout` | `120s` | `800s` | Default too short for large shuffles |
| `spark.rpc.askTimeout` | `120s` | `600s` | Driver ↔ executor RPC timeout |
| `spark.executor.heartbeatInterval` | `10s` | `20s` | Should be ≪ `network.timeout` |
| `spark.speculation` | `false` | `false` | Can cause duplicate writes; leave off |

### Event log and monitoring

| Config | Default | Recommended | Notes |
|---|---|---|---|
| `spark.eventLog.enabled` | `false` | `true` | Required for History Server |
| `spark.eventLog.dir` | (unset) | `s3://logs/` | Point to persistent storage |
| `spark.eventLog.compress` | `false` | `true` | Reduces log file size significantly |
| `spark.ui.retainedJobs` | `1000` | `1000` | Increase for long-running applications |

---

## Appendix B · Decision Matrices

### B1. Join Strategy Selection

**Inputs:** `left_size_gb`, `right_size_gb`, `broadcast_threshold_gb`, `join_type`
(equi/non-equi/cross), `aqe_enabled`, `both_bucketed`, `skew_ratio`, `spark_version`

**Priority rules (first match wins):**

| Priority | Condition | Strategy | Key Config |
|---|---|---|---|
| 1 | `join_type` is non-equi or cross | BROADCAST_NESTED_LOOP | `crossJoin.enabled=true` |
| 2 | `smaller_side ≤ broadcast_threshold` AND equi | BROADCAST_HASH_JOIN | `autoBroadcastJoinThreshold` |
| 3 | `both_bucketed=true` AND equi AND `skew_ratio < 5` | BUCKETED_SORT_MERGE | `bucketing.enabled=true` |
| 4 | `aqe_enabled=true` AND `skew_ratio ≥ 5` AND Spark 3+ | AQE_SKEW_JOIN | `skewJoin.enabled=true` |
| 5 | `skew_ratio ≥ 10` AND AQE unavailable | SALTED_JOIN | manual implementation |
| 6 | `size_ratio ≥ 3` AND `smaller ≤ 10 GB` AND `skew < 5` | SHUFFLE_HASH_JOIN | `adaptive.enabled=true` |
| 7 | equi AND `smaller > broadcast_threshold` AND `skew < 5` | SORT_MERGE_JOIN | `shuffle.partitions` |
| Default | Any remaining case | SORT_MERGE_JOIN | |

### B2. Shuffle Partition Count

**Inputs:** `data_size_gb`, `executor_count`, `cores_per_executor`,
`target_partition_size_mb` (default 128), `aqe_enabled`, `is_write_stage`

**Formulas:**
```
total_parallelism    = executor_count × cores_per_executor
raw_partition_count  = ceil((data_size_gb × 1024) / target_partition_size_mb)
aqe_upper_bound      = max(raw_partition_count × 2, total_parallelism × 4)
recommended_fixed    = max(raw_partition_count, total_parallelism × 2)
```

**Rules:**

| Condition | Action | Config |
|---|---|---|
| `aqe_enabled=true` AND not write stage | Set to `aqe_upper_bound`; AQE coalesces | `shuffle.partitions = aqe_upper_bound`; `advisoryPartitionSizeInBytes = 128m` |
| Write stage AND `data_size_gb ≤ 10` | `coalesce(target_file_count)` | No config change |
| Write stage AND `data_size_gb > 10` | `repartition(recommended_fixed)` | No config change |
| `data_size_gb < 1` AND no AQE | `total_parallelism` | `shuffle.partitions = total_parallelism` |
| Large dataset, no AQE | `recommended_fixed` | `shuffle.partitions = recommended_fixed` |

### B3. Cache Decision

**Inputs:** `reuse_count` (how many times the DF is accessed), `compute_cost_s`
(estimated seconds to recompute), `data_size_gb`, `available_storage_memory_gb`,
`is_streaming`

**Rules:**

| Condition | Decision | Storage Level |
|---|---|---|
| `is_streaming=true` | Never cache — use stateful streaming checkpoints | — |
| `reuse_count ≤ 1` | Do NOT cache — single-use cache is overhead | — |
| `compute_cost_s < 2` AND `reuse_count ≤ 3` | Usually not worth caching | — |
| `data_size_gb ≤ available_storage_memory_gb × 0.5` | Cache in memory | `MEMORY_ONLY` |
| `data_size_gb ≤ available_storage_memory_gb` | Cache with disk fallback | `MEMORY_AND_DISK` |
| `data_size_gb > available_storage_memory_gb` AND `compute_cost_s > 60` | Cache serialized | `MEMORY_ONLY_SER` |
| `data_size_gb > available_storage_memory_gb × 2` | Write to Parquet, re-read | — |

### B4. File Format Selection

**Inputs:** `access_pattern` (analytics/operational/archive/streaming), `schema_evolution_needed`,
`downstream_tool`, `update_delete_needed`, `data_size_gb`

| Access Pattern | Schema Evolution | Updates/Deletes | Recommended Format |
|---|---|---|---|
| Analytics (column scans) | No | No | Parquet + Snappy |
| Analytics (column scans) | No | No, large cold data | Parquet + Zstd |
| Analytics + Time Travel | Yes | Yes | Delta Lake |
| Streaming ingest | Yes | Append only | Avro or Parquet (micro-batch) |
| Row-level operational | Yes | Yes | Delta Lake or Iceberg |
| Human readable / debug | Yes | No | JSON (small only) |
| Legacy system integration | No | No | CSV (write gateway only) |

### B5. Skew Remediation

**Inputs:** `skew_ratio`, `aqe_enabled`, `spark_version`, `smaller_side_gb`,
`hot_key_fraction` (fraction of data in the top key)

| Condition | Recommended Approach |
|---|---|
| `skew_ratio < 5` | No action needed |
| `5 ≤ skew_ratio < 10` AND AQE + Spark 3.0+ | AQE skew join (zero code change) |
| `skew_ratio ≥ 5` AND `smaller_side_gb ≤ 2` | Raise broadcast threshold to include smaller side |
| `skew_ratio ≥ 10` AND AQE unavailable | Manual salting with `salt_factor = ceil(skew_ratio / 5)` |
| `hot_key_fraction > 0.5` | Filter hot keys; process separately; union results |
| Null-key skew | Pre-filter nulls; join non-nulls; union with null-keyed rows |

### B6. Memory Sizing Formula

See [Appendix C](#appendix-c--memory-sizing-formula).

### B7. Serialization Selection

| Condition | Serializer | Notes |
|---|---|---|
| Any production workload | Kryo | 10× faster; 3–5× smaller |
| Mixed domain classes, migration phase | Kryo + `registrationRequired=false` | Safe default |
| All classes registered | Kryo + `registrationRequired=true` | Maximum efficiency |
| Debugging serialization errors | Java (temporary) | Verbose error messages |

### B8. Compression Selection

| Subsystem | Default | Recommended | Rationale |
|---|---|---|---|
| Shuffle write | `lz4` | `lz4` | Low CPU overhead; fast path for transient data |
| Broadcast | `lz4` | `lz4` | Must compress/decompress on all executors; speed first |
| Parquet write (hot) | `snappy` | `snappy` | Balance: fast read/write, moderate ratio |
| Parquet write (cold/archive) | `snappy` | `zstd` | Best ratio; acceptable write overhead for infrequent reads |
| RDD persistence | `snappy` | `lz4` | Reduce cache footprint without excess CPU |
| Event log | none | `lz4` | Significant size reduction; minimal overhead |

---

## Appendix C · Memory Sizing Formula

### Executor memory sizing

```
executor_memory_total = data_per_task × concurrent_tasks × safety_factor + overhead

Where:
  data_per_task      = (dataset_size_bytes / total_tasks)
                       × expected_amplification  # sort: 2×; join: 3×; aggregation: 2–4×
  concurrent_tasks   = spark.executor.cores      # tasks running simultaneously per executor
  safety_factor      = 1.2 – 1.5                # headroom for GC, peak memory
  overhead           = max(384m, executor_memory_jvm × 0.10)  # off-JVM overhead

Practical starting points:
  Simple filter/select ETL:    4g executor, 512m overhead
  Join + aggregation pipeline: 8g – 16g executor, 1g – 2g overhead
  ML feature pipeline (wide):  16g – 32g executor, 2g – 4g overhead
  Python UDF heavy:            Base + 2g – 4g overhead (Python worker memory)
  Pandas UDF with Arrow:       Base + Arrow_batch_size × concurrent_tasks overhead
```

### Task-level memory amplification by operation

| Operation | Memory Amplification | Notes |
|---|---|---|
| `filter`, `project`, `map` | 1× (streaming) | Constant memory; no materialisation |
| `sort` | 2–3× input size | Input + sorted copy; spills when exceeded |
| `groupBy` + aggregate | 2–4× group key size | Hash table grows with distinct key count |
| `join` (hash build side) | 1.5–2× build side | Hash table for one partition |
| `join` (sort-merge) | 2–3× per partition | Sort both sides per partition |
| `explode` | N× input size | N = average array length; can be extreme |
| `distinct` | 2–3× input size | Sort + dedup or hash table |
| `window` (unbounded) | All rows for partition | Can be catastrophic on skewed partitions |
| `collect()` | Full dataset on driver | Never on large datasets |

### Dynamic allocation sizing

For variable-load clusters, configure the executor pool based on peak workload:

```python
.config("spark.dynamicAllocation.enabled",                "true")
.config("spark.dynamicAllocation.minExecutors",           str(ceil(base_load_cores / executor_cores)))
.config("spark.dynamicAllocation.maxExecutors",           str(ceil(peak_load_cores / executor_cores)))
.config("spark.dynamicAllocation.executorIdleTimeout",    "60s")     # retire idle executors quickly
.config("spark.dynamicAllocation.schedulerBacklogTimeout","1s")      # request new executors fast
```

`minExecutors` should be large enough to maintain shuffle service state for long-running
queries. `maxExecutors` should be capped to control cloud costs and prevent overloading
the cluster manager.

---

*This knowledge base is derived from the `spark-perf-lint` rule engine's internal YAML
knowledge files (`patterns.yaml`, `decision_matrices.yaml`, `recommendations.yaml`,
`spark_configs.yaml`) and reflects best practices for Apache Spark 3.x workloads.
For the machine-readable versions used by the lint rules, see
`src/spark_perf_lint/knowledge/`.*

*Last updated: 2026-04-10*
