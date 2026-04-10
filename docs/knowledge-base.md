---
layout: default
title: Knowledge Base
nav_order: 5
---

# Knowledge Base
{: .no_toc }

Patterns, decision matrices, and Spark configuration reference that power
spark-perf-lint's recommendations.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Join strategy decision matrix

| Left size | Right size | Strategy | Why |
|-----------|------------|----------|-----|
| Any | < 10 MB | Broadcast Hash Join | Right side fits in executor memory; zero shuffle |
| Any | 10–200 MB | Broadcast (with hint) | Use `broadcast()` hint; check `autoBroadcastJoinThreshold` |
| Large | Large, same key | Sort Merge Join | Both sides pre-sorted; minimal extra shuffle |
| Large | Large, skewed | AQE Skew Join | AQE splits skewed partitions automatically |
| Large | Large, unknown skew | Salted join | Pre-salt with `rand()` to distribute dominant keys |

---

## Partitioning patterns

### When to use repartition vs coalesce

| Goal | Use | Why |
|------|-----|-----|
| Increase partition count | `repartition(n)` | Full shuffle; balanced output |
| Decrease partition count | `repartition(n)` before write | Balanced files |
| Decrease without shuffle | `coalesce(n)` | No shuffle; unbalanced — OK only for final write to single file |
| Partition by column for storage | `write.partitionBy(col)` | Enables partition pruning on read |
| Co-locate join keys on disk | `write.bucketBy(n, col)` | Eliminates shuffle on future joins |

### Partition sizing guide

| Dataset size | Recommended partitions | Target partition size |
|-------------|----------------------|----------------------|
| < 1 GB | 8–32 | 32–128 MB |
| 1–100 GB | 100–800 | 128 MB |
| > 100 GB | 800–4000 | 128–256 MB |
| After AQE coalesce | Auto | `advisoryPartitionSizeInBytes` (default 64 MB) |

---

## Caching decision matrix

| Scenario | Recommendation |
|----------|---------------|
| DataFrame used once | Don't cache |
| DataFrame used 2+ times in same job | `cache()` or `persist(MEMORY_AND_DISK)` |
| Iterative algorithm (MLlib loops) | `checkpoint()` every N iterations to truncate lineage |
| Large DataFrame, uncertain memory | `MEMORY_AND_DISK` — safe fallback to disk |
| Small lookup table | `broadcast()` instead of `cache()` |
| After cache, no longer needed | Call `unpersist()` immediately |

---

## I/O format comparison

| Format | Column pruning | Predicate pushdown | Compression | Best for |
|--------|---------------|-------------------|-------------|----------|
| **Parquet** | ✅ | ✅ | Snappy/Zstd | Analytics, data lake |
| **ORC** | ✅ | ✅ | Zlib/Snappy | Hive-compatible analytics |
| **Delta Lake** | ✅ | ✅ + Z-order | Snappy | ACID, time travel |
| **Avro** | ❌ | ❌ | Deflate | Streaming, schema evolution |
| **CSV** | ❌ | ❌ | None | Interchange only |
| **JSON** | ❌ | ❌ | None | Interchange only |

---

## AQE configuration reference

| Config | Default | Recommended | Effect |
|--------|---------|-------------|--------|
| `spark.sql.adaptive.enabled` | `true` (3.2+) | `true` | Enables all AQE features |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | `true` | Merges small post-shuffle partitions |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64m` | `64m–128m` | Target size after coalescing |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | `true` | Splits skewed join partitions |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` | `3–5` | Multiplier over median to flag as skewed |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `256m` | `128m` | Minimum size to consider a partition skewed |
| `spark.sql.adaptive.localShuffleReader.enabled` | `true` | `true` | Reads shuffle data locally when possible |

---

## UDF vs native function guide

### Always prefer native functions

```python
# SLOW — Python UDF, row-at-a-time serialisation
@udf(returnType=StringType())
def my_lower(s):
    return s.lower()
df.withColumn('name', my_lower(col('name')))

# FAST — native SQL function, JVM-side, Catalyst-optimisable
from pyspark.sql.functions import lower
df.withColumn('name', lower(col('name')))
```

### When Python UDFs are acceptable

- Complex business logic with no SQL equivalent
- Third-party library calls (e.g., ML inference, regex with `re` module)
- One-off transformations on small DataFrames

### When to use pandas_udf instead of Python UDF

Use `@pandas_udf` when:
- Processing vectorised operations on a column (e.g., `pd.Series → pd.Series`)
- Using numpy/scipy/sklearn on batches
- Need Arrow-based serialisation (10–100× faster than row-at-a-time)

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf('double')
def score(col: pd.Series) -> pd.Series:
    return col * 1.5  # vectorised — no row loop
```

---

## Key Spark configurations

### Memory

| Config | Recommended | Notes |
|--------|-------------|-------|
| `spark.executor.memory` | `4g–16g` | Tune to workload; leave headroom for overhead |
| `spark.executor.memoryOverhead` | `512m–2g` | Min 10% of executor memory; more for Python UDFs |
| `spark.driver.memory` | `2g–8g` | Increase if collecting large results |
| `spark.python.worker.memory` | `512m` | Required if using Python UDFs |
| `spark.memory.fraction` | `0.6` | Fraction of JVM heap for execution + storage |
| `spark.memory.storageFraction` | `0.5` | Fraction of memory.fraction reserved for storage |

### Serialisation

| Config | Recommended | Notes |
|--------|-------------|-------|
| `spark.serializer` | `org.apache.spark.serializer.KryoSerializer` | 10× faster than Java serialiser |
| `spark.kryo.registrationRequired` | `false` | Set `true` in prod for strict validation |

### Shuffle

| Config | Recommended | Notes |
|--------|-------------|-------|
| `spark.sql.shuffle.partitions` | `cluster_cores × 2` | Default 200 is too low for large data |
| `spark.shuffle.file.buffer` | `1m` | Reduces shuffle write syscalls |
| `spark.reducer.maxSizeInFlight` | `96m` | Increase for large shuffle reads |

### Executor sizing

| Config | Recommended | Notes |
|--------|-------------|-------|
| `spark.executor.cores` | `4–5` | Higher degrades HDFS throughput |
| `spark.executor.instances` | Use dynamic allocation | Fixed instances waste resources |
| `spark.dynamicAllocation.enabled` | `true` | Scales executors to workload |
