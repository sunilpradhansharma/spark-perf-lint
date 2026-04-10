---
layout: default
title: D01 Cluster Config
nav_order: 1
parent: Rules
---

# D01 — Cluster Configuration
{: .no_toc }

10 rules that catch missing or incorrect Spark cluster configuration before the
job ever reaches the cluster.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## SPL-D01-001 — Missing Kryo serializer
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`spark.serializer` is not set to `KryoSerializer`.**

KryoSerializer is 10× faster than Java's default and produces 3–5× smaller
shuffle data. Almost every Spark job benefits.

```python
# Before
spark = SparkSession.builder.appName("job").getOrCreate()

# After
spark = (SparkSession.builder
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate())
```

**Impact:** 10× serialization speedup; 3–5× reduction in shuffle data size

---

## SPL-D01-002 — Executor memory not configured
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`spark.executor.memory` is not explicitly set.**

Without an explicit setting, executors use the cluster default (often 1g),
which is insufficient for any real workload.

```python
# Before
spark = SparkSession.builder.appName("job").getOrCreate()

# After
spark = (SparkSession.builder
    .config("spark.executor.memory", "4g")
    .getOrCreate())
```

**Impact:** Executor OOM or excessive GC on any dataset > JVM heap size

---

## SPL-D01-003 — Driver memory not configured
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`spark.driver.memory` is not explicitly set.**

```python
# Before
spark = SparkSession.builder.appName("job").getOrCreate()

# After
spark = (SparkSession.builder
    .config("spark.driver.memory", "2g")
    .getOrCreate())
```

**Impact:** Driver OOM when collecting results or using large broadcast variables

---

## SPL-D01-004 — Dynamic allocation not enabled
{: .d-inline-block }
INFO
{: .label .label-blue }

**`spark.dynamicAllocation.enabled` is not set to `true`.**

```python
# Before
spark = SparkSession.builder.config("spark.executor.instances", "10").getOrCreate()

# After
spark = (SparkSession.builder
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.minExecutors", "2")
    .config("spark.dynamicAllocation.maxExecutors", "20")
    .getOrCreate())
```

**Impact:** Idle executor waste; reduced cluster throughput for concurrent jobs

---

## SPL-D01-005 — Executor cores set too high
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`spark.executor.cores` is set above the recommended maximum of 5.**

More than 5 cores per executor degrades HDFS throughput (3–5 simultaneous reads
per executor is the sweet spot) and increases GC pause frequency.

```python
# Before
.config("spark.executor.cores", "10")

# After
.config("spark.executor.cores", "4")
```

**Impact:** HDFS throughput degradation; increased GC pause frequency

---

## SPL-D01-006 — Memory overhead too low
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`spark.executor.memoryOverhead` is configured below the recommended 384 MB.**

Memory overhead is used by the JVM itself, Python workers, and off-heap
operations. Too low causes executor containers to be killed by YARN/K8s without
an OOM log entry.

```python
# Before
.config("spark.executor.memoryOverhead", "128m")

# After
.config("spark.executor.memoryOverhead", "512m")
```

**Impact:** Executor container killed by YARN/K8s without OOM log entry

---

## SPL-D01-007 — Missing PySpark worker memory config
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**Python UDFs are present but `spark.python.worker.memory` is not configured.**

```python
# Before
@udf(returnType=StringType())
def transform(x): return x.upper()

# After
spark = (SparkSession.builder
    .config("spark.python.worker.memory", "512m")
    .getOrCreate())
```

**Impact:** Python worker OOM when UDFs process large inputs; silent task failures

---

## SPL-D01-008 — Network timeout too low
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`spark.network.timeout` is set below the recommended 120 seconds.**

Short timeouts cause false-positive executor evictions during GC pauses or
heavy shuffle operations.

```python
# Before
.config("spark.network.timeout", "30s")

# After
.config("spark.network.timeout", "120s")
```

**Impact:** False-positive executor evictions during GC or shuffle; wasted recomputation

---

## SPL-D01-009 — Speculation not enabled
{: .d-inline-block }
INFO
{: .label .label-blue }

**`spark.speculation` is not enabled.**

Speculation detects straggler tasks and launches duplicate copies on other
executors, preventing slow nodes from blocking entire stages.

```python
# After
spark = (SparkSession.builder
    .config("spark.speculation", "true")
    .config("spark.speculation.multiplier", "1.5")
    .getOrCreate())
```

**Impact:** Slow nodes can stall entire stages; 2–5× job slowdown on degraded hardware

---

## SPL-D01-010 — Default shuffle partitions unchanged
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`spark.sql.shuffle.partitions` is not set (default: 200).**

200 partitions is too few for large data (spill to disk) and too many for small
data (task-scheduler overhead). Tune to `2 × cluster_cores`.

```python
# Before
spark = SparkSession.builder.appName("job").getOrCreate()  # default: 200

# After
.config("spark.sql.shuffle.partitions", "400")  # tuned for cluster
```

**Impact:** Spill to disk on large datasets; small-file explosion on small datasets

---

## Suppress a rule

```python
spark = SparkSession.builder.getOrCreate()  # noqa: SPL-D01-001
```
