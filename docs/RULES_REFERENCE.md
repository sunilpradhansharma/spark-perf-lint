# spark-perf-lint Rules Reference

Complete documentation for all 93 rules across 11 performance dimensions.

This reference covers rules **SPL-D01** through **SPL-D11**. Full details for D01–D03 are
in this file. D04–D11 documentation follows the same structure.

---

## How to Read This Reference

Each rule entry contains:

| Field | Meaning |
|-------|---------|
| **Rule ID** | Unique identifier: `SPL-D{dimension}-{number}` |
| **Severity** | `CRITICAL` / `WARNING` / `INFO` — default; override in `.spark-perf-lint.yaml` |
| **Effort** | `Config only` · `Minor code change` · `Major refactor` |
| **What it detects** | Python AST pattern that triggers the rule |
| **Why it matters** | Spark internals explanation |
| **How to fix** | Corrected code |
| **Config options** | Spark configs or lint thresholds that affect this rule |
| **Related rules** | Other rules that address the same concern from a different angle |

### Severity Levels

- **CRITICAL** — near-certain performance disaster or correctness risk; fix before merging
- **WARNING** — significant performance impact; fix in the current sprint
- **INFO** — best-practice suggestion; fix when convenient

### Controlling Rules

Any rule can be disabled or have its severity overridden in `.spark-perf-lint.yaml`:

```yaml
rules:
  SPL-D01-001:
    enabled: false        # disable entirely
  SPL-D03-001:
    severity: WARNING     # downgrade from CRITICAL
```

---

## Table of Contents — All 93 Rules

### D01 · Cluster Configuration (10 rules)

| Rule ID | Name | Severity |
|---------|------|----------|
| [SPL-D01-001](#spl-d01-001--missing-kryo-serializer) | Missing Kryo serializer | WARNING |
| [SPL-D01-002](#spl-d01-002--executor-memory-not-configured) | Executor memory not configured | WARNING |
| [SPL-D01-003](#spl-d01-003--driver-memory-not-configured) | Driver memory not configured | WARNING |
| [SPL-D01-004](#spl-d01-004--dynamic-allocation-not-enabled) | Dynamic allocation not enabled | INFO |
| [SPL-D01-005](#spl-d01-005--executor-cores-set-too-high) | Executor cores set too high | WARNING |
| [SPL-D01-006](#spl-d01-006--memory-overhead-too-low) | Memory overhead too low | WARNING |
| [SPL-D01-007](#spl-d01-007--missing-pyspark-worker-memory-config) | Missing PySpark worker memory config | WARNING |
| [SPL-D01-008](#spl-d01-008--network-timeout-too-low) | Network timeout too low | WARNING |
| [SPL-D01-009](#spl-d01-009--speculation-not-enabled) | Speculation not enabled | INFO |
| [SPL-D01-010](#spl-d01-010--default-shuffle-partitions-unchanged) | Default shuffle partitions unchanged | WARNING |

### D02 · Shuffle (8 rules)

| Rule ID | Name | Severity |
|---------|------|----------|
| [SPL-D02-001](#spl-d02-001--groupbykey-instead-of-reducebykey) | groupByKey() instead of reduceByKey() | CRITICAL |
| [SPL-D02-002](#spl-d02-002--default-shuffle-partitions-unchanged) | Default shuffle partitions unchanged | WARNING |
| [SPL-D02-003](#spl-d02-003--unnecessary-repartition-before-join) | Unnecessary repartition before join | WARNING |
| [SPL-D02-004](#spl-d02-004--orderby--sort-without-limit) | orderBy/sort without limit | WARNING |
| [SPL-D02-005](#spl-d02-005--distinct-without-prior-filter) | distinct() without prior filter | INFO |
| [SPL-D02-006](#spl-d02-006--shuffle-followed-by-coalesce) | Shuffle followed by coalesce | INFO |
| [SPL-D02-007](#spl-d02-007--multiple-shuffles-in-sequence) | Multiple shuffles in sequence | WARNING |
| [SPL-D02-008](#spl-d02-008--shuffle-file-buffer-too-small) | Shuffle file buffer too small | INFO |

### D03 · Joins (10 rules)

| Rule ID | Name | Severity |
|---------|------|----------|
| [SPL-D03-001](#spl-d03-001--cross-join--cartesian-product) | Cross join / cartesian product | CRITICAL |
| [SPL-D03-002](#spl-d03-002--missing-broadcast-hint-on-small-dataframe) | Missing broadcast hint on small DataFrame | WARNING |
| [SPL-D03-003](#spl-d03-003--broadcast-threshold-disabled) | Broadcast threshold disabled | CRITICAL |
| [SPL-D03-004](#spl-d03-004--join-without-prior-filterselect) | Join without prior filter/select | WARNING |
| [SPL-D03-005](#spl-d03-005--join-key-type-mismatch-risk) | Join key type mismatch risk | WARNING |
| [SPL-D03-006](#spl-d03-006--multiple-joins-without-intermediate-repartition) | Multiple joins without intermediate repartition | INFO |
| [SPL-D03-007](#spl-d03-007--self-join-that-could-be-window-function) | Self-join that could be window function | INFO |
| [SPL-D03-008](#spl-d03-008--join-inside-loop) | Join inside loop | CRITICAL |
| [SPL-D03-009](#spl-d03-009--left-join-without-null-handling) | Left join without null handling | INFO |
| [SPL-D03-010](#spl-d03-010--cbostatics-not-enabled-for-complex-joins) | CBO/statistics not enabled for complex joins | WARNING |

### D04 · Partitioning (10 rules)

| Rule ID | Name | Severity |
|---------|------|----------|
| SPL-D04-001 | repartition(1) bottleneck | CRITICAL |
| SPL-D04-002 | coalesce(1) bottleneck | WARNING |
| SPL-D04-003 | Repartition with very high partition count | WARNING |
| SPL-D04-004 | Repartition with very low partition count | WARNING |
| SPL-D04-005 | Coalesce before write — unbalanced output files | INFO |
| SPL-D04-006 | Missing partitionBy on write | INFO |
| SPL-D04-007 | Over-partitioning — high-cardinality partition column | WARNING |
| SPL-D04-008 | Missing bucketBy for repeatedly joined tables | INFO |
| SPL-D04-009 | Partition column not used in query filters | WARNING |
| SPL-D04-010 | Repartition by column different from join key | WARNING |

### D05 · Data Skew (7 rules)

| Rule ID | Name | Severity |
|---------|------|----------|
| SPL-D05-001 | Join on low-cardinality column | WARNING |
| SPL-D05-002 | GroupBy on low-cardinality column without secondary key | WARNING |
| SPL-D05-003 | AQE skew join handling disabled | WARNING |
| SPL-D05-004 | AQE skew threshold too high | INFO |
| SPL-D05-005 | Missing salting pattern for known skewed keys | INFO |
| SPL-D05-006 | Window function partitioned by skew-prone column | WARNING |
| SPL-D05-007 | Null-heavy join key | INFO |

### D06 · Caching (8 rules)

| Rule ID | Name | Severity |
|---------|------|----------|
| SPL-D06-001 | cache() without unpersist() | WARNING |
| SPL-D06-002 | cache() used only once | WARNING |
| SPL-D06-003 | cache() inside loop | CRITICAL |
| SPL-D06-004 | cache() before filter | WARNING |
| SPL-D06-005 | MEMORY_ONLY storage level for potentially large datasets | INFO |
| SPL-D06-006 | Reused DataFrame without cache | WARNING |
| SPL-D06-007 | cache() after repartition | INFO |
| SPL-D06-008 | checkpoint vs cache misuse | INFO |

### D07 · I/O and File Formats (10 rules)

| Rule ID | Name | Severity |
|---------|------|----------|
| SPL-D07-001 | CSV/JSON used for analytical workload | WARNING |
| SPL-D07-002 | Schema inference enabled | WARNING |
| SPL-D07-003 | select("*") — no column pruning | WARNING |
| SPL-D07-004 | Filter applied after join — missing predicate pushdown | INFO |
| SPL-D07-005 | Small file problem on write | INFO |
| SPL-D07-006 | JDBC read without partition parameters | CRITICAL |
| SPL-D07-007 | Parquet compression not set | INFO |
| SPL-D07-008 | Write mode not specified | INFO |
| SPL-D07-009 | No format specified on read/write | INFO |
| SPL-D07-010 | mergeSchema enabled without necessity | INFO |

### D08 · Adaptive Query Execution (7 rules)

| Rule ID | Name | Severity |
|---------|------|----------|
| SPL-D08-001 | AQE disabled | CRITICAL |
| SPL-D08-002 | AQE coalesce partitions disabled | WARNING |
| SPL-D08-003 | AQE advisory partition size too small | INFO |
| SPL-D08-004 | AQE skew join disabled with skew-prone joins detected | WARNING |
| SPL-D08-005 | AQE skew factor too aggressive | INFO |
| SPL-D08-006 | AQE local shuffle reader disabled | INFO |
| SPL-D08-007 | Manual shuffle partition count set high with AQE enabled | INFO |

### D09 · UDF and Code Patterns (12 rules)

| Rule ID | Name | Severity |
|---------|------|----------|
| SPL-D09-001 | Python UDF (row-at-a-time) detected | WARNING |
| SPL-D09-002 | Python UDF replaceable with native Spark function | WARNING |
| SPL-D09-003 | withColumn() inside loop | CRITICAL |
| SPL-D09-004 | Row-by-row iteration over DataFrame | CRITICAL |
| SPL-D09-005 | .collect() without prior filter or limit | CRITICAL |
| SPL-D09-006 | .toPandas() on potentially large DataFrame | CRITICAL |
| SPL-D09-007 | .count() used for emptiness check | WARNING |
| SPL-D09-008 | .show() in production code | INFO |
| SPL-D09-009 | .explain() or .printSchema() in production code | INFO |
| SPL-D09-010 | .rdd conversion dropping out of DataFrame API | WARNING |
| SPL-D09-011 | pandas_udf without type annotations | INFO |
| SPL-D09-012 | Nested UDF calls | WARNING |

### D10 · Catalyst Optimizer (6 rules)

| Rule ID | Name | Severity |
|---------|------|----------|
| SPL-D10-001 | UDF blocks predicate pushdown | WARNING |
| SPL-D10-002 | CBO not enabled for complex queries | WARNING |
| SPL-D10-003 | Join reordering disabled for multi-table joins | INFO |
| SPL-D10-004 | Table statistics not collected | INFO |
| SPL-D10-005 | Non-deterministic function in filter | INFO |
| SPL-D10-006 | Deep method chain (>20 chained ops) | INFO |

### D11 · Monitoring and Observability (5 rules)

| Rule ID | Name | Severity |
|---------|------|----------|
| SPL-D11-001 | No explain() for plan validation in test file | INFO |
| SPL-D11-002 | No Spark listener configured | INFO |
| SPL-D11-003 | No metrics logging in long-running job | INFO |
| SPL-D11-004 | Missing error handling around Spark actions | INFO |
| SPL-D11-005 | Hardcoded storage path | INFO |

---

## D01 · Cluster Configuration

Poor cluster configuration is the most common source of "the job was slow from day one" problems.
These rules scan `SparkSession.builder.config()` and `spark.conf.set()` calls for missing or
misconfigured settings. Most findings require only a config change — zero code refactoring.

---

### SPL-D01-001 — Missing Kryo Serializer

| | |
|---|---|
| **Dimension** | D01 Cluster Configuration |
| **Severity** | WARNING |
| **Effort** | Config only |
| **Impact** | 10× serialization speedup; 3–5× reduction in shuffle data size |

**Description**

`spark.serializer` is not set to `KryoSerializer`. Spark defaults to Java serialization, which is
significantly slower and produces larger payloads for almost every data type encountered in
PySpark workloads.

**What it detects**

A `SparkSession` builder block that does not include a
`.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")` call.

```python
# Triggers SPL-D01-001
spark = SparkSession.builder.appName("etl").getOrCreate()
```

**Why it matters**

Java serialization requires every object to implement `java.io.Serializable`, forces defensive
copies, and produces verbose byte streams. Kryo is typically 10× faster at serializing and
produces 3–5× smaller byte representations, directly reducing shuffle network traffic and spill
file sizes. The Spark block manager also benefits: smaller serialized partitions fit more data
into the storage memory fraction.

**How to fix**

```python
spark = (
    SparkSession.builder
    .appName("etl")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    # Optional: register custom classes for maximum compression
    .config("spark.kryo.registrationRequired", "false")
    .getOrCreate()
)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.serializer` | `org.apache.spark.serializer.KryoSerializer` | Required change |
| `spark.kryo.registrationRequired` | `false` | Set to `true` after registering all classes for maximum efficiency |
| `spark.kryo.classesToRegister` | `com.example.MyClass,...` | Improves Kryo performance for custom types |

**Related rules**

- **SPL-D01-002** — Executor memory; Kryo reduces memory pressure from serialized shuffle data
- **SPL-D02-007** — Multiple shuffles in sequence; Kryo impact multiplies across shuffle stages
- **SPL-D06-005** — MEMORY_ONLY storage level; serialized caching with Kryo uses less heap

---

### SPL-D01-002 — Executor Memory Not Configured

| | |
|---|---|
| **Dimension** | D01 Cluster Configuration |
| **Severity** | WARNING |
| **Effort** | Config only |
| **Impact** | Executor OOM or excessive GC on any dataset > JVM heap size |

**Description**

`spark.executor.memory` is not explicitly set. Spark's deployment-mode default is typically 1 GB
on YARN and Kubernetes, which is insufficient for virtually all production workloads.

**What it detects**

A `SparkSession` builder that creates a session (has a `getOrCreate()` call) without a
`.config("spark.executor.memory", ...)` entry.

```python
# Triggers SPL-D01-002
spark = SparkSession.builder.appName("job").getOrCreate()
```

**Why it matters**

Spark divides executor heap into three fractions: execution memory (shuffle, sort, aggregation),
storage memory (cached RDDs), and user memory (internal structures). With only 1 GB total, even
a modest shuffle stage that processes 500 MB of data fills the execution fraction, triggering
spill to disk. GC pauses lengthen as the heap fills, which can trigger
`spark.network.timeout` and cause false executor evictions. Sizing executor memory correctly
is the single most impactful cluster configuration change for most jobs.

**How to fix**

```python
spark = (
    SparkSession.builder
    .appName("job")
    # Rule of thumb: tasks × data_per_task + 20% GC overhead
    .config("spark.executor.memory", "4g")
    .config("spark.executor.memoryOverhead", "512m")
    .getOrCreate()
)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.executor.memory` | `4g`–`16g` | Scale with data volume per partition |
| `spark.executor.memoryOverhead` | `max(512m, 0.10 × executor.memory)` | Off-heap; see SPL-D01-006 |
| `spark.memory.fraction` | `0.6` (default) | Fraction of heap for execution + storage |
| `spark.memory.storageFraction` | `0.5` (default) | Share of memory.fraction reserved for storage |

**Related rules**

- **SPL-D01-003** — Driver memory; driver also needs explicit sizing
- **SPL-D01-006** — Memory overhead too low; complements executor heap sizing
- **SPL-D01-007** — PySpark worker memory; Python processes consume additional off-heap memory

---

### SPL-D01-003 — Driver Memory Not Configured

| | |
|---|---|
| **Dimension** | D01 Cluster Configuration |
| **Severity** | WARNING |
| **Effort** | Config only |
| **Impact** | Driver OOM when collecting results or using large broadcast variables |

**Description**

`spark.driver.memory` is not explicitly set. The driver accumulates task metadata, broadcast
variables, and the results of `collect()` / `toPandas()` calls. Spark's 1 GB default is routinely
exhausted.

**What it detects**

A `SparkSession` builder that calls `getOrCreate()` without `.config("spark.driver.memory", ...)`.

```python
# Triggers SPL-D01-003
spark = SparkSession.builder.appName("job").getOrCreate()
```

**Why it matters**

The driver is the single point of failure for the entire application. It stores: (1) the complete
query plan for every active query, (2) the serialized bytes for every broadcast variable before
they are sent to executors, (3) the full result set when `collect()` or `toPandas()` is called.
A driver OOM kills the whole application, wasting all executor work already completed. Unlike
executor OOM, there is no automatic retry — the application must restart from scratch.

**How to fix**

```python
spark = (
    SparkSession.builder
    .appName("job")
    .config("spark.driver.memory", "2g")   # minimum for most jobs
    # Increase to 4g+ if: many broadcast joins, or collect() on large results
    .getOrCreate()
)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.driver.memory` | `2g`–`8g` | Scale up with broadcast variable and collect() usage |
| `spark.driver.maxResultSize` | `2g` (default `1g`) | Max size of collect() results; increase if collecting large sets |

**Related rules**

- **SPL-D01-002** — Executor memory; both driver and executor need explicit sizing
- **SPL-D03-002** — Broadcast hints; broadcast variables land in driver memory before distribution
- **SPL-D09-005** — collect() without limit; driver OOM root cause

---

### SPL-D01-004 — Dynamic Allocation Not Enabled

| | |
|---|---|
| **Dimension** | D01 Cluster Configuration |
| **Severity** | INFO |
| **Effort** | Config only |
| **Impact** | Idle executor waste; reduced cluster throughput for concurrent jobs |

**Description**

`spark.dynamicAllocation.enabled` is not set to `true`. Without it, Spark holds all requested
executors for the entire application lifetime, including idle phases between stages and during
driver-side computation.

**What it detects**

A `SparkSession` builder without `.config("spark.dynamicAllocation.enabled", "true")`.

```python
# Triggers SPL-D01-004
spark = SparkSession.builder.config("spark.executor.instances", "50").getOrCreate()
```

**Why it matters**

Spark jobs have naturally bursty resource requirements: a single-stage job may need 50 executors
for a shuffle but only 10 for the preceding filter. With static allocation, 40 executors sit
completely idle during the filter stage, consuming cluster resources that other jobs could use.
On shared YARN/Kubernetes clusters this is a cluster-wide efficiency loss. Dynamic allocation
returns idle executors to the cluster manager after a configurable idle timeout
(`spark.dynamicAllocation.executorIdleTimeout`, default 60 s).

> **Note:** Dynamic allocation requires either an external shuffle service or
> `spark.dynamicAllocation.shuffleTracking.enabled = true` (Spark 3+). Do not use with
> Structured Streaming without the shuffle service.

**How to fix**

```python
spark = (
    SparkSession.builder
    .appName("etl")
    .config("spark.dynamicAllocation.enabled", "true")
    # Spark 3+: no external shuffle service needed
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
    .config("spark.dynamicAllocation.minExecutors", "2")
    .config("spark.dynamicAllocation.maxExecutors", "50")
    .getOrCreate()
)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.dynamicAllocation.enabled` | `true` | The required change |
| `spark.dynamicAllocation.shuffleTracking.enabled` | `true` | Spark 3+; removes need for external shuffle service |
| `spark.dynamicAllocation.minExecutors` | `1`–`5` | Floor to avoid cold-start on first stage |
| `spark.dynamicAllocation.maxExecutors` | cluster max | Hard upper bound |
| `spark.dynamicAllocation.executorIdleTimeout` | `60s` | How long idle executors are kept before release |

**Related rules**

- **SPL-D01-002** — Executor memory; set a good per-executor size before enabling dynamic scaling
- **SPL-D01-005** — Executor cores; fewer cores per executor allows finer-grained scaling

---

### SPL-D01-005 — Executor Cores Set Too High

| | |
|---|---|
| **Dimension** | D01 Cluster Configuration |
| **Severity** | WARNING |
| **Effort** | Config only |
| **Impact** | HDFS throughput degradation; increased GC pause frequency |

**Description**

`spark.executor.cores` exceeds the recommended maximum of 5 (configurable via the lint threshold
`max_executor_cores`). More than 5 cores per executor causes HDFS throughput contention and
increased GC pressure.

**What it detects**

A `.config("spark.executor.cores", N)` call where `N` is greater than the configured threshold
(default 5).

```python
# Triggers SPL-D01-005 (cores = 10 > max_executor_cores = 5)
spark = SparkSession.builder.config("spark.executor.cores", "10").getOrCreate()
```

**Why it matters**

All cores in one executor share a single JVM heap and a single HDFS client. The HDFS client
supports roughly 5 concurrent read streams efficiently per node — beyond that, streams compete
for the same network bandwidth and produce long tail latencies. Within the JVM, many concurrent
tasks amplify GC pressure: more live objects, more frequent minor GC cycles, and longer stop-the-world
pauses. The Cloudera and Databricks tuning guides both recommend 4–5 cores per executor as
the practical sweet spot. If you need more task parallelism, add more executors instead.

**How to fix**

```python
# Wrong: 10 cores per executor — HDFS contention, GC pressure
spark = SparkSession.builder.config("spark.executor.cores", "10").getOrCreate()

# Right: 4 cores, more executors for the same total parallelism
spark = (
    SparkSession.builder
    .config("spark.executor.cores", "4")
    .config("spark.executor.instances", "25")  # 25 × 4 = 100 total cores
    .getOrCreate()
)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.executor.cores` | `4`–`5` | HDFS-safe sweet spot |

| Lint Threshold | Default | Description |
|---|---|---|
| `thresholds.max_executor_cores` | `5` | Override in `.spark-perf-lint.yaml` to raise/lower the threshold |

**Related rules**

- **SPL-D01-002** — Executor memory; memory-per-core = executor.memory / executor.cores
- **SPL-D01-004** — Dynamic allocation; fewer cores enables finer executor granularity

---

### SPL-D01-006 — Memory Overhead Too Low

| | |
|---|---|
| **Dimension** | D01 Cluster Configuration |
| **Severity** | WARNING |
| **Effort** | Config only |
| **Impact** | Executor container killed by YARN/K8s without an OOM log entry |

**Description**

`spark.executor.memoryOverhead` is explicitly configured below the 384 MB minimum. Values below
this floor cause container-level kill events on YARN and Kubernetes that do not produce a Java
`OutOfMemoryError`, making them extremely difficult to diagnose.

**What it detects**

A `.config("spark.executor.memoryOverhead", X)` call where `X` parses to fewer than 384 MB.

```python
# Triggers SPL-D01-006 (128 MB < 384 MB minimum)
spark = SparkSession.builder.config("spark.executor.memoryOverhead", "128m").getOrCreate()
```

**Why it matters**

`memoryOverhead` covers off-heap memory used by the JVM process itself: thread stacks, JVM
metadata, NIO direct buffers, and — critically for PySpark — the native memory of Python worker
subprocesses. YARN and Kubernetes enforce container memory limits at the OS level (RSS, not JVM
heap). When the combined JVM heap + off-heap consumption exceeds `executor.memory +
memoryOverhead`, the container manager sends `SIGKILL`. The executor vanishes without a Java
stack trace, appearing in logs only as "lost executor" or "container killed by YARN".
Spark's internal default — `max(384 MB, 0.10 × executor.memory)` — exists precisely to prevent this.

**How to fix**

```python
spark = (
    SparkSession.builder
    .config("spark.executor.memory", "4g")
    # At minimum 384m; for PySpark UDFs increase to 1g–2g
    .config("spark.executor.memoryOverhead", "512m")
    .getOrCreate()
)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.executor.memoryOverhead` | `max(512m, 0.10 × executor.memory)` | Never set below 384m |
| `spark.executor.pyspark.memory` | `1g`–`2g` | Separate Python process memory budget (Spark 3+) |

**Related rules**

- **SPL-D01-002** — Executor memory; overhead is additive to the heap allocation
- **SPL-D01-007** — PySpark worker memory; Python processes consume this overhead budget

---

### SPL-D01-007 — Missing PySpark Worker Memory Config

| | |
|---|---|
| **Dimension** | D01 Cluster Configuration |
| **Severity** | WARNING |
| **Effort** | Config only |
| **Impact** | Python worker OOM when UDFs process large inputs; silent task failures |

**Description**

Python UDFs are present in the file but `spark.python.worker.memory` is not configured. Each
Python UDF invocation spawns a worker subprocess whose memory is bounded by this setting
(default 512 MB). UDFs that process large pandas DataFrames or load ML models easily exceed
this limit.

**What it detects**

Files that contain `@udf` or `udf()` definitions but no `.config("spark.python.worker.memory", ...)`.

```python
# Triggers SPL-D01-007
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def transform(x):
    import re
    return re.sub(r'\s+', '_', x.lower())

# spark not configured with python.worker.memory
```

**Why it matters**

Each executor task slot that runs a Python UDF forks a separate Python worker process. The worker
loads the Python interpreter, imports all modules referenced by the UDF, and processes rows in
batches. The 512 MB default was set conservatively for simple string transformations. UDFs that
call pandas, NumPy, or scikit-learn, or that reference large dictionaries loaded at module level,
consume several GB per worker. When a worker exceeds its budget it is killed with
`WorkerLostFailureReason` — a confusing error that does not mention memory explicitly.

**How to fix**

```python
spark = (
    SparkSession.builder
    .config("spark.python.worker.memory", "1g")  # increase for ML UDFs
    .config("spark.executor.memoryOverhead", "2g")  # Python memory counts against overhead
    .getOrCreate()
)

@udf(returnType=StringType())
def transform(x):
    import re
    return re.sub(r'\s+', '_', x.lower())
```

> **Better alternative:** Replace row-at-a-time `@udf` with `@pandas_udf` (see SPL-D09-001).
> `@pandas_udf` processes batches, requires one worker process instead of one per task slot,
> and is significantly more memory-efficient.

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.python.worker.memory` | `1g`–`4g` | Scale with UDF memory footprint |
| `spark.executor.pyspark.memory` | `1g`–`2g` | Spark 3.1+; more precise bound than worker.memory |

**Related rules**

- **SPL-D01-006** — Memory overhead; Python worker memory is drawn from the overhead budget
- **SPL-D09-001** — Row-at-a-time UDF; consider replacing with `@pandas_udf`
- **SPL-D09-002** — UDF replaceable with native function; no Python overhead at all

---

### SPL-D01-008 — Network Timeout Too Low

| | |
|---|---|
| **Dimension** | D01 Cluster Configuration |
| **Severity** | WARNING |
| **Effort** | Config only |
| **Impact** | False-positive executor evictions during GC or shuffle; wasted recompute |

**Description**

`spark.network.timeout` is explicitly set below 120 seconds. This is the global timeout for all
network interactions between Spark components. Values below 120 s trigger false executor
evictions during normal GC pauses and heavy shuffle phases.

**What it detects**

A `.config("spark.network.timeout", X)` call where `X` parses to fewer than 120 seconds.

```python
# Triggers SPL-D01-008 (30 s < 120 s minimum)
spark = SparkSession.builder.config("spark.network.timeout", "30s").getOrCreate()
```

**Why it matters**

During a large shuffle write or a G1GC stop-the-world pause, an executor goes completely silent
— it stops responding to heartbeats, block manager requests, and task status updates. If the
network timeout fires before GC completes, the driver marks the executor as dead, cancels its
tasks, and reschedules them on other executors. This wasted recompute can cascade: the
rescheduled tasks also encounter GC pressure, causing a chain of evictions. The Spark default
of 120 s is deliberately conservative because GC pauses on large heaps (> 16 GB) can legitimately
take 30–90 seconds.

**How to fix**

```python
# Wrong: 30 s fires during normal GC pauses
spark = SparkSession.builder.config("spark.network.timeout", "30s").getOrCreate()

# Right: use the default (remove the override entirely) or set ≥ 120 s
spark = (
    SparkSession.builder
    .config("spark.network.timeout", "120s")
    # If you need fast failure detection, tune heartbeat separately:
    .config("spark.executor.heartbeatInterval", "10s")
    .getOrCreate()
)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.network.timeout` | `120s` (default) | Remove override or set ≥ 120 s |
| `spark.executor.heartbeatInterval` | `10s` (default) | Tune this for failure detection speed instead |
| `spark.rpc.askTimeout` | `120s` | Individual RPC timeout; also governed by network.timeout |

**Related rules**

- **SPL-D01-002** — Executor memory; large heaps cause the GC pauses that trigger this problem
- **SPL-D01-005** — Executor cores; more concurrent tasks per executor → more GC pressure

---

### SPL-D01-009 — Speculation Not Enabled

| | |
|---|---|
| **Dimension** | D01 Cluster Configuration |
| **Severity** | INFO |
| **Effort** | Config only |
| **Impact** | Slow nodes can stall entire stages; 2–5× job slowdown on degraded hardware |

**Description**

`spark.speculation` is not enabled. Speculative execution detects straggler tasks — tasks running
significantly slower than the stage median — and launches duplicate copies on other executors,
using whichever finishes first.

**What it detects**

A `SparkSession` builder without `.config("spark.speculation", "true")`.

```python
# Triggers SPL-D01-009
spark = SparkSession.builder.appName("etl").getOrCreate()
```

**Why it matters**

On cloud hardware, instance performance varies by 20–30% due to noisy neighbours, hypervisor
scheduling jitter, and hardware degradation. A single slow task in a 500-task stage holds up
the entire stage until it completes. Speculation launches a duplicate task on a different
executor and lets the first finisher win. Without speculation, a degraded instance in the cluster
can repeatedly cause the longest-running tasks to be on that node, adding minutes to every stage.

> **Caution:** Speculation can cause duplicate side effects for non-idempotent operations
> (e.g., writing to an external database). Enable only for idempotent workloads.
> Writing to Parquet/ORC with overwrite semantics is idempotent; appending to a
> non-transactional sink is not.

**How to fix**

```python
spark = (
    SparkSession.builder
    .appName("etl")
    .config("spark.speculation", "true")
    # Task is a straggler when > 1.5× median AND > quantile 0.9 of tasks done
    .config("spark.speculation.multiplier", "1.5")
    .config("spark.speculation.quantile", "0.9")
    .getOrCreate()
)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.speculation` | `true` | Enable for idempotent workloads |
| `spark.speculation.multiplier` | `1.5` | A task is a straggler if it is > this × median task time |
| `spark.speculation.quantile` | `0.9` | Start speculation after this fraction of tasks complete |
| `spark.speculation.interval` | `100ms` | How often to check for stragglers |

**Related rules**

- **SPL-D01-008** — Network timeout; speculation and GC-pause timeouts interact on degraded nodes
- **SPL-D05-001** — Low-cardinality join skew; skewed tasks look like stragglers to speculation

---

### SPL-D01-010 — Default Shuffle Partitions Unchanged

| | |
|---|---|
| **Dimension** | D01 Cluster Configuration |
| **Severity** | WARNING |
| **Effort** | Config only |
| **Impact** | Spill to disk on large datasets; small-file explosion on small datasets |

**Description**

`spark.sql.shuffle.partitions` is not set, or is explicitly set to the default value of 200.
This value was chosen for a benchmark cluster circa 2014 and is almost never correct for
production workloads.

**What it detects**

A `SparkSession` builder with `getOrCreate()` but no `spark.sql.shuffle.partitions` config, or
one that explicitly sets it to `"200"`.

```python
# Triggers SPL-D01-010 — missing entirely
spark = SparkSession.builder.appName("job").getOrCreate()

# Also triggers — explicitly set to the default
spark = SparkSession.builder.config("spark.sql.shuffle.partitions", "200").getOrCreate()
```

**Why it matters**

Every wide transformation (join, groupBy, orderBy, window function) produces exactly
`spark.sql.shuffle.partitions` output partitions. At 200 partitions: a 1 TB dataset produces
~5 GB per partition — well above the executor memory fraction available for sorting, causing
massive spill to disk. A 1 GB dataset produces ~5 MB per partition — tiny tasks with more
scheduler overhead than computation, and hundreds of small output files that degrade downstream
reader performance. AQE (Spark 3+) can coalesce small partitions automatically, partially
compensating for over-partitioning.

**How to fix**

```python
spark = (
    SparkSession.builder
    .appName("job")
    # Formula: max(total_executor_cores × 2, dataset_size_gb × 8)
    # For a 100-core cluster processing 50 GB: max(200, 400) = 400
    .config("spark.sql.shuffle.partitions", "400")
    # Let AQE coalesce down if partitions turn out too small
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.shuffle.partitions` | `2–3 × total executor cores` | Starting point; tune with AQE |
| `spark.sql.adaptive.enabled` | `true` | AQE coalesces small post-shuffle partitions automatically |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64m`–`128m` | AQE target partition size |

**Related rules**

- **SPL-D02-002** — Same rule from the D02 shuffle perspective (code-side detection)
- **SPL-D08-001** — AQE disabled; AQE mitigates the impact of a wrong partition count
- **SPL-D08-007** — Shuffle partitions set high with AQE; redundant when AQE is active

---

## D02 · Shuffle

Shuffles are the most expensive operations in Spark: every wide transformation serializes all
rows, writes shuffle files to disk, transfers data across the network, and sorts or
hash-partitions the result on the receiving side. These rules detect unnecessary shuffles,
anti-patterns that worsen existing shuffles, and configurations that prevent Spark from
eliminating them.

---

### SPL-D02-001 — groupByKey() Instead of reduceByKey()

| | |
|---|---|
| **Dimension** | D02 Shuffle |
| **Severity** | CRITICAL |
| **Effort** | Minor code change |
| **Impact** | 10–100× shuffle data reduction for aggregation workloads |

**Description**

`groupByKey()` shuffles all values for every key to a single reducer before any aggregation
occurs. `reduceByKey()` and `aggregateByKey()` apply a partial aggregation map-side first,
then shuffle only the reduced values — often 100× less data.

**What it detects**

Any call to `.groupByKey()` on an RDD.

```python
# Triggers SPL-D02-001
word_counts = rdd.groupByKey().mapValues(sum)
```

**Why it matters**

`groupByKey()` is a full shuffle of all values: every row is serialized, sent across the network
to a designated reducer, and held in memory until all values for the key arrive. For a key with
100,000 values, all 100,000 values cross the network even if the final answer is a single integer.
`reduceByKey()` applies the combine function within each partition first — called a "map-side
combine" — so only the per-partition partial result crosses the network. For commutative,
associative operations (sum, count, max, min, concat) this reduces shuffle data by a factor
equal to the average number of rows per key.

**How to fix**

```python
# Wrong: all values shuffled to reducers
word_counts = rdd.groupByKey().mapValues(sum)

# Right: partial sum on each partition; only per-partition sums shuffled
word_counts = rdd.reduceByKey(lambda a, b: a + b)

# For more complex aggregations with a non-commutative combine step:
word_counts = rdd.aggregateByKey(
    zeroValue=0,
    seqFunc=lambda acc, v: acc + v,   # map-side: add to partition accumulator
    combFunc=lambda a, b: a + b,      # reduce-side: merge two partition accumulators
)

# In the DataFrame API, groupBy().agg() already handles map-side combines internally:
df.groupBy("word").agg(count("*"))  # no groupByKey needed
```

**Config options**

No Spark configuration affects this rule. The fix is always a code change.

**Related rules**

- **SPL-D02-007** — Multiple shuffles in sequence; `groupByKey` chains amplify shuffle cost
- **SPL-D05-002** — GroupBy on low-cardinality column; even `reduceByKey` skews on few keys

---

### SPL-D02-002 — Default Shuffle Partitions Unchanged

| | |
|---|---|
| **Dimension** | D02 Shuffle |
| **Severity** | WARNING |
| **Effort** | Config only |
| **Impact** | Spill to disk on large datasets; task-scheduler overhead on small datasets |

**Description**

`spark.sql.shuffle.partitions` is not configured in this file, while a `SparkSession` is created
here. This is the D02 (code-level) counterpart to SPL-D01-010.

**What it detects**

A file that creates a `SparkSession` (has `getOrCreate()`) but does not configure
`spark.sql.shuffle.partitions`. Fires at the builder line.

```python
# Triggers SPL-D02-002
spark = SparkSession.builder.getOrCreate()  # 200 partitions by default

df.groupBy("category").agg(sum("revenue"))  # uses 200 partitions
```

**Why it matters**

See SPL-D01-010 for the full internals explanation. From the D02 perspective: the partition count
directly controls how many shuffle map tasks are created and how large each shuffle file is.
With the wrong count, every wide transformation in the file pays a correctness or performance
penalty — either hundreds of tiny tasks (scheduler overhead dominates) or a handful of giant
partitions (spill to disk dominates).

**How to fix**

```python
spark = (
    SparkSession.builder
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

df.groupBy("category").agg(sum("revenue"))  # now uses 400 partitions → tuned
```

**Config options**

Same as SPL-D01-010. See that entry for the full table.

**Related rules**

- **SPL-D01-010** — Same issue detected at the config level rather than the code level
- **SPL-D08-001** — AQE disabled; AQE is the recommended companion to a high partition count
- **SPL-D08-007** — High shuffle partitions with AQE enabled; tune `advisoryPartitionSizeInBytes` instead

---

### SPL-D02-003 — Unnecessary Repartition Before Join

| | |
|---|---|
| **Dimension** | D02 Shuffle |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | One extra full shuffle per join; 2× shuffle I/O for that stage |

**Description**

`repartition()` immediately before `join()` causes a redundant shuffle. The join itself already
performs a full shuffle to co-locate matching keys, so the preceding repartition accomplishes
nothing except doubling the shuffle I/O.

**What it detects**

A `repartition()` call that is either chained directly before `join()` or appears within 5 source
lines above a `join()` call.

```python
# Triggers SPL-D02-003 — chained
result = df.repartition(200, "key").join(other, "key")

# Also triggers — multi-statement within 5 lines
df2 = df.repartition(200)
result = df2.join(other, "key")
```

**Why it matters**

Spark's sort-merge join (the default for large-table joins) performs a full hash-partitioned
shuffle of both DataFrames before sorting and merging them. Calling `repartition()` on the left
side immediately before the join performs a first shuffle by the repartition key, then a second
shuffle by the join key inside the join. If the repartition key equals the join key, both shuffles
produce identically partitioned data — the first is pure waste. If the keys differ, the second
shuffle immediately invalidates the first. Spark 3+ AQE exchange reuse can sometimes avoid
duplicate shuffles, but explicit `repartition()` before `join()` prevents this optimization from
activating.

**How to fix**

```python
# Wrong: repartition + join = 2 shuffles
result = df.repartition(200, "key").join(other, "key")

# Right: join shuffles by key automatically
result = df.join(other, "key")

# If partition count control is needed, use a hint instead:
result = df.hint("repartition", 400).join(other, "key")

# Or set globally:
spark.conf.set("spark.sql.shuffle.partitions", "400")
result = df.join(other, "key")
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.exchange.reuse` | `true` (default); allows AQE to detect and reuse identical shuffle plans |

**Related rules**

- **SPL-D04-010** — Repartition by column different from join key; the worst-case variant
- **SPL-D02-007** — Multiple shuffles in sequence; repartition + join is a common chain
- **SPL-D03-006** — Multiple joins without repartition; sometimes repartition IS the fix

---

### SPL-D02-004 — orderBy/sort Without Limit

| | |
|---|---|
| **Dimension** | D02 Shuffle |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Full global sort: O(n log n) shuffle + sort vs O(n log k) for top-N |

**Description**

`orderBy()` or `sort()` on an unlimited DataFrame forces a full total-order sort: a range-partition
shuffle followed by a sort across all executors. Adding `.limit(n)` triggers the
`TakeOrderedAndProject` Catalyst optimization that never materializes the full sorted output.

**What it detects**

An `orderBy()` or `sort()` call where no `limit()` appears in the same method chain or within
3 lines after it.

```python
# Triggers SPL-D02-004
top_customers = df.orderBy("revenue", ascending=False)

# Also triggers — no limit within 3 lines
sorted_df = df.orderBy("ts")
output = sorted_df.write.parquet("out/")
```

**Why it matters**

A globally sorted output requires Spark to (1) sample the data to determine range boundaries,
(2) shuffle all rows to buckets determined by those boundaries, and (3) sort within each bucket.
This is an O(n log n) distributed sort with O(n) shuffle traffic. In contrast,
`orderBy().limit(k)` is optimized to `TakeOrderedAndProject`: each task maintains a min-heap of
the top-k results locally, then a single reducer merges the per-task heaps. Total shuffle data
is O(k × parallelism) instead of O(n). For returning the top 100 rows from a 1 billion-row
dataset, this difference is roughly 10,000,000×.

**How to fix**

```python
# Wrong: full global sort of all rows
top_customers = df.orderBy("revenue", ascending=False)

# Right: TakeOrderedAndProject — O(n log k) instead of O(n log n)
top_customers = df.orderBy("revenue", ascending=False).limit(100)

# For partition-local sorting without a global shuffle (e.g. ordered output files):
sorted_df = df.sortWithinPartitions("ts")
```

**Config options**

No Spark configuration changes the behavior of this pattern. The fix is always a code change.

**Related rules**

- **SPL-D02-005** — distinct() without prior filter; another unbounded operation that triggers a full shuffle
- **SPL-D04-009** — Partition column not used in query filters; full scans compound the sort cost

---

### SPL-D02-005 — distinct() Without Prior Filter

| | |
|---|---|
| **Dimension** | D02 Shuffle |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Full shuffle of all rows and all columns; high memory pressure |

**Description**

`distinct()` triggers a full shuffle equivalent to `groupBy(*all_columns).count()`. Calling it
without a prior `filter()`, `where()`, or `select()` shuffles every row and every column of the
DataFrame.

**What it detects**

A `distinct()` call where no `filter()`, `where()`, or `select()` appears earlier in the same
method chain.

```python
# Triggers SPL-D02-005
unique_events = df.distinct()
```

**Why it matters**

`distinct()` de-duplicates by all columns simultaneously. On a wide table with 50 columns and
100 million rows, Spark must serialize all 50 columns of all 100 million rows into shuffle files,
transfer them across the network, and sort-dedup them on the receiving side. If only 3 columns
are needed for the downstream computation, `select()` before `distinct()` reduces the shuffle
payload by a factor of 50/3 ≈ 17×. A `filter()` applied first reduces the row count. Either
transformation applied before `distinct()` is always a win.

**How to fix**

```python
# Wrong: shuffles all 50 columns, all 100 M rows
unique_events = df.distinct()

# Right: project to needed columns first, then dedup
unique_events = df.select("user_id", "event_type").distinct()

# With a filter to reduce rows before the shuffle:
unique_recent = (
    df
    .filter('date >= "2024-01-01"')
    .select("user_id", "event_type")
    .distinct()
)

# For dedup on a subset of columns (keeps one row per group):
unique_users = df.dropDuplicates(["user_id"])
```

**Config options**

No Spark configuration changes the behavior of this pattern.

**Related rules**

- **SPL-D02-004** — orderBy without limit; another full-shuffle operation that benefits from prior filtering
- **SPL-D07-003** — select("*"); the two anti-patterns often appear together

---

### SPL-D02-006 — Shuffle Followed by Coalesce

| | |
|---|---|
| **Dimension** | D02 Shuffle |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Unbalanced tasks; long-tail executor stragglers after the shuffle |

**Description**

`coalesce(n)` after a shuffle operation (groupBy, join, orderBy, distinct, repartition) merges
partitions without redistribution, creating highly unbalanced tasks that eliminate the
data-parallelism the shuffle was designed to establish.

**What it detects**

A `coalesce()` call where the method chain or the 5 preceding lines contain a shuffle operation
(`groupBy`, `join`, `orderBy`, `sort`, `distinct`, `repartition`).

```python
# Triggers SPL-D02-006
result = df.groupBy("col").agg(count("*")).coalesce(10)
```

**Why it matters**

`coalesce(n)` avoids a shuffle by merging existing partitions in-place: it assigns multiple
consecutive partition IDs to the same task. After a shuffle that produced 200 balanced partitions,
`coalesce(10)` assigns 20 partitions to each of 10 tasks. Those 20 partitions are not
redistributed — each task processes its 20 partitions sequentially. If the original shuffle
produced balanced 10 MB partitions, each coalesced task processes 200 MB while other executors
are idle. The correct tool after a shuffle is `repartition(n)`, which performs another shuffle
to redistribute data evenly.

**How to fix**

```python
# Wrong: unbalanced tasks after groupBy shuffle
result = df.groupBy("col").agg(count("*")).coalesce(10)

# Right: repartition evenly redistributes after the shuffle
result = df.groupBy("col").agg(count("*")).repartition(10)

# Alternative: set shuffle.partitions to produce the target count directly
spark.conf.set("spark.sql.shuffle.partitions", "10")
result = df.groupBy("col").agg(count("*"))  # already 10 partitions post-shuffle

# coalesce(1) specifically for single output files is acceptable — see SPL-D04-002
result.coalesce(1).write.mode("overwrite").parquet("output/")
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.shuffle.partitions` | Set to the target output count to avoid coalesce entirely |

**Related rules**

- **SPL-D04-002** — coalesce(1) bottleneck; the extreme case of this pattern
- **SPL-D04-005** — Coalesce before write; related write-specific variant
- **SPL-D02-007** — Multiple shuffles in sequence; coalesce inside a shuffle chain amplifies the problem

---

### SPL-D02-007 — Multiple Shuffles in Sequence

| | |
|---|---|
| **Dimension** | D02 Shuffle |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Compounded shuffle I/O; full lineage re-execution on any task failure |

**Description**

Two or more shuffle-wide operations (join, groupBy, orderBy, distinct, repartition) appear in
sequence without intermediate caching or checkpointing. Each shuffle re-reads the full output
of the preceding shuffle.

**What it detects**

Two or more shuffle operations where no `cache()`, `persist()`, `checkpoint()`, or
`localCheckpoint()` call appears between them.

```python
# Triggers SPL-D02-007 — three shuffles with no materialization
df2 = df.groupBy("a").agg(sum("b"))   # shuffle 1
df3 = df2.join(lookup, "a")           # shuffle 2
df4 = df3.orderBy("b")                # shuffle 3 — triggers the rule
```

**Why it matters**

Each shuffle writes its input data to disk and transfers it across the network. When three
shuffles appear in sequence, the data passes through disk and network three times. Without
intermediate materialization, Spark's lineage graph for `df4` references the full chain from
the original source. If any executor fails during the third shuffle, Spark must re-execute
the full lineage: re-reading the source data, re-running the first groupBy, re-running the join,
and then retrying the sort. Caching after the second shuffle truncates the lineage, so a failure
in the third shuffle only reruns that final stage.

**How to fix**

```python
# Right: cache after an expensive intermediate result
df2 = df.groupBy("a").agg(sum("b")).cache()  # truncates lineage here
df3 = df2.join(lookup, "a")                 # failure here reruns only from cache
df4 = df3.orderBy("b")
df2.unpersist()  # release when no longer needed — see SPL-D06-001

# For iterative algorithms, prefer checkpoint() over cache():
df2 = df.groupBy("a").agg(sum("b")).checkpoint()  # writes to HDFS; survives executor loss
```

**Config options**

No Spark configuration changes the detection. Tuning `spark.rdd.compress` and
`spark.memory.storageFraction` affects the efficiency of the recommended `cache()`.

**Related rules**

- **SPL-D06-001** — cache() without unpersist(); the solution introduces a new risk
- **SPL-D06-006** — Reused DataFrame without cache; the symmetric problem (uses without cache)
- **SPL-D02-003** — Unnecessary repartition before join; repartition is itself a shuffle

---

### SPL-D02-008 — Shuffle File Buffer Too Small

| | |
|---|---|
| **Dimension** | D02 Shuffle |
| **Severity** | INFO |
| **Effort** | Config only |
| **Impact** | Up to 32× reduction in shuffle-write syscall count |

**Description**

`spark.shuffle.file.buffer` is explicitly configured below the recommended 1 MB. Spark's default
of 32 KB causes a large number of small `write()` syscalls during shuffle output, increasing
I/O overhead for shuffle-heavy workloads.

**What it detects**

A `.config("spark.shuffle.file.buffer", X)` call where `X` parses to fewer than 1024 KB.

```python
# Triggers SPL-D02-008 (32 KB < 1024 KB recommended)
spark = SparkSession.builder.config("spark.shuffle.file.buffer", "32k").getOrCreate()
```

**Why it matters**

Every shuffle map task writes one file per reducer partition. With the default 32 KB buffer,
each `write()` syscall flushes 32 KB to the OS page cache. Increasing to 1 MB reduces the
number of syscalls by 32× (1024/32). For a job with 200 shuffle partitions and 1000 map tasks,
the 32× reduction in syscalls translates to measurably lower shuffle write time visible in
the Spark UI stage details. Memory cost is negligible: one 1 MB buffer per concurrent shuffle
map task.

**How to fix**

```python
spark = (
    SparkSession.builder
    .config("spark.shuffle.file.buffer", "1m")
    # Optional: off-heap buffers reduce GC pressure further
    .config("spark.shuffle.io.preferDirectBufs", "true")
    .getOrCreate()
)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.shuffle.file.buffer` | `1m` | Buffer per shuffle write task |
| `spark.shuffle.io.preferDirectBufs` | `true` | Uses off-heap NIO direct buffers; reduces GC |
| `spark.reducer.maxSizeInFlight` | `48m` (default) | Per-reducer fetch buffer; increase for large shuffles |

**Related rules**

- **SPL-D02-007** — Multiple shuffles in sequence; buffer improvement multiplies across stages
- **SPL-D01-001** — Kryo serializer; reducing serialized data size compounds with a larger buffer

---

## D03 · Joins

Join operations are the single most common source of performance problems in analytical Spark
workloads. Every non-broadcast join triggers a full shuffle of both input DataFrames. Missing
broadcast hints, cross products, and joins inside loops can each make the difference between a
2-minute job and a 2-hour one.

---

### SPL-D03-001 — Cross Join / Cartesian Product

| | |
|---|---|
| **Dimension** | D03 Joins |
| **Severity** | CRITICAL |
| **Effort** | Minor code change |
| **Impact** | Unbounded row explosion; can OOM or run indefinitely |

**Description**

`crossJoin()` or `join(how='cross')` creates an O(n²) cartesian product: every row in the left
DataFrame is paired with every row in the right DataFrame. Two 10 million-row DataFrames produce
100 trillion rows. This pattern almost always indicates a missing join key.

**What it detects**

Any call to `.crossJoin()`, or a `.join()` call with `how="cross"` as a keyword or third
positional argument.

```python
# Triggers SPL-D03-001
result = df.crossJoin(other)                     # explicit crossJoin
result = df.join(other, how="cross")             # join with cross type
result = df.join(other, df.date == other.date, "cross")  # positional how
```

**Why it matters**

Spark does not warn when a join key is accidentally omitted — it silently promotes the query to
a cartesian product. The `spark.sql.crossJoin.enabled` setting (default `true` in Spark 3+)
allows cross joins without an explicit error. A cross join on two tables of 1 million rows each
produces 1 trillion rows: at 100 bytes per row that is 100 TB of data. Even with broadcast,
this fills executor memory and causes OOM. Even if the result is filtered to a handful of rows,
Spark must materialize the full product before applying any downstream filter.

**How to fix**

```python
# Wrong: O(n²) cartesian product
result = df.crossJoin(other)

# Right: explicit join key
result = df.join(other, on="id", how="inner")

# If a cross product is genuinely needed (tiny reference table):
SMALL_THRESHOLD = 1000  # rows
assert small_ref.count() < SMALL_THRESHOLD, "Reference table too large for cross join"
result = df.crossJoin(broadcast(small_ref))
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.crossJoin.enabled` | `true` (Spark 3+ default); set to `false` to make Spark error on accidental cross joins |

**Related rules**

- **SPL-D03-002** — Missing broadcast hint; if a small cross join is intentional, broadcast the small side
- **SPL-D03-008** — Join inside loop; loops that build cross products compound the O(n²) problem

---

### SPL-D03-002 — Missing Broadcast Hint on Small DataFrame

| | |
|---|---|
| **Dimension** | D03 Joins |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Eliminates shuffle on both sides; 10–100× faster for small lookups |

**Description**

`join()` is called without a `broadcast()` hint or `hint("broadcast")` on either side.
When one DataFrame is significantly smaller than the other, broadcasting it eliminates the
shuffle entirely.

**What it detects**

A `.join()` call where neither side has a `broadcast()` function call or a `hint("broadcast")`
call within 2 lines, and `spark.sql.autoBroadcastJoinThreshold` is not set to a positive value.

```python
# Triggers SPL-D03-002
result = df.join(lookup, "product_id")   # lookup is small but not broadcast
```

**Why it matters**

A sort-merge join (the default for large-table joins) shuffles **both** DataFrames: the left side
is shuffled by the join key hash, and the right side is shuffled by the same hash. For a
fact-dimension join where the dimension table has 10,000 rows and the fact table has 10 billion
rows, the sort-merge join shuffles 10 billion rows from the fact table and 10,000 rows from the
dimension table — then does the same for every subsequent execution. A broadcast join sends the
10,000-row dimension to every executor as an in-memory hash map and processes each fact partition
locally, with zero network traffic for the dimension data.

**How to fix**

```python
from pyspark.sql.functions import broadcast

# Wrong: both sides shuffled
result = df.join(lookup, "product_id")

# Right: small side broadcast — eliminates shuffle on lookup side entirely
result = df.join(broadcast(lookup), "product_id")

# Alternative: hint syntax (useful when broadcast() import is unavailable)
result = df.join(lookup.hint("broadcast"), "product_id")
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` | `10485760` (10 MB, default) | Tables smaller than this are auto-broadcast |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | `10485760` | AQE runtime broadcast threshold (Spark 3+) |

**Related rules**

- **SPL-D03-003** — Broadcast threshold disabled; if this is `-1`, no auto-broadcast occurs
- **SPL-D03-001** — Cross join; if cross join is intentional, always broadcast the small side
- **SPL-D03-010** — CBO not enabled; CBO can auto-select broadcast for tables below threshold

---

### SPL-D03-003 — Broadcast Threshold Disabled

| | |
|---|---|
| **Dimension** | D03 Joins |
| **Severity** | CRITICAL |
| **Effort** | Config only |
| **Impact** | Every join shuffles both sides; disables all broadcast optimisations |

**Description**

`spark.sql.autoBroadcastJoinThreshold` is set to `-1`, which completely disables Spark's
automatic broadcast join optimization. Every join — including those on tiny lookup tables with
a few hundred rows — falls back to a full sort-merge join.

**What it detects**

A `.config("spark.sql.autoBroadcastJoinThreshold", "-1")` call.

```python
# Triggers SPL-D03-003
spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
```

**Why it matters**

Auto-broadcast is Spark's most impactful automatic join optimization. Disabling it with `-1`
means that even a 100-row country-code lookup table will be sorted, shuffled, and sort-merged
against a 10-billion-row fact table on every run. This setting was sometimes used to work around
a race condition in Spark 2.x where broadcast variables could be garbage-collected mid-query.
That bug was fixed in Spark 2.4+. In Spark 3+, there is no valid reason to disable auto-broadcast
globally. If a specific join causes OOM from over-eager broadcasting, use a per-join
`hint("merge")` to opt that join out.

**How to fix**

```python
# Wrong: disables all broadcast joins
spark = SparkSession.builder.config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()

# Right: use the default (10 MB) or set a custom threshold
spark = (
    SparkSession.builder
    .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))  # 10 MB
    .getOrCreate()
)

# To opt a specific join out of broadcasting:
result = big_df.join(medium_df.hint("merge"), "key")  # force sort-merge for this one
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.autoBroadcastJoinThreshold` | `10485760` (10 MB) | Remove `-1` or set a positive value |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | `10485760` | AQE runtime override (Spark 3+) |

**Related rules**

- **SPL-D03-002** — Missing broadcast hint; explicit hints bypass the threshold entirely
- **SPL-D03-010** — CBO not enabled; CBO provides statistics that improve threshold decisions

---

### SPL-D03-004 — Join Without Prior Filter/Select

| | |
|---|---|
| **Dimension** | D03 Joins |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Up to 10–100× shuffle reduction when large fractions of data are pruned |

**Description**

`join()` is called on a DataFrame that was read directly from storage (parquet, csv, json, orc,
or a table) without a preceding `filter()`, `where()`, `select()`, or `limit()`. The full table
is shuffled including rows and columns that will be discarded.

**What it detects**

A `join()` call where the method chain includes a read terminal (`parquet`, `csv`, `json`, `orc`,
`load`, `table`, `avro`) but does not include a filter/projection method before the join.

```python
# Triggers SPL-D03-004
df = spark.read.parquet("s3://bucket/events")
result = df.join(users, "user_id")         # no filter on df before join
```

**Why it matters**

Joining full tables without filtering forces Spark to shuffle and process all rows and all columns
of both DataFrames. Two 500 GB tables joined without filtering produce 1 TB of shuffle traffic
just to move the join keys. A `filter()` that removes 90% of rows before the join reduces
shuffle data by 90%. A `select()` that removes 40 of 50 columns reduces shuffle payload by 80%.
Together they can reduce a multi-TB shuffle to a few GB. Catalyst's predicate pushdown will
push `filter()` conditions into the Parquet reader for row-group elimination, so predicates
applied before `join()` are doubly effective.

**How to fix**

```python
# Wrong: full table shuffled
df = spark.read.parquet("s3://bucket/events")
result = df.join(users, "user_id")

# Right: filter and project before the shuffle
df = (
    spark.read.parquet("s3://bucket/events")
    .filter('date >= "2024-01-01"')          # partition pruning + row-group skip
    .select("user_id", "event_type", "ts")   # column pruning
)
result = df.join(users.select("user_id", "name"), "user_id")
```

**Config options**

No Spark configuration affects this rule. The fix is always a code change.

**Related rules**

- **SPL-D03-002** — Missing broadcast hint; filtering reduces the table below the broadcast threshold
- **SPL-D07-003** — select("*"); often co-occurs — no column projection before a join
- **SPL-D04-009** — Partition column not used in filters; filter on the partition column for maximum pruning

---

### SPL-D03-005 — Join Key Type Mismatch Risk

| | |
|---|---|
| **Dimension** | D03 Joins |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Silent wrong-result bugs or empty join output due to type mismatch |

**Description**

The `join()` condition compares columns with different attribute names (e.g.,
`df1.order_id == df2.id`), which is a common indicator that the two columns may have different
types. Spark silently coerces types on mismatched joins, potentially producing zero rows or
incorrect results.

**What it detects**

A `join()` where the condition argument is an AST `Compare` node with `==`, and the two
sides are `Attribute` accesses with different attribute names.

```python
# Triggers SPL-D03-005
result = df1.join(df2, df1.order_id == df2.id)   # order_id vs id — different names
```

**Why it matters**

When joining on a condition like `df1.order_id == df2.id`, Spark must cast one side to a common
type. If `order_id` is `LongType` and `id` is `StringType`, Spark attempts to cast the string to
long. String `"abc"` casts to `null`, so all string IDs that are non-numeric produce null on one
side and therefore produce no match. This is a silent correctness bug: the join succeeds (no
exception), produces a result, but that result has zero rows or fewer rows than expected. The
risk is highest when one side comes from a CSV/JSON file with inferred schema, which types all
columns as `StringType` by default.

**How to fix**

```python
# Wrong: implicit type coercion — may silently drop rows
result = df1.join(df2, df1.order_id == df2.id)

# Right: explicit cast to align types
result = df1.join(
    df2,
    df1.order_id.cast("long") == df2.id.cast("long")
)

# Better: rename one column to use the simpler string-key join syntax
df2_renamed = df2.withColumnRenamed("id", "order_id")
result = df1.join(df2_renamed, "order_id")  # same name = safe string join
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.ansi.enabled` | `true` in Spark 3.2+; makes type mismatches raise errors instead of silently coercing |

**Related rules**

- **SPL-D03-004** — Join without filter; type mismatches produce more confusing results on large unfiltered joins
- **SPL-D07-002** — Schema inference; inferred schemas are the root cause of most type mismatch bugs

---

### SPL-D03-006 — Multiple Joins Without Intermediate Repartition

| | |
|---|---|
| **Dimension** | D03 Joins |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | 3+ full shuffles; each shuffle writes/reads the entire intermediate dataset |

**Description**

Three or more `join()` calls appear in sequence without any `broadcast()`, `repartition()`,
`cache()`, or `checkpoint()` between them. Star-schema join patterns without broadcast hints
on the dimension tables each trigger an independent full shuffle.

**What it detects**

Three or more `join()` calls where consecutive calls are within 15 source lines of each other
and no break (broadcast, repartition, cache, checkpoint) appears between them.

```python
# Triggers SPL-D03-006 — three consecutive sort-merge joins
result = (
    facts
    .join(dim_a, "key_a")    # shuffle 1
    .join(dim_b, "key_b")    # shuffle 2
    .join(dim_c, "key_c")    # shuffle 3 — triggers the rule
)
```

**Why it matters**

In a star-schema join (one fact table joined to N dimension tables), every non-broadcast join
triggers a full shuffle of the accumulated intermediate result. After three sort-merge joins,
the data has been serialized, transferred, and deserialized six times (write+read per shuffle).
The intermediate result after each join is also typically larger than the inputs, amplifying the
shuffle cost of subsequent joins. The standard solution is to broadcast dimension tables that
are below the threshold — a 10 MB dimension table that is broadcast eliminates its shuffle
entirely across all query executions.

**How to fix**

```python
from pyspark.sql.functions import broadcast

# Wrong: 3 sort-merge joins = 3 full shuffles
result = facts.join(dim_a, "key_a").join(dim_b, "key_b").join(dim_c, "key_c")

# Right: broadcast small dimensions — only 0 or 1 shuffle
result = (
    facts
    .join(broadcast(dim_a), "key_a")   # no shuffle
    .join(broadcast(dim_b), "key_b")   # no shuffle
    .join(dim_c, "key_c")              # shuffle only for the large dimension
)

# For large dimensions, enable CBO to auto-reorder joins:
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
spark.sql("ANALYZE TABLE dim_c COMPUTE STATISTICS FOR ALL COLUMNS")
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.cbo.enabled` | `true` | Required for CBO join reordering |
| `spark.sql.cbo.joinReorder.enabled` | `true` | Enables DP-based multi-join reordering |
| `spark.sql.statistics.histogram.enabled` | `true` | More accurate cardinality estimates for CBO |

**Related rules**

- **SPL-D03-002** — Missing broadcast hint; the primary fix for star-schema joins
- **SPL-D03-010** — CBO not enabled; CBO is the secondary fix for large-dimension joins
- **SPL-D02-007** — Multiple shuffles in sequence; multi-join is a specific case of this

---

### SPL-D03-007 — Self-Join That Could Be Window Function

| | |
|---|---|
| **Dimension** | D03 Joins |
| **Severity** | INFO |
| **Effort** | Major refactor |
| **Impact** | 2× shuffle data volume; window functions need zero cross-node transfer |

**Description**

`join()` where both sides appear to reference the same DataFrame (a self-join). Most self-join
patterns — running totals, lead/lag comparisons, rank within group, deduplication — can be
rewritten as a single `Window` function pass that requires no shuffle.

**What it detects**

A `join()` where the same variable name appears on both sides of the call within the same
scope (detected via basic name tracking in the AST).

```python
# Triggers SPL-D03-007
df_prev = df.withColumnRenamed("value", "prev_value").withColumnRenamed("ts", "prev_ts")
result = df.join(df_prev, (df.user_id == df_prev.user_id) & (df.ts > df_prev.prev_ts))
```

**Why it matters**

A self-join serializes, shuffles, and deserializes the same data twice — both the left and right
sides of the join are derived from the same source. Window functions (`rank()`, `lag()`, `sum()
over window`) compute the same result within a single stage by partitioning data locally on each
executor. `lag(col, 1).over(Window.partitionBy("user_id").orderBy("ts"))` replaces a self-join
and requires zero cross-node data transfer for the previous-row access. For deduplication,
`row_number().over(...) == 1` replaces a self-join + filter.

**How to fix**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, rank, sum as spark_sum

# Wrong: self-join to get previous row value
df_prev = df.withColumnRenamed("value", "prev_value")
result = df.join(df_prev, ["user_id"])

# Right: window function — single partition pass, no shuffle
w = Window.partitionBy("user_id").orderBy("ts")
result = df.withColumn("prev_value", lag("value", 1).over(w))

# Wrong: self-join for rank within group
ranked = df.join(df.groupBy("cat").agg(count("*").alias("cat_count")), "cat")

# Right: rank with window
w = Window.partitionBy("cat").orderBy("score")
result = df.withColumn("rank", rank().over(w))
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D05-006** — Window function partitioned by skew-prone column; window functions can themselves skew
- **SPL-D03-006** — Multiple joins without repartition; self-joins contribute to join chain cost

---

### SPL-D03-008 — Join Inside Loop

| | |
|---|---|
| **Dimension** | D03 Joins |
| **Severity** | CRITICAL |
| **Effort** | Major refactor |
| **Impact** | N shuffles where N = loop iterations; query plan explosion for large loops |

**Description**

`join()` is called inside a Python `for` or `while` loop. Each loop iteration adds a new shuffle
stage to the query plan. With 100 iterations, the job performs 100 full shuffles even if the
final result is a single row.

**What it detects**

A `join()` call that is inside a `for` or `while` loop body (detected by checking if the call
node is nested within a `For` or `While` AST node).

```python
# Triggers SPL-D03-008
for date in date_range:
    daily_data = spark.read.parquet(f"s3://bucket/events/{date}")
    result = result.join(daily_data, "user_id")  # new shuffle on every iteration
```

**Why it matters**

Calling `join()` inside a loop does not accumulate rows — it builds a new query plan on each
iteration. Spark DataFrames are lazy: the loop runs entirely on the driver, constructing a
query plan graph with N nested joins. When an action is finally called, Spark executes all N
shuffle stages sequentially. For 100 iterations each joining a 10 GB table, the total shuffle
I/O is 1 TB. The query plan also grows linearly with the loop count: at 500+ iterations,
Catalyst plan serialization itself becomes the bottleneck and can cause `StackOverflowError`
during plan analysis.

**How to fix**

```python
# Wrong: N shuffle stages, plan explosion
result = spark.createDataFrame([], schema)
for date in date_range:
    daily = spark.read.parquet(f"s3://bucket/events/{date}")
    result = result.join(daily, "user_id")

# Right: union all dates, join once
all_dates = spark.read.parquet("s3://bucket/events/").filter(
    col("date").isin(list(date_range))
)
result = base_df.join(all_dates, "user_id")  # single shuffle

# Alternative: use date range filter instead of per-date loop
result = base_df.join(
    spark.read.parquet("s3://bucket/events/")
              .filter(col("date").between(start_date, end_date)),
    "user_id"
)
```

**Config options**

No Spark configuration changes the behavior of this anti-pattern.

**Related rules**

- **SPL-D03-002** — Missing broadcast hint; if the looped DataFrame is small, broadcast it
- **SPL-D09-003** — withColumn() inside loop; the same loop anti-pattern for column additions
- **SPL-D03-006** — Multiple joins in sequence; the loop produces the extreme case

---

### SPL-D03-009 — Left Join Without Null Handling

| | |
|---|---|
| **Dimension** | D03 Joins |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Silent null propagation; wrong aggregation results and data quality bugs |

**Description**

A left join is performed without any explicit null handling (`fillna`, `dropna`, `isNotNull`
filter, or `coalesce`) on the result. Left joins always produce `null` values for right-side
columns when no matching row exists, and those nulls propagate silently through downstream
operations.

**What it detects**

A `join()` with `how="left"` (or `"left_outer"` / `"leftouter"`) where no null-handling method
appears within 10 lines after the join.

```python
# Triggers SPL-D03-009
result = df.join(lookup, "id", "left")         # nulls for unmatched rows
total = result.groupBy("category").agg(sum("amount"))  # silently ignores nulls in amount
```

**Why it matters**

Spark follows SQL null semantics: `NULL + 1 = NULL`, `sum(NULL) = skips the row`,
`count(column) ≠ count(*)` when nulls are present, and `filter(col > 0)` silently drops rows
where `col` is `NULL`. A left join that produces unmatched rows creates a class of bugs where
aggregations produce wrong totals, counts are inconsistent between `count('*')` and
`count('column')`, and downstream filters silently eliminate valid data. These bugs are difficult
to detect because the job completes without errors — the output is just quietly wrong.

**How to fix**

```python
# Wrong: nulls from unmatched rows silently corrupt downstream aggregations
result = df.join(lookup, "id", "left")
total = result.agg(sum("amount"))  # nulls in amount are silently skipped

# Right option 1: fill nulls with defaults
result = (
    df.join(lookup, "id", "left")
    .fillna({"amount": 0.0, "status": "unknown", "category": "uncategorized"})
)

# Right option 2: filter to only matched rows (if unmatched rows are not needed)
result = (
    df.join(lookup, "id", "left")
    .filter(col("lookup_id").isNotNull())
)

# Right option 3: use coalesce to provide a default for specific columns
result = (
    df.join(lookup, "id", "left")
    .withColumn("safe_amount", coalesce(col("amount"), lit(0.0)))
)
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D03-005** — Join key type mismatch; also produces unexpected nulls in join results
- **SPL-D09-005** — collect() without limit; null-propagation bugs often discovered via collect()

---

### SPL-D03-010 — CBO/Statistics Not Enabled for Complex Joins

| | |
|---|---|
| **Dimension** | D03 Joins |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Sub-optimal join ordering; large intermediate results on multi-join queries |

**Description**

`spark.sql.cbo.enabled` is not set while the file contains multiple `join()` calls. Without
Cost-Based Optimization, Catalyst executes joins in source order without regard to table sizes,
potentially choosing the most expensive possible plan for multi-table queries.

**What it detects**

A file that contains 2+ `join()` calls but does not set `spark.sql.cbo.enabled = true` in its
`SparkSession` configuration.

```python
# Triggers SPL-D03-010
spark = SparkSession.builder.getOrCreate()  # CBO off by default

result = (
    facts                              # 10 billion rows
    .join(big_dim, "region_id")        # 10 million rows
    .join(small_dim, "product_id")     # 1,000 rows
)
# Without CBO: joins in source order: facts×big_dim first = huge intermediate result
```

**Why it matters**

Catalyst's default join ordering is left-deep and follows the source order of the query. Without
column statistics (collected via `ANALYZE TABLE`), Catalyst cannot estimate intermediate result
sizes. For a three-table join where the optimal order is `small_dim × facts × big_dim` (starting
with the most selective join), the source-order plan `facts × big_dim × small_dim` may produce
an intermediate result 10,000× larger. CBO uses per-column histograms and cardinality estimates
to enumerate join orderings and select the one with the lowest estimated cost.

**How to fix**

```python
spark = (
    SparkSession.builder
    .config("spark.sql.cbo.enabled", "true")
    .config("spark.sql.cbo.joinReorder.enabled", "true")
    .config("spark.sql.statistics.histogram.enabled", "true")
    .getOrCreate()
)

# Collect statistics on joined tables (run once after table creation/update)
spark.sql("ANALYZE TABLE facts COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql("ANALYZE TABLE big_dim COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql("ANALYZE TABLE small_dim COMPUTE STATISTICS FOR ALL COLUMNS")

result = facts.join(big_dim, "region_id").join(small_dim, "product_id")
# Catalyst now reorders: small_dim × facts × big_dim (optimal)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.cbo.enabled` | `true` | Enables cost-based join strategy selection |
| `spark.sql.cbo.joinReorder.enabled` | `true` | Enables DP-based join order enumeration |
| `spark.sql.statistics.histogram.enabled` | `true` | Column histograms for better cardinality estimates |
| `spark.sql.cbo.joinReorder.dp.threshold` | `12` (default) | Max tables before CBO falls back to greedy reordering |

**Related rules**

- **SPL-D03-006** — Multiple joins without repartition; CBO is the preferred fix for large-dimension chains
- **SPL-D10-002** — CBO not enabled for complex queries; D10 counterpart with deeper Catalyst context
- **SPL-D10-003** — Join reordering disabled; extends this rule with join reorder configuration details
