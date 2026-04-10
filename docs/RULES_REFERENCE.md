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

---

## D04 · Partitioning

Partition count and partition strategy determine how evenly Spark distributes work across the
cluster. A single `repartition(1)` call can serialize an entire dataset through one task;
partitioning by a high-cardinality column can produce millions of tiny files. These rules catch
both extremes — too few partitions, too many, and the wrong column choices.

---

### SPL-D04-001 — repartition(1) Bottleneck

| | |
|---|---|
| **Dimension** | D04 Partitioning |
| **Severity** | CRITICAL |
| **Effort** | Minor code change |
| **Impact** | Full parallelism lost; single-core execution for the entire dataset |

**Description**

`repartition(1)` triggers a full shuffle and then funnels the entire dataset through a single
reducer task, reducing a multi-hundred-core cluster to one active core for that stage and all
downstream stages.

**What it detects**

Any call to `.repartition(1)`.

```python
# Triggers SPL-D04-001
df.repartition(1).write.parquet("s3://bucket/out")
```

**Why it matters**

`repartition(1)` performs a hash shuffle of the entire dataset into exactly one output partition.
Every row from every executor crosses the network to reach the single designated reducer task.
That task must then process the entire dataset serially: sort it, write it, or pass it to
downstream operations. On a 100-executor cluster processing 100 GB, 99 executors contribute
their data and then sit idle while one executor does all the remaining work. If that single task
fails, the full dataset must be reshuffled from scratch. Even when the intent is to produce a
single output file, `coalesce(1)` is always cheaper because it avoids the upfront shuffle.

**How to fix**

```python
# Wrong: full shuffle to one partition — 99% of cluster sits idle
df.repartition(1).write.parquet("s3://bucket/out")

# Right: let Spark use natural parallelism — multiple output files
df.write.parquet("s3://bucket/out")

# If a single output file is required (e.g., legacy system constraint):
# Use coalesce(1) — no shuffle; merges partitions locally
df.coalesce(1).write.parquet("s3://bucket/out")
```

**Config options**

No Spark configuration affects this rule. The fix is a code change.

**Related rules**

- **SPL-D04-002** — coalesce(1) bottleneck; the cheaper single-file alternative
- **SPL-D04-004** — Repartition with very low partition count; repartition(2–9) has the same problem at smaller scale
- **SPL-D02-006** — Shuffle followed by coalesce; coalesce after a shuffle creates similar imbalance

---

### SPL-D04-002 — coalesce(1) Bottleneck

| | |
|---|---|
| **Dimension** | D04 Partitioning |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | All subsequent stages run single-threaded; long-tail straggler |

**Description**

`coalesce(1)` merges all partitions onto a single task without a shuffle. When placed before
further transformations it destroys parallelism for every downstream stage. It is only
acceptable as the very last step before writing a single output file.

**What it detects**

Any call to `.coalesce(1)`.

```python
# Triggers SPL-D04-002
df.coalesce(1).groupBy("key").count()   # single-partition aggregation
```

**Why it matters**

Unlike `repartition(1)`, `coalesce(1)` avoids an upfront shuffle by merging existing partitions
in-place. The result is identical: one enormous partition processed by a single executor core.
Any transformation applied after `coalesce(1)` — filter, join, aggregation, window function —
is entirely single-threaded. The only legitimate use of `coalesce(1)` is as the final step
before a write that must produce one output file (a legacy ingestion requirement). Even then,
it should appear after all transformations are complete so that upstream stages retain full
parallelism.

**How to fix**

```python
# Wrong: coalesce(1) before a transformation — kills parallelism
df.coalesce(1).groupBy("key").count()

# Right: remove coalesce — let the aggregation run in parallel
df.groupBy("key").count()

# Only acceptable pattern: coalesce(1) as the very last step before write
df.filter("status = 'active'").groupBy("key").count().coalesce(1).write.csv("output/")
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D04-001** — repartition(1); the shuffle-based equivalent with identical symptoms
- **SPL-D04-005** — Coalesce before write — unbalanced files; the milder variant of this pattern
- **SPL-D02-006** — Shuffle followed by coalesce; coalesce after a shuffle is a related anti-pattern

---

### SPL-D04-003 — Repartition With Very High Partition Count

| | |
|---|---|
| **Dimension** | D04 Partitioning |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Driver OOM; millions of shuffle files; slow task scheduling |

**Description**

`repartition(n)` where `n` exceeds 10,000. Tens of thousands of partitions cause excessive
task-scheduler overhead on the driver, millions of small shuffle files on disk, and slow
speculative-execution scans.

**What it detects**

Any `.repartition(N)` call where `N` is a numeric literal greater than 10,000.

```python
# Triggers SPL-D04-003
df.repartition(50_000)
```

**Why it matters**

Each Spark partition maps to one task. With 50,000 tasks in a single stage, the driver must
track the status of every task, schedule retries, and run speculative-execution scans — all
O(n) operations over the task count. Each shuffle map task also writes one file per reducer
partition: 50,000 tasks writing to 200 reducers produces 10 million shuffle files per stage.
Object stores (S3, GCS) and HDFS both show significant latency degradation beyond a few hundred
thousand files in one directory. The practical target is 128–256 MB of input data per partition,
which for most workloads means partition counts in the hundreds to low thousands.

**How to fix**

```python
# Wrong: 50,000 partitions — driver overwhelmed, millions of shuffle files
df.repartition(50_000)

# Right: target 128–256 MB per partition
# For a 50 GB dataset: 50,000 MB / 128 MB ≈ 400 partitions
df.repartition(400)

# With AQE enabled, start slightly high and let coalescing bring it down:
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
df.repartition(1000)  # AQE coalesces down to ~400
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` | AQE coalesces over-partitioned shuffle output |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `67108864`–`134217728` (64–128 MB) | Target post-coalesce partition size |

| Lint Threshold | Default | Description |
|---|---|---|
| `thresholds.max_repartition_count` | `10000` | Override in `.spark-perf-lint.yaml` |

**Related rules**

- **SPL-D04-004** — Repartition with very low partition count; the opposite extreme
- **SPL-D08-001** — AQE disabled; AQE is the recommended mitigation for over-partitioning
- **SPL-D07-005** — Small file problem on write; over-partitioned DataFrames produce many small output files

---

### SPL-D04-004 — Repartition With Very Low Partition Count

| | |
|---|---|
| **Dimension** | D04 Partitioning |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Cluster cores sit idle; up to 25× slower than optimal parallelism |

**Description**

`repartition(n)` where `n` is between 2 and 9 caps parallelism for all downstream stages to
at most `n` concurrent tasks, leaving the rest of the cluster idle.

**What it detects**

Any `.repartition(N)` call where `N` is a numeric literal in the range 2–9 (inclusive).

```python
# Triggers SPL-D04-004 (n=4, likely a 100+ core cluster)
df.repartition(4).groupBy("key").agg(sum("val"))
```

**Why it matters**

Repartitioning to a very small number of partitions permanently caps parallelism for the
repartitioned DataFrame and all downstream stages derived from it. On a 100-core cluster,
`repartition(4)` means at most 4 tasks run simultaneously — 96% of the cluster is idle for
the duration. This commonly occurs when developers test on a small local machine and hardcode
a partition count that matches their laptop's core count, then deploy to a large cluster without
changing the value.

**How to fix**

```python
# Wrong: 4 partitions on a 100-core cluster — 96% idle
df.repartition(4).groupBy("key").agg(sum("val"))

# Right: scale to cluster size — 2–3x total executor cores
df.repartition(200).groupBy("key").agg(sum("val"))

# If the DataFrame is genuinely tiny (< 100 MB), remove repartition entirely:
df.groupBy("key").agg(sum("val"))  # let Spark determine parallelism naturally
```

**Config options**

| Lint Threshold | Default | Description |
|---|---|---|
| `thresholds.min_repartition_count` | `10` | Override in `.spark-perf-lint.yaml` |

**Related rules**

- **SPL-D04-001** — repartition(1); the extreme single-task case
- **SPL-D04-003** — Repartition with very high count; the opposite extreme
- **SPL-D01-010** — Default shuffle partitions unchanged; the source of many low-partition defaults

---

### SPL-D04-005 — Coalesce Before Write — Unbalanced Output Files

| | |
|---|---|
| **Dimension** | D04 Partitioning |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Unbalanced output files; downstream reader performance degradation |

**Description**

`coalesce(n)` is used before a write operation (parquet, orc, csv, etc.) with `n > 1`. Because
`coalesce` merges partitions without redistribution, the resulting files are unbalanced —
some are large, some are small — degrading downstream reader performance.

**What it detects**

A `coalesce(N)` call (where `N > 1`) that is chained or immediately precedes a `.write` call.

```python
# Triggers SPL-D04-005
df.coalesce(20).write.parquet("s3://bucket/data")
```

**Why it matters**

`coalesce(n)` merges consecutive existing partitions without shuffling. If the original 200
partitions have uneven data distribution (common after joins and aggregations), the coalesced
partitions are also uneven — some coalesced partitions may hold 10× more data than others.
Downstream readers (Spark, Presto, Athena) split files by block boundaries, not by
Spark-partition boundaries. A very large file forces one reader task to process the full file
sequentially while other tasks finish their small files and sit idle. On object storage (S3, GCS),
a mix of huge and tiny files also degrades prefix-listing performance.

**How to fix**

```python
# Wrong: unbalanced files due to coalesce merging uneven partitions
df.coalesce(20).write.parquet("s3://bucket/data")

# Right: repartition produces evenly-sized output files at the cost of one shuffle
df.repartition(20).write.parquet("s3://bucket/data")

# Alternative: set shuffle.partitions to the target file count so
# the upstream shuffle already produces the desired number of partitions
spark.conf.set("spark.sql.shuffle.partitions", "20")
df.groupBy("key").agg(sum("val")).write.parquet("s3://bucket/data")
```

**Config options**

No Spark configuration affects this rule directly.

**Related rules**

- **SPL-D04-002** — coalesce(1) bottleneck; the single-file extreme of this pattern
- **SPL-D04-006** — Missing partitionBy on write; complement — how data is laid out on disk
- **SPL-D02-006** — Shuffle followed by coalesce; same imbalance concern in a transformation chain

---

### SPL-D04-006 — Missing partitionBy on Write

| | |
|---|---|
| **Dimension** | D04 Partitioning |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Full table scans on every query; no partition pruning possible |

**Description**

Writing to a columnar format (parquet, orc, delta) without a `partitionBy()` clause places all
data in a single directory. Queries that filter on date, region, or status must scan every file
rather than pruning to a relevant subset.

**What it detects**

A `.write` chain ending in `.parquet()`, `.orc()`, or `.save()` that does not include a
`.partitionBy()` call.

```python
# Triggers SPL-D04-006
df.write.mode("overwrite").parquet("s3://bucket/events")
```

**Why it matters**

Partition pruning is one of the highest-impact Spark read optimizations. A 10 TB table
partitioned by `date` (3 years of data ≈ 1,095 partitions) and filtered on a single day
reduces the scan from 10 TB to ~9 GB — a 1,000× reduction before a single row is deserialized.
Without `partitionBy`, every query must open, read, and decode every file in the output directory,
regardless of the filter condition. The Spark query planner cannot eliminate files it has no
metadata for.

**How to fix**

```python
# Wrong: no partition strategy — full scan on every query
df.write.mode("overwrite").parquet("s3://bucket/events")

# Right: partition by low-cardinality query dimensions
df.write.partitionBy("date", "region").mode("overwrite").parquet("s3://bucket/events")

# Good partition column choices: date, year/month, region, status, event_type
# Bad partition column choices: user_id, order_id, timestamp (too many values — see SPL-D04-007)
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D04-007** — Over-partitioning — high-cardinality column; don't partition by user_id or UUID
- **SPL-D04-009** — Partition column not used in query filters; the read-side complement to this write-side rule
- **SPL-D07-001** — CSV/JSON for analytical workload; columnar formats are required for partition pruning

---

### SPL-D04-007 — Over-Partitioning — High-Cardinality Partition Column

| | |
|---|---|
| **Dimension** | D04 Partitioning |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Millions of tiny files; driver OOM on partition listing; high storage cost |

**Description**

`partitionBy()` is called with a high-cardinality column (name contains `_id`, `_at`, `_uuid`,
`_key`, or `_ts`). Partitioning by millions of distinct values creates millions of directories
and files — the classic small-files problem.

**What it detects**

A `.partitionBy(col)` call where `col` matches a high-cardinality naming pattern
(`*_id`, `*_at`, `*_uuid`, `*_key`, `*_ts`).

```python
# Triggers SPL-D04-007 — user_id has millions of distinct values
df.write.partitionBy("user_id").parquet("s3://bucket/events")
```

**Why it matters**

Each distinct partition column value becomes a directory. With 10 million distinct `user_id`
values, `partitionBy("user_id")` creates 10 million directories. Spark's partition discovery
(the `LIST` operation on S3/GCS/ADLS that finds all files) must enumerate all 10 million
directories on every read — a sequential operation that can take tens of minutes and consume
significant driver memory. Object storage providers charge per LIST/PUT operation, so millions
of tiny files also dramatically increase storage costs. The Spark driver assembles the full
file list in memory, causing OOM for very high cardinality cases.

**How to fix**

```python
# Wrong: high-cardinality partition key — millions of tiny files
df.write.partitionBy("user_id").parquet("s3://bucket/events")

# Right: use a lower-cardinality equivalent
df.write.partitionBy("date", "region").parquet("s3://bucket/events")

# If user_id-level access patterns are required, use bucketing instead:
df.write.bucketBy(256, "user_id").sortBy("user_id").saveAsTable("events")
# Bucketing co-locates the data without creating per-value directories
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.sources.partitionOverwriteMode` | `dynamic` — only overwrites touched partitions, not all |

**Related rules**

- **SPL-D04-006** — Missing partitionBy; the opposite problem — no partitioning at all
- **SPL-D04-008** — Missing bucketBy; bucketing is the correct tool for high-cardinality join keys
- **SPL-D07-005** — Small file problem on write; high-cardinality partitioning is a primary cause

---

### SPL-D04-008 — Missing bucketBy for Repeatedly Joined Tables

| | |
|---|---|
| **Dimension** | D04 Partitioning |
| **Severity** | INFO |
| **Effort** | Major refactor |
| **Impact** | Every join on this table requires a full shuffle; 0 shuffles with bucketing |

**Description**

`saveAsTable()` is called without `bucketBy()`. For tables that are repeatedly joined on the
same key, bucketing pre-distributes rows by join key hash so that subsequent joins require no
shuffle at all.

**What it detects**

A `.saveAsTable()` call in a write chain that does not include a `.bucketBy()` call.

```python
# Triggers SPL-D04-008
df.write.mode("overwrite").saveAsTable("fact_orders")
```

**Why it matters**

`bucketBy(n, "join_key")` writes exactly `n` files, where each file contains rows whose
`join_key` hashes to the same bucket. When two bucketed tables share the same bucket count and
the same join key, Spark can detect the pre-partitioned co-location at query time and replace
the sort-merge join with a bucket join — no shuffle, no sort stage. For a fact table that is
joined dozens of times per day in separate jobs, the amortised shuffle savings across all joins
far exceed the one-time cost of writing a bucketed table. The tradeoff: bucketed tables must be
written to a Hive-compatible Metastore location (`saveAsTable`), not a plain `parquet` path.

**How to fix**

```python
# Wrong: no bucketing — every join triggers a full shuffle
df.write.mode("overwrite").saveAsTable("fact_orders")

# Right: bucket by the join key — subsequent joins on order_id skip the shuffle
df.write \
    .bucketBy(256, "order_id") \
    .sortBy("order_id") \
    .mode("overwrite") \
    .saveAsTable("fact_orders")

# When joining two bucketed tables with the same bucket count and key:
spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
orders = spark.table("fact_orders")       # 256 buckets on order_id
items = spark.table("order_items")        # 256 buckets on order_id
result = orders.join(items, "order_id")   # zero shuffle — bucket join
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.sources.bucketing.enabled` | `true` (default) | Must be enabled for bucket join optimization |
| `spark.sql.bucketing.coalesceBucketsInJoin.enabled` | `true` | Allows joining tables with different bucket counts |

**Related rules**

- **SPL-D04-007** — High-cardinality partition column; bucketing is the correct alternative for high-cardinality keys
- **SPL-D03-002** — Missing broadcast hint; broadcast is the other shuffle-elimination strategy
- **SPL-D03-006** — Multiple joins without repartition; bucketed tables eliminate shuffle for all joins on the key

---

### SPL-D04-009 — Partition Column Not Used in Query Filters

| | |
|---|---|
| **Dimension** | D04 Partitioning |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Full table scan instead of partition-pruned scan; 10–1000× more data read |

**Description**

A read from storage flows directly into an expensive operation (groupBy, distinct, orderBy, agg)
without a `filter()` or `where()` on the likely partition column. The entire table is scanned
when only a fraction of it may be needed.

**What it detects**

A read terminal (`parquet`, `csv`, `json`, `orc`, `table`, `load`) that chains directly into
a shuffle operation (`groupBy`, `distinct`, `orderBy`, `agg`) without an intermediate `filter()`
or `where()`.

```python
# Triggers SPL-D04-009
spark.read.parquet("s3://datalake/events").groupBy("user_id").count()
```

**Why it matters**

A partitioned Parquet table stores its directory structure on disk: `events/date=2024-01-01/`,
`events/date=2024-01-02/`, etc. When Spark sees `filter('date = "2024-01-01"')`, it lists only
the matching directory — partition pruning. Without a filter on the partition column, Spark must
list every directory, open every file, and decode every row group before the groupBy can begin.
On a 3-year history table with 365×3 = 1,095 date partitions, a missing date filter scans
1,095× more data than needed.

**How to fix**

```python
# Wrong: full table scan
spark.read.parquet("s3://datalake/events").groupBy("user_id").count()

# Right: filter on the partition column before the expensive operation
(
    spark.read.parquet("s3://datalake/events")
    .filter('date = "2024-01-01"')          # partition pruning: 1/1095 directories scanned
    .groupBy("user_id").count()
)

# Verify pruning is active: check "Input" bytes in Spark UI stage details
# or run df.explain() and confirm PartitionFilters in the FileScan node
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.parquet.filterPushdown` | `true` (default); must be enabled for partition pruning to work |
| `spark.sql.orc.filterPushdown` | `true` (default) |

**Related rules**

- **SPL-D04-006** — Missing partitionBy on write; without `partitionBy` on write, pruning cannot occur on read
- **SPL-D03-004** — Join without prior filter; the same pattern for join inputs specifically
- **SPL-D07-003** — select("*"); no column pruning compounds the cost of a full table scan

---

### SPL-D04-010 — Repartition by Column Different From Join Key

| | |
|---|---|
| **Dimension** | D04 Partitioning |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Two full shuffles instead of one; 2× shuffle I/O for that stage |

**Description**

`repartition('col_a')` is followed by `join(other, 'col_b')` where `col_a ≠ col_b`. The
repartition shuffle is immediately invalidated by the join shuffle, making the repartition call
pure wasted I/O.

**What it detects**

A `repartition(col)` call followed within 5 lines by a `join()` call, detected when the
repartition column name and the join key differ.

```python
# Triggers SPL-D04-010 — repartitions by user_id, but joins on order_id
df.repartition("user_id").join(other, "order_id")
```

**Why it matters**

A sort-merge join always performs its own shuffle to co-locate matching keys. If the preceding
`repartition` used a different column, it distributed rows by a hash that the join immediately
discards — the join must re-shuffle every row by `order_id` regardless of how they were
partitioned going in. The result: two full shuffles of the dataset where one would suffice.
The only scenario where a pre-join `repartition` saves work is when the repartition key
equals the join key and Spark's exchange-reuse optimization detects the match — but that
detection is more reliably triggered by letting the join perform its own shuffle.

**How to fix**

```python
# Wrong: repartition by user_id, then join on order_id — 2 shuffles
df.repartition("user_id").join(other, "order_id")

# Right: remove the repartition — join shuffles on order_id automatically
df.join(other, "order_id")

# If you specifically need to pre-partition by the join key (e.g., to reuse the
# partitioning for multiple subsequent joins), repartition by the SAME column:
df_partitioned = df.repartition("order_id")
result1 = df_partitioned.join(a, "order_id")   # exchange reuse may eliminate shuffle
result2 = df_partitioned.join(b, "order_id")   # second join reuses same partitioning
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.exchange.reuse` | `true` (default); enables Spark to detect and reuse identical shuffle plans |

**Related rules**

- **SPL-D02-003** — Unnecessary repartition before join; the same-key variant of this problem
- **SPL-D03-006** — Multiple joins without repartition; when pre-partitioning IS the right fix

---

## D05 · Data Skew

Data skew occurs when one partition holds disproportionately more data than others. The entire
stage waits for the slowest ("straggler") task — every other executor core sits idle once its
balanced partitions finish. These rules detect join keys, groupBy columns, and window partitions
that structurally produce skewed output.

---

### SPL-D05-001 — Join on Low-Cardinality Column

| | |
|---|---|
| **Dimension** | D05 Data Skew |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | One task processes the dominant group; rest of cluster sits idle |

**Description**

`join()` is called on a column whose name suggests low cardinality (`status`, `type`, `is_*`,
`flag`, `category`). All rows sharing the dominant value are routed to a single reduce task,
creating a straggler that holds up the entire stage.

**What it detects**

A `join()` call where the join key is a single column whose name matches a low-cardinality
naming pattern.

```python
# Triggers SPL-D05-001
result = df.join(other, "status")   # if 80% have status='active', one task gets 80% of data
```

**Why it matters**

Hash partitioning routes all rows with the same key value to the same reduce task. If 80% of
a 10-billion-row table has `status = 'active'`, the task for the `'active'` partition processes
8 billion rows while the 199 other tasks process an average of 10 million rows each. The stage
wall time is determined by the slowest task — 200 tasks completing in 1 minute each, except the
one that takes 40 minutes. AQE's skew join handling (Spark 3+) can automatically detect and
split such partitions, but only when `spark.sql.adaptive.skewJoin.enabled = true`.

**How to fix**

```python
# Wrong: all 'active' rows go to one task
result = df.join(other, "status")

# Right option 1: add a secondary high-cardinality key to spread load
result = df.join(other, ["status", "user_id"])

# Right option 2: enable AQE skew join handling (lowest-effort fix)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
result = df.join(other, "status")   # AQE auto-splits the skewed partition

# Right option 3: filter before joining to reduce the dominant group
result = df.filter("status != 'active'").join(other, "status")
active_result = df.filter("status = 'active'").join(broadcast(other_active), "status")
final = result.union(active_result)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Automatic skew partition splitting |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `268435456` (256 MB) | Partition is skewed if above this size |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` | And above `factor × median partition size` |

**Related rules**

- **SPL-D05-002** — GroupBy on low-cardinality column; the aggregation variant of this problem
- **SPL-D05-003** — AQE skew join disabled; enable AQE as the first mitigation step
- **SPL-D08-004** — AQE skew join disabled with skew-prone joins; D08 counterpart

---

### SPL-D05-002 — GroupBy on Low-Cardinality Column Without Secondary Key

| | |
|---|---|
| **Dimension** | D05 Data Skew |
| **Severity** | WARNING |
| **Effort** | Major refactor |
| **Impact** | Dominant group task runs 10–100× longer than median; straggler stage |

**Description**

`groupBy()` uses only low-cardinality columns (`status`, `type`, `is_*`). All rows with the
dominant value are aggregated by a single task. Unlike join skew, AQE cannot automatically
split an aggregation partition — the fix requires a two-phase aggregation.

**What it detects**

A `groupBy()` call where all column arguments match a low-cardinality naming pattern.

```python
# Triggers SPL-D05-002
df.groupBy("status").agg(count("*"))
```

**Why it matters**

`groupBy("status")` routes all rows with the same status to the same reducer task for final
aggregation. If 90% of rows are `status = 'active'`, that task holds 90% of the dataset in
memory while sorting and aggregating. Unlike join skew (which AQE's skew join handles by
splitting partitions and re-running them), aggregation skew cannot be split mid-execution —
the partial sums from all input partitions for a given key must arrive at the same reducer.
The two-phase aggregation pattern introduces an artificial sub-key (a random bucket number)
to distribute the first aggregation pass, then removes it in the second pass.

**How to fix**

```python
# Wrong: all 'active' rows → single reducer task
df.groupBy("status").agg(count("*"))

# Right: two-phase aggregation distributes the skewed key across N buckets
from pyspark.sql.functions import rand, floor, col, sum as spark_sum, count

N = 10  # number of sub-buckets
result = (
    df
    # Phase 1: aggregate within (status, bucket) — distributes 'active' across 10 tasks
    .withColumn("bucket", floor(rand() * N).cast("int"))
    .groupBy("status", "bucket")
    .agg(count("*").alias("partial_count"))
    # Phase 2: sum the partial counts — now only 10 tasks for 'active' instead of 1
    .groupBy("status")
    .agg(spark_sum("partial_count").alias("total_count"))
)
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.adaptive.enabled` | AQE helps with post-shuffle partition merging but does NOT split aggregation skew |

**Related rules**

- **SPL-D05-001** — Join on low-cardinality column; join skew is addressable by AQE; agg skew is not
- **SPL-D05-006** — Window function partitioned by skew-prone column; window functions have the same structural problem
- **SPL-D08-004** — AQE skew join; AQE handles join skew but not aggregation skew

---

### SPL-D05-003 — AQE Skew Join Handling Disabled

| | |
|---|---|
| **Dimension** | D05 Data Skew |
| **Severity** | WARNING |
| **Effort** | Config only |
| **Impact** | Skewed partitions are not split; one task processes majority of data |

**Description**

`spark.sql.adaptive.skewJoin.enabled` is explicitly set to `false`. This disables the AQE
feature that automatically detects and splits oversized shuffle partitions during joins, leaving
skew to cause straggler tasks with no automatic mitigation.

**What it detects**

A `.config("spark.sql.adaptive.skewJoin.enabled", "false")` call.

```python
# Triggers SPL-D05-003
spark = SparkSession.builder.config("spark.sql.adaptive.skewJoin.enabled", "false").getOrCreate()
```

**Why it matters**

AQE's skew join optimization monitors actual partition sizes after each shuffle stage and,
when it detects a partition that is both above `skewedPartitionThresholdInBytes` and above
`skewedPartitionFactor × median partition size`, it automatically splits that partition and
runs multiple tasks against it. This is the lowest-effort fix for join skew — zero code
changes required. Disabling it forces every skewed join to run as a straggler, with the
dominant partition always blocking stage completion.

**How to fix**

```python
# Wrong: skew join handling disabled
spark = SparkSession.builder.config("spark.sql.adaptive.skewJoin.enabled", "false").getOrCreate()

# Right: remove the override — enabled by default when AQE is on
spark = (
    SparkSession.builder
    .config("spark.sql.adaptive.enabled", "true")
    # skewJoin.enabled defaults to true when adaptive.enabled is true
    .getOrCreate()
)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.adaptive.skewJoin.enabled` | `true` (default with AQE) | Remove the `false` override |
| `spark.sql.adaptive.enabled` | `true` | Required parent setting |

**Related rules**

- **SPL-D08-004** — AQE skew join disabled with skew-prone joins; D08 counterpart that fires when skew-prone patterns are also present
- **SPL-D05-001** — Join on low-cardinality column; AQE skew join is the first mitigation
- **SPL-D05-004** — AQE skew threshold too high; even with skew join enabled, a high threshold can miss skew

---

### SPL-D05-004 — AQE Skew Threshold Too High

| | |
|---|---|
| **Dimension** | D05 Data Skew |
| **Severity** | INFO |
| **Effort** | Config only |
| **Impact** | Moderate skew (below threshold) silently degrades stage performance |

**Description**

`spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` is set above 1 GB. The AQE
skew detection threshold is so high that moderately skewed partitions (256 MB–1 GB) are not
split, allowing them to become stragglers.

**What it detects**

A `.config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", X)` call where `X`
parses to more than 1 GB.

```python
# Triggers SPL-D05-004 (2 GB > 1 GB limit)
spark = SparkSession.builder.config(
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "2g"
).getOrCreate()
```

**Why it matters**

AQE's skew detection uses two conditions: a partition is skewed only when it is **both** above
the byte threshold **and** above `skewedPartitionFactor × median partition size`. Setting the
threshold to 2 GB means that a 1.5 GB partition that is 10× larger than the median (256 MB ×
`factor=5` = 1.28 GB threshold exceeded, but 2 GB threshold not exceeded) is not split. That
1.5 GB partition will be processed by a single task while all other 200 MB partitions finish
in parallel. The default of 256 MB is calibrated for a target partition size of ~64–128 MB.

**How to fix**

```python
# Wrong: threshold too high — 256 MB–2 GB skewed partitions not detected
spark = SparkSession.builder.config(
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "2g"
).getOrCreate()

# Right: use the default (256 MB) or lower for better skew detection
spark = (
    SparkSession.builder
    .config("spark.sql.adaptive.enabled", "true")
    # Remove the threshold override, or set it explicitly to the default:
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "268435456")  # 256 MB
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    .getOrCreate()
)
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `268435456` (256 MB, default) | Lower = more aggressive skew detection |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` (default) | Both conditions must be met |

**Related rules**

- **SPL-D05-003** — AQE skew join disabled; ensure skew handling is enabled before tuning thresholds
- **SPL-D08-005** — AQE skew factor too aggressive; the opposite extreme — factor set too low

---

### SPL-D05-005 — Missing Salting Pattern for Known Skewed Keys

| | |
|---|---|
| **Dimension** | D05 Data Skew |
| **Severity** | INFO |
| **Effort** | Major refactor |
| **Impact** | 'Whale' keys cause one reducer to handle O(N) more rows than average |

**Description**

`join()` is performed on a column name suggesting user, customer, or product IDs
(`user_id`, `customer_id`, `product_id`, etc.) without any `rand()` / `explode()` salting
pattern. ID columns frequently follow power-law distributions where a handful of 'whale' values
generate millions of rows.

**What it detects**

A `join()` on a column whose name contains `user_id`, `customer_id`, or `product_id`, where no
`rand()` call appears in the vicinity (suggesting no salting has been applied).

```python
# Triggers SPL-D05-005
df.join(events, "user_id")   # user_id may be heavily skewed toward power users
```

**Why it matters**

In e-commerce and social platforms, the top 0.1% of users generate 20–50% of all events. When
joining a user profile table against an events table on `user_id`, the reducer task for a
'whale' user ID may receive 100× more rows than the average task. AQE's skew join can handle
this automatically in many cases, but when the skew is extreme (a single user with 100 million
events) even post-split tasks are unbalanced. Salting distributes one logical key across N
physical keys by appending a random bucket number, spreading the rows across N tasks.

**How to fix**

```python
from pyspark.sql.functions import rand, floor, col, explode, array, lit

# Enable AQE skew join first — handles most cases automatically
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# If AQE is insufficient, implement salting:
N = 10  # salt factor — spread each skewed key across 10 tasks

# Salt the large (potentially skewed) side: assign a random bucket
events_salted = events.withColumn("salt", floor(rand() * N).cast("int"))

# Explode the small side: replicate each row N times with each bucket value
users_salted = users.withColumn(
    "salt", explode(array([lit(i) for i in range(N)]))
)

# Join on (user_id, salt) — each 'whale' user's rows now spread across N tasks
result = events_salted.join(users_salted, ["user_id", "salt"]).drop("salt")
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Try AQE before implementing manual salting |

**Related rules**

- **SPL-D05-001** — Join on low-cardinality column; salting works for both low-cardinality and power-law keys
- **SPL-D05-003** — AQE skew join disabled; enable AQE as the first, simpler mitigation
- **SPL-D05-007** — Null-heavy join key; nulls accumulate in one partition similar to skewed keys

---

### SPL-D05-006 — Window Function Partitioned by Skew-Prone Column

| | |
|---|---|
| **Dimension** | D05 Data Skew |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Executor OOM for dominant partition; entire stage blocked by straggler |

**Description**

`Window.partitionBy()` uses a low-cardinality column (`status`, `type`, `is_*`). All rows with
the dominant value are buffered on one executor for the window computation, risking OOM. Unlike
join skew, AQE cannot split window partitions.

**What it detects**

A `Window.partitionBy(col)` call where `col` matches a low-cardinality naming pattern.

```python
# Triggers SPL-D05-006
w = Window.partitionBy("status").orderBy("ts")
df.withColumn("rank", rank().over(w))
```

**Why it matters**

Window functions (`rank`, `lag`, `sum over window`) must buffer all rows for a given partition
on a single executor task because the computation requires seeing all rows together. Unlike a
join (where AQE can split skewed partitions and re-run sub-partitions), a window function cannot
be split mid-execution — the entire `status = 'active'` partition must fit in one executor's
memory. If 90% of rows have `status = 'active'`, that single executor task must hold 90% of the
dataset in memory simultaneously, causing OOM for any non-trivial dataset size.

**How to fix**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Wrong: 90% of rows → single executor memory
w = Window.partitionBy("status").orderBy("ts")
df.withColumn("rank", rank().over(w))

# Right: add a secondary high-cardinality column to partition more finely
w = Window.partitionBy("status", "user_id").orderBy("ts")
df.withColumn("rank", rank().over(w))
# Now each (status, user_id) pair is one partition — balanced across executors

# If global rank within status is required (cannot add secondary key):
# Consider a two-pass approach: partition-level rank followed by merge
```

**Config options**

No Spark configuration mitigates window partition skew. AQE skew join handles joins, not windows.

**Related rules**

- **SPL-D05-001** — Join on low-cardinality column; same structural skew, different operation
- **SPL-D05-002** — GroupBy on low-cardinality column; similar aggregation skew
- **SPL-D03-007** — Self-join that could be window function; replacing self-joins with windows can introduce this skew

---

### SPL-D05-007 — Null-Heavy Join Key

| | |
|---|---|
| **Dimension** | D05 Data Skew |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | All null-key rows hash to same partition; straggler task on null bucket |

**Description**

`join()` is called on a column whose name suggests it commonly contains nulls (`parent_id`,
`foreign_id`, `optional_*`, `nullable_*`) without a prior `isNotNull()` filter or `dropna()`.
Null values hash to the same partition, creating a straggler task.

**What it detects**

A `join()` on a column whose name contains `parent_id`, `foreign_id`, or `optional_`, where
no `isNotNull()`, `dropna()`, or `fillna()` call appears in the 5 preceding lines.

```python
# Triggers SPL-D05-007
df.join(other, "parent_id")   # parent_id is null for root-level records
```

**Why it matters**

In Spark, `NULL != NULL` in most join contexts (NULLs do not match each other in inner joins).
However, `hash(NULL)` is a constant — typically 0 or a fixed value — so all null-key rows are
routed to the same hash partition. In a tree-structured dataset where all root nodes have
`parent_id = NULL`, the root's partition may contain millions of rows while all other partitions
contain tens of rows. The null-key task takes 1000× longer than every other task and holds up
the entire stage.

**How to fix**

```python
# Wrong: null parent_ids all route to the same partition
df.join(other, "parent_id")

# Right option 1: filter nulls before joining (if null keys are not needed in output)
df.filter(col("parent_id").isNotNull()).join(other, "parent_id")

# Right option 2: handle nulls in a separate pass
non_null = df.filter(col("parent_id").isNotNull()).join(other, "parent_id")
null_rows = df.filter(col("parent_id").isNull())
result = non_null.union(null_rows.withColumn("parent_name", lit(None)))
```

**Config options**

No Spark configuration mitigates null-key skew directly. Enable AQE skew join as a partial
mitigation (`spark.sql.adaptive.skewJoin.enabled = true`).

**Related rules**

- **SPL-D05-001** — Join on low-cardinality column; null skew is structurally identical
- **SPL-D03-009** — Left join without null handling; nulls from the join output vs. nulls in the join key

---

## D06 · Caching

Caching (`cache()`, `persist()`, `checkpoint()`) is a powerful optimization for reused DataFrames
but a source of memory leaks and wasted compute when misused. These rules catch the most common
caching anti-patterns: caching without cleanup, caching single-use DataFrames, and caching
before filters that would have reduced the cached size.

---

### SPL-D06-001 — cache() Without unpersist()

| | |
|---|---|
| **Dimension** | D06 Caching |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Executor memory leak; evicts other cached data; OOM in long-running jobs |

**Description**

`cache()` or `persist()` is called without a corresponding `unpersist()` anywhere in the file.
Cached blocks remain pinned in executor memory for the lifetime of the SparkContext, accumulating
across stages and jobs until the executor runs out of storage memory.

**What it detects**

Any file where `cache()` or `persist()` calls outnumber `unpersist()` calls (more caches than
releases).

```python
# Triggers SPL-D06-001
df_cached = df.join(other, "id").cache()
result = df_cached.groupBy("cat").count()
# df_cached.unpersist() — never called; memory pinned until SparkContext ends
```

**Why it matters**

Spark's storage memory fraction (default: 50% of `spark.executor.memory`) is shared across all
cached DataFrames. Each `cache()` call pins a portion of this budget until `unpersist()` is
called or the SparkContext terminates. In a job that caches several DataFrames across multiple
stages without unpersisting, the storage budget fills up. When full, Spark evicts older cached
partitions using an LRU policy — but evicted partitions must be recomputed from scratch on the
next access, negating the benefit of caching. In long-running Spark Streaming jobs or notebooks
that run many cells, this becomes a gradual memory leak.

**How to fix**

```python
# Right: cache for multi-use, unpersist when done
df_cached = df.join(other, "id").cache()

result1 = df_cached.groupBy("cat").count()
result2 = df_cached.filter("status = 'active'").agg(sum("amount"))

df_cached.unpersist()   # release storage memory explicitly
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.cleaner.periodicGC.interval` | Controls GC frequency for unreferenced cached blocks |
| `spark.memory.storageFraction` | Fraction of memory reserved for cached data |

**Related rules**

- **SPL-D06-002** — cache() used only once; if only used once, don't cache at all
- **SPL-D06-003** — cache() inside loop; per-iteration caches accumulate fastest
- **SPL-D06-006** — Reused DataFrame without cache; the inverse problem — missing cache

---

### SPL-D06-002 — cache() Used Only Once

| | |
|---|---|
| **Dimension** | D06 Caching |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Unnecessary serialization pass; wastes executor memory for no benefit |

**Description**

A DataFrame is cached but then used only once afterward. Caching is only beneficial when a
DataFrame is consumed multiple times — otherwise the cache write is pure overhead.

**What it detects**

A variable that is assigned from a `cache()` or `persist()` call and then referenced only once
in subsequent code.

```python
# Triggers SPL-D06-002 — cached but only counted once
df_cached = df.join(other, "id").cache()
result = df_cached.count()   # single use — cache wasted
```

**Why it matters**

`cache()` adds an extra pass through the data: Spark must materialize the DataFrame, serialize
it, and write it to executor memory (and optionally disk). This serialization pass itself takes
time and memory proportional to the cached data size. When the DataFrame is used only once
after caching, this overhead is pure waste — the job would have been faster without the
`cache()` call, because the single downstream action would read directly from the source without
the intermediate serialization step.

**How to fix**

```python
# Wrong: cache with single use — slower than no cache
df_cached = df.join(other, "id").cache()
result = df_cached.count()

# Right: remove cache if used only once
result = df.join(other, "id").count()

# Cache is correct when used 2+ times:
df_cached = df.join(other, "id").cache()
result1 = df_cached.count()                          # use 1
result2 = df_cached.filter("status = 'a'").collect() # use 2
df_cached.unpersist()
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D06-001** — cache() without unpersist(); unnecessary cache that is never cleaned up
- **SPL-D06-006** — Reused DataFrame without cache; the symmetric problem — multi-use without cache

---

### SPL-D06-003 — cache() Inside Loop

| | |
|---|---|
| **Dimension** | D06 Caching |
| **Severity** | CRITICAL |
| **Effort** | Major refactor |
| **Impact** | OOM after O(N) iterations; executor memory accumulates without release |

**Description**

`cache()` or `persist()` is called inside a `for` or `while` loop without a matching
`unpersist()` within the same loop iteration. Each iteration pins a new set of partitions in
executor memory, accumulating until the executor runs out of storage memory.

**What it detects**

A `cache()` or `persist()` call that is nested inside a loop body.

```python
# Triggers SPL-D06-003
for epoch in range(100):
    df = model.transform(df).cache()
    loss = df.agg(mean("loss")).collect()
    # previous iteration's cached data never released — memory fills after ~10 iterations
```

**Why it matters**

Each loop iteration creates a new cached RDD representing the current state of `df`. Without
`unpersist()`, the previous iteration's cached data remains pinned. After N iterations,
N versions of the transformed DataFrame are cached simultaneously. For ML training loops where
each iteration adds a new layer of transformations, both the lineage tree and the cached memory
grow with each step. On a 16 GB executor with 8 GB storage fraction, a 1 GB transformed
DataFrame fills memory after 8 iterations, causing subsequent caches to evict earlier ones —
which then get recomputed from scratch on the next access, defeating the entire purpose.

**How to fix**

```python
# Right option 1: unpersist at the start of each iteration
df_cached = df.cache()
for epoch in range(100):
    prev = df_cached
    df_cached = model.transform(df_cached).cache()
    prev.unpersist()   # release previous iteration's memory
    loss = df_cached.agg(mean("loss")).collect()

# Right option 2: use checkpoint() for iterative algorithms
# checkpoint() writes to HDFS and truncates the growing lineage tree
spark.sparkContext.setCheckpointDir("hdfs://checkpoints/")
for epoch in range(100):
    df = model.transform(df).checkpoint()
    loss = df.agg(mean("loss")).collect()
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.memory.storageFraction` | Fraction of executor heap for cached data; raise to reduce eviction rate |

**Related rules**

- **SPL-D06-001** — cache() without unpersist(); the per-iteration leak is the same root cause
- **SPL-D06-008** — checkpoint vs cache misuse; checkpoint is often the correct choice for iterative loops
- **SPL-D03-008** — Join inside loop; loops and Spark operations are a recurring anti-pattern class

---

### SPL-D06-004 — cache() Before Filter

| | |
|---|---|
| **Dimension** | D06 Caching |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Caches N rows but only M < N are used; wastes (N−M)/N of cache memory |

**Description**

`cache()` is called before `filter()` or `where()`, caching the full unfiltered dataset. Moving
the filter before the cache reduces the cached size proportional to the filter selectivity.

**What it detects**

A method chain where `cache()` (or `persist()`) appears before `filter()` or `where()` in the
same chain.

```python
# Triggers SPL-D06-004
df.cache().filter("active = true").groupBy("status").count()
```

**Why it matters**

When `cache()` precedes `filter()`, Spark caches every row of the DataFrame including rows that
will be immediately discarded by the filter. If `active = true` applies to 10% of rows, 90% of
the cached memory holds data that will never be accessed again. The filter runs on every
downstream action against the cached data, re-scanning and discarding the 90% of cached rows
each time. Moving `filter()` before `cache()` reduces the cached size by 90%, leaving 90% more
storage memory available for other DataFrames.

**How to fix**

```python
# Wrong: caches 100% of rows, uses only 10%
df.cache().filter("active = true").groupBy("status").count()

# Right: filter first, cache only what will be used
df.filter("active = true").cache().groupBy("status").count()

# With unpersist:
active_cached = df.filter("active = true").cache()
result1 = active_cached.groupBy("status").count()
result2 = active_cached.agg(sum("revenue"))
active_cached.unpersist()
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D06-001** — cache() without unpersist(); don't forget to release the filtered cache
- **SPL-D03-004** — Join without prior filter; the same push-filter-early principle for joins
- **SPL-D04-009** — Partition column not used in filters; combining partition pruning with pre-cache filtering maximizes savings

---

### SPL-D06-005 — MEMORY_ONLY Storage Level for Potentially Large Datasets

| | |
|---|---|
| **Dimension** | D06 Caching |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Silent partition eviction leads to full recomputation under memory pressure |

**Description**

`persist(StorageLevel.MEMORY_ONLY)` is used. When executor storage memory is exhausted, Spark
silently evicts `MEMORY_ONLY` partitions, which must then be recomputed from scratch on the
next access. `MEMORY_AND_DISK` provides a disk fallback that prevents silent recomputation.

**What it detects**

Any `.persist(StorageLevel.MEMORY_ONLY)` call.

```python
# Triggers SPL-D06-005
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_ONLY)
```

**Why it matters**

`MEMORY_ONLY` is also the default for `df.cache()` in the RDD API. When the executor's storage
fraction fills up, Spark evicts partitions using an LRU policy — but with `MEMORY_ONLY`,
evicted partitions are simply dropped. The next action that needs those partitions re-executes
the full lineage from the source, which may include expensive shuffles and joins that were
precisely why you cached in the first place. This eviction is silent: the job continues without
an error, but suddenly runs much slower. `MEMORY_AND_DISK` spills evicted partitions to disk
instead of dropping them, guaranteeing they are available on the next access without full
recomputation.

**How to fix**

```python
from pyspark import StorageLevel

# Wrong: evicted partitions are silently dropped and recomputed
df.persist(StorageLevel.MEMORY_ONLY)

# Right: disk fallback prevents silent recomputation
df.persist(StorageLevel.MEMORY_AND_DISK)

# For Kryo-serialized storage (smaller footprint, slightly slower deserialization):
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.memory.storageFraction` | Increase (e.g. `0.7`) to reduce eviction frequency |
| `spark.serializer` | Set to Kryo (SPL-D01-001) to reduce memory footprint for `_SER` storage levels |

**Related rules**

- **SPL-D01-001** — Missing Kryo serializer; Kryo + `MEMORY_AND_DISK_SER` is the most memory-efficient caching combination
- **SPL-D06-001** — cache() without unpersist(); memory pressure from leaked caches causes the evictions this rule addresses

---

### SPL-D06-006 — Reused DataFrame Without Cache

| | |
|---|---|
| **Dimension** | D06 Caching |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | N downstream uses trigger N full pipeline recomputations instead of 1 |

**Description**

A DataFrame built from an expensive operation (join, groupBy, aggregation) is used as the input
for multiple downstream actions or transformations without being cached. Each downstream use
re-executes the full lineage from the source.

**What it detects**

A variable assigned from a chain containing a shuffle operation (`join`, `groupBy`, `agg`,
`distinct`, `orderBy`) that is referenced in 2+ subsequent method calls without an intervening
`cache()` or `persist()`.

```python
# Triggers SPL-D06-006
df_agg = df.groupBy("id").agg(sum("v"))    # expensive aggregation
result1 = df_agg.join(lookup, "id")        # triggers full recompute of df_agg
total = df_agg.count()                      # triggers another full recompute
```

**Why it matters**

Spark DataFrames are lazy: each action triggers re-execution of the full lineage. If `df_agg`
was built from a 10-minute join + aggregation and is used in 3 downstream actions, Spark
re-runs that 10-minute computation 3 times — 30 minutes total instead of 10. The extra 20
minutes is pure waste. Adding `cache()` after the aggregation materializes the result once and
serves all 3 downstream actions from the cached copy in executor memory.

**How to fix**

```python
# Right: cache after the expensive computation
df_agg = df.groupBy("id").agg(sum("v")).cache()

result1 = df_agg.join(lookup, "id")        # reads from cache
total = df_agg.count()                      # reads from cache
report = df_agg.filter("v > 100").show()   # reads from cache

df_agg.unpersist()   # release when all downstream uses are complete
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D06-001** — cache() without unpersist(); adding cache() introduces the leak risk
- **SPL-D06-002** — cache() used only once; the inverse — cache present but unnecessary
- **SPL-D02-007** — Multiple shuffles in sequence; caching between shuffles truncates lineage

---

### SPL-D06-007 — cache() After repartition

| | |
|---|---|
| **Dimension** | D06 Caching |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Unnecessary cache write after an already-expensive shuffle |

**Description**

`cache()` is called immediately after `repartition()`. This is only justified when the
repartitioned DataFrame is consumed 2+ times — otherwise the cache write is overhead after an
already-expensive shuffle.

**What it detects**

A `cache()` or `persist()` call that appears in a chain or within 2 lines after `repartition()`.

```python
# Triggers SPL-D06-007
df2 = df.repartition(200).cache()   # cached — but is it used more than once?
```

**Why it matters**

`repartition()` already performs a full shuffle — a costly write+network+read cycle. Immediately
caching the result adds a second serialization pass to write the shuffled data to executor
storage memory. If `df2` is only used once, this extra pass is pure overhead on top of an
already expensive operation. The rule fires as an INFO to prompt the developer to confirm
whether the cache is justified by multiple downstream uses.

**How to fix**

```python
# If the repartitioned DataFrame is used only once — remove cache()
df2 = df.repartition(200)
result = df2.write.parquet("output/")   # single use — no cache needed

# If used 2+ times — cache is correct and this finding can be suppressed
# Add a comment to document the multi-use intent:
df2 = df.repartition(200).cache()   # cached: used by both train and validate splits
train = df2.filter("split = 'train'")
validate = df2.filter("split = 'validate'")
df2.unpersist()
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D06-002** — cache() used only once; same underlying concern
- **SPL-D06-001** — cache() without unpersist(); if cache is justified, add unpersist()
- **SPL-D02-006** — Shuffle followed by coalesce; repartition + coalesce is a related anti-pattern

---

### SPL-D06-008 — checkpoint vs cache Misuse

| | |
|---|---|
| **Dimension** | D06 Caching |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Unnecessary disk write + checkpoint dir overhead in non-iterative pipelines |

**Description**

`checkpoint()` is used in non-iterative code. Checkpointing writes data to HDFS and truncates
the DAG lineage — it is designed for iterative algorithms where lineage grows unboundedly.
For non-iterative pipelines, `cache()` achieves the same reuse benefit without the HDFS write.

**What it detects**

A `checkpoint()` call that does not appear inside a `for` or `while` loop body.

```python
# Triggers SPL-D06-008
df_intermediate = df.join(other, "id").checkpoint()   # no loop — HDFS write unnecessary
```

**Why it matters**

`checkpoint()` performs two operations: (1) it materializes the DataFrame by triggering an
action that writes every partition to the configured checkpoint directory (HDFS or a cloud
storage path), and (2) it truncates the lineage graph so Spark forgets how to recompute the
DataFrame. Operation 1 is a full write to durable storage — significantly slower than
`cache()`, which writes to executor memory. Operation 2 is beneficial only in iterative
algorithms where the lineage grows with each iteration (ML training, PageRank). In a linear
pipeline with no loops, the lineage does not grow unboundedly and `cache()` is always faster.

**How to fix**

```python
# Wrong for non-iterative pipelines: HDFS write with no lineage-growth benefit
df_intermediate = df.join(other, "id").checkpoint()

# Right: cache() is faster and sufficient for fixed-depth lineage
df_intermediate = df.join(other, "id").cache()
result = df_intermediate.groupBy("cat").count()
df_intermediate.unpersist()

# checkpoint() IS correct inside iterative loops where lineage grows:
spark.sparkContext.setCheckpointDir("hdfs://checkpoints/")
for iteration in range(100):
    model = update(model, df)
    df = model.transform(df).checkpoint()  # truncates growing lineage tree
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.checkpoint.compress` | `true` — compress checkpoint data to reduce HDFS write size |

**Related rules**

- **SPL-D06-003** — cache() inside loop; checkpoint() is the correct replacement for cache() inside loops
- **SPL-D06-001** — cache() without unpersist(); cache() introduced by this fix needs cleanup
- **SPL-D06-005** — MEMORY_ONLY storage level; if memory pressure is the concern, use MEMORY_AND_DISK instead of checkpoint()

---

## D07 · I/O and File Formats

Every byte that crosses the storage layer costs network bandwidth, deserialization CPU, and
driver memory for metadata. These rules enforce columnar formats, explicit schemas, predicate
push-down discipline, and safe write semantics — the cheapest category of improvements because
most fixes are one-line changes that benefit every future job execution.

---

### SPL-D07-001 — CSV/JSON Used for Analytical Workload

| | |
|---|---|
| **Dimension** | D07 I/O and File Formats |
| **Severity** | WARNING |
| **Effort** | Major refactor |
| **Impact** | 5–20× slower scans vs Parquet; no column pruning or predicate pushdown |

**Description**

The job reads from CSV or JSON for an analytical workload. These row-oriented text formats have
no column pruning, no predicate pushdown, and no built-in compression — making them 5–20× slower
than Parquet or ORC for any query that touches only a subset of columns.

**What it detects**

A `spark.read.csv(...)` or `spark.read.json(...)` call (or `.format("csv")` / `.format("json")`).

```python
# Triggers SPL-D07-001
df = spark.read.csv("data/", header=True, inferSchema=True)
df = spark.read.json("events/")
```

**Why it matters**

Parquet and ORC store data in column-major order: all values for a given column are stored
together, encoded with column-specific codecs, and annotated with min/max statistics per row
group. Spark exploits this to read only the columns referenced in the query (column pruning)
and skip row groups whose statistics rule out any matching rows (predicate pushdown). CSV and
JSON have none of this — every byte of every column must be read, decoded from UTF-8, and
type-cast on every scan. A query that needs 3 of 50 columns from a 500 GB CSV file reads
500 GB; the same query on Parquet reads roughly 30 GB. The 16× difference compounds across
every job execution for the lifetime of the dataset.

**How to fix**

```python
# Wrong: row-oriented text format — full file scan always
df = spark.read.csv("data/", header=True, inferSchema=True)

# Right: convert once, read as columnar format forever
# Step 1: one-time conversion job
spark.read.csv("data/", header=True, schema=my_schema) \
    .write.mode("overwrite") \
    .partitionBy("date") \
    .parquet("data_parquet/")

# Step 2: all subsequent reads
df = spark.read.parquet("data_parquet/")

# For streaming ingestion requiring ACID + schema evolution: use Delta Lake
df.write.format("delta").partitionBy("date").save("data_delta/")
```

**Config options**

No Spark configuration changes the cost of reading CSV/JSON. The fix is a format migration.

**Related rules**

- **SPL-D07-002** — Schema inference; CSV reads commonly include `inferSchema=True`, doubling the I/O cost
- **SPL-D04-006** — Missing partitionBy; migrate to Parquet and partition at the same time
- **SPL-D07-003** — select("*"); columnar formats only benefit from column pruning when explicit selects are used

---

### SPL-D07-002 — Schema Inference Enabled

| | |
|---|---|
| **Dimension** | D07 I/O and File Formats |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | 2× I/O per job; unstable schemas cause intermittent type errors |

**Description**

`inferSchema=True` (or `option("inferSchema", "true")`) is set on a reader. Spark performs a
full pass over the entire input to detect column types before running the actual query,
doubling I/O on every execution.

**What it detects**

Any reader call with `.option("inferSchema", "true")` or `inferSchema=True` as a keyword
argument.

```python
# Triggers SPL-D07-002
df = spark.read.option("inferSchema", "true").csv("path/")
df = spark.read.csv("path/", inferSchema=True)
```

**Why it matters**

Schema inference works by reading the entire dataset twice: first to sample types, then to
execute the query. For a 500 GB CSV file, this is 1 TB of I/O per job execution. Beyond
performance, inferred schemas are fragile: a single malformed row or a file with an extra
column changes the inferred schema, causing downstream jobs to fail with `AnalysisException`
or to silently produce wrong results when a column shifts position. An explicit schema is a
contract that makes the job deterministic regardless of data anomalies.

**How to fix**

```python
# Wrong: 2× I/O; schema changes with data anomalies
df = spark.read.csv("path/", inferSchema=True, header=True)

# Right: explicit schema — 1× I/O; deterministic behavior
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

schema = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
])
df = spark.read.schema(schema).csv("path/", header=True)

# DDL string syntax (more concise):
df = spark.read.schema("id LONG NOT NULL, name STRING, amount DOUBLE").csv("path/", header=True)
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D07-001** — CSV/JSON format; `inferSchema` is only relevant for text formats
- **SPL-D03-005** — Join key type mismatch; inferred schemas are the leading cause of type mismatch bugs on joins

---

### SPL-D07-003 — select("*") — No Column Pruning

| | |
|---|---|
| **Dimension** | D07 I/O and File Formats |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | All columns decoded from storage; no columnar pruning benefit |

**Description**

`select("*")` (or `select(col("*"))`) is a no-op that retains every column. For columnar
formats it defeats column pruning: the storage layer must deserialize every column even when
only a few are needed downstream.

**What it detects**

Any `.select("*")` or `.select(col("*"))` call.

```python
# Triggers SPL-D07-003
df.select("*").groupBy("status").count()
df.select(col("*")).filter("active = true")
```

**Why it matters**

Parquet and ORC column pruning is one of Spark's most effective I/O optimizations: it instructs
the reader to deserialize only the columns referenced in the query plan. `select("*")` injects
a wildcard projection into the plan that references all columns, forcing the reader to
deserialize everything. On a 200-column table where the query only needs 3 columns, removing
`select("*")` reduces deserialization work by 66× and cuts network I/O proportionally. The
fix is to replace `select("*")` with an explicit column list or to simply remove it — a bare
`df.groupBy("status").count()` prunes columns automatically.

**How to fix**

```python
# Wrong: reads and decodes all 200 columns
df.select("*").groupBy("status").count()

# Right: explicit columns — only status is read from disk
df.select("status").groupBy("status").count()

# Or simply omit the select entirely (Catalyst prunes automatically):
df.groupBy("status").count()

# When building a derived DataFrame and you need all columns plus new ones:
# Avoid select("*", new_col) — instead use withColumn:
df.withColumn("score", col("amount") * 1.1)  # appends without wildcard select
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D07-001** — CSV/JSON format; column pruning only applies to columnar formats (Parquet, ORC)
- **SPL-D04-009** — Partition column not used in filters; combine column pruning with partition pruning for maximum I/O reduction
- **SPL-D03-004** — Join without prior filter/select; `select("*")` in a join chain doubles this problem

---

### SPL-D07-004 — Filter Applied After Join — Missing Predicate Pushdown

| | |
|---|---|
| **Dimension** | D07 I/O and File Formats |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Shuffle includes rows that will be discarded; wasted network I/O |

**Description**

`filter()` or `where()` is applied after `join()` when the filter predicate is on a column
from one of the join inputs. Moving the filter before the join reduces the data shuffled by the
join.

**What it detects**

A `filter()` or `where()` call that follows a `join()` call in the same method chain.

```python
# Triggers SPL-D07-004
df.join(other, "id").filter('status = "active"').groupBy("x").count()
```

**Why it matters**

Catalyst's predicate pushdown optimizer moves `filter()` conditions as close to the data source
as possible — ideally into the Parquet row-group reader for predicate pushdown, or at minimum
before the shuffle for join inputs. However, when the filter is written after the join in
source code, Catalyst can only push it down if it can prove the predicate does not depend on
any column produced by the join. For filters on original input columns (not derived columns),
manual reordering makes the intent unambiguous and avoids any analysis ambiguity. Every row
that passes the filter but would have been discarded post-join represents wasted serialization,
network transfer, and deserialization in the shuffle.

**How to fix**

```python
# Wrong: join shuffles unfiltered rows; filter discards them after the fact
df.join(other, "id").filter('status = "active"').groupBy("x").count()

# Right: filter before the join reduces shuffle payload
df.filter('status = "active"').join(other, "id").groupBy("x").count()

# Filter both sides when possible:
(
    df.filter('status = "active"')
    .join(other.filter('region = "EU"'), "id")
    .groupBy("x").count()
)
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.optimizer.nestedSchemaPruning.enabled` | `true` (default Spark 3+); ensures Catalyst pushes struct field access into the reader |

**Related rules**

- **SPL-D03-004** — Join without prior filter/select; the full join-filtering guidance
- **SPL-D04-009** — Partition column not used in filters; partition pruning is the most impactful form of pre-join filtering
- **SPL-D10-001** — UDF blocks predicate pushdown; UDFs prevent Catalyst from pushing filters through them

---

### SPL-D07-005 — Small File Problem on Write

| | |
|---|---|
| **Dimension** | D07 I/O and File Formats |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | O(partitions × partition_values) small files; slow reads and metastore overload |

**Description**

`write.partitionBy()` is used without a preceding `coalesce()` or `repartition()`. With
Spark's default of 200 shuffle partitions, a dataset spanning many partition values can produce
thousands of tiny output files — one per (shuffle partition × partition value).

**What it detects**

A `.write.partitionBy(...)` call in a chain that does not include `coalesce()` or `repartition()`
within 5 preceding lines.

```python
# Triggers SPL-D07-005
df.write.partitionBy("date").parquet("s3://bucket/data")
# With 200 shuffle partitions and 365 date values: up to 73,000 output files
```

**Why it matters**

`write.partitionBy("date")` writes one file per Spark partition per unique date value. With 200
shuffle partitions and 365 days of data, Spark creates up to 200 × 365 = 73,000 files. Object
storage (S3, GCS) must open a separate HTTP connection per file during reads; the Spark driver
must LIST all 73,000 file paths and hold them in memory; the Hive Metastore must track each
file as a separate partition entry. Each tiny file reads slowly because the storage overhead
(open, seek, read footer, close) dominates the actual data-read time. Target files of 128–256 MB.

**How to fix**

```python
# Wrong: 200 shuffle partitions × N date values = many tiny files
df.write.partitionBy("date").parquet("s3://bucket/data")

# Right: coalesce to a reasonable file count per date partition
# If you have ~10 GB per date and want ~128 MB files: 10 GB / 128 MB ≈ 80 files per date
df.coalesce(80).write.partitionBy("date").parquet("s3://bucket/data")

# Alternative: repartition by date so each date gets one or a few partitions
df.repartition(365, "date").write.partitionBy("date").parquet("s3://bucket/data")
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.shuffle.partitions` | Reduce to limit initial file count per partition value |
| `spark.sql.adaptive.enabled` | AQE coalesces small post-shuffle partitions, which reduces file count |

**Related rules**

- **SPL-D04-007** — High-cardinality partition column; both rules address the small-files problem from different angles
- **SPL-D04-005** — Coalesce before write — unbalanced files; covers the general pre-write coalesce guidance
- **SPL-D04-006** — Missing partitionBy; the pair to this rule — write with `partitionBy`, but control file count

---

### SPL-D07-006 — JDBC Read Without Partition Parameters

| | |
|---|---|
| **Dimension** | D07 I/O and File Formats |
| **Severity** | CRITICAL |
| **Effort** | Minor code change |
| **Impact** | Sequential single-thread read; does not scale to large JDBC tables |

**Description**

`spark.read.jdbc(url, table)` is called without `column`, `lowerBound`, `upperBound`, and
`numPartitions` parameters. Without these, Spark creates exactly one partition and reads the
entire table through a single JDBC connection on the driver, serializing all I/O through one
thread.

**What it detects**

A `spark.read.jdbc(...)` call that lacks both `column` and `numPartitions` arguments.

```python
# Triggers SPL-D07-006
df = spark.read.jdbc(url, "orders")
df = spark.read.format("jdbc").option("url", url).option("dbtable", "orders").load()
```

**Why it matters**

The JDBC data source creates one Spark partition per query range. Without partition parameters,
there is only one range — `SELECT * FROM orders` — executed by a single thread on the driver.
A table with 1 billion rows will block the driver for hours, using 0% of the executor cluster.
The driver also accumulates all fetched rows before distributing them, risking driver OOM. The
partition parameters split the query into `numPartitions` sub-queries with `WHERE column
BETWEEN lowerBound AND upperBound / numPartitions`, each executed in parallel by a separate
executor task.

**How to fix**

```python
# Wrong: single JDBC connection, entire table on driver thread
df = spark.read.jdbc(url, "orders")

# Right: parallel reads using partition column ranges
df = spark.read.jdbc(
    url,
    "orders",
    column="order_id",       # numeric column for range partitioning
    lowerBound=0,
    upperBound=10_000_000,   # approximate max value
    numPartitions=20,        # 20 parallel JDBC connections
    properties={"user": "...", "password": "..."},
)

# For non-numeric partition columns, use predicates:
predicates = [
    "region = 'NA'",
    "region = 'EU'",
    "region = 'APAC'",
]
df = spark.read.jdbc(url, "orders", predicates=predicates, properties={...})
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.executor.extraJavaOptions` | Add JDBC driver JAR path if not already on classpath |

**Related rules**

- **SPL-D04-004** — Repartition with very low count; the resulting single-partition DataFrame has the same parallelism problem
- **SPL-D07-002** — Schema inference; JDBC reads also support explicit schema via `.schema()`

---

### SPL-D07-007 — Parquet Compression Not Set

| | |
|---|---|
| **Dimension** | D07 I/O and File Formats |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Cluster-dependent codec; may write uncompressed on some environments |

**Description**

Writing Parquet without an explicit `.option("compression", ...)` relies on the cluster default
(`spark.sql.parquet.compression.codec`), which varies by Spark version and deployment
configuration. The code becomes non-deterministic across environments.

**What it detects**

A `.parquet(...)` write call that does not include `.option("compression", ...)` in the chain.

```python
# Triggers SPL-D07-007
df.write.mode("overwrite").parquet("s3://bucket/data")
```

**Why it matters**

In Spark 1.x the Parquet compression default was `uncompressed`; in Spark 2.0+ it changed to
`snappy`. Jobs migrated between clusters with different Spark versions silently change their
output file sizes. An uncompressed 500 GB file becomes a 166 GB snappy file (3× compression
typical), confusing downstream size-based monitoring. More practically: `snappy` offers good
read/write speed with moderate compression; `zstd` offers higher compression (roughly 2× better
than snappy) with comparable speed on modern hardware. `gzip` offers the best compression but is
not splittable within a file, limiting parallelism for large files.

**How to fix**

```python
# Wrong: codec depends on cluster configuration
df.write.mode("overwrite").parquet("s3://bucket/data")

# Right: explicit codec — deterministic behavior across all environments
df.write.mode("overwrite").option("compression", "snappy").parquet("s3://bucket/data")

# For better compression with similar read performance (Spark 3+):
df.write.mode("overwrite").option("compression", "zstd").parquet("s3://bucket/data")

# For maximum compatibility (e.g., reading with older tools):
df.write.mode("overwrite").option("compression", "gzip").parquet("s3://bucket/data")
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.parquet.compression.codec` | `snappy` | Sets the default; explicit option overrides this |

**Related rules**

- **SPL-D07-008** — Write mode not specified; both settings affect write determinism across environments
- **SPL-D07-009** — No format specified; format + compression + mode together define fully deterministic writes

---

### SPL-D07-008 — Write Mode Not Specified

| | |
|---|---|
| **Dimension** | D07 I/O and File Formats |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Job fails on re-run if output path exists; silent data duplication on 'append' |

**Description**

A `write` call does not include `.mode(...)`. Spark's default write mode is `"error"` (also
known as `"errorifexists"`), which raises `AnalysisException` if the output path already
exists. Production pipelines that need to refresh output must specify the mode explicitly.

**What it detects**

A write terminal (`.parquet()`, `.orc()`, `.csv()`, `.json()`, `.save()`, `.saveAsTable()`)
in a chain that does not include `.mode(...)`.

```python
# Triggers SPL-D07-008
df.write.parquet("s3://bucket/output")
```

**Why it matters**

The default `"error"` mode is intentionally conservative — it prevents accidental overwrites
during development. In production, however, virtually every pipeline re-runs to refresh data,
and a failed write due to an existing path produces a confusing `AnalysisException: path
already exists` that does not hint at the root cause. `"append"` mode is equally dangerous
without documentation: silently appending to an existing dataset on re-run creates duplicate
rows that downstream consumers cannot detect without row counts. Explicit mode declarations
make the pipeline's intent clear and prevent accidental data loss.

**How to fix**

```python
# Wrong: fails on re-run if output exists
df.write.parquet("s3://bucket/output")

# Right: explicit overwrite for refresh semantics
df.write.mode("overwrite").parquet("s3://bucket/output")

# For incremental append pipelines — document the intent with a comment:
df.write.mode("append").parquet("s3://bucket/output")  # idempotency handled by upstream dedup

# For Delta Lake tables: prefer 'overwrite' with replaceWhere for partial partition refresh:
df.write.format("delta").mode("overwrite").option("replaceWhere", "date = '2024-01-01'") \
    .save("s3://bucket/delta_table")
```

**Config options**

No Spark configuration changes the default write mode behavior.

**Related rules**

- **SPL-D07-007** — Parquet compression not set; both are write-determinism issues
- **SPL-D04-006** — Missing partitionBy; all three write options (mode, format, partitioning) should be set explicitly

---

### SPL-D07-009 — No Format Specified on Read/Write

| | |
|---|---|
| **Dimension** | D07 I/O and File Formats |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Format depends on cluster config; silent mismatches across environments |

**Description**

`spark.read.load("path")` or `df.write.save("path")` is called without `.format(...)`. These
generic methods rely on `spark.sql.sources.default` (default: `"parquet"` in Spark 2+), making
the actual format implicit and cluster-dependent.

**What it detects**

A `.load("path")` read or `.save("path")` write call that is not preceded by `.format(...)` in
the same chain.

```python
# Triggers SPL-D07-009
df = spark.read.load("path/")
df.write.save("output/")
```

**Why it matters**

`spark.sql.sources.default` can be overridden at the cluster level. A job that relies on the
default format reads Parquet in one environment and ORC in another — or reads nothing at all
when deployed to a cluster where the default is `"com.databricks.spark.csv"`. Format-specific
reader methods (`.parquet()`, `.orc()`, `.csv()`) are unambiguous: the format is encoded in
the source code, not in a runtime configuration value that may vary.

**How to fix**

```python
# Wrong: format inferred from spark.sql.sources.default
df = spark.read.load("path/")
df.write.save("output/")

# Right: explicit format — unambiguous across all environments
df = spark.read.parquet("path/")
df.write.mode("overwrite").parquet("output/")

# Or using the .format() fluent API:
df = spark.read.format("parquet").load("path/")
df.write.format("parquet").mode("overwrite").save("output/")
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.sources.default` | `parquet` (default Spark 2+); avoid relying on this |

**Related rules**

- **SPL-D07-008** — Write mode not specified; both rules address implicit write behavior
- **SPL-D07-007** — Parquet compression not set; use all three explicit write options together

---

### SPL-D07-010 — mergeSchema Enabled Without Necessity

| | |
|---|---|
| **Dimension** | D07 I/O and File Formats |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | O(files) metadata scan on every read; seconds to minutes for tables with many files |

**Description**

`option("mergeSchema", "true")` or `spark.sql.parquet.mergeSchema = true` is set. This
instructs Spark to read the schema footer from every Parquet file in the dataset on every
read — an O(files) metadata operation regardless of how much data is actually read.

**What it detects**

A reader with `.option("mergeSchema", "true")` or a SparkSession configured with
`spark.sql.parquet.mergeSchema = true`.

```python
# Triggers SPL-D07-010
df = spark.read.option("mergeSchema", "true").parquet("path/")
```

**Why it matters**

A Parquet dataset built over time may accumulate files with slightly different schemas (added
or removed columns). `mergeSchema=true` reconciles these differences by reading the footer
metadata from every file before returning a unified schema. For a table with 10,000 Parquet
files, this is 10,000 footer reads — each requiring a network round-trip to object storage —
on every single query, regardless of whether the schema has changed since the last read.
For tables where the schema is stable, this is pure overhead. If schema evolution is a
genuine requirement, Delta Lake handles it transparently without per-read overhead.

**How to fix**

```python
# Wrong: O(files) footer scan on every read
df = spark.read.option("mergeSchema", "true").parquet("path/")

# Right option 1: define schema explicitly and remove mergeSchema
from pyspark.sql.types import StructType, StructField, LongType, StringType
schema = StructType([
    StructField("id",   LongType(),   True),
    StructField("name", StringType(), True),
    StructField("ts",   LongType(),   True),
])
df = spark.read.schema(schema).parquet("path/")

# Right option 2: if schema evolution is required, use Delta Lake
df = spark.read.format("delta").load("delta_path/")  # schema tracked in transaction log
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.parquet.mergeSchema` | `false` (default); do not set to `true` globally |

**Related rules**

- **SPL-D07-002** — Schema inference; both trade schema safety for I/O cost; prefer explicit schemas for both
- **SPL-D07-001** — CSV/JSON format; migrating to Delta Lake also eliminates `mergeSchema` concerns

---

## D08 · Adaptive Query Execution

Adaptive Query Execution (AQE, enabled by default in Spark 3.2+) re-optimizes query plans at
runtime using shuffle statistics collected after each stage. Its three core features —
partition coalescing, skew join splitting, and dynamic join strategy switching — each require
specific configuration. These rules detect cases where AQE is disabled, misconfigured, or
working against itself.

---

### SPL-D08-001 — AQE Disabled

| | |
|---|---|
| **Dimension** | D08 AQE |
| **Severity** | CRITICAL |
| **Effort** | Config only |
| **Impact** | Loses automatic partition coalescing, skew handling, and join strategy adaptation |

**Description**

`spark.sql.adaptive.enabled` is explicitly set to `false`, disabling all three AQE
optimizations. In Spark 3.2+ AQE is enabled by default; an explicit `false` override
removes a significant class of automatic runtime optimizations with no compensating benefit.

**What it detects**

A `.config("spark.sql.adaptive.enabled", "false")` call.

```python
# Triggers SPL-D08-001
spark = SparkSession.builder.config("spark.sql.adaptive.enabled", "false").getOrCreate()
```

**Why it matters**

AQE operates in three phases after each shuffle stage completes, using the actual shuffle map
statistics: (1) **Partition coalescing** merges small output partitions into ones near the
advisory size, eliminating hundreds of tiny tasks; (2) **Skew join splitting** detects
partitions disproportionately larger than the median and runs multiple tasks against them,
eliminating straggler bottlenecks; (3) **Dynamic join strategy** upgrades a sort-merge join
to a broadcast hash join at runtime when the actual smaller side is below the broadcast
threshold — even if the planner estimated it would be large. All three optimizations apply
at zero developer cost. AQE was disabled by default only in Spark 3.0 and 3.1 due to
maturity concerns; it is stable and on-by-default since Spark 3.2.

**How to fix**

```python
# Wrong: disables all AQE optimizations
spark = SparkSession.builder.config("spark.sql.adaptive.enabled", "false").getOrCreate()

# Right: remove the override entirely (Spark 3.2+ default is true)
spark = SparkSession.builder.getOrCreate()

# Or set explicitly if running on Spark 3.0/3.1:
spark = SparkSession.builder.config("spark.sql.adaptive.enabled", "true").getOrCreate()
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` (default Spark 3.2+) | Remove the `false` override |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` (default) | Partition coalescing sub-feature |
| `spark.sql.adaptive.skewJoin.enabled` | `true` (default) | Skew join sub-feature |

**Related rules**

- **SPL-D08-002** — AQE coalesce partitions disabled; check that sub-features are also enabled
- **SPL-D05-003** — AQE skew join handling disabled; specific sub-feature disable
- **SPL-D01-010** — Default shuffle partitions; AQE mitigates a wrong partition count via coalescing

---

### SPL-D08-002 — AQE Coalesce Partitions Disabled

| | |
|---|---|
| **Dimension** | D08 AQE |
| **Severity** | WARNING |
| **Effort** | Config only |
| **Impact** | Hundreds of tiny tasks per stage; driver scheduling overhead |

**Description**

`spark.sql.adaptive.coalescePartitions.enabled` is explicitly set to `false`. This disables the
AQE sub-feature that merges small post-shuffle partitions, forcing every stage to run with the
full `spark.sql.shuffle.partitions` count regardless of actual data volume.

**What it detects**

A `.config("spark.sql.adaptive.coalescePartitions.enabled", "false")` call.

```python
# Triggers SPL-D08-002
spark = SparkSession.builder \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .getOrCreate()
```

**Why it matters**

After a shuffle, AQE examines the actual size of each output partition. When consecutive
partitions are each smaller than `advisoryPartitionSizeInBytes` (default 64 MB), AQE merges
them into a single partition and assigns them to one task. Without coalescing, a 10 GB dataset
with `shuffle.partitions = 200` produces 200 tasks of ~50 MB each — harmless but slightly
inefficient. A 100 MB dataset with the same setting produces 200 tasks of 500 KB each, with
task-startup overhead dominating actual computation time. The driver must schedule, track, and
clean up 200 tasks when 2 would suffice.

**How to fix**

```python
# Wrong: disables partition coalescing
spark = SparkSession.builder \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .getOrCreate()

# Right: remove the override — coalescing is enabled by default with AQE
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` (default) | Remove the `false` override |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `67108864` (64 MB, default) | Target size after coalescing |
| `spark.sql.adaptive.coalescePartitions.minPartitionNum` | `1` | Minimum partitions after coalescing |

**Related rules**

- **SPL-D08-001** — AQE disabled; coalescing requires AQE to be enabled
- **SPL-D08-003** — AQE advisory partition size too small; complementary tuning of the coalesce target size
- **SPL-D01-010** — Default shuffle partitions; AQE coalescing compensates for over-partitioning

---

### SPL-D08-003 — AQE Advisory Partition Size Too Small

| | |
|---|---|
| **Dimension** | D08 AQE |
| **Severity** | INFO |
| **Effort** | Config only |
| **Impact** | Too many small post-coalesce partitions; task scheduling overhead |

**Description**

`spark.sql.adaptive.advisoryPartitionSizeInBytes` is set below 16 MB. AQE coalesces partitions
to reach this target size; too small a target produces many small post-coalesce partitions that
incur task-scheduling overhead without meaningful parallelism benefit.

**What it detects**

A `.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", X)` call where `X` parses to
fewer than 16 MB.

```python
# Triggers SPL-D08-003 (4 MB << 64 MB recommended)
spark = SparkSession.builder \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "4m") \
    .getOrCreate()
```

**Why it matters**

`advisoryPartitionSizeInBytes` is the target size AQE aims for when merging adjacent shuffle
output partitions. Setting it very low (e.g., 4 MB) means AQE coalesces fewer partitions than
it could — the result is many 4 MB tasks rather than fewer 64 MB tasks. For a 10 GB post-shuffle
dataset: at 4 MB target → 2,500 tasks; at 64 MB target → 156 tasks. The 2,500-task version
spends proportionally more time on task scheduling, heartbeat processing, and result
serialization overhead than the 156-task version, with no parallelism advantage on a cluster
with fewer than 2,500 cores.

**How to fix**

```python
# Wrong: 4 MB target produces excessive task counts
spark = SparkSession.builder \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "4m") \
    .getOrCreate()

# Right: 64–128 MB target balances parallelism and overhead
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "67108864")  # 64 MB
    .getOrCreate()
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `67108864`–`134217728` (64–128 MB) | Target post-coalesce partition size |

**Related rules**

- **SPL-D08-002** — AQE coalesce partitions disabled; set the target size only after enabling coalescing
- **SPL-D08-007** — High shuffle partitions with AQE; use `advisoryPartitionSizeInBytes` instead of a high initial count

---

### SPL-D08-004 — AQE Skew Join Disabled With Skew-Prone Joins Detected

| | |
|---|---|
| **Dimension** | D08 AQE |
| **Severity** | WARNING |
| **Effort** | Config only |
| **Impact** | Skewed partitions on joins not auto-split; straggler tasks persist |

**Description**

`spark.sql.adaptive.skewJoin.enabled` is set to `false` while the file contains `join()` calls
on columns that commonly produce skewed output (columns matching low-cardinality or power-law
naming patterns). The combination means skew will manifest but AQE will not mitigate it.

**What it detects**

A `.config("spark.sql.adaptive.skewJoin.enabled", "false")` call in a file that also contains
join operations on skew-prone column names.

```python
# Triggers SPL-D08-004
spark = SparkSession.builder \
    .config("spark.sql.adaptive.skewJoin.enabled", "false") \
    .getOrCreate()
# ...
df.join(events, "user_id")   # user_id is skew-prone; AQE skew join disabled
```

**Why it matters**

AQE's skew join works by monitoring partition sizes after the shuffle map phase. When a
partition exceeds both `skewedPartitionThresholdInBytes` (default 256 MB) and
`skewedPartitionFactor × median` (default 5×), AQE splits that partition into multiple
sub-partitions and runs one task per sub-partition, effectively distributing the skewed data
across multiple executors. Disabling this feature in a job that has skew-prone join keys means
every execution will have at least one straggler task processing a disproportionately large
partition while all other tasks sit idle.

**How to fix**

```python
# Wrong: skew join disabled with known skew-prone join key
spark = SparkSession.builder \
    .config("spark.sql.adaptive.skewJoin.enabled", "false") \
    .getOrCreate()
df.join(events, "user_id")

# Right: remove the override — skew join is enabled by default with AQE
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
df.join(events, "user_id")  # AQE now auto-splits skewed user_id partitions
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.adaptive.skewJoin.enabled` | `true` (default with AQE) | Remove the `false` override |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `268435456` (256 MB) | Lower = more aggressive split detection |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` | Partition is skewed if > factor × median |

**Related rules**

- **SPL-D05-003** — AQE skew join handling disabled; D05 counterpart covering the same config
- **SPL-D08-001** — AQE disabled; skew join requires AQE to be enabled
- **SPL-D05-001** — Join on low-cardinality column; the code-side pattern this rule pairs with

---

### SPL-D08-005 — AQE Skew Factor Too Aggressive

| | |
|---|---|
| **Dimension** | D08 AQE |
| **Severity** | INFO |
| **Effort** | Config only |
| **Impact** | Excessive partition splits for balanced data; unnecessary task count increase |

**Description**

`spark.sql.adaptive.skewJoin.skewedPartitionFactor` is set below 2. A very low factor means
nearly every partition that is above the median size is treated as skewed and split, even for
naturally balanced data with moderate variance.

**What it detects**

A `.config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", X)` call where `X` is a
numeric literal less than 2.

```python
# Triggers SPL-D08-005 (factor=1 means any partition > median is "skewed")
spark = SparkSession.builder \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "1") \
    .getOrCreate()
```

**Why it matters**

AQE's skew detection uses two thresholds jointly. A partition is skewed only when it is **both**
above `skewedPartitionThresholdInBytes` AND above `skewedPartitionFactor × median size`.
Setting the factor to 1 means any partition larger than the median is considered skewed —
including partitions that are only 10% above the median due to natural data variance. AQE then
splits those partitions unnecessarily, increasing task count and scheduling overhead for data
that was already well-balanced. The default of 5 (a partition must be 5× the median) is
calibrated to distinguish genuine outliers from normal variance.

**How to fix**

```python
# Wrong: factor=1 splits nearly every above-average partition
spark = SparkSession.builder \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "1") \
    .getOrCreate()

# Right: use the default (5) or at least 2
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")  # default
    .getOrCreate()
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` (default) | Set to at least 2; use default unless tuning for specific workload |

**Related rules**

- **SPL-D05-004** — AQE skew threshold too high; the complementary threshold (size threshold vs. factor threshold)
- **SPL-D08-004** — AQE skew join disabled; ensure skew join is enabled before tuning the factor

---

### SPL-D08-006 — AQE Local Shuffle Reader Disabled

| | |
|---|---|
| **Dimension** | D08 AQE |
| **Severity** | INFO |
| **Effort** | Config only |
| **Impact** | Unnecessary remote shuffle reads; extra network I/O even for locally available data |

**Description**

`spark.sql.adaptive.localShuffleReader.enabled` is set to `false`. This disables the AQE
optimization that reads shuffle data locally when all required shuffle blocks are on the same
executor, eliminating unnecessary cross-node network transfers for those tasks.

**What it detects**

A `.config("spark.sql.adaptive.localShuffleReader.enabled", "false")` call.

```python
# Triggers SPL-D08-006
spark = SparkSession.builder \
    .config("spark.sql.adaptive.localShuffleReader.enabled", "false") \
    .getOrCreate()
```

**Why it matters**

After a shuffle, each reducer task fetches its partition data from multiple shuffle map output
files, potentially from many different executors (remote reads). The local shuffle reader
optimization detects cases where all the shuffle blocks needed by a task happen to be on the
same executor (e.g., after a broadcast join or when shuffle.partitions is small relative to
executors). In those cases it reads the data locally from disk without going over the network.
Disabling this optimization forces remote network reads even when the data is sitting on the
local disk of the same JVM, adding unnecessary network latency and congestion.

**How to fix**

```python
# Wrong: forces remote shuffle reads even when data is local
spark = SparkSession.builder \
    .config("spark.sql.adaptive.localShuffleReader.enabled", "false") \
    .getOrCreate()

# Right: remove the override — local shuffle reader is safe and enabled by default
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.adaptive.localShuffleReader.enabled` | `true` (default with AQE) | Remove the `false` override |

**Related rules**

- **SPL-D08-001** — AQE disabled; local shuffle reader requires AQE enabled
- **SPL-D08-002** — AQE coalesce partitions disabled; coalescing increases the likelihood that local reads apply

---

### SPL-D08-007 — Manual Shuffle Partition Count Set High With AQE Enabled

| | |
|---|---|
| **Dimension** | D08 AQE |
| **Severity** | INFO |
| **Effort** | Config only |
| **Impact** | Unnecessary map-side overhead proportional to initial partition count |

**Description**

`spark.sql.shuffle.partitions` is set above 400 while AQE is enabled. With AQE on, the initial
partition count is the maximum from which AQE coalesces downward — setting it very high
imposes unnecessary map-side overhead with no benefit over using a moderate count plus a
well-tuned `advisoryPartitionSizeInBytes`.

**What it detects**

A `.config("spark.sql.shuffle.partitions", N)` call where `N > 400` in a file that also sets
`spark.sql.adaptive.enabled = true` (or where AQE is inferred to be on).

```python
# Triggers SPL-D08-007
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "2000") \
    .getOrCreate()
```

**Why it matters**

`spark.sql.shuffle.partitions` controls how many shuffle map-side output files are created.
Each map task writes one buffer per reducer partition; with 2,000 reducers that is 2,000 open
file handles per map task. Even though AQE will merge most of the 2,000 small post-shuffle
partitions down to a manageable number of reduce tasks, the map-side cost is already paid:
2,000 buffers allocated, 2,000 file handles opened, 2,000 files written per map task. Setting
`shuffle.partitions = 200` and `advisoryPartitionSizeInBytes = 128m` achieves the same
post-coalesce partition count with 10× fewer map-side files.

**How to fix**

```python
# Wrong: high initial count wastes map-side resources even with AQE coalescing
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "2000") \
    .getOrCreate()

# Right: moderate initial count + advisory size lets AQE tune to actual data
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200")   # initial; AQE adjusts from here
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")  # 128 MB target
    .getOrCreate()
```

**Config options**

| Spark Config | Recommended Value | Notes |
|---|---|---|
| `spark.sql.shuffle.partitions` | `200`–`400` when AQE is enabled | AQE coalesces down; a high initial count wastes map-side I/O |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `67108864`–`134217728` | Target size; drives AQE coalescing decision |

**Related rules**

- **SPL-D08-003** — AQE advisory partition size too small; both rules tune the coalescing outcome
- **SPL-D01-010** — Default shuffle partitions unchanged; with AQE on, focus on advisory size rather than initial count

---

## D09 · UDF and Code Patterns

Python UDFs and driver-side iteration are the most impactful code-level anti-patterns in PySpark.
A single `@udf` can slow a pipeline 10× relative to native functions; a `collect()` without
`limit()` can crash the driver entirely. These rules cover the full spectrum from serialization
overhead to query-plan explosion.

---

### SPL-D09-001 — Python UDF (Row-at-a-Time) Detected

| | |
|---|---|
| **Dimension** | D09 UDF and Code Patterns |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | 2–10× slowdown per row; blocks Catalyst code-generation pipeline |

**Description**

A `@udf`-decorated function (or `udf(lambda ...)`) is detected. Row-at-a-time Python UDFs
serialize every row from the JVM to a Python worker process and back, bypassing Catalyst's
whole-stage code generation and incurring a per-row IPC round-trip.

**What it detects**

Any function decorated with `@udf` or `@udf(returnType=...)`, or a `udf(lambda ...)` call.

```python
# Triggers SPL-D09-001
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def normalise(s):
    return s.lower().strip() if s else None
```

**Why it matters**

For every row in every partition, a row-at-a-time UDF forces Spark to: (1) serialize the row's
relevant columns from JVM internal binary format to Python pickle; (2) send the bytes over a
local socket to a Python worker subprocess; (3) deserialize and execute the Python function;
(4) serialize the result back to pickle; (5) send it back over the socket; (6) deserialize it
into JVM format. This round-trip takes roughly 1–10 µs per row. At 100 million rows per
partition, that is 100–1,000 seconds just in serialization overhead, before any actual
computation. It also prevents Catalyst's whole-stage code generation — the JIT-compiled
bytecode path that runs native Spark operations at near-JVM speed.

**How to fix**

```python
# Wrong: row-at-a-time UDF — full JVM↔Python round-trip per row
@udf(returnType=StringType())
def normalise(s):
    return s.lower().strip() if s else None

df.withColumn("normalised", normalise(col("name")))

# Right option 1: native Spark function — no serialization at all
from pyspark.sql.functions import lower, trim
df.withColumn("normalised", lower(trim(col("name"))))

# Right option 2: pandas_udf — vectorized; processes a batch of rows as pd.Series
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("string")
def normalise_batch(s: pd.Series) -> pd.Series:
    return s.str.lower().str.strip()

df.withColumn("normalised", normalise_batch(col("name")))
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.python.worker.memory` | See SPL-D01-007; size appropriately for UDF memory footprint |
| `spark.sql.execution.arrow.pyspark.enabled` | `true` — required for `@pandas_udf` Arrow-based batching |

**Related rules**

- **SPL-D09-002** — UDF replaceable with native function; check if the UDF body has a direct Spark SQL equivalent
- **SPL-D10-001** — UDF blocks predicate pushdown; UDFs create opaque Catalyst boundaries
- **SPL-D01-007** — Missing PySpark worker memory; Python workers need adequate memory headroom

---

### SPL-D09-002 — Python UDF Replaceable With Native Spark Function

| | |
|---|---|
| **Dimension** | D09 UDF and Code Patterns |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Per-row JVM↔Python serialization for a zero-logic transformation |

**Description**

The UDF body consists of a single call to a Python string method (`lower`, `upper`, `strip`,
`replace`, etc.) that has a direct Spark SQL built-in equivalent. The UDF adds full Python
serialization overhead for a transformation that Catalyst can execute natively.

**What it detects**

A `@udf` function whose body is a single expression applying a Python string method that maps
directly to a Spark SQL function (`lower`, `upper`, `strip`/`trim`, `replace`, `len`/`length`).

```python
# Triggers SPL-D09-002 — entire body is s.lower(), which maps to lower(col)
@udf(returnType=StringType())
def my_lower(s):
    return s.lower()

df.withColumn("name_lower", my_lower(col("name")))
```

**Why it matters**

This is the most wasteful form of Python UDF: the UDF performs zero logic beyond calling a
function that already exists as a Spark SQL built-in. The full JVM→Python→JVM serialization
round-trip per row applies — at 100 million rows that is potentially hundreds of seconds of
pure overhead — for a transformation that the native function executes in a JIT-compiled
loop inside the JVM with no serialization at all.

**How to fix**

```python
from pyspark.sql.functions import lower, upper, trim, length, regexp_replace

# Wrong: UDF wrapping a built-in — full serialization per row
@udf(returnType=StringType())
def my_lower(s): return s.lower()
df.withColumn("n", my_lower(col("name")))

# Right: direct native function — zero serialization
df.withColumn("n", lower(col("name")))

# Common UDF → native function replacements:
# s.lower()         → lower(col)
# s.upper()         → upper(col)
# s.strip()         → trim(col)
# s.replace(a, b)   → regexp_replace(col, a, b)
# len(s)            → length(col)
# s[:n]             → col.substr(1, n)
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D09-001** — Row-at-a-time UDF; even complex UDFs should be replaced with `@pandas_udf` if possible
- **SPL-D10-001** — UDF blocks predicate pushdown; eliminating UDFs restores full Catalyst optimization

---

### SPL-D09-003 — withColumn() Inside Loop

| | |
|---|---|
| **Dimension** | D09 UDF and Code Patterns |
| **Severity** | CRITICAL |
| **Effort** | Minor code change |
| **Impact** | O(N²) plan building time; driver hangs for N > 100 columns |

**Description**

`withColumn()` is called inside a `for` or `while` loop. Each call adds a `Project` node to
the logical plan, wrapping all previous nodes. For N loop iterations the plan has N nested
projections — analysis time grows quadratically and can hang the driver for large N.

**What it detects**

A `withColumn()` call that is nested inside a loop body.

```python
# Triggers SPL-D09-003
for col_name in column_list:           # 500 columns → O(500²) = 250,000 plan ops
    df = df.withColumn(col_name, compute(col(col_name)))
```

**Why it matters**

Spark's `withColumn()` does not mutate the DataFrame — it creates a new `Project` logical plan
node that wraps the previous plan. After N iterations the plan is a chain of N `Project` nodes,
each referencing the previous. Catalyst's analysis phase must traverse this chain for every
column in every rule pass. With 500 columns, each rule pass traverses 500 nodes × each of the
~50 analysis rules = 25,000 traversal operations, and the plan contains 500 nested `Project`
nodes. Observed behavior: driver CPU at 100%, job appears to hang at the action that triggers
analysis, sometimes for tens of minutes, before eventually raising `StackOverflowError` for
very large N.

**How to fix**

```python
from pyspark.sql.functions import col

# Wrong: O(N²) plan growth
for col_name in column_list:
    df = df.withColumn(col_name, compute(col(col_name)))

# Right: build all expressions first, apply in a single select()
existing = [col(c) for c in df.columns]
new_cols = [compute(col(c)).alias(c) for c in column_list]
df = df.select(*existing, *new_cols)

# Alternatively, for adding computed columns alongside originals:
df = df.select(
    "*",                                          # retain all existing columns
    *[compute(col(c)).alias(f"{c}_computed") for c in column_list]
)
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D03-008** — Join inside loop; the same loop + Spark operation anti-pattern for joins
- **SPL-D10-006** — Deep method chain; `withColumn` loops produce the deepest chains

---

### SPL-D09-004 — Row-by-Row Iteration Over DataFrame

| | |
|---|---|
| **Dimension** | D09 UDF and Code Patterns |
| **Severity** | CRITICAL |
| **Effort** | Major refactor |
| **Impact** | Driver OOM; entire cluster idle while driver processes rows sequentially |

**Description**

The code iterates over a Spark DataFrame via `df.collect()`, `df.toLocalIterator()`, or direct
DataFrame iteration in a Python `for` loop. This pulls all data to the driver and processes
it one row at a time, eliminating all distributed parallelism.

**What it detects**

A Python `for` loop where the iterable is a `collect()` call, `toLocalIterator()` call, or a
DataFrame variable directly (which implicitly calls `toLocalIterator()`).

```python
# Triggers SPL-D09-004
for row in df.collect():
    process(row)

for row in df.toLocalIterator():
    write_to_db(row)
```

**Why it matters**

`df.collect()` materializes the entire DataFrame as a Python list on the driver, transferring
every row from every executor over the network. While that transfer is happening, all executors
are idle — their work is done and they sit waiting for the next action. The driver then
processes rows one at a time in a serial Python loop. A 100 GB dataset collected to the driver
requires 100 GB of driver heap (driver OOM for any realistic heap size), and the serial loop
runs at single-thread Python speed rather than distributed executor speed. Even if the driver
has enough memory, this pattern typically runs 100–1,000× slower than the distributed alternative.

**How to fix**

```python
# Wrong: all data to driver; serial processing
for row in df.collect():
    process(row)

# Right option 1: push processing to executors with foreach/foreachPartition
df.foreach(process)
df.foreachPartition(lambda rows: [process(r) for r in rows])

# Right option 2: use DataFrame transformations to compute the result
result = df.groupBy("key").agg(compute_agg("value"))

# Right option 3: for writing to an external system
df.write.format("jdbc").option("url", ...).save()  # parallel write via executor tasks
```

**Config options**

No Spark configuration mitigates this pattern.

**Related rules**

- **SPL-D09-005** — collect() without prior filter; the same driver-collection risk
- **SPL-D09-006** — toPandas() on large DataFrame; equivalent driver-memory risk via pandas

---

### SPL-D09-005 — .collect() Without Prior Filter or Limit

| | |
|---|---|
| **Dimension** | D09 UDF and Code Patterns |
| **Severity** | CRITICAL |
| **Effort** | Minor code change |
| **Impact** | Driver OOM for datasets > available driver heap; job failure |

**Description**

`.collect()` is called without a preceding `filter()`, `where()`, or `limit()`. An unbounded
`collect()` pulls the entire DataFrame — potentially terabytes — into driver heap as a Python
list.

**What it detects**

A `.collect()` call where no `filter()`, `where()`, or `limit()` appears in the same method
chain or within 3 preceding lines.

```python
# Triggers SPL-D09-005
rows = df.join(other, "id").collect()          # unbounded — may OOM driver
all_data = spark.read.parquet("s3://...").collect()
```

**Why it matters**

`collect()` is a driver action: it requests that every executor serialize its partitions and
send them to the driver, where they are assembled into a single Python list. The driver must
hold the full result in heap simultaneously — for a 100 GB DataFrame that requires 100 GB of
driver heap (plus Python list overhead, typically 2–5× the raw data size). The default driver
heap is 1 GB (or whatever `spark.driver.memory` is set to). A driver OOM crash terminates the
entire application and wastes all executor work already completed. Legitimate uses of
`collect()` are confined to small bounded result sets: configuration lookups, distinct values
of a low-cardinality column, or the output of an aggregation with known small output.

**How to fix**

```python
# Wrong: unbounded collect — driver OOM for large DataFrames
rows = df.join(other, "id").collect()

# Right option 1: add limit() for bounded samples
rows = df.join(other, "id").limit(1000).collect()

# Right option 2: write to storage instead of collecting to driver
df.join(other, "id").write.mode("overwrite").parquet("output/")

# Right option 3: use first()/take() for single rows or small samples
first_row = df.first()
sample = df.take(10)

# Legitimate collect() patterns (small known-bounded results):
config_values = spark.table("config").collect()       # small reference table
distinct_dates = df.select("date").distinct().collect()  # O(dates), not O(rows)
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.driver.maxResultSize` | Hard limit on total collect() result size (default 1 GB); raise with caution |

**Related rules**

- **SPL-D09-004** — Row-by-row iteration; often co-occurs with collect()
- **SPL-D09-006** — toPandas() on large DataFrame; identical driver-OOM risk via pandas path
- **SPL-D01-003** — Driver memory not configured; undersized driver heap makes this risk acute

---

### SPL-D09-006 — .toPandas() on Potentially Large DataFrame

| | |
|---|---|
| **Dimension** | D09 UDF and Code Patterns |
| **Severity** | CRITICAL |
| **Effort** | Minor code change |
| **Impact** | Driver OOM proportional to total dataset size; entire data in one process |

**Description**

`.toPandas()` is called without a preceding `filter()`, `where()`, or `limit()`. Like
`collect()`, `toPandas()` transfers the entire DataFrame to the driver — but also incurs
additional pandas object-creation overhead on top of the collection cost.

**What it detects**

A `.toPandas()` call where no `filter()`, `where()`, or `limit()` appears in the same method
chain or within 3 preceding lines.

```python
# Triggers SPL-D09-006
pdf = df.join(other, "id").toPandas()   # entire join result to driver as pandas DataFrame
```

**Why it matters**

`toPandas()` is equivalent to `collect()` followed by `pandas.DataFrame(rows)`. Every row
crosses the network from executors to the driver, where it is assembled into a pandas
`DataFrame`. The pandas representation has significantly higher memory overhead than raw data:
each Python string is a separate heap object; numeric columns require boxing when null values
are present. A 10 GB Spark DataFrame can require 30–50 GB of driver heap after `toPandas()`.
For visualization or reporting use cases, the correct pattern is to aggregate the data
distributed first (summarize, sample, or limit) and call `toPandas()` only on the small result.

**How to fix**

```python
# Wrong: entire DataFrame to driver memory
pdf = df.join(other, "id").toPandas()

# Right option 1: limit before collecting for visualization/reporting
pdf = df.join(other, "id").limit(50_000).toPandas()

# Right option 2: aggregate first, then collect the small summary
pdf = (
    df.join(other, "id")
    .groupBy("category")
    .agg({"revenue": "sum", "orders": "count"})
    .toPandas()                  # small aggregated result; safe to collect
)

# Right option 3: for full dataset processing, use pyspark.pandas (Koalas API)
import pyspark.pandas as ps
psdf = ps.read_parquet("path/")  # distributed pandas-like API; no driver collection
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.execution.arrow.pyspark.enabled` | `true` — enables Arrow-based `toPandas()`, which is 10× faster and uses less memory |

**Related rules**

- **SPL-D09-005** — collect() without prior limit; same root cause, different method
- **SPL-D01-003** — Driver memory not configured; undersized driver heap makes toPandas() risk acute

---

### SPL-D09-007 — .count() Used for Emptiness Check

| | |
|---|---|
| **Dimension** | D09 UDF and Code Patterns |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Full table scan to answer a yes/no question; O(N) instead of O(1) |

**Description**

`df.count()` is compared to `0` to check whether a DataFrame is empty. `count()` scans every
row in every partition to return a count — an O(N) distributed operation — when a single-row
short-circuit check is all that is needed.

**What it detects**

A `count()` call whose result is compared to `0` (e.g., `df.count() == 0`,
`df.count() > 0`, `df.count() != 0`).

```python
# Triggers SPL-D09-007
if df.count() == 0:
    raise ValueError("No data found for the given date range")

if df.count() > 0:
    df.write.mode("overwrite").parquet("output/")
```

**Why it matters**

`count()` schedules a full distributed job across all executors: each executor counts its
partition's rows, then the driver sums all partial counts. For a 1 billion-row DataFrame this
takes tens of seconds. `df.isEmpty` (Spark 3.3+) or `df.limit(1).count() == 0` reads at most
one row per partition (with `limit(1)`) and short-circuits as soon as any non-empty partition
is found — O(1) in the best case. The performance difference is especially significant when the
emptiness check is in a frequently-executed code path or a streaming micro-batch.

**How to fix**

```python
# Wrong: full table scan to answer yes/no
if df.count() == 0:
    raise ValueError("No data found")

# Right option 1: df.isEmpty (Spark 3.3+) — short-circuits on first row
if df.isEmpty:
    raise ValueError("No data found")

# Right option 2: df.limit(1).count() — reads at most 1 row; works in all Spark 3.x
if df.limit(1).count() == 0:
    raise ValueError("No data found")

# For existence check with a filter:
if df.filter("status = 'error'").isEmpty:
    logger.info("No errors found")
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D09-005** — collect() without limit; both involve triggering expensive actions for simple checks
- **SPL-D11-003** — No metrics logging; `count()` for logging row counts is a legitimate use of count()

---

### SPL-D09-008 — .show() in Production Code

| | |
|---|---|
| **Dimension** | D09 UDF and Code Patterns |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Wasted query execution; output lost in production log redirection |

**Description**

`df.show()` is called in code that is not inside a test or notebook. In production pipelines,
`show()` triggers a materialisation, collects rows to the driver, and prints them to `stdout` —
where the output is typically swallowed by log aggregators.

**What it detects**

Any `.show()` call.

```python
# Triggers SPL-D09-008
df.groupBy("status").count().show()
df.show(100, truncate=False)
```

**Why it matters**

`df.show(n)` is equivalent to `df.limit(n).collect()` plus a formatted print to `stdout`. In
production, `stdout` is typically redirected to a log file or a log aggregation system (e.g.,
CloudWatch, Stackdriver) where tables formatted with box-drawing characters are unreadable. The
compute cost of the underlying `collect()` is real — it triggers a complete upstream query
execution. `show()` in a loop or in a frequently-called function multiplies this cost. The
correct pattern for production observability is structured logging with `logger.info()` after a
bounded `collect()`.

**How to fix**

```python
# Wrong: stdout output lost in production; wasted query execution
df.groupBy("status").count().show()

# Right: structured logging after bounded collect
result = df.groupBy("status").count()
logger.info("Status distribution: %s", result.collect())

# Or use df.first() for a single-row sanity check:
logger.debug("Sample row: %s", df.first())
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D09-009** — explain()/printSchema() in production; the same debugging-in-production anti-pattern
- **SPL-D11-003** — No metrics logging; `show()` is sometimes a proxy for missing structured logging

---

### SPL-D09-009 — .explain() or .printSchema() in Production Code

| | |
|---|---|
| **Dimension** | D09 UDF and Code Patterns |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Debugging output in production logs; extra driver analysis overhead |

**Description**

`df.explain()` or `df.printSchema()` is called. These are debugging tools that print query
plan or schema information to `stdout`. In production their output is lost in log files, and
`explain()` adds unnecessary plan-analysis work on the driver.

**What it detects**

Any `.explain()` or `.printSchema()` call.

```python
# Triggers SPL-D09-009
df.join(other, "id").explain(True)
df.printSchema()
```

**Why it matters**

`df.explain()` forces Catalyst to fully analyze and optimize the query plan (triggering analysis
that would not otherwise happen until an action), then serializes the plan to a string and
prints it to `stdout`. In production this is: (1) extra driver-side CPU for plan analysis,
(2) unstructured output in log files where it is invisible or confusing, and (3) a sign that
debugging code was not cleaned up before deployment. `df.printSchema()` similarly emits schema
information that is only useful during development. If query plan debugging is needed in
production (e.g., for automated regression detection), capture the output via the JVM API and
log it at `DEBUG` level.

**How to fix**

```python
# Wrong: debug output in production pipeline
df.join(other, "id").explain(True)
df.printSchema()

# Right: remove entirely before production deployment
# Or, if plan validation in production is intentional:
import logging
logger = logging.getLogger(__name__)
plan_str = df._jdf.queryExecution().explainString("formatted")
logger.debug("Query plan: %s", plan_str)

# For automated plan quality checks in tests (not production):
def test_uses_broadcast_join(df, other):
    result = df.join(broadcast(other), "id")
    plan = result._jdf.queryExecution().explainString("formatted")
    assert "BroadcastHashJoin" in plan
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D09-008** — show() in production; same debugging-in-production category
- **SPL-D11-001** — No explain() for plan validation in test file; `explain()` belongs in tests, not production

---

### SPL-D09-010 — .rdd Conversion Dropping Out of DataFrame API

| | |
|---|---|
| **Dimension** | D09 UDF and Code Patterns |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Columnar→row deserialization; disables Catalyst and Tungsten optimizations |

**Description**

`.rdd` is accessed on a DataFrame, converting it to an RDD of `Row` objects. This forces full
deserialization of the columnar binary format into Python `Row` objects, bypassing Catalyst's
query optimization and Tungsten's whole-stage code generation.

**What it detects**

Any access to `.rdd` on a DataFrame (detected as a `.rdd` attribute access).

```python
# Triggers SPL-D09-010
result = df.rdd.map(lambda r: (r["id"], r["val"] * 2)).toDF()
counts = df.rdd.flatMap(lambda r: r["tags"]).countByValue()
```

**Why it matters**

Spark DataFrames keep data in Tungsten's off-heap binary columnar format throughout their
lifecycle, enabling JIT-compiled operators, SIMD-vectorized aggregations, and Catalyst
expression code generation — all without any Python involvement. Accessing `.rdd` forces a
full deserialization pass: every partition is converted from Tungsten binary rows to Python
`Row` objects (or to JVM `InternalRow` objects for the Java/Scala RDD path). This conversion
costs CPU, memory, and completely bypasses all Catalyst optimizations applied to the downstream
RDD operations. The resulting RDD runs at JVM-generic-object speed rather than Tungsten-optimized
speed, typically 3–10× slower for CPU-bound operations.

**How to fix**

```python
# Wrong: forces Tungsten→Row deserialization; exits optimized execution path
result = df.rdd.map(lambda r: (r["id"], r["val"] * 2)).toDF()

# Right: native DataFrame expression — stays in Tungsten binary format
result = df.select(col("id"), (col("val") * 2).alias("val"))

# For complex logic that genuinely requires per-row Python processing:
# Use mapInPandas (processes whole partitions as DataFrames via Arrow)
def transform(iterator):
    for pdf in iterator:
        pdf["val"] = pdf["val"] * 2
        yield pdf
result = df.mapInPandas(transform, schema=df.schema)

# For UDF-like transformations: use @pandas_udf for vectorized processing
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D09-001** — Row-at-a-time UDF; `.rdd.map(lambda)` is essentially a manual UDF without the `@udf` wrapper
- **SPL-D10-001** — UDF blocks predicate pushdown; `.rdd` access has the same Catalyst boundary effect

---

### SPL-D09-011 — pandas_udf Without Type Annotations

| | |
|---|---|
| **Dimension** | D09 UDF and Code Patterns |
| **Severity** | INFO |
| **Effort** | Minor code change |
| **Impact** | Potential type inference errors; deprecated legacy API usage |

**Description**

A `@pandas_udf`-decorated function lacks Python type annotations on its parameters and/or
return type. Spark 3.0+ uses type hints to determine the vectorization mode; without them
Spark falls back to legacy behavior that may not vectorize correctly.

**What it detects**

A `@pandas_udf` function whose parameters lack `pd.Series` / `pd.DataFrame` type annotations.

```python
# Triggers SPL-D09-011 — no type annotations
@pandas_udf(returnType=DoubleType())
def my_udf(col):
    return col * 2
```

**Why it matters**

Spark 3.0 introduced the type-annotation-based `@pandas_udf` API as the recommended style,
replacing the legacy `pandas_udf(f, returnType, functionType)` three-argument form. Without
type hints, Spark uses the legacy `functionType` inference logic — which, for ambiguous cases,
may select the wrong execution mode (`SCALAR` vs `SCALAR_ITER` vs `GROUPED_MAP`), causing
incorrect output or a runtime error. Type annotations also serve as documentation, making the
function's expected input/output types explicit for both humans and static type checkers.

**How to fix**

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

# Wrong: no type annotations — legacy API; may infer wrong execution mode
@pandas_udf(returnType=DoubleType())
def my_udf(col):
    return col * 2

# Right: explicit pd.Series type hints — unambiguous vectorization mode
@pandas_udf("double")
def my_udf(col: pd.Series) -> pd.Series:
    return col * 2

# For multi-column pandas UDFs:
@pandas_udf("double")
def weighted_avg(values: pd.Series, weights: pd.Series) -> pd.Series:
    return values * weights / weights.sum()
```

**Config options**

| Spark Config | Notes |
|---|---|
| `spark.sql.execution.arrow.pyspark.enabled` | `true` — required for Arrow-based pandas UDF serialization |

**Related rules**

- **SPL-D09-001** — Row-at-a-time UDF; `@pandas_udf` is the recommended replacement
- **SPL-D09-012** — Nested UDF calls; type-annotated pandas UDFs also need clean composition

---

### SPL-D09-012 — Nested UDF Calls

| | |
|---|---|
| **Dimension** | D09 UDF and Code Patterns |
| **Severity** | WARNING |
| **Effort** | Minor code change |
| **Impact** | Confusing code; inner `@udf` is ignored by Spark planner when called inside outer UDF |

**Description**

A `@udf`-decorated function calls another `@udf`-decorated function directly in its body. The
inner UDF's `@udf` decorator is bypassed when called from inside a UDF — Spark executes it as
a plain Python function — creating misleading code that appears to register two Spark UDFs but
effectively only registers one.

**What it detects**

A `@udf` function body that contains a call to another function that is also decorated with
`@udf`.

```python
# Triggers SPL-D09-012
@udf(returnType=StringType())
def inner(s):
    return s.strip()

@udf(returnType=StringType())
def outer(s):
    return inner(s).lower()   # inner's @udf is bypassed here
```

**Why it matters**

When Spark executes a Python UDF, it sends the function object to Python worker processes via
serialization. Inside the worker, the outer UDF is called as a regular Python function. When
`outer` calls `inner(s)`, Python sees `inner` as a `UserDefinedFunction` object (the wrapper
created by `@udf`) rather than the raw Python function. The `UserDefinedFunction.__call__`
method is designed to be called from the JVM side — it expects Spark `Column` objects, not
raw Python values — and calling it with a plain string produces confusing type errors or
silently incorrect results. The correct pattern is for helper functions called inside UDFs to
be plain Python functions without the `@udf` decorator.

**How to fix**

```python
# Wrong: inner's @udf is bypassed inside outer's execution; confusing behavior
@udf(returnType=StringType())
def inner(s):
    return s.strip()

@udf(returnType=StringType())
def outer(s):
    return inner(s).lower()

# Right: inner is a plain Python helper (no @udf); outer is the only Spark UDF
def _strip(s: str) -> str:
    """Plain Python helper — called inside the UDF, not registered as a Spark UDF."""
    return s.strip()

@udf(returnType=StringType())
def outer(s):
    return _strip(s).lower()

# Better: replace both with native functions if possible
from pyspark.sql.functions import lower, trim
df.withColumn("result", lower(trim(col("s"))))
```

**Config options**

No Spark configuration affects this rule.

**Related rules**

- **SPL-D09-001** — Row-at-a-time UDF; nested UDFs compound the serialization overhead
- **SPL-D09-002** — UDF replaceable with native function; check if the nested logic maps to Spark SQL builtins
