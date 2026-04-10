---
layout: default
title: D05 Data Skew
nav_order: 5
parent: Rules
---

# D05 — Data Skew
{: .no_toc }

7 rules for detecting and mitigating data skew. Skew causes one task to
process 10–1000× more data than others, blocking the entire stage while the
rest of the cluster sits idle.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## SPL-D05-001 — Join on low-cardinality column
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`join()` on a low-cardinality column sends matching rows to a tiny number of tasks.**

Columns like `status`, `type`, `is_active`, or `country` typically have fewer
than 100 distinct values. All rows with the dominant value hash to the same
reducer, causing extreme skew.

```python
# Before
df.join(other, 'status')

# After
df.join(other, ['status', 'user_id'])  # user_id breaks ties
```

**Impact:** One task processes the dominant group; rest of cluster sits idle

---

## SPL-D05-002 — GroupBy on low-cardinality column without secondary key
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`groupBy()` using only low-cardinality columns creates unbalanced partitions.**

```python
# Before
df.groupBy('status').agg(count('*'))

# After — two-phase aggregation
df.withColumn('bucket', (rand() * 10).cast('int')) \
  .groupBy('status', 'bucket').agg(count('*').alias('cnt')) \
  .groupBy('status').agg(sum('cnt'))
```

**Impact:** Dominant group task runs 10–100× longer than median; straggler stage

---

## SPL-D05-003 — AQE skew join handling disabled
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`spark.sql.adaptive.skewJoin.enabled = false` disables automatic skew mitigation.**

AQE's skew join handling splits oversized partitions automatically at runtime.
Disabling it means skewed joins must be handled manually.

```python
# Before
.config("spark.sql.adaptive.skewJoin.enabled", "false")

# After — remove the line, or explicitly enable:
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Impact:** Skewed partitions are not split; one task processes majority of data

---

## SPL-D05-004 — AQE skew threshold too high
{: .d-inline-block }
INFO
{: .label .label-blue }

**AQE skew detection threshold exceeds 1 GB — moderate skew may not be detected.**

```python
# Before
.config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "2g")

# After
.config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
```

**Impact:** Moderate skew (< threshold) silently degrades stage performance

---

## SPL-D05-005 — Missing salting pattern for known skewed keys
{: .d-inline-block }
INFO
{: .label .label-blue }

**`join()` on a potentially skewed ID column without a salting pattern.**

For known-skewed keys (e.g., celebrity users, viral products), salt both sides
of the join to distribute the load.

```python
# Before
df.join(events, 'user_id')  # user_id may be highly skewed

# After — salted join
N = 10
df_salted = df.withColumn('salt', (rand() * N).cast('int'))
events_exploded = events.withColumn('salt', explode(array([lit(i) for i in range(N)])))
df_salted.join(events_exploded, ['user_id', 'salt'])
```

**Impact:** 'Whale' keys cause one reducer to handle O(N) more rows than average

---

## SPL-D05-006 — Window function partitioned by skew-prone column
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`Window.partitionBy()` on a low-cardinality column routes most rows to one task.**

```python
# Before
w = Window.partitionBy('status').orderBy('ts')

# After
w = Window.partitionBy('status', 'user_id').orderBy('ts')
```

**Impact:** Executor OOM for dominant partition; entire stage blocked by straggler

---

## SPL-D05-007 — Null-heavy join key
{: .d-inline-block }
INFO
{: .label .label-blue }

**`join()` on a column that commonly contains nulls without prior null filtering.**

In Spark, `null != null` in join conditions, so null-key rows don't match. But
they all hash to the same partition, creating a skewed "null bucket" task.

```python
# Before
df.join(other, 'parent_id')  # parent_id is null for root records

# After
df.filter(col('parent_id').isNotNull()).join(other, 'parent_id')
```

**Impact:** All null-key rows hash to same partition; straggler task on null bucket
