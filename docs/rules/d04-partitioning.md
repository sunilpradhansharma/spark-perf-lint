---
layout: default
title: D04 Partitioning
nav_order: 4
parent: Rules
---

# D04 — Partitioning
{: .no_toc }

10 rules covering partition count, balance, storage partitioning, and bucketing.
Getting partitioning right is the difference between full parallelism and a
single-threaded bottleneck.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## SPL-D04-001 — repartition(1) bottleneck
{: .d-inline-block }
CRITICAL
{: .label .label-red }

**`repartition(1)` forces all data through a single task, eliminating parallelism.**

```python
# Before
df.repartition(1).write.parquet('s3://bucket/out')

# After
df.write.parquet('s3://bucket/out')  # let Spark decide partition count
# Or if you genuinely need one output file:
df.coalesce(1).write.parquet('s3://bucket/out')  # no shuffle, just merge
```

**Impact:** Full parallelism lost; single-core execution for the entire dataset

---

## SPL-D04-002 — coalesce(1) bottleneck
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`coalesce(1)` reduces all data to one partition, serialising subsequent operations.**

Unlike `repartition(1)`, `coalesce(1)` avoids a full shuffle — but all
downstream stages still run single-threaded.

```python
# Before
df.coalesce(1).groupBy('key').count()

# After
df.groupBy('key').count()  # restore parallelism
```

**Impact:** All subsequent stages run single-threaded; long-tail straggler

---

## SPL-D04-003 — Repartition with very high partition count
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`repartition()` with a count above 10,000 causes excessive task-scheduler overhead.**

Millions of shuffle files, driver memory pressure, and task-scheduling
overhead outweigh any parallelism benefit.

```python
# Before
df.repartition(50_000)

# After
df.repartition(400)  # ~128 MB per partition at 50 GB data
```

**Impact:** Driver OOM; millions of shuffle files; slow task scheduling

---

## SPL-D04-004 — Repartition with very low partition count
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`repartition(n)` where `n < 10` under-uses cluster parallelism.**

```python
# Before
df.repartition(4).groupBy('key').agg(sum('val'))

# After
df.repartition(200).groupBy('key').agg(sum('val'))
```

**Impact:** Cluster cores sit idle; up to 25× slower than optimal parallelism

---

## SPL-D04-005 — Coalesce before write — unbalanced output files
{: .d-inline-block }
INFO
{: .label .label-blue }

**`coalesce()` before `write()` creates unbalanced output files — use `repartition()`.**

`coalesce()` merges the largest partitions into fewer ones, producing uneven
files. Downstream readers spend more time on the large files.

```python
# Before
df.coalesce(20).write.parquet('s3://bucket/out')

# After
df.repartition(20).write.parquet('s3://bucket/out')
```

**Impact:** Unbalanced output files; downstream reader performance degradation

---

## SPL-D04-006 — Missing partitionBy on write
{: .d-inline-block }
INFO
{: .label .label-blue }

**Writing to parquet/orc/delta without `partitionBy` — consider a partition strategy.**

Without `partitionBy`, every query that filters by date, region, or status
must scan the entire table.

```python
# Before
df.write.mode('overwrite').parquet('s3://bucket/events')

# After
df.write.partitionBy('date', 'region').mode('overwrite').parquet('s3://bucket/events')
```

**Impact:** Full table scans on every query; no partition pruning possible

---

## SPL-D04-007 — Over-partitioning — high-cardinality partition column
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`partitionBy()` on a high-cardinality column creates millions of tiny files.**

Partitioning by `user_id`, `order_id`, or timestamps creates one directory per
value. With millions of users that is millions of directories — driver OOM on
`SHOW PARTITIONS` and slow metastore operations.

```python
# Before
df.write.partitionBy('user_id').parquet('s3://bucket/events')

# After
df.write.partitionBy('date', 'region').parquet('s3://bucket/events')
```

**Impact:** Millions of tiny files; driver OOM on partition listing; high storage cost

---

## SPL-D04-008 — Missing bucketBy for repeatedly joined tables
{: .d-inline-block }
INFO
{: .label .label-blue }

**`saveAsTable()` without `bucketBy` — joins on this table cause repeated shuffles.**

Bucketed tables pre-sort and co-locate rows by key. Every subsequent join on
that key requires zero shuffle.

```python
# Before
df.write.mode('overwrite').saveAsTable('fact_orders')

# After
(df.write
    .bucketBy(256, 'order_id')
    .sortBy('order_id')
    .mode('overwrite')
    .saveAsTable('fact_orders'))
```

**Impact:** Every join on this table requires a full shuffle; 0 shuffles with bucketing

---

## SPL-D04-009 — Partition column not used in query filters
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**Reading from storage directly into an expensive operation without a partition filter.**

```python
# Before
spark.read.parquet('s3://datalake/events').groupBy('user_id').count()

# After
(spark.read.parquet('s3://datalake/events')
    .filter('date = "2024-01-01"')
    .groupBy('user_id').count())
```

**Impact:** Full table scan instead of partition-pruned scan; 10–1000× more data read

---

## SPL-D04-010 — Repartition by column different from join key
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`repartition()` by a column that differs from the subsequent join key.**

The repartition shuffle is wasted — the join will shuffle again on the join key.

```python
# Before
df.repartition('user_id').join(other, 'order_id')

# After
df.join(other, 'order_id')  # join shuffles on order_id already
```

**Impact:** Two full shuffles instead of one; 2× shuffle I/O for that stage
