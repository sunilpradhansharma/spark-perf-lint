---
layout: default
title: D02 Shuffle & Sort
nav_order: 2
parent: Rules
---

# D02 — Shuffle & Sort
{: .no_toc }

8 rules covering shuffle-heavy operations that dominate Spark job runtime.
Shuffle is the single most expensive operation in Spark — every rule here
reduces or eliminates unnecessary data movement.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## SPL-D02-001 — groupByKey() instead of reduceByKey()
{: .d-inline-block }
CRITICAL
{: .label .label-red }

**`groupByKey()` shuffles all values to reducers; use `reduceByKey()` instead.**

`groupByKey` sends every value across the network before aggregating.
`reduceByKey` combines locally on each partition first, shuffling only partial
results — 10–100× less data in the shuffle.

```python
# Before
rdd.groupByKey().mapValues(sum)

# After
rdd.reduceByKey(lambda a, b: a + b)
```

**Impact:** 10–100× shuffle data reduction for aggregation workloads

---

## SPL-D02-002 — Default shuffle partitions unchanged
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`spark.sql.shuffle.partitions` is not set — shuffle stages use the default of 200.**

```python
# Before
spark = SparkSession.builder.getOrCreate()  # 200 partitions by default

# After
spark = (SparkSession.builder
    .config("spark.sql.shuffle.partitions", "400")
    .getOrCreate())
```

**Impact:** Spill to disk on large datasets; task-scheduler overhead on small data

---

## SPL-D02-003 — Unnecessary repartition before join
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`repartition()` immediately before `join()` causes a redundant shuffle.**

`join()` already performs a shuffle to co-locate matching keys. Adding
`repartition()` first means two full shuffles instead of one.

```python
# Before
df.repartition(200, 'key').join(other, 'key')

# After
df.join(other, 'key')  # join already shuffles on key
```

**Impact:** One extra full shuffle per join; 2× shuffle I/O for that stage

---

## SPL-D02-004 — orderBy/sort without limit
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`orderBy()`/`sort()` on an unlimited dataset forces a full total-order sort.**

A global sort is O(n log n) shuffle + sort. If you only need the top-N rows,
add `.limit(N)` — Spark will use a partial sort optimisation.

```python
# Before
df.orderBy('revenue', ascending=False)

# After
df.orderBy('revenue', ascending=False).limit(100)
```

**Impact:** Full global sort vs O(n log k) for top-N; significant for large datasets

---

## SPL-D02-005 — distinct() without prior filter
{: .d-inline-block }
INFO
{: .label .label-blue }

**`distinct()` triggers a full shuffle; apply filters first to reduce data volume.**

```python
# Before
df.distinct()

# After
df.select('id', 'event_type').filter('date > "2024-01-01"').distinct()
```

**Impact:** Full shuffle of all rows and all columns; high memory pressure

---

## SPL-D02-006 — Shuffle followed by coalesce
{: .d-inline-block }
INFO
{: .label .label-blue }

**`coalesce()` after a shuffle creates unbalanced partitions.**

`coalesce()` merges partitions without shuffling, creating tasks of very
different sizes. Use `repartition()` for balanced output after a shuffle.

```python
# Before
df.groupBy('col').agg(count('*')).coalesce(10)

# After
df.groupBy('col').agg(count('*')).repartition(10)
```

**Impact:** Unbalanced tasks; long-tail executor stragglers after the shuffle

---

## SPL-D02-007 — Multiple shuffles in sequence
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**Two or more shuffle-wide operations appear in sequence without intermediate caching.**

Compounded shuffles multiply I/O. Cache the intermediate result when it is
reused, or restructure the pipeline to reduce the shuffle count.

```python
# Before
df2 = df.groupBy('a').agg(sum('b'))
df3 = df2.join(lookup, 'a')
df4 = df3.orderBy('b')

# After
df2 = df.groupBy('a').agg(sum('b')).cache()
df3 = df2.join(lookup, 'a')
df4 = df3.orderBy('b')
df2.unpersist()
```

**Impact:** Compounded shuffle I/O; full lineage re-execution on any task failure

---

## SPL-D02-008 — Shuffle file buffer too small
{: .d-inline-block }
INFO
{: .label .label-blue }

**`spark.shuffle.file.buffer` is configured below the recommended 1 MB.**

A larger buffer means fewer `fsync` calls when writing shuffle files to disk.

```python
# Before
.config("spark.shuffle.file.buffer", "32k")

# After
.config("spark.shuffle.file.buffer", "1m")
```

**Impact:** Up to 32× reduction in shuffle-write syscall count
