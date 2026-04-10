---
layout: default
title: D06 Caching
nav_order: 6
parent: Rules
---

# D06 — Caching & Persistence
{: .no_toc }

8 rules for correct cache/persist usage. Caching at the wrong place, with the
wrong storage level, or forgetting to unpersist are common sources of executor
OOM and silent recomputation.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## SPL-D06-001 — cache() without unpersist()
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`cache()` / `persist()` without a corresponding `unpersist()` leaks executor memory.**

Cached DataFrames stay in executor memory until the SparkContext is stopped or
the cache is evicted. Long-running jobs accumulate leaks that eventually cause
OOM or evict other useful cached data.

```python
# Before
df_cached = df.join(other, 'id').cache()
result = df_cached.groupBy('x').count()
# df_cached is never released

# After
df_cached = df.join(other, 'id').cache()
result = df_cached.groupBy('x').count()
df_cached.unpersist()  # release when done
```

**Impact:** Executor memory leak; evicts other cached data; OOM in long-running jobs

---

## SPL-D06-002 — cache() used only once
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**Caching a DataFrame that is only read once adds serialisation overhead without benefit.**

```python
# Before
df_cached = df.join(other, 'id').cache()
result = df_cached.count()

# After
result = df.join(other, 'id').count()  # no cache needed
```

**Impact:** Unnecessary serialisation pass; wastes executor memory for no benefit

---

## SPL-D06-003 — cache() inside loop
{: .d-inline-block }
CRITICAL
{: .label .label-red }

**`cache()`/`persist()` inside a loop fills executor memory on each iteration.**

Each iteration adds a new cached DataFrame. Without `unpersist()` in the loop
body, executor memory accumulates until OOM.

```python
# Before
for epoch in range(10):
    df = model.transform(df).cache()
    loss = df.agg(sum('error')).collect()[0][0]

# After
for epoch in range(10):
    prev = df
    df = model.transform(df).checkpoint()  # truncates lineage
    prev.unpersist()
```

**Impact:** OOM after O(N) iterations; executor memory accumulates without release

---

## SPL-D06-004 — cache() before filter
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**Filtering after `cache()` caches unnecessary rows — push the filter before cache.**

```python
# Before
df.cache().filter('active = true').groupBy('status').count()

# After
df.filter('active = true').cache().groupBy('status').count()
```

**Impact:** Caches N rows but only M < N are used; wastes (N-M) / N of cache memory

---

## SPL-D06-005 — MEMORY_ONLY storage level for potentially large datasets
{: .d-inline-block }
INFO
{: .label .label-blue }

**`MEMORY_ONLY` drops partitions silently when memory is full; prefer `MEMORY_AND_DISK`.**

Dropped partitions are silently recomputed from scratch on access, negating
the benefit of caching.

```python
# Before
df.persist(StorageLevel.MEMORY_ONLY)

# After
df.persist(StorageLevel.MEMORY_AND_DISK)  # safe fallback to disk
```

**Impact:** Silent partition eviction leads to full recomputation under memory pressure

---

## SPL-D06-006 — Reused DataFrame without cache
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**A DataFrame built from an expensive operation (join/groupBy/agg) is used multiple
times without caching.**

Each use triggers a full recomputation of the entire lineage from scratch.

```python
# Before
df_agg = df.groupBy('id').agg(sum('v'))
result1 = df_agg.join(lookup, 'id')
result2 = df_agg.filter('total > 100')

# After
df_agg = df.groupBy('id').agg(sum('v')).cache()
result1 = df_agg.join(lookup, 'id')
result2 = df_agg.filter('total > 100')
df_agg.unpersist()
```

**Impact:** N downstream uses trigger N full pipeline recomputations instead of 1

---

## SPL-D06-007 — cache() after repartition
{: .d-inline-block }
INFO
{: .label .label-blue }

**`cache()` after `repartition()` is only worthwhile if the result is used 2+ times.**

`repartition()` is already an expensive shuffle. Adding `cache()` on top for a
single-use result is wasteful.

```python
# Before — used once
df2 = df.repartition(200).cache()
result = df2.groupBy('x').count()

# After
df2 = df.repartition(200)
result = df2.groupBy('x').count()
```

**Impact:** Unnecessary cache write after an already-expensive shuffle

---

## SPL-D06-008 — checkpoint vs cache misuse
{: .d-inline-block }
INFO
{: .label .label-blue }

**`checkpoint()` in non-iterative code writes to disk unnecessarily; use `cache()` instead.**

`checkpoint()` truncates the RDD lineage and writes to HDFS/S3, which is only
needed in iterative algorithms (MLlib, PageRank) where lineage grows unbounded.
For single-pass pipelines, `cache()` is faster.

```python
# Before
df_intermediate = df.join(other, 'id').checkpoint()

# After
df_intermediate = df.join(other, 'id').cache()  # faster; no disk write
```

**Impact:** Unnecessary disk write + checkpoint dir overhead in non-iterative pipelines
