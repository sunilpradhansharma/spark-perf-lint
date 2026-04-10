---
layout: default
title: D03 Joins
nav_order: 3
parent: Rules
---

# D03 — Joins
{: .no_toc }

10 rules covering join anti-patterns. Joins are the most common source of
performance problems in PySpark — wrong strategy, missing broadcast hints, and
joins inside loops can make jobs orders of magnitude slower.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## SPL-D03-001 — Cross join / cartesian product
{: .d-inline-block }
CRITICAL
{: .label .label-red }

**`crossJoin()` or `join(how='cross')` creates an O(n²) cartesian product.**

Every row on the left is paired with every row on the right. For 1M × 1M rows
that is 1 trillion output rows. This is almost always unintentional.

```python
# Before
df.crossJoin(other)  # O(n²) rows!

# After
df.join(other, on='id', how='inner')  # explicit key
```

**Impact:** Unbounded row explosion; can OOM or run indefinitely

---

## SPL-D03-002 — Missing broadcast hint on small DataFrame
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`join()` without any broadcast hint — small lookup tables should be broadcast.**

Without a broadcast hint, Catalyst may choose a Sort Merge Join (two full
shuffles). Broadcasting the small side eliminates the shuffle entirely.

```python
# Before
df.join(lookup, 'product_id')

# After
from pyspark.sql.functions import broadcast
df.join(broadcast(lookup), 'product_id')
```

**Impact:** Eliminates shuffle on both sides; 10–100× faster for small lookups

---

## SPL-D03-003 — Broadcast threshold disabled
{: .d-inline-block }
CRITICAL
{: .label .label-red }

**`spark.sql.autoBroadcastJoinThreshold = -1` disables all broadcast joins.**

This forces every join to shuffle both sides, even when one side is tiny.

```python
# Before
.config("spark.sql.autoBroadcastJoinThreshold", "-1")

# After
.config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB
```

**Impact:** Every join shuffles both sides; disables all broadcast optimisations

---

## SPL-D03-004 — Join without prior filter/select
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`join()` on an unfiltered read — apply `filter()`/`select()` first.**

Pushing filters before the join reduces the data volume in the shuffle by the
filter selectivity — often 10–100×.

```python
# Before
df = spark.read.parquet('s3://bucket/events')
result = df.join(users, 'user_id')

# After
df = spark.read.parquet('s3://bucket/events').filter('date >= "2024-01-01"')
result = df.select('user_id', 'amount').join(users, 'user_id')
```

**Impact:** Up to 10–100× shuffle reduction when large fractions of data are pruned

---

## SPL-D03-005 — Join key type mismatch risk
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`join()` condition compares different column names — verify the types match.**

Type mismatches in join keys produce empty results silently, with no error.

```python
# Before
df1.join(df2, df1.order_id == df2.id)

# After
df1.join(df2, df1.order_id.cast('long') == df2.id.cast('long'))
```

**Impact:** Silent wrong-result bugs or empty join output due to type mismatch

---

## SPL-D03-006 — Multiple joins without intermediate repartition
{: .d-inline-block }
INFO
{: .label .label-blue }

**Three or more `join()` calls without any broadcast or repartition hint.**

Each join adds a full shuffle. Broadcasting dimension tables eliminates most
of the cost for star-schema patterns.

```python
# Before
result = (facts
    .join(dim_a, 'key_a')
    .join(dim_b, 'key_b')
    .join(dim_c, 'key_c'))

# After
result = (facts
    .join(broadcast(dim_a), 'key_a')
    .join(broadcast(dim_b), 'key_b')
    .join(broadcast(dim_c), 'key_c'))
```

**Impact:** 3+ full shuffles; each shuffle writes/reads entire intermediate datasets

---

## SPL-D03-007 — Self-join that could be window function
{: .d-inline-block }
INFO
{: .label .label-blue }

**`join()` where both sides reference the same DataFrame.**

Self-joins double shuffle volume. Window functions accomplish the same result
with zero cross-node data transfer.

```python
# Before
df.join(df.withColumnRenamed('value', 'prev_value'), on='key')

# After
from pyspark.sql.window import Window
w = Window.partitionBy('key').orderBy('ts')
df.withColumn('prev_value', lag('value').over(w))
```

**Impact:** 2× shuffle data volume; window functions need zero cross-node transfer

---

## SPL-D03-008 — Join inside loop
{: .d-inline-block }
CRITICAL
{: .label .label-red }

**`join()` inside a loop creates a new shuffle stage on every iteration.**

Each loop iteration adds a new join to the query plan. For N iterations you
get N shuffles and a query plan that grows O(N), eventually causing driver OOM
during plan analysis.

```python
# Before
for date in date_range:
    result = result.join(daily_df.filter(f'date = "{date}"'), 'id')

# After
all_dates = spark.createDataFrame(date_range, StringType()).toDF('date')
daily_all = daily_df.join(all_dates, 'date')
result = result.join(daily_all, 'id')
```

**Impact:** N shuffles where N = loop iterations; plan explosion for large loops

---

## SPL-D03-009 — Left join without null handling
{: .d-inline-block }
INFO
{: .label .label-blue }

**Left `join()` without null handling — null rows may propagate silently.**

```python
# Before
result = df.join(lookup, 'id', 'left')

# After
result = (df.join(lookup, 'id', 'left')
    .fillna({'status': 'n/a', 'score': 0.0}))
```

**Impact:** Silent null propagation; wrong aggregation results and data quality bugs

---

## SPL-D03-010 — CBO/statistics not enabled for complex joins
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`spark.sql.cbo.enabled` not set; Catalyst cannot reorder joins by cost.**

Without CBO, Catalyst uses the order joins appear in the code. With CBO and
table statistics it picks the cheapest order automatically.

```python
# Before
spark = SparkSession.builder.getOrCreate()  # CBO off by default

# After
spark = (SparkSession.builder
    .config("spark.sql.cbo.enabled", "true")
    .config("spark.sql.statistics.histogram.enabled", "true")
    .getOrCreate())
```

**Impact:** Sub-optimal join ordering; large intermediate results on multi-join queries
