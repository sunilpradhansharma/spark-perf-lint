---
layout: default
title: D07 I/O & Formats
nav_order: 7
parent: Rules
---

# D07 — I/O & File Formats
{: .no_toc }

10 rules covering file format choice, schema inference, predicate pushdown,
and JDBC parallelism. I/O is often the bottleneck for analytical workloads —
the right format can eliminate 80–95% of the bytes read.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## SPL-D07-001 — CSV/JSON used for analytical workload
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**CSV/JSON are row-oriented formats with no column pruning or predicate pushdown.**

Parquet and ORC read only the columns you need and skip row groups that don't
match filters. CSV reads everything.

```python
# Before
df = spark.read.csv('data/', header=True, inferSchema=True)

# After
df = spark.read.parquet('data.parquet')
```

**Impact:** 5–20× slower scans vs Parquet; no column pruning or predicate pushdown

---

## SPL-D07-002 — Schema inference enabled
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`inferSchema=True` reads the entire dataset twice — once to infer types, once for processing.**

Schema inference also produces unstable types that change when the sample data
changes, causing intermittent type errors in production.

```python
# Before
df = spark.read.option('inferSchema', 'true').csv('path')

# After
schema = 'id LONG, name STRING, amount DOUBLE, date DATE'
df = spark.read.schema(schema).csv('path')
```

**Impact:** 2× I/O per job; unstable schemas cause intermittent type errors

---

## SPL-D07-003 — select("*") — no column pruning
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`select("*")` disables column pruning — all columns are decoded from storage.**

Columnar formats like Parquet only decode the columns referenced in the query.
`select("*")` forces decoding of every column.

```python
# Before
df.select('*').groupBy('status').count()

# After
df.select('status').groupBy('status').count()
```

**Impact:** All columns decoded from storage; no columnar pruning benefit

---

## SPL-D07-004 — Filter applied after join — missing predicate pushdown
{: .d-inline-block }
INFO
{: .label .label-blue }

**`filter()`/`where()` applied after `join()` processes more data in the shuffle.**

Pushing the filter before the join reduces shuffle volume by the filter selectivity.

```python
# Before
df.join(other, 'id').filter('status = "active"').groupBy('x').count()

# After
df.filter('status = "active"').join(other, 'id').groupBy('x').count()
```

**Impact:** Shuffle includes rows that will be discarded; wasted network I/O

---

## SPL-D07-005 — Small file problem on write
{: .d-inline-block }
INFO
{: .label .label-blue }

**Writing with `partitionBy()` without controlling partition count may produce many small files.**

Each Spark partition produces one output file per partition value. Without
`coalesce()`/`repartition()`, the number of output files = spark_partitions ×
partition_values.

```python
# Before
df.write.partitionBy('date').parquet('s3://bucket/data')

# After
df.repartition('date').write.partitionBy('date').parquet('s3://bucket/data')
```

**Impact:** O(partitions × partition_values) small files; slow reads and metastore operations

---

## SPL-D07-006 — JDBC read without partition parameters
{: .d-inline-block }
CRITICAL
{: .label .label-red }

**Reading from JDBC without `partitionColumn`/`numPartitions` serialises the entire table read.**

Without partition parameters, a single Spark task reads the entire JDBC table
sequentially. This doesn't scale beyond what one database connection can handle.

```python
# Before
df = spark.read.jdbc(url, 'orders')

# After
df = spark.read.jdbc(
    url, 'orders',
    column='order_id',
    lowerBound=1,
    upperBound=10_000_000,
    numPartitions=50,
    properties={"user": "...", "password": "..."}
)
```

**Impact:** Sequential single-thread read; does not scale to large JDBC tables

---

## SPL-D07-007 — Parquet compression not set
{: .d-inline-block }
INFO
{: .label .label-blue }

**Writing Parquet without explicit compression relies on the cluster default.**

The default varies by cluster configuration — some environments write
uncompressed Parquet, wasting storage and I/O bandwidth.

```python
# Before
df.write.mode('overwrite').parquet('s3://bucket/data')

# After
df.write.mode('overwrite').option('compression', 'snappy').parquet('s3://bucket/data')
```

**Impact:** Cluster-dependent codec; may write uncompressed on some environments

---

## SPL-D07-008 — Write mode not specified
{: .d-inline-block }
INFO
{: .label .label-blue }

**`write` without `.mode()` defaults to `'error'` and fails if the output path already exists.**

```python
# Before
df.write.parquet('s3://bucket/output')

# After
df.write.mode('overwrite').parquet('s3://bucket/output')
```

**Impact:** Job fails on re-run if output path exists; silent data loss on 'append' if unintended

---

## SPL-D07-009 — No format specified on read/write
{: .d-inline-block }
INFO
{: .label .label-blue }

**`read.load()` / `write.save()` without `.format()` relies on the cluster default format.**

```python
# Before
df = spark.read.load('path')

# After
df = spark.read.parquet('path')
# or
df = spark.read.format('parquet').load('path')
```

**Impact:** Format depends on cluster config; silent mismatches across environments

---

## SPL-D07-010 — mergeSchema enabled without necessity
{: .d-inline-block }
INFO
{: .label .label-blue }

**`mergeSchema=true` reads the footer of every Parquet file to reconcile schemas — O(files) cost.**

```python
# Before
df = spark.read.option('mergeSchema', 'true').parquet('path')

# After
df = spark.read.schema(explicit_schema).parquet('path')
# Or, if schema evolution is truly needed, enable mergeSchema only for that read
```

**Impact:** O(files) metadata scan on every read; adds 10s for tables with many files
