---
layout: default
title: D10 Catalyst
nav_order: 10
parent: Rules
---

# D10 — Catalyst Optimizer
{: .no_toc }

6 rules for helping (and not blocking) Catalyst, Spark's query optimiser.
Catalyst rewrites, simplifies, and reorders your query plan automatically —
but UDFs, missing statistics, and certain code patterns create opaque
boundaries it cannot optimise through.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## SPL-D10-001 — UDF blocks predicate pushdown
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`@udf` creates an opaque boundary that prevents Catalyst from pushing filters below it.**

Catalyst cannot inspect or optimise inside a Python UDF. A filter applied to
the output of a UDF cannot be pushed down to the data source — the source must
be fully scanned first, then the UDF applied, then the filter.

```python
# Before
@udf(returnType=StringType())
def normalise(s):
    return s.lower().strip()

df.withColumn('name', normalise(col('raw_name'))).filter(col('name') == 'alice')

# After — native functions — Catalyst can push filter to the source
from pyspark.sql.functions import lower, trim
df.filter(trim(lower(col('raw_name'))) == 'alice')
```

**Impact:** Full table scan before filter; no partition/row-group elimination

---

## SPL-D10-002 — CBO not enabled for complex queries
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**File contains 2+ joins but Cost-Based Optimisation is not enabled.**

Without CBO, Catalyst uses rule-based optimisation — it applies heuristics but
cannot choose between join strategies based on actual table sizes and
cardinalities. With CBO enabled and table statistics collected, Catalyst picks
the cheapest join order automatically.

```python
# Before
spark = SparkSession.builder.getOrCreate()
result = a.join(b, 'id').join(c, 'key')

# After
spark = (SparkSession.builder
    .config('spark.sql.cbo.enabled', 'true')
    .config('spark.sql.statistics.histogram.enabled', 'true')
    .getOrCreate())
```

**Impact:** Sub-optimal join strategies; 5–20× slower plans for multi-join queries

---

## SPL-D10-003 — Join reordering disabled for multi-table joins
{: .d-inline-block }
INFO
{: .label .label-blue }

**File contains 3+ joins but join reordering (CBO) is not enabled.**

Without join reordering, Catalyst executes joins left-to-right as written.
This can produce large intermediate results if a large table is joined first.
With CBO, Catalyst reorders to minimise intermediate result size.

```python
# Before
result = a.join(huge, 'id').join(b, 'key').join(small, 'ref')

# After
spark.conf.set('spark.sql.cbo.enabled', 'true')
spark.conf.set('spark.sql.cbo.joinReorder.enabled', 'true')
# Also run: spark.sql('ANALYZE TABLE huge COMPUTE STATISTICS FOR ALL COLUMNS')
result = a.join(huge, 'id').join(b, 'key').join(small, 'ref')
```

**Impact:** Sub-optimal join order; large intermediate DataFrames; possible OOM

---

## SPL-D10-004 — Table statistics not collected
{: .d-inline-block }
INFO
{: .label .label-blue }

**File performs joins but contains no `ANALYZE TABLE` statement.**

CBO relies on table statistics (row count, column cardinalities, histograms)
to make good decisions. Without statistics, CBO falls back to heuristics.

```python
# Before
result = spark.table('events').join(spark.table('users'), 'user_id')

# After
spark.sql('ANALYZE TABLE events COMPUTE STATISTICS FOR ALL COLUMNS')
spark.sql('ANALYZE TABLE users COMPUTE STATISTICS FOR ALL COLUMNS')
result = spark.table('events').join(spark.table('users'), 'user_id')
```

**Impact:** CBO operates without accurate stats; poor join strategies; missed optimisations

---

## SPL-D10-005 — Non-deterministic function in filter
{: .d-inline-block }
INFO
{: .label .label-blue }

**`filter()`/`where()` contains a non-deterministic function (`rand`, `uuid`, …).**

Non-deterministic functions can be evaluated multiple times by Catalyst due to
plan transformations, producing inconsistent results. Use `df.sample()` for
sampling — Catalyst handles it specially to ensure single evaluation.

```python
# Before
sample = df.filter(rand() < 0.1)

# After
sample = df.sample(fraction=0.1, seed=42)  # deterministic, single-evaluation
```

**Impact:** Non-reproducible results; potential double-evaluation by Catalyst

---

## SPL-D10-006 — Deep method chain (>20 chained ops)
{: .d-inline-block }
INFO
{: .label .label-blue }

**A single expression chains >20 DataFrame operations, producing a deeply nested logical plan.**

Very deep chains can cause Catalyst's plan visitor to overflow the JVM stack
during plan analysis, and make debugging significantly harder.

```python
# Before
result = (
    df.filter('a > 0')
      .dropDuplicates(['id'])
      .join(b, 'id')
      .join(c, 'key')
      .withColumn('x', col('a') + col('b'))
      # ... 15+ more operations ...
)

# After — break into named intermediate steps
clean    = df.filter('a > 0').dropDuplicates(['id'])
enriched = clean.join(b, 'id').join(c, 'key')
result   = enriched.withColumn('x', col('a') + col('b'))
```

**Impact:** Driver-side plan analysis overhead; risk of plan visitor stack overflow
