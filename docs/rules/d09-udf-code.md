---
layout: default
title: D09 UDFs & Code
nav_order: 9
parent: Rules
---

# D09 — UDFs & Code Patterns
{: .no_toc }

12 rules for Python UDFs, driver-side anti-patterns, and code patterns that
produce exploding query plans. This dimension catches the most CRITICAL issues
in typical PySpark code.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## SPL-D09-001 — Python UDF (row-at-a-time) detected
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`@udf` functions execute row-at-a-time, serialising every row between the JVM and Python.**

Each row crosses the JVM↔Python boundary twice (serialise out, deserialise back).
For 100M rows that is 200M serialisation operations — and Catalyst cannot optimise
through the UDF boundary.

```python
# Before
@udf(returnType=StringType())
def normalise(s):
    return s.lower().strip()

df.withColumn('name', normalise(col('name')))

# After — native SQL function, JVM-side, Catalyst-optimisable
from pyspark.sql.functions import lower, trim
df.withColumn('name', trim(lower(col('name'))))
```

**Impact:** 2–10× slowdown per row; blocks Catalyst code-generation pipeline

---

## SPL-D09-002 — Python UDF replaceable with native Spark function
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`@udf` body applies a built-in string method that has a direct Spark SQL equivalent.**

```python
# Before
@udf(returnType=StringType())
def my_lower(s):
    return s.lower()

df.withColumn('name', my_lower(col('name')))

# After
from pyspark.sql.functions import lower
df.withColumn('name', lower(col('name')))
```

**Impact:** Per-row JVM↔Python serialisation for a zero-logic transformation

---

## SPL-D09-003 — withColumn() inside loop
{: .d-inline-block }
CRITICAL
{: .label .label-red }

**`withColumn()` inside a loop rebuilds the full query plan on every iteration.**

Each `withColumn()` call wraps the existing plan in a new `Project` node. For
N iterations, the plan has N nested `Project` nodes. Plan analysis is O(N²);
for N > 100 the driver hangs.

```python
# Before
for col_name in column_list:
    df = df.withColumn(col_name, compute(col(col_name)))

# After
exprs = [compute(col(c)).alias(c) for c in column_list]
df = df.select('*', *exprs)
```

**Impact:** O(N²) plan building; driver hangs for N > 100 columns

---

## SPL-D09-004 — Row-by-row iteration over DataFrame
{: .d-inline-block }
CRITICAL
{: .label .label-red }

**Iterating over `df.collect()` or `df.toLocalIterator()` forces all data to the driver.**

This turns a distributed computation into a sequential single-machine loop,
wasting the entire cluster.

```python
# Before
for row in df.collect():
    process(row)

# After — distribute processing, no data movement to driver
df.foreach(process)
# Or for transformations:
df.map(transform)
```

**Impact:** Driver OOM; entire cluster idle while driver processes rows sequentially

---

## SPL-D09-005 — .collect() without prior filter or limit
{: .d-inline-block }
CRITICAL
{: .label .label-red }

**`.collect()` without `filter()`/`limit()` pulls the entire DataFrame to the driver.**

```python
# Before
rows = df.join(other, 'id').collect()

# After
rows = df.join(other, 'id').limit(1000).collect()
# Or for aggregations, collect only the result:
summary = df.groupBy('status').count().collect()
```

**Impact:** Driver OOM for datasets > available driver heap; job failure

---

## SPL-D09-006 — .toPandas() on potentially large DataFrame
{: .d-inline-block }
CRITICAL
{: .label .label-red }

**`.toPandas()` without `limit()`/`filter()` materialises the entire DataFrame in driver memory.**

```python
# Before
pdf = df.join(other, 'id').toPandas()

# After
pdf = df.join(other, 'id').limit(50_000).toPandas()
```

**Impact:** Driver OOM proportional to total dataset size; entire data in one process

---

## SPL-D09-007 — .count() used for emptiness check
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`df.count()` in a boolean comparison triggers a full table scan.**

```python
# Before
if df.count() == 0:
    raise ValueError('No data found')

# After — Spark 3.3+
if df.isEmpty:
    raise ValueError('No data found')
# Or for older Spark:
if df.head(1) == []:
    raise ValueError('No data found')
```

**Impact:** Full table scan to answer a yes/no question; O(N) instead of O(1)

---

## SPL-D09-008 — .show() in production code
{: .d-inline-block }
INFO
{: .label .label-blue }

**`.show()` triggers a materialisation and prints to stdout — remove before production.**

```python
# Before
df.groupBy('status').count().show()

# After
result = df.groupBy('status').count()
logger.info('status counts: %s', result.collect())
```

**Impact:** Wasted query execution; output lost in production log redirection

---

## SPL-D09-009 — .explain() or .printSchema() in production code
{: .d-inline-block }
INFO
{: .label .label-blue }

**`.explain()` and `.printSchema()` are debugging tools — remove before production.**

```python
# Before
df.join(other, 'id').explain(True)
df.printSchema()

# After
# Remove entirely, or guard with a debug flag:
if os.getenv('SPARK_DEBUG'):
    df.explain(True)
```

**Impact:** Debugging output in production logs; extra driver analysis overhead

---

## SPL-D09-010 — .rdd conversion dropping out of DataFrame API
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**Accessing `.rdd` deserialises columnar DataFrame rows to Python `Row` objects.**

Dropping to the RDD API bypasses Catalyst optimisations and Tungsten
code-generation. Almost every RDD pattern has a DataFrame equivalent.

```python
# Before
result = df.rdd.map(lambda r: (r['id'], r['val'] * 2)).toDF()

# After
result = df.select(col('id'), (col('val') * 2).alias('val'))
```

**Impact:** Columnar→row deserialisation; disables Catalyst and Tungsten optimisations

---

## SPL-D09-011 — pandas_udf without type annotations
{: .d-inline-block }
INFO
{: .label .label-blue }

**`@pandas_udf` without parameter/return type hints relies on runtime type inference.**

```python
# Before
@pandas_udf(returnType=DoubleType())
def my_udf(col):
    return col * 1.5

# After
import pandas as pd

@pandas_udf('double')
def my_udf(col: pd.Series) -> pd.Series:
    return col * 1.5
```

**Impact:** Potential type inference errors; deprecated legacy API usage

---

## SPL-D09-012 — Nested UDF calls
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**A UDF calls another UDF function in its body — each UDF adds a separate serialisation boundary.**

```python
# Before
@udf(returnType=StringType())
def inner(s):
    return s.strip()

@udf(returnType=StringType())
def outer(s):
    return inner(s).lower()  # calls inner UDF

# After
def _strip(s):  # plain Python helper — no @udf overhead
    return s.strip()

@udf(returnType=StringType())
def outer(s):
    return _strip(s).lower()
```

**Impact:** Inner UDF bypasses Spark planner when called inside outer UDF; confusing execution model
