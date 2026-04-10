---
layout: default
title: D11 Monitoring
nav_order: 11
parent: Rules
---

# D11 — Monitoring & Observability
{: .no_toc }

5 rules for production-readiness. A Spark job that works in testing but lacks
observability, error handling, and path parameterisation is a production
incident waiting to happen.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## SPL-D11-001 — No explain() for plan validation in test file
{: .d-inline-block }
INFO
{: .label .label-blue }

**Test file contains complex Spark operations but no `explain()` call for plan validation.**

`explain()` in tests can catch plan regressions — a join that was a
`BroadcastHashJoin` silently becoming a `SortMergeJoin` after a code change,
for example. Adding plan assertions makes performance regressions visible
before production.

```python
# Before — test_etl.py
def test_join():
    result = users.join(events, 'user_id').groupBy('status').count()
    assert result.count() > 0

# After
def test_join():
    result = users.join(events, 'user_id').groupBy('status').count()
    plan = result._jdf.queryExecution().simpleString()
    assert 'BroadcastHashJoin' in plan, 'Expected broadcast join — did the table get large?'
    assert result.count() > 0
```

**Impact:** Plan regressions (join strategy, pushdown) invisible until production

---

## SPL-D11-002 — No Spark listener configured
{: .d-inline-block }
INFO
{: .label .label-blue }

**SparkSession is created without registering a listener — job metrics and query events are not captured.**

Spark listeners receive real-time events for every stage, task, and SQL query.
Without a listener, you're flying blind on job performance in production.

```python
# Before
spark = SparkSession.builder.appName('etl').getOrCreate()

# After
spark = (SparkSession.builder
    .appName('etl')
    .config('spark.extraListeners', 'com.company.MetricsListener')
    .getOrCreate())
```

**Impact:** Blind to job performance degradation; metrics only available post-mortem via Spark UI

---

## SPL-D11-003 — No metrics logging in long-running job
{: .d-inline-block }
INFO
{: .label .label-blue }

**File has 2+ Spark write operations but no Python logging — job progress and row counts are invisible.**

```python
# Before
enriched = raw.join(meta, 'id').withColumn('score', compute(col('v')))
enriched.write.parquet(output_path)

# After
import logging
logger = logging.getLogger(__name__)

enriched = raw.join(meta, 'id').withColumn('score', compute(col('v')))
count = enriched.count()
logger.info('enriched: %d rows', count)
enriched.write.parquet(output_path)
logger.info('write complete: %s', output_path)
```

**Impact:** Silent data drops and slow stages invisible without post-mortem log analysis

---

## SPL-D11-004 — Missing error handling around Spark actions
{: .d-inline-block }
INFO
{: .label .label-blue }

**Spark action calls (`count`, `collect`, `write`, …) are not wrapped in try/except.**

Unhandled exceptions in Spark actions can cause partial writes, leave
checkpoints in an inconsistent state, and produce cryptic JVM stack traces
with no business context.

```python
# Before
df = spark.read.parquet(input_path)
result = df.join(other, 'id')
result.write.parquet(output_path)

# After
try:
    df = spark.read.parquet(input_path)
    result = df.join(other, 'id')
    result.write.parquet(output_path)
    logger.info('Pipeline complete')
except Exception as e:
    logger.error('Pipeline failed: %s', e)
    raise
```

**Impact:** Unhandled driver crashes; partial writes; no error context in logs

---

## SPL-D11-005 — Hardcoded storage path
{: .d-inline-block }
INFO
{: .label .label-blue }

**A Spark read/write uses a hardcoded string path — paths should be passed as parameters.**

Hardcoded paths couple the job to a specific environment, make it impossible
to test safely, and put production paths in source control.

```python
# Before
df = spark.read.parquet('s3://prod-data/events/2024-01/')
df.write.parquet('s3://prod-output/enriched/')

# After
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--input-path', required=True)
parser.add_argument('--output-path', required=True)
args = parser.parse_args()

df = spark.read.parquet(args.input_path)
df.write.parquet(args.output_path)
```

**Impact:** Environment coupling; production paths in source control; untestable
