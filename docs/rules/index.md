---
layout: default
title: Rules
nav_order: 4
has_children: true
---

# Rules Reference
{: .no_toc }

93 rules across 11 dimensions. Each rule has a unique ID (`SPL-D{NN}-{NNN}`),
a severity level, and actionable before/after examples.

## Severity levels

| Badge | Level | Meaning |
|-------|-------|---------|
| 🔴 CRITICAL | Blocks commit | Major performance killer or correctness risk |
| 🟡 WARNING | Should fix | Significant performance impact in most workloads |
| 🔵 INFO | Consider fixing | Best-practice improvement, situational |

---

## All dimensions

| Dimension | Topic | Rules | Link |
|-----------|-------|------:|------|
| D01 | Cluster Configuration | 10 | [View →]({{ site.baseurl }}/rules/d01-cluster-config) |
| D02 | Shuffle & Sort | 8 | [View →]({{ site.baseurl }}/rules/d02-shuffle) |
| D03 | Joins | 10 | [View →]({{ site.baseurl }}/rules/d03-joins) |
| D04 | Partitioning | 10 | [View →]({{ site.baseurl }}/rules/d04-partitioning) |
| D05 | Data Skew | 7 | [View →]({{ site.baseurl }}/rules/d05-skew) |
| D06 | Caching & Persistence | 8 | [View →]({{ site.baseurl }}/rules/d06-caching) |
| D07 | I/O & File Formats | 10 | [View →]({{ site.baseurl }}/rules/d07-io-format) |
| D08 | Adaptive Query Execution | 7 | [View →]({{ site.baseurl }}/rules/d08-aqe) |
| D09 | UDFs & Code Patterns | 12 | [View →]({{ site.baseurl }}/rules/d09-udf-code) |
| D10 | Catalyst Optimizer | 6 | [View →]({{ site.baseurl }}/rules/d10-catalyst) |
| D11 | Monitoring & Observability | 5 | [View →]({{ site.baseurl }}/rules/d11-monitoring) |

---

## Complete rules table

| Rule ID | Severity | Name |
|---------|----------|------|
| SPL-D01-001 | WARNING | Missing Kryo serializer |
| SPL-D01-002 | WARNING | Executor memory not configured |
| SPL-D01-003 | WARNING | Driver memory not configured |
| SPL-D01-004 | INFO | Dynamic allocation not enabled |
| SPL-D01-005 | WARNING | Executor cores set too high |
| SPL-D01-006 | WARNING | Memory overhead too low |
| SPL-D01-007 | WARNING | Missing PySpark worker memory config |
| SPL-D01-008 | WARNING | Network timeout too low |
| SPL-D01-009 | INFO | Speculation not enabled |
| SPL-D01-010 | WARNING | Default shuffle partitions unchanged |
| SPL-D02-001 | CRITICAL | groupByKey() instead of reduceByKey() |
| SPL-D02-002 | WARNING | Default shuffle partitions unchanged |
| SPL-D02-003 | WARNING | Unnecessary repartition before join |
| SPL-D02-004 | WARNING | orderBy/sort without limit |
| SPL-D02-005 | INFO | distinct() without prior filter |
| SPL-D02-006 | INFO | Shuffle followed by coalesce |
| SPL-D02-007 | WARNING | Multiple shuffles in sequence |
| SPL-D02-008 | INFO | Shuffle file buffer too small |
| SPL-D03-001 | CRITICAL | Cross join / cartesian product |
| SPL-D03-002 | WARNING | Missing broadcast hint on small DataFrame |
| SPL-D03-003 | CRITICAL | Broadcast threshold disabled |
| SPL-D03-004 | WARNING | Join without prior filter/select |
| SPL-D03-005 | WARNING | Join key type mismatch risk |
| SPL-D03-006 | INFO | Multiple joins without intermediate repartition |
| SPL-D03-007 | INFO | Self-join that could be window function |
| SPL-D03-008 | CRITICAL | Join inside loop |
| SPL-D03-009 | INFO | Left join without null handling |
| SPL-D03-010 | WARNING | CBO/statistics not enabled for complex joins |
| SPL-D04-001 | CRITICAL | repartition(1) bottleneck |
| SPL-D04-002 | WARNING | coalesce(1) bottleneck |
| SPL-D04-003 | WARNING | Repartition with very high partition count |
| SPL-D04-004 | WARNING | Repartition with very low partition count |
| SPL-D04-005 | INFO | Coalesce before write — unbalanced output files |
| SPL-D04-006 | INFO | Missing partitionBy on write |
| SPL-D04-007 | WARNING | Over-partitioning — high-cardinality partition column |
| SPL-D04-008 | INFO | Missing bucketBy for repeatedly joined tables |
| SPL-D04-009 | WARNING | Partition column not used in query filters |
| SPL-D04-010 | WARNING | Repartition by column different from join key |
| SPL-D05-001 | WARNING | Join on low-cardinality column |
| SPL-D05-002 | WARNING | GroupBy on low-cardinality column without secondary key |
| SPL-D05-003 | WARNING | AQE skew join handling disabled |
| SPL-D05-004 | INFO | AQE skew threshold too high |
| SPL-D05-005 | INFO | Missing salting pattern for known skewed keys |
| SPL-D05-006 | WARNING | Window function partitioned by skew-prone column |
| SPL-D05-007 | INFO | Null-heavy join key |
| SPL-D06-001 | WARNING | cache() without unpersist() |
| SPL-D06-002 | WARNING | cache() used only once |
| SPL-D06-003 | CRITICAL | cache() inside loop |
| SPL-D06-004 | WARNING | cache() before filter |
| SPL-D06-005 | INFO | MEMORY_ONLY storage level for potentially large datasets |
| SPL-D06-006 | WARNING | Reused DataFrame without cache |
| SPL-D06-007 | INFO | cache() after repartition |
| SPL-D06-008 | INFO | checkpoint vs cache misuse |
| SPL-D07-001 | WARNING | CSV/JSON used for analytical workload |
| SPL-D07-002 | WARNING | Schema inference enabled |
| SPL-D07-003 | WARNING | select("*") — no column pruning |
| SPL-D07-004 | INFO | Filter applied after join — missing predicate pushdown |
| SPL-D07-005 | INFO | Small file problem on write |
| SPL-D07-006 | CRITICAL | JDBC read without partition parameters |
| SPL-D07-007 | INFO | Parquet compression not set |
| SPL-D07-008 | INFO | Write mode not specified |
| SPL-D07-009 | INFO | No format specified on read/write |
| SPL-D07-010 | INFO | mergeSchema enabled without necessity |
| SPL-D08-001 | CRITICAL | AQE disabled |
| SPL-D08-002 | WARNING | AQE coalesce partitions disabled |
| SPL-D08-003 | INFO | AQE advisory partition size too small |
| SPL-D08-004 | WARNING | AQE skew join disabled with skew-prone joins detected |
| SPL-D08-005 | INFO | AQE skew factor too aggressive |
| SPL-D08-006 | INFO | AQE local shuffle reader disabled |
| SPL-D08-007 | INFO | Manual shuffle partition count set high with AQE enabled |
| SPL-D09-001 | WARNING | Python UDF (row-at-a-time) detected |
| SPL-D09-002 | WARNING | Python UDF replaceable with native Spark function |
| SPL-D09-003 | CRITICAL | withColumn() inside loop |
| SPL-D09-004 | CRITICAL | Row-by-row iteration over DataFrame |
| SPL-D09-005 | CRITICAL | .collect() without prior filter or limit |
| SPL-D09-006 | CRITICAL | .toPandas() on potentially large DataFrame |
| SPL-D09-007 | WARNING | .count() used for emptiness check |
| SPL-D09-008 | INFO | .show() in production code |
| SPL-D09-009 | INFO | .explain() or .printSchema() in production code |
| SPL-D09-010 | WARNING | .rdd conversion dropping out of DataFrame API |
| SPL-D09-011 | INFO | pandas_udf without type annotations |
| SPL-D09-012 | WARNING | Nested UDF calls |
| SPL-D10-001 | WARNING | UDF blocks predicate pushdown |
| SPL-D10-002 | WARNING | CBO not enabled for complex queries |
| SPL-D10-003 | INFO | Join reordering disabled for multi-table joins |
| SPL-D10-004 | INFO | Table statistics not collected |
| SPL-D10-005 | INFO | Non-deterministic function in filter |
| SPL-D10-006 | INFO | Deep method chain (>20 chained ops) |
| SPL-D11-001 | INFO | No explain() for plan validation in test file |
| SPL-D11-002 | INFO | No Spark listener configured |
| SPL-D11-003 | INFO | No metrics logging in long-running job |
| SPL-D11-004 | INFO | Missing error handling around Spark actions |
| SPL-D11-005 | INFO | Hardcoded storage path |
