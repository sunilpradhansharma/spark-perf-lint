---
layout: default
title: Home
nav_order: 1
description: "Enterprise-grade Apache Spark performance linter — 93 rules, 11 dimensions, 3 tiers"
permalink: /
---

# ⚡ spark-perf-lint
{: .fs-9 }

Enterprise-grade Apache Spark performance linter
{: .fs-6 .fw-300 }

Catches anti-patterns before they reach production, explains *why* they hurt,
and hands you a concrete fix.
{: .fs-5 }

[Get Started]({{ site.baseurl }}/getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/sunilpradhansharma/spark-perf-lint){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## Why spark-perf-lint?

PySpark jobs are easy to write and hard to tune. Small mistakes — an unbroadcast
join, a default shuffle partition count, a Python UDF where a native function
would do — compound into hours of wasted cluster time. spark-perf-lint finds
those mistakes statically, before the job ever runs.

---

## Feature highlights

<div style="display: flex; gap: 1.5rem; flex-wrap: wrap; margin: 1.5rem 0;">

<div style="flex: 1; min-width: 180px; border: 1px solid #444; border-radius: 8px; padding: 1.25rem; text-align: center;">
  <div style="font-size: 2.5rem; font-weight: 700; color: #7c3aed;">93</div>
  <div style="font-size: 1.1rem; font-weight: 600;">Rules</div>
  <div style="font-size: 0.85rem; color: #aaa; margin-top: 0.4rem;">CRITICAL · WARNING · INFO</div>
</div>

<div style="flex: 1; min-width: 180px; border: 1px solid #444; border-radius: 8px; padding: 1.25rem; text-align: center;">
  <div style="font-size: 2.5rem; font-weight: 700; color: #7c3aed;">11</div>
  <div style="font-size: 1.1rem; font-weight: 600;">Dimensions</div>
  <div style="font-size: 0.85rem; color: #aaa; margin-top: 0.4rem;">Config · Shuffle · Joins · I/O · …</div>
</div>

<div style="flex: 1; min-width: 180px; border: 1px solid #444; border-radius: 8px; padding: 1.25rem; text-align: center;">
  <div style="font-size: 2.5rem; font-weight: 700; color: #7c3aed;">3</div>
  <div style="font-size: 1.1rem; font-weight: 600;">Tiers</div>
  <div style="font-size: 0.85rem; color: #aaa; margin-top: 0.4rem;">Pre-commit · CI/LLM · Runtime</div>
</div>

</div>

---

## Quick start

**Install and run your first scan in 30 seconds:**

```bash
# Install
pip install spark-perf-lint

# Scan your PySpark code
spark-perf-lint scan src/

# List all rules
spark-perf-lint rules

# Explain a specific rule
spark-perf-lint explain SPL-D03-001
```

**Add to pre-commit:**

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/sunilpradhansharma/spark-perf-lint
    rev: v0.1.0
    hooks:
      - id: spark-perf-lint
```

---

## Three tiers of analysis

| Tier | When | What |
|------|------|------|
| **Tier 1** — Pre-commit | Every commit | Fast AST analysis, zero Spark dependency, blocks bad commits |
| **Tier 2** — CI / LLM | Pull requests | Claude-powered context-aware recommendations with full file understanding |
| **Tier 3** — Deep audit | Release gates | Live Spark runtime — physical plan analysis and benchmark comparisons |

---

## The 11 dimensions

| # | Dimension | Rules | Key patterns caught |
|---|-----------|------:|---------------------|
| D01 | [Cluster Config]({{ site.baseurl }}/rules/d01-cluster-config) | 10 | Missing Kryo, wrong executor sizing, shuffle partition count |
| D02 | [Shuffle & Sort]({{ site.baseurl }}/rules/d02-shuffle) | 8 | groupByKey, redundant repartition, orderBy without limit |
| D03 | [Joins]({{ site.baseurl }}/rules/d03-joins) | 10 | Cross joins, missing broadcast, join inside loop |
| D04 | [Partitioning]({{ site.baseurl }}/rules/d04-partitioning) | 10 | repartition(1), over-partitioning, missing partitionBy |
| D05 | [Data Skew]({{ site.baseurl }}/rules/d05-skew) | 7 | Low-cardinality keys, missing salting, null-heavy joins |
| D06 | [Caching]({{ site.baseurl }}/rules/d06-caching) | 8 | cache without unpersist, cache inside loop, wrong storage level |
| D07 | [I/O & Formats]({{ site.baseurl }}/rules/d07-io-format) | 10 | CSV for analytics, schema inference, JDBC without partitioning |
| D08 | [AQE]({{ site.baseurl }}/rules/d08-aqe) | 7 | AQE disabled, coalesce disabled, skew join off |
| D09 | [UDFs & Code]({{ site.baseurl }}/rules/d09-udf-code) | 12 | Python UDFs, collect without limit, withColumn in loop |
| D10 | [Catalyst]({{ site.baseurl }}/rules/d10-catalyst) | 6 | CBO disabled, UDF blocks pushdown, deep chains |
| D11 | [Monitoring]({{ site.baseurl }}/rules/d11-monitoring) | 5 | No listeners, no logging, hardcoded paths |

---

## Sample output

```
╭─────────────────────────────────────────────────────────╮
│  spark-perf-lint scan results                           │
├─────────────────────────────────────────────────────────┤
│  Files scanned     12                                   │
│  Findings          7  (2 CRITICAL · 3 WARNING · 2 INFO) │
│  Scan time         0.3 s                                │
╰─────────────────────────────────────────────────────────╯

CRITICAL  SPL-D03-001  etl/orders.py:42
  Cross join / cartesian product
  crossJoin() creates an O(n²) row explosion.
  Fix: df.join(other, on='id', how='inner')

CRITICAL  SPL-D08-001  etl/orders.py:8
  AQE disabled
  spark.sql.adaptive.enabled = false disables all AQE optimisations.
  Fix: Remove the line — AQE is enabled by default in Spark 3.2+
```

---

## License

Free to use for any purpose including commercial. Credit to Sunil Pradhan Sharma required. See [LICENSE](https://github.com/sunilpradhansharma/spark-perf-lint/blob/main/LICENSE).
