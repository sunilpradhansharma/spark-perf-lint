---
layout: default
title: Home
nav_order: 1
---

<div class="hero">
  <h1>⚡ spark-perf-lint</h1>
  <p class="tagline">Enterprise-grade Apache Spark performance linter — catches anti-patterns
  before they reach production, explains <em>why</em> they hurt, and hands you a concrete fix.</p>
  <a href="getting-started" class="btn btn-primary fs-5 mb-4 mb-md-0 mr-2">Get Started</a>
  <a href="https://github.com/sunilpradhansharma/spark-perf-lint" class="btn fs-5 mb-4 mb-md-0">View on GitHub</a>
</div>

<div class="feature-cards">
  <div class="feature-card">
    <div class="number">93</div>
    <div>Performance Rules</div>
  </div>
  <div class="feature-card">
    <div class="number">11</div>
    <div>Dimensions</div>
  </div>
  <div class="feature-card">
    <div class="number">3</div>
    <div>Tiers</div>
  </div>
</div>

## Quick Start

```bash
pip install spark-perf-lint
spark-perf-lint scan src/
```

## What it catches

| Dimension | Examples |
|-----------|----------|
| Joins | Cross joins, missing broadcast hints, skewed keys |
| Shuffles | groupByKey, unnecessary repartition, sort without limit |
| Caching | cache without unpersist, cache used once |
| I/O | CSV for analytics, JDBC without partitions |
| UDFs | Python UDFs, withColumn loops, collect without filter |

[See all 93 rules →](rules/)
