---
layout: default
title: D08 AQE
nav_order: 8
parent: Rules
---

# D08 — Adaptive Query Execution
{: .no_toc }

7 rules covering AQE configuration. AQE (enabled by default in Spark 3.2+)
dynamically re-optimises queries at runtime based on real partition statistics.
Disabling any part of it gives up free performance.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## SPL-D08-001 — AQE disabled
{: .d-inline-block }
CRITICAL
{: .label .label-red }

**`spark.sql.adaptive.enabled = false` disables all AQE optimisations.**

AQE provides three major features for free: dynamic partition coalescing
(merges tiny post-shuffle partitions), skew join handling (splits oversized
partitions), and dynamic join strategy switching (broadcasts small tables
discovered at runtime). Disabling it opts out of all three.

```python
# Before
.config("spark.sql.adaptive.enabled", "false")

# After
# Remove the line — AQE is enabled by default in Spark 3.2+
```

**Impact:** Loses automatic partition coalescing, skew handling, and join strategy switching

---

## SPL-D08-002 — AQE coalesce partitions disabled
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`spark.sql.adaptive.coalescePartitions.enabled = false` prevents AQE from merging tiny partitions.**

After a shuffle with 1000+ initial partitions, many partitions may contain only
a few KB. AQE coalesces these into larger tasks. Disabling this creates hundreds
of tiny tasks with high scheduling overhead.

```python
# Before
.config("spark.sql.adaptive.coalescePartitions.enabled", "false")

# After
# Remove the line — coalescing is enabled by default with AQE
```

**Impact:** Hundreds of tiny tasks per stage; driver scheduling overhead

---

## SPL-D08-003 — AQE advisory partition size too small
{: .d-inline-block }
INFO
{: .label .label-blue }

**`advisoryPartitionSizeInBytes` below 16 MB causes AQE to coalesce into too many small partitions.**

```python
# Before
.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "4m")

# After
.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")  # default
```

**Impact:** Too many small post-coalesce partitions; task scheduling overhead

---

## SPL-D08-004 — AQE skew join disabled with skew-prone joins detected
{: .d-inline-block }
WARNING
{: .label .label-yellow }

**`spark.sql.adaptive.skewJoin.enabled = false` is set while the file contains joins.**

AQE skew join handling splits partitions larger than the skew threshold
automatically at runtime, without requiring manual salting.

```python
# Before
.config("spark.sql.adaptive.skewJoin.enabled", "false")
# ...
df.join(events, 'user_id')

# After
# Remove the skewJoin.enabled = false line
df.join(events, 'user_id')  # AQE handles skew automatically
```

**Impact:** Skewed partitions on joins not auto-split; straggler tasks persist

---

## SPL-D08-005 — AQE skew factor too aggressive
{: .d-inline-block }
INFO
{: .label .label-blue }

**`skewedPartitionFactor` below 2 treats nearly every above-median partition as skewed.**

A factor of 1 means any partition larger than the median is considered skewed
and gets split — even for balanced data. This causes unnecessary splits and
higher task counts.

```python
# Before
.config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "1")

# After
.config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")  # default
```

**Impact:** Excessive partition splits for balanced data; higher task count

---

## SPL-D08-006 — AQE local shuffle reader disabled
{: .d-inline-block }
INFO
{: .label .label-blue }

**`spark.sql.adaptive.localShuffleReader.enabled = false` forces remote shuffle reads.**

When a broadcast join replaces a sort-merge join at runtime, the local shuffle
reader allows executors to read shuffle files locally without going through the
network. Disabling it forces remote reads even for local data.

```python
# Before
.config("spark.sql.adaptive.localShuffleReader.enabled", "false")

# After
# Remove the line — local shuffle reader is enabled by default
```

**Impact:** Unnecessary remote shuffle reads; extra network I/O even for local data

---

## SPL-D08-007 — Manual shuffle partition count set high with AQE enabled
{: .d-inline-block }
INFO
{: .label .label-blue }

**`spark.sql.shuffle.partitions` set high (> 400) with AQE enabled.**

With AQE enabled, the initial partition count controls map-side overhead. AQE
will coalesce small partitions after the shuffle. Setting a very high initial
count adds unnecessary map-side cost proportional to the partition count.

```python
# Before
.config("spark.sql.shuffle.partitions", "2000")

# After
.config("spark.sql.shuffle.partitions", "200")  # AQE coalesces as needed
# Tune advisoryPartitionSizeInBytes instead:
.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
```

**Impact:** Unnecessary map-side overhead proportional to initial partition count
