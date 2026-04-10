---
layout: default
title: Configuration
nav_order: 6
---

# Configuration
{: .no_toc }

Complete reference for all configuration options.

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## How configuration works

spark-perf-lint uses a **four-layer configuration stack**:

```
Priority (lowest → highest)

 1. Built-in defaults       — hardcoded in the tool; always applied
 2. Project YAML file       — .spark-perf-lint.yaml, auto-discovered
 3. Environment variables   — SPARK_PERF_LINT_* prefixed
 4. CLI arguments           — flags passed at invocation time
```

Layers are merged recursively — setting one key does not wipe out others.

**File discovery:** When you run `spark-perf-lint scan`, the tool walks up the
directory tree from the scanned path, stopping at the git root, and loads the
first `.spark-perf-lint.yaml` it finds.

---

## Generate a starter config

```bash
spark-perf-lint init
```

Creates `.spark-perf-lint.yaml` in the current directory with all options
documented inline.

---

## Full config reference

```yaml
# .spark-perf-lint.yaml

# ── General ────────────────────────────────────────────────────────────────
general:
  # Minimum severity that causes spark-perf-lint to exit non-zero.
  # Values: CRITICAL | WARNING | INFO
  fail_on_severity: CRITICAL

  # Only report findings at or above this severity.
  # Values: CRITICAL | WARNING | INFO
  min_severity: INFO

  # Maximum number of findings to report (0 = unlimited).
  max_findings: 0

  # File encoding for reading source files.
  encoding: utf-8


# ── Rules ──────────────────────────────────────────────────────────────────
rules:
  # Completely disable specific rules by ID.
  disabled:
    # - SPL-D11-001

  # Enable only specific rules (all others are disabled).
  # enabled_only:
  #   - SPL-D01-001
  #   - SPL-D03-001

  # Run only rules in specific dimensions.
  # dimensions:
  #   - D01
  #   - D03


# ── Severity overrides ─────────────────────────────────────────────────────
# Change the default severity of any rule.
severity_override:
  # SPL-D01-004: INFO        # downgrade dynamic-allocation hint to INFO
  # SPL-D09-008: WARNING     # upgrade show() in production to WARNING


# ── Thresholds ─────────────────────────────────────────────────────────────
thresholds:
  # DataFrames smaller than this are expected to be broadcast.
  broadcast_threshold_mb: 10

  # repartition() counts outside this range trigger warnings.
  partition_count_min: 10
  partition_count_max: 10000

  # spark.executor.cores above this triggers SPL-D01-005.
  executor_cores_max: 5

  # spark.executor.memoryOverhead below this triggers SPL-D01-006.
  memory_overhead_min_mb: 384

  # spark.network.timeout below this triggers SPL-D01-008.
  network_timeout_min_s: 120

  # Number of sequential shuffle operations that triggers SPL-D02-007.
  shuffle_chain_length: 2

  # Number of joins without broadcast/repartition that triggers SPL-D03-006.
  join_chain_length: 3

  # withColumn() loop iteration threshold for SPL-D09-003.
  withcolumn_loop_threshold: 1

  # Chain depth above which SPL-D10-006 fires.
  method_chain_depth: 20


# ── Ignore patterns ────────────────────────────────────────────────────────
ignore:
  paths:
    - "tests/fixtures/**"
    - "notebooks/**"
    - "**/*_generated.py"
  files:
    - "conftest.py"
    - "setup.py"


# ── Spark config audit ─────────────────────────────────────────────────────
spark_config_audit:
  enabled: true
  extra_forbidden_configs: {}


# ── LLM (Tier 2) ───────────────────────────────────────────────────────────
llm:
  enabled: false
  provider: anthropic
  model: claude-opus-4-6
  max_tokens: 2048
  min_findings_for_llm: 1
  # api_key: sk-ant-...


# ── Observability ──────────────────────────────────────────────────────────
observability:
  enabled: true
  trace_dir: .spark-perf-lint-traces
  max_traces: 100
```

---

## Environment variables

| Variable | Equivalent config | Example |
|----------|-------------------|---------|
| `SPARK_PERF_LINT_FAIL_ON` | `general.fail_on_severity` | `CRITICAL` |
| `SPARK_PERF_LINT_MIN_SEVERITY` | `general.min_severity` | `WARNING` |
| `SPARK_PERF_LINT_CONFIG` | Path to YAML config | `/etc/spark-lint.yaml` |
| `ANTHROPIC_API_KEY` | `llm.api_key` | `sk-ant-...` |

---

## CLI flags

```
spark-perf-lint scan [OPTIONS] PATH...

Options:
  --format [terminal|json|markdown|github]   Output format (default: terminal)
  --output FILE                              Write output to file
  --min-severity [CRITICAL|WARNING|INFO]     Minimum severity to report
  --fail-on [CRITICAL|WARNING|INFO]          Exit non-zero on this severity
  --config FILE                              Path to .spark-perf-lint.yaml
  --quiet                                    Suppress all output (exit code only)
  --no-color                                 Disable color in terminal output
  --tier2                                    Enable LLM Tier 2 analysis
```

---

## Common configurations

### Strict CI (fail on any warning)

```yaml
general:
  fail_on_severity: WARNING
  min_severity: WARNING
```

### Developer-friendly (CRITICAL only, ignore test fixtures)

```yaml
general:
  fail_on_severity: CRITICAL
ignore:
  paths:
    - "tests/**"
```

### Data science / notebook profile

```yaml
rules:
  disabled:
    - SPL-D09-005   # collect() without limit
    - SPL-D09-006   # toPandas()
    - SPL-D09-008   # show() in production
    - SPL-D09-009   # explain()
severity_override:
  SPL-D01-001: INFO
```

### Large-cluster profile

```yaml
thresholds:
  partition_count_max: 50000
  broadcast_threshold_mb: 200
  executor_cores_max: 8
```
