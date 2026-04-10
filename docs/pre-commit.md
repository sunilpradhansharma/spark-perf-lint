---
layout: default
title: Pre-Commit
nav_order: 7
---

# Pre-Commit Integration
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

spark-perf-lint ships as a [pre-commit](https://pre-commit.com) hook. It runs
automatically on every `git commit`, scanning only the staged `.py` files. If
any CRITICAL finding is detected, the commit is blocked and the developer sees
an actionable error message.

---

## Quick setup

### 1. Install pre-commit

```bash
pip install pre-commit
```

### 2. Add the hook

Create or edit `.pre-commit-config.yaml` at your repo root:

```yaml
repos:
  - repo: https://github.com/sunilpradhansharma/spark-perf-lint
    rev: v0.1.0
    hooks:
      - id: spark-perf-lint
```

### 3. Install the hook

```bash
pre-commit install
```

### 4. Test it

```bash
pre-commit run spark-perf-lint --all-files
```

---

## Hook configuration

### Default behaviour

The hook scans each staged `.py` file and exits non-zero if any CRITICAL finding
is found. All 93 rules are enabled with their default severities.

### Block on WARNING too

```yaml
repos:
  - repo: https://github.com/sunilpradhansharma/spark-perf-lint
    rev: v0.1.0
    hooks:
      - id: spark-perf-lint
        args: [--fail-on, WARNING]
```

### Quiet mode (errors only)

```yaml
hooks:
  - id: spark-perf-lint
    args: [--quiet]
```

### Custom config file

```yaml
hooks:
  - id: spark-perf-lint
    args: [--config, path/to/custom.yaml]
```

### Exclude paths

Use pre-commit's built-in `exclude` pattern:

```yaml
hooks:
  - id: spark-perf-lint
    exclude: ^(tests/fixtures/|notebooks/)
```

---

## Project-level config for pre-commit

Place `.spark-perf-lint.yaml` at your repo root to customise rule behaviour
without changing hook arguments:

```yaml
# .spark-perf-lint.yaml
general:
  fail_on_severity: CRITICAL

rules:
  disabled:
    - SPL-D11-001   # no explain() in test files — too noisy in pre-commit

severity_override:
  SPL-D01-004: INFO  # dynamic allocation not always available in dev

thresholds:
  broadcast_threshold_mb: 50   # our small tables are up to 50 MB
```

---

## Suppressing false positives

### Suppress a single line

```python
# This crossJoin is intentional — salting pattern for skew mitigation
df_salted = df.crossJoin(spark.range(N).toDF('salt'))  # noqa: SPL-D03-001
```

### Suppress multiple rules on one line

```python
df.collect()  # noqa: SPL-D09-004, SPL-D09-005
```

### Suppress in a test fixture

```python
# tests/fixtures/bad_code/bad_joins.py
df.crossJoin(other)  # noqa: SPL-D03-001  -- intentional bad pattern for test
```

---

## Advanced: local repo (monorepo / offline)

For monorepos or air-gapped environments, point to your local checkout:

```yaml
repos:
  - repo: local
    hooks:
      - id: spark-perf-lint
        name: spark-perf-lint
        language: system
        entry: spark-perf-lint scan
        types: [python]
        pass_filenames: true
```

This requires `spark-perf-lint` to be installed in the system/virtual env that
pre-commit uses (`language: system` uses the active environment).

---

## CI: run pre-commit in GitHub Actions

```yaml
# .github/workflows/pre-commit.yml
name: Pre-commit
on: [push, pull_request]

jobs:
  pre-commit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - uses: pre-commit/action@v3.0.1
```

This runs all hooks including spark-perf-lint against every changed file on
every push and PR.

---

## Bypass for emergencies

```bash
git commit --no-verify -m "hotfix: emergency deploy"
```

{: .warning }
Use `--no-verify` only for genuine emergencies. The hook exists to protect
production. Suppressed findings should be addressed in the next commit.

---

## Troubleshooting

### Hook not blocking on bad code

Check the fail threshold in `.spark-perf-lint.yaml`. The default blocks only on
CRITICAL. If your finding is WARNING, add `args: [--fail-on, WARNING]`.

### `rev: HEAD` fails for local repos

Pre-commit requires a concrete SHA or tag for `rev`. Use:

```bash
git rev-parse HEAD  # get current SHA
```

Then use that SHA as the `rev` value.

### Hook is slow

By default the hook uses parallel file scanning. If you have thousands of
Python files in the repo that are not PySpark, add an `exclude` pattern to
skip them:

```yaml
hooks:
  - id: spark-perf-lint
    exclude: ^(frontend/|scripts/|setup\.py)
```
