---
layout: default
title: Getting Started
nav_order: 2
---

# Getting Started
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Installation

### Requirements

- Python 3.10 or later
- No Spark installation required for Tier 1

### pip

```bash
pip install spark-perf-lint
```

Verify:

```bash
spark-perf-lint version
# spark-perf-lint 0.1.0
```

### With LLM support (Tier 2)

```bash
pip install "spark-perf-lint[llm]"
```

This adds the `anthropic` SDK needed for Claude-powered analysis.

### Development install

```bash
git clone https://github.com/sunilpradhansharma/spark-perf-lint.git
cd spark-perf-lint
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

---

## First scan

### Scan a directory

```bash
spark-perf-lint scan src/
```

spark-perf-lint discovers every `.py` file that contains a PySpark import and
runs all 93 rules against it. Files without PySpark imports are skipped
automatically.

### Scan specific files

```bash
spark-perf-lint scan etl/orders.py etl/customers.py
```

### Output formats

```bash
# Rich terminal output (default)
spark-perf-lint scan src/

# Machine-readable JSON
spark-perf-lint scan src/ --format json --output findings.json

# Markdown (for PR comments)
spark-perf-lint scan src/ --format markdown

# GitHub PR annotations
spark-perf-lint scan src/ --format github
```

### Severity threshold

```bash
# Only show CRITICAL and WARNING
spark-perf-lint scan src/ --min-severity WARNING

# Only CRITICAL
spark-perf-lint scan src/ --min-severity CRITICAL
```

### Exit code for CI

```bash
# Non-zero exit if any CRITICAL finding
spark-perf-lint scan src/ --fail-on CRITICAL

# Non-zero exit if any WARNING or CRITICAL
spark-perf-lint scan src/ --fail-on WARNING
```

---

## Exploring rules

### List all rules

```bash
spark-perf-lint rules
```

### Filter by dimension

```bash
spark-perf-lint rules --dimension D03
```

### Get detailed documentation for a rule

```bash
spark-perf-lint explain SPL-D03-001
```

Output includes: description, explanation, before/after code examples,
estimated performance impact, effort level, and references.

---

## Configuration

### Generate a starter config

```bash
spark-perf-lint init
```

This creates `.spark-perf-lint.yaml` in the current directory with all options
documented. Edit it to customise behaviour for your project.

### Minimal config example

```yaml
# .spark-perf-lint.yaml
general:
  fail_on_severity: WARNING

thresholds:
  broadcast_threshold_mb: 50

rules:
  disabled:
    - SPL-D11-001   # suppress test-file explain() hint

severity_override:
  SPL-D01-004: INFO  # downgrade dynamic-allocation to INFO
```

### Suppress a specific line

Add a `# noqa: SPL-DXX-XXX` comment to the line that triggers the finding:

```python
df.crossJoin(other)  # noqa: SPL-D03-001  -- intentional for salting
```

---

## Pre-commit hook

### Install

Add to `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/sunilpradhansharma/spark-perf-lint
    rev: v0.1.0
    hooks:
      - id: spark-perf-lint
```

Then:

```bash
pre-commit install
```

### Test the hook

```bash
pre-commit run spark-perf-lint --all-files
```

The hook scans only staged `.py` files on each `git commit`. It exits non-zero
(blocking the commit) on any CRITICAL finding by default.

See the full [Pre-Commit guide]({{ site.baseurl }}/pre-commit) for advanced options.

---

## CI / GitHub Actions

### Quick integration

```yaml
# .github/workflows/spark-lint.yml
name: Spark Lint
on: [push, pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install spark-perf-lint
      - run: spark-perf-lint scan src/ --fail-on CRITICAL
```

See the full [CI/CD guide]({{ site.baseurl }}/ci-cd) for Tier 2 (LLM) integration.

---

## CLI reference

| Command | Description |
|---------|-------------|
| `spark-perf-lint scan PATH` | Scan files for anti-patterns |
| `spark-perf-lint rules` | List all rules |
| `spark-perf-lint explain RULE_ID` | Show full rule documentation |
| `spark-perf-lint init` | Create default config file |
| `spark-perf-lint traces` | Generate HTML observability report |
| `spark-perf-lint version` | Print version |

Run `spark-perf-lint COMMAND --help` for per-command options.
