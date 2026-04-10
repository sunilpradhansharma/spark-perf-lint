---
layout: default
title: CI/CD
nav_order: 8
---

# CI/CD Integration
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Tier 1 — Static analysis in CI

### Minimal workflow

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

### Upload findings as artefact

```yaml
- name: Scan
  run: spark-perf-lint scan src/ --format json --output findings.json

- name: Upload findings
  uses: actions/upload-artifact@v4
  with:
    name: spark-lint-findings
    path: findings.json
```

### Post findings as PR comment (Markdown)

```yaml
- name: Scan
  run: |
    spark-perf-lint scan src/ --format markdown --output findings.md
    echo "FINDINGS_FILE=findings.md" >> $GITHUB_ENV

- name: Comment on PR
  if: github.event_name == 'pull_request'
  uses: marocchino/sticky-pull-request-comment@v2
  with:
    path: findings.md
```

### GitHub inline PR annotations

```yaml
- name: Scan with annotations
  run: spark-perf-lint scan src/ --format github
```

This emits GitHub Actions [workflow commands](https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/workflow-commands-for-github-actions)
that appear as inline annotations on the PR diff.

---

## Tier 2 — LLM-powered CI analysis

Tier 2 sends Tier 1 findings to the Claude API for context-aware
recommendations. Requires an Anthropic API key.

### Setup

1. Add your API key as a repository secret: `ANTHROPIC_API_KEY`
2. Install with LLM extras: `pip install "spark-perf-lint[llm]"`

### Workflow with LLM enrichment

```yaml
name: Spark Lint (LLM)
on: [pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install
        run: pip install "spark-perf-lint[llm]"

      - name: Tier 1 scan
        run: spark-perf-lint scan src/ --format json --output tier1.json

      - name: Tier 2 LLM enrichment
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
        run: |
          spark-perf-lint scan src/ \
            --tier2 \
            --format markdown \
            --output enriched.md

      - name: Post PR comment
        uses: marocchino/sticky-pull-request-comment@v2
        with:
          path: enriched.md
```

---

## Reusable GitHub Action

spark-perf-lint ships a reusable composite action at
`.github/actions/spark-perf-lint/action.yml`:

```yaml
# In your repo's workflow:
- name: Spark Performance Lint
  uses: sunilpradhansharma/spark-perf-lint/.github/actions/spark-perf-lint@v0.1.0
  with:
    path: src/
    fail-on: CRITICAL
    format: github
    anthropic-api-key: ${{ secrets.ANTHROPIC_API_KEY }}  # optional, for Tier 2
```

### Action inputs

| Input | Default | Description |
|-------|---------|-------------|
| `path` | `.` | Path(s) to scan |
| `fail-on` | `CRITICAL` | Minimum severity that fails the workflow |
| `format` | `github` | Output format (`terminal`/`json`/`markdown`/`github`) |
| `config` | `` | Path to custom `.spark-perf-lint.yaml` |
| `anthropic-api-key` | `` | API key for Tier 2 LLM analysis (optional) |

---

## Full CI pipeline example

```yaml
name: PySpark CI
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  spark-lint:
    name: Spark Performance Lint
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install spark-perf-lint
        run: pip install "spark-perf-lint[llm]"

      - name: Tier 1 — static scan (block on CRITICAL)
        run: |
          spark-perf-lint scan src/ \
            --format github \
            --fail-on CRITICAL

      - name: Tier 1 — save JSON artefact
        if: always()
        run: spark-perf-lint scan src/ --format json --output findings.json

      - name: Upload findings
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: spark-lint-findings
          path: findings.json

      - name: Tier 2 — LLM enrichment (PR only)
        if: github.event_name == 'pull_request'
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
        run: |
          spark-perf-lint scan src/ --tier2 --format markdown --output enriched.md

      - name: Post enriched comment
        if: github.event_name == 'pull_request'
        uses: marocchino/sticky-pull-request-comment@v2
        with:
          header: spark-perf-lint
          path: enriched.md
```

---

## Environment variables

All CLI flags can also be set via environment variables with the
`SPARK_PERF_LINT_` prefix:

| Env var | CLI equivalent |
|---------|---------------|
| `SPARK_PERF_LINT_FAIL_ON` | `--fail-on` |
| `SPARK_PERF_LINT_MIN_SEVERITY` | `--min-severity` |
| `SPARK_PERF_LINT_FORMAT` | `--format` |
| `SPARK_PERF_LINT_CONFIG` | `--config` |
| `ANTHROPIC_API_KEY` | (Tier 2 API key) |
