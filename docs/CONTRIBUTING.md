---
layout: default
title: Contributing
nav_order: 9
---

# Contributing
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Development setup

### Prerequisites

- Python 3.10 or later
- Git
- (Optional) PySpark 3.3+ for Tier 3 benchmark tests

### Clone and install

```bash
git clone https://github.com/sunilpradhansharma/spark-perf-lint.git
cd spark-perf-lint
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### Verify

```bash
spark-perf-lint version       # spark-perf-lint 0.1.0
spark-perf-lint scan src/     # 0 findings
pytest -q                     # 1377 tests, all green
```

### Install pre-commit hooks

```bash
pre-commit install
pre-commit run --all-files
```

---

## Running tests

```bash
# All tests
pytest -q

# With coverage
pytest --cov=spark_perf_lint --cov-report=term-missing -q

# Single dimension
pytest tests/test_rules_d03_joins.py -v

# Real git workflow tests (~3 s extra)
pytest tests/test_precommit_integration.py::TestRealGitWorkflow -v

# Exclude slow tests
pytest -q -m "not slow"

# Lint the linter itself
spark-perf-lint scan src/
```

---

## Code style

```bash
black src/ tests/
ruff check src/ tests/ --fix
```

Conventions:

- **Type hints** on every public function and method signature
- **Google-style docstrings** on every class and public method
- **No PySpark import** anywhere in `src/spark_perf_lint/` except `tier3/`
- **`# noqa: SPL-DXX-XXX`** to suppress intentional anti-patterns in fixtures

---

## Adding a new rule

Rules live in `src/spark_perf_lint/rules/d{NN}_{name}.py`.

### Step 1 — pick an ID

Rule IDs follow `SPL-D{NN}-{NNN}`. Find the next available number in the
dimension file.

### Step 2 — implement the class

```python
from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.rules.base import CodeRule
from spark_perf_lint.types import Dimension, Severity, EffortLevel

@register_rule
class MyNewJoinRule(CodeRule):
    rule_id   = "SPL-D03-011"
    name      = "My new join anti-pattern"
    dimension = Dimension.D03_JOINS
    default_severity = Severity.WARNING
    description = "One-line description."
    explanation = "Multi-sentence explanation of why this pattern is harmful."
    recommendation_template = "Actionable fix — what to do instead."
    before_example = "df.join(other)  # missing key"
    after_example  = "df.join(other, on='id', how='inner')"
    estimated_impact = "3–10× slowdown"
    effort_level = EffortLevel.MINOR_CODE_CHANGE
    references = ["https://spark.apache.org/docs/latest/..."]

    def check(self, analyzer, config):
        if not self.is_enabled(config):
            return []
        # ... detection logic ...
        return []
```

### Step 3 — add tests

```python
class TestMyNewJoinRule:
    def test_fires_on_anti_pattern(self, tmp_path):
        code = "df.join(other)  # no key"
        findings = run_rule(MyNewJoinRule, code, tmp_path)
        assert len(findings) == 1
        assert findings[0].rule_id == "SPL-D03-011"

    def test_does_not_fire_on_correct_pattern(self, tmp_path):
        code = "df.join(other, on='id', how='inner')"
        findings = run_rule(MyNewJoinRule, code, tmp_path)
        assert findings == []
```

### Step 4 — verify

```bash
pytest tests/test_rules_d03_joins.py -v
spark-perf-lint rules --dimension D03
spark-perf-lint explain SPL-D03-011
```

The CLI auto-discovers new rules from the registry — no manual catalogue
entry required.

---

## Adding a new reporter

1. Create `src/spark_perf_lint/reporters/my_reporter.py` exporting
   `render(report: AuditReport, config: LintConfig) -> str`
2. Register the format name in `cli.py` (`_FORMAT_CHOICES`)
3. Wire it in the `scan` command's output dispatch block
4. Add tests in `tests/test_reporters.py`

---

## Submitting a pull request

1. Fork and create a feature branch from `main`
2. One logical change per PR
3. Ensure all checks pass:
   ```bash
   black src/ tests/
   ruff check src/ tests/
   pytest -q
   spark-perf-lint scan src/
   ```
4. Update `CHANGELOG.md` under `## [Unreleased]`
5. Open a PR against `main`

### Commit message format

```
type: short summary (≤ 72 chars)
```

Types: `feat`, `fix`, `docs`, `test`, `chore`, `refactor`, `perf`

---

## Release process

1. Update version in `pyproject.toml` and `src/spark_perf_lint/__init__.py`
2. Move `## [Unreleased]` to `## [X.Y.Z] — YYYY-MM-DD` in `CHANGELOG.md`
3. Commit: `chore: prepare vX.Y.Z release`
4. Tag: `git tag -a vX.Y.Z -m "vX.Y.Z"`
5. Push tag — `release.yml` CI builds and publishes to PyPI automatically
