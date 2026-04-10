# Contributing to spark-perf-lint

Thank you for your interest in contributing. This guide covers the development
setup, code conventions, testing workflow, and the process for submitting
changes.

---

## Table of contents

- [Development setup](#development-setup)
- [Project structure](#project-structure)
- [Running tests](#running-tests)
- [Code style](#code-style)
- [Adding a new rule](#adding-a-new-rule)
- [Adding a new reporter](#adding-a-new-reporter)
- [Submitting a pull request](#submitting-a-pull-request)
- [Release process](#release-process)

---

## Development setup

### Prerequisites

- Python 3.10 or later
- Git
- (Optional) PySpark 3.3+ for Tier 3 benchmark tests

### Clone and install

```bash
git clone https://github.com/sucandra-dasa/spark-perf-lint.git
cd spark-perf-lint

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate          # macOS/Linux
# .venv\Scripts\activate           # Windows

# Install the package in editable mode with all dev dependencies
pip install -e ".[dev]"
```

### Verify the installation

```bash
spark-perf-lint version            # spark-perf-lint 0.1.0
spark-perf-lint scan src/ --quiet  # should produce 0 findings
pytest -q                          # 1377 tests, all green
```

### Install the pre-commit hooks (optional but recommended)

```bash
pre-commit install
pre-commit run --all-files         # baseline run; should pass
```

---

## Project structure

```
spark-perf-lint/
├── src/spark_perf_lint/
│   ├── cli.py               # Click command group — scan, rules, explain, init, version
│   ├── config.py            # Four-layer config stack (defaults → YAML → env → CLI)
│   ├── types.py             # Finding, AuditReport, Severity, Dimension, EffortLevel
│   ├── engine/
│   │   ├── ast_analyzer.py  # Python AST parsing; Spark-aware fact extraction
│   │   ├── pattern_matcher.py  # 25+ high-level anti-pattern detectors
│   │   ├── file_scanner.py  # Recursive discovery; PySpark import detection
│   │   └── orchestrator.py  # Ties engine + rules + reporters; noqa suppression
│   ├── rules/
│   │   ├── base.py          # BaseRule / ConfigRule / CodeRule with metadata validation
│   │   ├── registry.py      # Singleton registry; @register_rule decorator
│   │   └── d01_*.py … d11_*.py   # One module per dimension
│   ├── reporters/           # terminal, json_reporter, markdown_reporter, github_pr
│   ├── llm/                 # Tier 2: Claude API integration
│   ├── observability/       # Tracer interface, FileTracer, LangSmithTracer stub
│   └── tier3/               # Benchmarks and synthetic data generators
├── tests/
│   ├── fixtures/
│   │   ├── bad_code/        # 10 anti-pattern files (produce 311 findings)
│   │   └── good_code/       # 10 clean files (produce 0 findings)
│   └── test_*.py
├── docs/                    # Markdown documentation
├── examples/                # Example config files
├── .pre-commit-hooks.yaml   # Hook definition for pre-commit framework
├── pyproject.toml
└── CHANGELOG.md
```

---

## Running tests

```bash
# All tests (fast, ~5 s)
pytest -q

# With coverage report
pytest --cov=spark_perf_lint --cov-report=term-missing -q

# Single module
pytest tests/test_rules_d03_joins.py -v

# Real git workflow tests (spawns subprocesses, ~3 s extra)
pytest tests/test_precommit_integration.py::TestRealGitWorkflow -v

# Exclude slow tests for rapid iteration
pytest -q -m "not slow"

# Run the linter on itself
spark-perf-lint scan src/
```

---

## Code style

All code is formatted with **Black** (100-char line length) and linted with
**Ruff**. Both run automatically as pre-commit hooks; you can also run them
manually:

```bash
black src/ tests/
ruff check src/ tests/ --fix
```

Conventions enforced throughout the codebase:

- **Type hints** on every public function and method signature
- **Google-style docstrings** on every class and public method
- **No PySpark import** anywhere in `src/spark_perf_lint/` except `tier3/`
  (Tier 1 must work without Spark installed)
- **`# noqa: SPL-DXX-XXX`** to suppress false positives in fixture files;
  standard Ruff noqa codes (`# noqa: E501`) for Ruff suppressions

---

## Adding a new rule

Rules live in `src/spark_perf_lint/rules/d{NN}_{name}.py`. Each rule is a
concrete subclass of `CodeRule` or `ConfigRule` decorated with `@register_rule`.

### Step 1 — pick an ID and dimension

Rule IDs follow the pattern `SPL-D{NN}-{NNN}`. Find the next available number
in the relevant dimension file.

### Step 2 — implement the class

```python
# src/spark_perf_lint/rules/d03_joins.py

from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.rules.base import CodeRule
from spark_perf_lint.types import Dimension, Severity, EffortLevel

@register_rule
class MyNewJoinRule(CodeRule):
    rule_id = "SPL-D03-011"
    name = "My new join anti-pattern"
    dimension = Dimension.D03_JOINS
    default_severity = Severity.WARNING
    description = "One-line description shown in 'spark-perf-lint rules'."
    explanation = (
        "Multi-sentence explanation of *why* this pattern is harmful, "
        "referencing Spark internals where relevant."
    )
    recommendation_template = "Actionable fix — what to do instead."
    before_example = "df.join(other)  # missing key"
    after_example = "df.join(other, on='id', how='inner')  # explicit key"
    estimated_impact = "Brief quantified impact, e.g. '3–10× slowdown'"
    effort_level = EffortLevel.MINOR_CODE_CHANGE
    references = ["https://spark.apache.org/docs/latest/..."]

    def check(self, analyzer, config):
        if not self.is_enabled(config):
            return []
        pm = PatternMatcher(analyzer)
        results = []
        for match in pm.find_some_pattern():
            results.append(self.create_finding(
                file_path=analyzer.filename,
                line_number=match.first_line,
                message="Specific description of what was found",
                config=config,
            ))
        return results
```

### Step 3 — add tests

Every rule needs at minimum two test cases in `tests/test_rules_d{NN}_*.py`:

```python
class TestMyNewJoinRule:
    def test_fires_on_anti_pattern(self, tmp_path):
        """Rule must fire on the bad pattern."""
        code = "df.join(other)  # no key"
        findings = run_rule(MyNewJoinRule, code, tmp_path)
        assert len(findings) == 1
        assert findings[0].rule_id == "SPL-D03-011"
        assert findings[0].severity == Severity.WARNING

    def test_does_not_fire_on_correct_pattern(self, tmp_path):
        """Rule must not fire on valid code."""
        code = "df.join(other, on='id', how='inner')"
        findings = run_rule(MyNewJoinRule, code, tmp_path)
        assert findings == []
```

### Step 4 — add to the catalogue and docs

Add the rule to `_RULE_CATALOGUE` in `cli.py` and to `docs/RULES_REFERENCE.md`.

### Step 5 — verify

```bash
pytest tests/test_rules_d03_joins.py -v
spark-perf-lint rules --dimension D03   # new rule should appear
spark-perf-lint explain SPL-D03-011
```

---

## Adding a new reporter

Reporters live in `src/spark_perf_lint/reporters/`. Each reporter is a module
that exports a single `render(report: AuditReport, config: LintConfig) -> str`
function (or a class following the same interface).

1. Create `src/spark_perf_lint/reporters/my_reporter.py`
2. Register the format name in `cli.py` (`_FORMAT_CHOICES`) and wire it in the
   `scan` command's output dispatch block
3. Add tests in `tests/test_reporters.py`

---

## Submitting a pull request

1. Fork the repository and create a feature branch from `main`
2. Make your changes — one logical change per PR is easiest to review
3. Ensure all checks pass locally:
   ```bash
   black src/ tests/
   ruff check src/ tests/
   pytest -q
   spark-perf-lint scan src/ tests/fixtures/bad_code/
   ```
4. Update `CHANGELOG.md` under an `## [Unreleased]` heading
5. Open a PR against `main` — the CI workflow runs the full suite

### Commit message format

```
type: short summary (≤ 72 chars)

Optional longer body explaining *why*, not *what*.
```

Types: `feat`, `fix`, `docs`, `test`, `chore`, `refactor`, `perf`

---

## Release process

1. Update the version in `pyproject.toml`, `setup.cfg`, and
   `src/spark_perf_lint/__init__.py`
2. Move `## [Unreleased]` entries to a new `## [X.Y.Z] — YYYY-MM-DD` section
   in `CHANGELOG.md`
3. Commit: `chore: prepare vX.Y.Z release`
4. Tag: `git tag -a vX.Y.Z -m "vX.Y.Z"`
5. Build: `python -m build`
6. Publish: `twine upload dist/*`
7. Create a GitHub release pointing at the tag; paste the changelog section
   as the release notes

The CI `release.yml` workflow automates steps 5–7 when a `v*` tag is pushed.
