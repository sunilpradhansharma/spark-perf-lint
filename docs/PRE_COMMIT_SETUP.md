# Pre-commit Hook Setup

`spark-perf-lint` ships as a [pre-commit](https://pre-commit.com/) hook source.  
Drop a single stanza into your project's `.pre-commit-config.yaml` and every
`git commit` will automatically scan staged PySpark files for performance
anti-patterns before the commit lands in history.

---

## Quick Setup (2 minutes)

### Step 1 — Install pre-commit

```bash
pip install pre-commit
```

Or with your project's dev dependencies:

```bash
pip install "spark-perf-lint[dev]"
```

### Step 2 — Add to `.pre-commit-config.yaml`

Place the hook alongside your existing formatters and linters.  
Full example with `ruff`, `mypy`, and standard hooks:

```yaml
repos:
  # Code formatting & linting
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.4.0
    hooks:
      - id: ruff-format
      - id: ruff
        args: [--fix]

  # Type checking
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.9.0
    hooks:
      - id: mypy
        additional_dependencies: [pyspark-stubs]

  # Spark performance linter
  - repo: https://github.com/YOUR_USERNAME/spark-perf-lint
    rev: v0.1.0
    hooks:
      - id: spark-perf-lint
        args:
          - '--severity-threshold'
          - 'WARNING'
          - '--fail-on'
          - 'CRITICAL'
          - '--format'
          - 'terminal'

  # Standard file hygiene
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-merge-conflict
```

### Step 3 — Install hooks

```bash
pre-commit install
```

This installs the Git hook script into `.git/hooks/pre-commit`.

### Step 4 — (Optional) Create a project config

```bash
spark-perf-lint init
```

This writes a `.spark-perf-lint.yaml` to the current directory with all
available options pre-filled and documented.  
Edit it to tune thresholds, enable/disable dimensions, and override severities.

### Step 5 — Test the setup

Run against all staged files manually:

```bash
pre-commit run spark-perf-lint --all-files
```

Or target a specific file:

```bash
pre-commit run spark-perf-lint --files src/jobs/etl.py
```

---

## Common Configurations

### Strict mode — block on WARNING and above

```yaml
- id: spark-perf-lint
  args: ['--fail-on', 'CRITICAL', '--fail-on', 'WARNING']
```

### Gradual adoption — CRITICAL only, ignore legacy directories

```yaml
- id: spark-perf-lint
  args: ['--fail-on', 'CRITICAL']
  exclude: '^(legacy/|deprecated/|notebooks/)'
```

Or via `.spark-perf-lint.yaml`:

```yaml
general:
  fail_on: [CRITICAL]
ignore:
  directories: [legacy, deprecated, notebooks, .venv]
  files: ["**/*_test.py", "**/conftest.py"]
```

### Quiet output — findings only, no header or footer

```yaml
- id: spark-perf-lint
  args: ['--quiet', '--fail-on', 'CRITICAL']
```

### Verbose output — full explanations and before/after diffs

```yaml
- id: spark-perf-lint
  args: ['--verbose', '--fail-on', 'CRITICAL']
```

### Scan specific dimensions only

```yaml
- id: spark-perf-lint
  args: ['--dimension', 'D03,D08', '--fail-on', 'CRITICAL']
```

### CI only — no local hook, annotations in GitHub Actions

Omit `stages` from the local config and add a dedicated workflow step instead:

```yaml
# .github/workflows/lint.yml
- name: Spark performance lint
  run: spark-perf-lint scan src/ --format github-pr --fail-on CRITICAL
  env:
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

---

## Skipping the Hook

To skip `spark-perf-lint` for a single commit (e.g. a WIP checkpoint):

```bash
git commit -m "WIP: sketching new pipeline" --no-verify
```

To skip only `spark-perf-lint` while running other hooks:

```bash
SKIP=spark-perf-lint git commit -m "WIP"
```

> **Note:** `--no-verify` skips *all* hooks. Use `SKIP=` to be surgical.

---

## Updating the Hook

Pin a specific release in `rev:` and run:

```bash
pre-commit autoupdate
```

Or update a single hook:

```bash
pre-commit autoupdate --repo https://github.com/YOUR_USERNAME/spark-perf-lint
```

---

## Troubleshooting

### "Executable `spark-perf-lint` not found"

pre-commit creates an isolated virtual environment for each hook.  If the
install step fails, check that the `rev:` tag exists in the remote repo and
that the package's `[project.scripts]` entry is correctly defined in
`pyproject.toml`.

Force a clean reinstall of the hook environment:

```bash
pre-commit clean
pre-commit install
```

### Hook is slow on first run

pre-commit builds a fresh virtual environment the first time a hook runs.
Subsequent runs reuse the cached environment and are fast (typically < 0.5s
for a Spark file).

### False positives in test files

Test files often contain intentional anti-patterns.  Suppress them globally:

```yaml
# .spark-perf-lint.yaml
ignore:
  files:
    - "**/*_test.py"
    - "**/conftest.py"
    - "**/test_*.py"
```

Or on a per-rule basis:

```yaml
ignore:
  rules:
    - SPL-D03-001   # Test fixtures use cross joins deliberately
```

### Hook passes locally but fails in CI

Ensure the `rev:` tag is pushed to the remote repo before CI runs.  
For monorepos, point to the subdirectory:

```yaml
- repo: https://github.com/YOUR_USERNAME/spark-perf-lint
  rev: v0.1.0
  hooks:
    - id: spark-perf-lint
      additional_dependencies: []
```

### "Configuration error" at startup

Validate your `.spark-perf-lint.yaml` syntax:

```bash
python -c "import yaml; yaml.safe_load(open('.spark-perf-lint.yaml'))"
```

---

## How It Works

1. `git commit` triggers `.git/hooks/pre-commit` (installed by `pre-commit install`).
2. pre-commit collects the list of staged `.py` files.
3. It calls `spark-perf-lint scan <staged-files…>` in an isolated environment.
4. The linter runs all enabled rules (Tier 1: AST analysis, zero Spark dependency).
5. If any finding matches a `fail_on` severity, the process exits `1` and the
   commit is **blocked**.  The terminal reporter prints the findings inline.
6. The developer fixes the issues (or uses `--no-verify` to bypass) and re-commits.

Total added latency per commit: **< 0.5s** for typical PySpark files on modern hardware.

---

## Hook Definition Reference

```yaml
- id: spark-perf-lint
  name: Spark Performance Linter
  description: Lint PySpark code for performance anti-patterns (D01–D11, 93 rules).
  entry: spark-perf-lint scan
  language: python
  files: '\.py$'
  types: [python]
  stages: [pre-commit]
  pass_filenames: true
  require_serial: false
  minimum_pre_commit_version: '3.0.0'
```

| Field | Value | Reason |
|-------|-------|--------|
| `language: python` | Python | pre-commit manages a venv with the package installed |
| `pass_filenames: true` | true | Only staged files are scanned — fast and targeted |
| `require_serial: false` | false | Can run in parallel with other hooks |
| `stages: [pre-commit]` | pre-commit | Doesn't run during `commit-msg` or `push` stages |
| `files: '\.py$'` | `.py` files | Skips non-Python files automatically |
