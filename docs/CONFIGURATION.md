# spark-perf-lint Configuration Guide

Complete reference for configuring spark-perf-lint via the YAML config file,
environment variables, and CLI flags.

---

## Contents

1. [How Configuration Works](#1-how-configuration-works)
2. [Quick Start](#2-quick-start)
3. [Config File Reference](#3-config-file-reference)
   - [general](#31-general)
   - [rules](#32-rules)
   - [thresholds](#33-thresholds)
   - [severity_override](#34-severity_override)
   - [ignore](#35-ignore)
   - [spark_config_audit](#36-spark_config_audit)
   - [llm](#37-llm)
   - [observability](#38-observability)
4. [Environment Variable Overrides](#4-environment-variable-overrides)
5. [CLI Reference](#5-cli-reference)
6. [Common Configurations](#6-common-configurations)
7. [Ignoring Files and Rules](#7-ignoring-files-and-rules)

---

## 1. How Configuration Works

spark-perf-lint uses a **four-layer configuration stack**. Each layer overrides
the one below it; you only need to specify the keys you want to change.

```
Priority (lowest → highest)

 1. Built-in defaults       hardcoded in the tool; always applied
 2. Project YAML file       .spark-perf-lint.yaml, auto-discovered
 3. Environment variables   SPARK_PERF_LINT_* prefixed vars
 4. CLI arguments           flags passed at invocation time
```

**File discovery** — when you run `spark-perf-lint scan`, the tool walks
up the directory tree from the scanned path, stopping at the git root (or the
filesystem root if not in a git repo), and loads the first `.spark-perf-lint.yaml`
it finds. This means a file at the repo root applies to the whole project; a
file in a subdirectory overrides settings only for that subtree.

**Merge semantics** — layers are merged recursively. A YAML file that sets only
`thresholds.broadcast_threshold_mb` does not wipe out the other thresholds; it
adds or overrides that single key.

---

## 2. Quick Start

Generate a starter config in the current directory:

```bash
# Full file with every option commented — best for reading and exploring
spark-perf-lint init

# Concise stub with just the most commonly changed keys
spark-perf-lint init --minimal

# Overwrite an existing file without prompting
spark-perf-lint init --force
```

The generated `.spark-perf-lint.yaml` contains every available option with
inline comments. Edit it to tune the linter for your project, then run:

```bash
spark-perf-lint scan .
```

---

## 3. Config File Reference

The config file is a YAML mapping. Every key is optional; omit a section
entirely to accept its defaults.

### 3.1 `general`

Top-level scan behavior.

```yaml
general:
  severity_threshold: INFO   # minimum severity to include in output
  fail_on:                   # severities that cause a non-zero exit code
    - CRITICAL
  report_format:             # one or more output formats
    - terminal
  max_findings: 0            # 0 = unlimited
```

#### `general.severity_threshold`

| | |
|---|---|
| **Type** | string |
| **Values** | `INFO` · `WARNING` · `CRITICAL` |
| **Default** | `INFO` |

Minimum severity level to include in the report output. Findings below this
threshold are silently suppressed — they are not scanned for or counted.

Use `WARNING` in CI to reduce noise on large codebases, and `INFO` locally
during development to see everything.

```yaml
general:
  severity_threshold: WARNING   # hide INFO findings
```

#### `general.fail_on`

| | |
|---|---|
| **Type** | list of strings |
| **Values** | Any subset of `[CRITICAL, WARNING, INFO]` |
| **Default** | `[CRITICAL]` |

Severity levels that cause the process to exit with code `1` (failure). The
pre-commit framework and most CI systems treat exit code `1` as a blocking
failure.

```yaml
general:
  fail_on:
    - CRITICAL
    - WARNING   # block on warnings too — strictest mode
```

```yaml
general:
  fail_on: []   # never fail (report-only mode)
```

#### `general.report_format`

| | |
|---|---|
| **Type** | list of strings |
| **Values** | `terminal` · `json` · `markdown` · `github_pr` |
| **Default** | `[terminal]` |

Output format(s) to generate. Multiple formats can be listed; each is rendered
independently.

| Format | Description |
|---|---|
| `terminal` | Rich-formatted coloured output for human reading in a terminal |
| `json` | Machine-readable JSON array of findings — for downstream tooling |
| `markdown` | GitHub-flavoured Markdown — paste into PR descriptions or wikis |
| `github_pr` | GitHub Actions annotations + `$GITHUB_STEP_SUMMARY` + optional PR comment |

```yaml
general:
  report_format:
    - terminal
    - json       # also write findings.json alongside terminal output
```

#### `general.max_findings`

| | |
|---|---|
| **Type** | non-negative integer |
| **Default** | `0` (unlimited) |

Cap the number of findings included in the output. Findings are sorted by
severity (CRITICAL first), then file path, then line number before the cap is
applied, so the most critical findings always survive truncation.

Useful in CI on large legacy codebases to avoid overwhelming output on the
first scan. Incrementally lower the cap as findings are fixed.

```yaml
general:
  max_findings: 50   # report only the top 50 findings
```

---

### 3.2 `rules`

Enable or disable individual rules or entire dimensions.

```yaml
rules:
  d01_cluster_config:
    enabled: true
    rules:
      SPL-D01-001: true
      SPL-D01-002: false   # disable this specific rule
  d02_shuffle:
    enabled: false         # disable the entire D02 dimension
```

#### Dimension block keys

Each dimension has a block named after its module:

| Block key | Dimension |
|---|---|
| `d01_cluster_config` | D01 · Cluster Configuration |
| `d02_shuffle` | D02 · Shuffle |
| `d03_joins` | D03 · Joins |
| `d04_partitioning` | D04 · Partitioning |
| `d05_skew` | D05 · Data Skew |
| `d06_caching` | D06 · Caching |
| `d07_io_format` | D07 · I/O and File Formats |
| `d08_aqe` | D08 · Adaptive Query Execution |
| `d09_udf_code` | D09 · UDF and Code Patterns |
| `d10_catalyst` | D10 · Catalyst Optimizer |
| `d11_monitoring` | D11 · Monitoring and Observability |

#### `rules.<dimension>.enabled`

Setting `enabled: false` on a dimension block disables **all** rules in that
dimension with a single key. Individual rule overrides inside a disabled block
are ignored.

```yaml
rules:
  d11_monitoring:
    enabled: false   # skip all monitoring rules for this project
```

#### `rules.<dimension>.rules.<RULE_ID>`

Set a specific rule ID to `false` to disable only that rule, leaving all other
rules in the dimension active.

```yaml
rules:
  d09_udf_code:
    enabled: true
    rules:
      SPL-D09-008: false   # suppress .show() in production warning
      SPL-D09-009: false   # suppress .explain() in production warning
```

> **Tip:** To suppress a rule for a specific reason across a whole team, prefer
> `severity_override` (to demote rather than silence) or `ignore.rules` (for
> rules your project is intentionally violating). Rule-level toggles here are
> better for permanent architectural exceptions.

---

### 3.3 `thresholds`

Numeric limits that drive rule decisions. Every threshold must be a positive
number. Ordering constraints are validated at load time (e.g.
`skew_ratio_warning` must be less than `skew_ratio_critical`).

```yaml
thresholds:
  broadcast_threshold_mb: 10
  max_shuffle_partitions: 2000
  min_shuffle_partitions: 10
  target_partition_size_mb: 128
  max_cache_without_unpersist: 5
  skew_ratio_warning: 5.0
  skew_ratio_critical: 10.0
  max_partition_count: 10000
  min_partition_count: 2
  max_udf_complexity: 10
  min_executor_memory_gb: 4
  min_driver_memory_gb: 2
  max_executor_cores: 5
  min_executor_cores: 2
  max_with_column_chain: 10
  max_chained_filters: 3
```

#### Threshold reference

| Key | Default | Unit | Affects rules | Description |
|---|---|---|---|---|
| `broadcast_threshold_mb` | `10` | MB | SPL-D03-002, SPL-D03-003 | Maximum DataFrame size eligible for broadcast join — corresponds to `spark.sql.autoBroadcastJoinThreshold` |
| `max_shuffle_partitions` | `2000` | count | SPL-D02-001, SPL-D04-003 | Upper bound for `spark.sql.shuffle.partitions` before flagging over-partitioning |
| `min_shuffle_partitions` | `10` | count | SPL-D02-001, SPL-D04-004 | Lower bound for `spark.sql.shuffle.partitions` before flagging under-partitioning |
| `target_partition_size_mb` | `128` | MB | SPL-D04-003, SPL-D04-005 | Recommended target partition size; used to assess whether `repartition()` values are reasonable |
| `max_cache_without_unpersist` | `5` | count | SPL-D06-001 | Maximum cached DataFrames without a corresponding `unpersist()` call in the same scope |
| `skew_ratio_warning` | `5.0` | ratio | SPL-D05-001 | Max partition rows ÷ median partition rows that triggers a WARNING (Tier 3 only) |
| `skew_ratio_critical` | `10.0` | ratio | SPL-D05-001 | Skew ratio that triggers a CRITICAL finding; must be > `skew_ratio_warning` |
| `max_partition_count` | `10000` | count | SPL-D04-001 | Total partition count above which overhead from too many small tasks is flagged |
| `min_partition_count` | `2` | count | SPL-D04-002 | Partition count below which under-utilisation is flagged; must be < `max_partition_count` |
| `max_udf_complexity` | `10` | score | SPL-D09-004 | Maximum cyclomatic complexity (McCabe) for a UDF body |
| `min_executor_memory_gb` | `4` | GB | SPL-D01-001 | Minimum executor memory before flagging under-provisioning |
| `min_driver_memory_gb` | `2` | GB | SPL-D01-002 | Minimum driver memory before flagging under-provisioning |
| `max_executor_cores` | `5` | count | SPL-D01-005 | Maximum executor cores before flagging (too many cores per executor hurts GC) |
| `min_executor_cores` | `2` | count | SPL-D01-005 | Minimum executor cores |
| `max_with_column_chain` | `10` | count | SPL-D09-003 | Maximum consecutive `withColumn()` calls before flagging as a loop or chain risk |
| `max_chained_filters` | `3` | count | SPL-D10-006 | Maximum chained `filter()` calls before suggesting they be merged into one |

**Tuning for large clusters** — if your typical production cluster has 32 GB
executors and 1000+ shuffle partitions, raise the thresholds to match:

```yaml
thresholds:
  min_executor_memory_gb: 16
  min_driver_memory_gb: 8
  max_shuffle_partitions: 8000
  min_shuffle_partitions: 200
  broadcast_threshold_mb: 50   # larger cluster can afford bigger broadcasts
```

---

### 3.4 `severity_override`

Override the default severity of specific rules without disabling them.
This is the primary tuning mechanism for adjusting signal-to-noise ratio to
your organisation's risk tolerance.

```yaml
severity_override:
  SPL-D03-001: CRITICAL   # cross join is always critical for us
  SPL-D02-001: WARNING    # relax shuffle partition default from WARNING
  SPL-D07-007: INFO       # write mode check is informational only
  SPL-D11-001: WARNING    # monitoring is important but not blocking
```

| | |
|---|---|
| **Type** | mapping of rule ID → severity string |
| **Values** | `CRITICAL` · `WARNING` · `INFO` |
| **Default** | `{}` (no overrides) |

Overrides apply globally — they affect every file in the scan. To adjust
severity for a specific file, use inline suppression comments instead (see
[Ignoring Files and Rules](#7-ignoring-files-and-rules)).

---

### 3.5 `ignore`

Exclude files, directories, or rules from scanning entirely.

```yaml
ignore:
  files:
    - "**/*_test.py"
    - "**/conftest.py"
    - "**/migrations/*.py"
  rules:
    - SPL-D11-001   # custom telemetry framework, not SparkListener
    - SPL-D07-002   # legacy CSV pipeline, migration in progress
  directories:
    - ".venv"
    - "venv"
    - "build"
    - "dist"
    - ".eggs"
    - "notebooks"
```

#### `ignore.files`

| | |
|---|---|
| **Type** | list of glob patterns |
| **Default** | `[]` |

Glob patterns for files to exclude. Patterns are matched against both the full
path and the filename alone using Python's `fnmatch` module (not full
`pathlib.glob` — `**` is supported for path segments).

```yaml
ignore:
  files:
    - "**/*_test.py"           # any file ending in _test.py
    - "**/conftest.py"         # pytest configuration files
    - "**/migrations/*.py"     # database migration scripts
    - "src/legacy/**/*.py"     # entire legacy subtree
    - "setup.py"               # repo root setup file by name
```

#### `ignore.rules`

| | |
|---|---|
| **Type** | list of rule IDs |
| **Default** | `[]` |

Rule IDs to suppress globally across all files. All findings for these rules
are silently discarded regardless of the file they appear in.

Prefer `severity_override` when a rule fires correctly but you want it to be
non-blocking. Use `ignore.rules` only for rules that are permanently
inapplicable to your project (e.g. you have a custom telemetry framework that
replaces `SparkListener`).

```yaml
ignore:
  rules:
    - SPL-D11-001   # we use DataDog APM, not SparkListener
    - SPL-D11-002   # Spark listener registered via deployment config, not code
```

#### `ignore.directories`

| | |
|---|---|
| **Type** | list of directory names or path components |
| **Default** | `[".venv", "venv", "build", "dist", ".eggs"]` |

Directory names to exclude. Any file whose path contains one of these names as
a path component is skipped. This is a string-equality check on path parts, not
a glob.

```yaml
ignore:
  directories:
    - ".venv"
    - "venv"
    - "build"
    - "dist"
    - ".eggs"
    - "notebooks"      # exploratory notebooks are not production code
    - "archive"        # legacy code pending deletion
    - "vendor"         # vendored third-party code
```

---

### 3.6 `spark_config_audit`

When enabled, the linter checks that `SparkSession.builder.config()` and
`spark.conf.set()` calls in your source code match the expected values defined
here. Mismatches become findings.

```yaml
spark_config_audit:
  enabled: false   # set to true to enforce org-wide config standards

  configs:
    spark.executor.memory:
      expected_value: "8g"
      severity: WARNING
      note: "Minimum recommended for production workloads"

    spark.executor.cores:
      expected_value: null    # assert the config is present; any value accepted
      severity: WARNING
      note: "Must be set explicitly; never rely on the default of 1"

    spark.sql.adaptive.enabled:
      expected_value: "true"
      severity: CRITICAL
      note: "AQE is the single highest-ROI Spark 3 feature; always enable"
```

#### `spark_config_audit.enabled`

| | |
|---|---|
| **Type** | boolean |
| **Default** | `false` |

Set to `true` to activate config audit checks. When `false` the entire section
is ignored.

#### `spark_config_audit.configs.<spark.key>`

Each entry under `configs` defines an expected value for one Spark configuration
key:

| Sub-key | Type | Description |
|---|---|---|
| `expected_value` | string or `null` | The value the config must be set to. Use `null` to assert that the config is present but accept any value. |
| `severity` | string | `CRITICAL` · `WARNING` · `INFO` — severity of the finding if the config is missing or set to the wrong value. |
| `note` | string | Optional explanatory note included in the finding message. |

The default `.spark-perf-lint.yaml` ships with ~75 pre-defined entries covering
all major Spark performance settings across D01 (cluster), D02 (shuffle), D08
(AQE), D10 (SQL/Catalyst), and D11 (monitoring). Generate the full config with
`spark-perf-lint init` to see them all.

---

### 3.7 `llm`

Tier 2 LLM analysis via the Claude API. When enabled, findings are enriched
with context-aware explanations and a cross-file executive summary is generated.

Requires the `llm` optional dependency group:

```bash
pip install "spark-perf-lint[llm]"
```

```yaml
llm:
  enabled: false
  provider: anthropic
  model: claude-sonnet-4-6
  api_key_env_var: ANTHROPIC_API_KEY
  max_tokens: 1024
  batch_size: 5
  min_severity_for_llm: WARNING
  max_llm_calls: 20
```

#### Parameters

| Key | Default | Description |
|---|---|---|
| `enabled` | `false` | Activate Tier 2 LLM analysis |
| `provider` | `"anthropic"` | LLM provider (only `anthropic` is currently supported) |
| `model` | `"claude-sonnet-4-6"` | Claude model ID. Use `claude-opus-4-6` for deeper analysis, `claude-haiku-4-5-20251001` for speed |
| `api_key_env_var` | `"ANTHROPIC_API_KEY"` | Name of the environment variable that holds the API key. The key itself is never stored in this file |
| `max_tokens` | `1024` | Maximum tokens per LLM call |
| `batch_size` | `5` | Number of findings sent to the LLM per API call |
| `min_severity_for_llm` | `"WARNING"` | Only findings at or above this severity are sent for LLM enrichment |
| `max_llm_calls` | `20` | Hard cap on the total number of API calls per scan |

**Enabling per-run via CLI** — enable Tier 2 for a single scan without changing
the config file:

```bash
ANTHROPIC_API_KEY=sk-... spark-perf-lint scan src/ --llm
```

---

### 3.8 `observability`

Run tracing and performance telemetry for spark-perf-lint itself. Useful for
teams that want to track linter performance over time (e.g. scan duration,
finding trends).

```yaml
observability:
  enabled: false
  backend: file
  output_dir: .spark-perf-lint-traces
  trace_level: standard
  langsmith_project: spark-perf-lint
  langsmith_api_key_env_var: LANGCHAIN_API_KEY
```

#### Parameters

| Key | Default | Description |
|---|---|---|
| `enabled` | `false` | Activate run tracing |
| `backend` | `"file"` | Trace backend: `file` (JSON files in `output_dir`) or `langsmith` |
| `output_dir` | `".spark-perf-lint-traces"` | Directory for file-based trace output (created if it doesn't exist) |
| `trace_level` | `"standard"` | `minimal` (run-level only) · `standard` (per-file stats) · `verbose` (per-rule timing) |
| `langsmith_project` | `"spark-perf-lint"` | LangSmith project name (used when `backend: langsmith`) |
| `langsmith_api_key_env_var` | `"LANGCHAIN_API_KEY"` | Environment variable holding the LangSmith API key |

---

## 4. Environment Variable Overrides

Environment variables provide a third configuration layer, sitting between the
YAML file and CLI flags. They are useful for CI pipelines and Docker containers
where config file changes are impractical.

All variables use the `SPARK_PERF_LINT_` prefix. List values use
comma-separated strings. Boolean values accept `true/false`, `1/0`, or
`yes/no` (case-insensitive).

| Variable | Maps to | Type | Example |
|---|---|---|---|
| `SPARK_PERF_LINT_SEVERITY_THRESHOLD` | `general.severity_threshold` | string | `WARNING` |
| `SPARK_PERF_LINT_FAIL_ON` | `general.fail_on` | comma-separated | `CRITICAL,WARNING` |
| `SPARK_PERF_LINT_REPORT_FORMAT` | `general.report_format` | comma-separated | `terminal,json` |
| `SPARK_PERF_LINT_MAX_FINDINGS` | `general.max_findings` | integer | `100` |
| `SPARK_PERF_LINT_LLM_ENABLED` | `llm.enabled` | boolean | `true` |
| `SPARK_PERF_LINT_LLM_MODEL` | `llm.model` | string | `claude-opus-4-6` |
| `SPARK_PERF_LINT_LLM_MAX_CALLS` | `llm.max_llm_calls` | integer | `10` |
| `SPARK_PERF_LINT_OBSERVABILITY_ENABLED` | `observability.enabled` | boolean | `true` |
| `SPARK_PERF_LINT_OBSERVABILITY_BACKEND` | `observability.backend` | string | `langsmith` |
| `SPARK_PERF_LINT_OBSERVABILITY_OUTPUT_DIR` | `observability.output_dir` | string | `/tmp/traces` |

**Examples:**

```bash
# CI: fail on warnings and above, show only WARNING+ in output
export SPARK_PERF_LINT_SEVERITY_THRESHOLD=WARNING
export SPARK_PERF_LINT_FAIL_ON=CRITICAL,WARNING

# Enable LLM analysis in a GitHub Actions step
export SPARK_PERF_LINT_LLM_ENABLED=true
export ANTHROPIC_API_KEY=${{ secrets.ANTHROPIC_API_KEY }}

# Cap findings to 50 in a noisy legacy repo
export SPARK_PERF_LINT_MAX_FINDINGS=50
```

Only the variables listed in the table above are recognised; any other
`SPARK_PERF_LINT_*` variables are silently ignored. This prevents typos in
variable names from having undetected effects.

---

## 5. CLI Reference

spark-perf-lint provides five sub-commands. Run `spark-perf-lint --help` or
`spark-perf-lint COMMAND --help` for the full option list at any time.

### `spark-perf-lint scan`

Run the linter on one or more paths.

```
spark-perf-lint scan [OPTIONS] PATH...
```

`PATH` can be one or more Python files or directories. Directories are scanned
recursively; non-`.py` files are skipped. If no path is given, the current
directory is scanned.

#### Exit codes

| Code | Meaning |
|---|---|
| `0` | No findings at or above `--fail-on` severity |
| `1` | One or more findings at or above `--fail-on` severity |
| `2` | Configuration or I/O error |

#### Options

| Option | Default | Description |
|---|---|---|
| `--config PATH` | auto-discovered | Explicit path to a `.spark-perf-lint.yaml` config file, bypassing auto-discovery |
| `--severity-threshold LEVEL` | from config | Minimum severity level to include in output (`CRITICAL`, `WARNING`, `INFO`) |
| `--fail-on LEVEL` | from config | Severity that causes exit code 1. Repeatable: `--fail-on CRITICAL --fail-on WARNING` |
| `--format FORMAT` | from config | Output format. Repeatable: `--format terminal --format json`. Values: `terminal`, `json`, `markdown`, `github-pr` |
| `--output PATH` | stdout | Write the report to this file instead of stdout (single-format only) |
| `--dimension CODES` | all dimensions | Scan only specific dimensions. Comma-separated codes: `D03,D08`. Prefix with `!` to exclude: `!D11` |
| `--rule RULE_IDS` | all rules | Scan only specific rule IDs. Comma-separated: `SPL-D03-001,SPL-D08-002` |
| `--fix / --no-fix` | `--fix` | Show before/after code suggestions in the report |
| `--verbose` / `-v` | off | Show scan progress, rule counts, and timing details |
| `--quiet` / `-q` | off | Only print findings; suppress header, footer, and progress output |
| `--llm / --no-llm` | from config | Enable/disable Tier 2 LLM analysis for this run |

#### Examples

```bash
# Scan the current directory with all defaults
spark-perf-lint scan .

# Show only warnings and above, write JSON output to a file
spark-perf-lint scan src/ --severity-threshold WARNING --format json --output findings.json

# Fail on both CRITICAL and WARNING; show progress
spark-perf-lint scan jobs/ --fail-on CRITICAL --fail-on WARNING --verbose

# Scan only joins and AQE rules, quiet output for scripting
spark-perf-lint scan . --dimension D03,D08 --quiet

# Scan a single file, explicitly pointing to a config
spark-perf-lint scan jobs/etl.py --config /etc/spark-lint/team.yaml

# Run a targeted check on one rule
spark-perf-lint scan src/ --rule SPL-D03-001

# Scan without code-fix suggestions (faster terminal output)
spark-perf-lint scan . --no-fix

# Enable Tier 2 LLM analysis for a single run
ANTHROPIC_API_KEY=sk-... spark-perf-lint scan src/ --llm
```

---

### `spark-perf-lint rules`

List all available rules with IDs, dimensions, and default severities.

```
spark-perf-lint rules [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--dimension CODE` | all | Show only rules for this dimension (e.g. `D03`) |
| `--severity LEVEL` | all | Show only rules with this default severity |
| `--format FORMAT` | `table` | Output format: `table` or `json` |

```bash
# List all rules
spark-perf-lint rules

# List only CRITICAL rules
spark-perf-lint rules --severity CRITICAL

# List all join rules in JSON format (for scripting)
spark-perf-lint rules --dimension D03 --format json
```

---

### `spark-perf-lint explain`

Show detailed documentation for a specific rule.

```
spark-perf-lint explain RULE_ID [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--format FORMAT` | `text` | Output format: `text`, `json`, or `markdown` |

```bash
spark-perf-lint explain SPL-D03-001
spark-perf-lint explain SPL-D08-001 --format markdown
spark-perf-lint explain SPL-D09-003 --format json
```

---

### `spark-perf-lint init`

Create a default `.spark-perf-lint.yaml` in the current directory.

```
spark-perf-lint init [OPTIONS]
```

| Option | Description |
|---|---|
| `--force` | Overwrite an existing config file without prompting |
| `--minimal` | Write only the most commonly customised keys instead of the full file |

---

### `spark-perf-lint version`

Print the installed version.

```
spark-perf-lint version [--short]
```

`--short` prints only the version number (e.g. `0.4.1`) with no package name —
useful in scripts.

---

## 6. Common Configurations

### Pre-commit hook (fast, blocking)

Optimised for speed and a low false-positive rate: fails only on critical issues,
suppresses noisy informational rules, and skips test files.

```yaml
# .spark-perf-lint.yaml
general:
  severity_threshold: WARNING   # hide INFO findings in pre-commit output
  fail_on:
    - CRITICAL

ignore:
  files:
    - "**/*_test.py"
    - "**/conftest.py"
    - "tests/**/*.py"
  directories:
    - ".venv"
    - "venv"
    - "build"
    - "notebooks"

severity_override:
  SPL-D11-001: INFO   # monitoring rules are informational in pre-commit
  SPL-D11-002: INFO
  SPL-D11-003: INFO
  SPL-D11-004: INFO
  SPL-D11-005: INFO
```

`.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/your-org/spark-perf-lint
    rev: v0.4.1
    hooks:
      - id: spark-perf-lint
        args: [--severity-threshold, WARNING, --fail-on, CRITICAL]
```

---

### CI / PR analysis (comprehensive)

Catches all issues but uses `WARNING` as the blocking threshold so INFO findings
are visible without breaking the build.

```yaml
# .spark-perf-lint.yaml (or a separate ci.yaml passed with --config)
general:
  severity_threshold: INFO       # show everything
  fail_on:
    - CRITICAL
    - WARNING                    # PRs fail on warnings
  report_format:
    - terminal
    - github_pr                  # post annotations and step summary

ignore:
  directories:
    - ".venv"
    - "venv"
    - "build"
    - "dist"
    - "notebooks"
```

GitHub Actions step:

```yaml
- name: Spark Performance Lint
  run: spark-perf-lint scan src/ --format github-pr --fail-on CRITICAL --fail-on WARNING
  env:
    SPARK_PERF_LINT_SEVERITY_THRESHOLD: INFO
```

---

### Strict mode (block on all findings)

For mature codebases with no existing findings — every new finding blocks the build.

```yaml
general:
  severity_threshold: INFO
  fail_on:
    - CRITICAL
    - WARNING
    - INFO   # nothing gets through
  max_findings: 0
```

---

### Gradual adoption (legacy codebase)

When introducing spark-perf-lint to a codebase with hundreds of existing
findings, cap output and block only on CRITICALs while you pay down the
technical debt:

```yaml
general:
  severity_threshold: WARNING
  fail_on:
    - CRITICAL     # only new criticals block
  max_findings: 20 # cap output to top 20

ignore:
  rules:
    # Temporarily ignore rules with known legacy violations:
    - SPL-D01-001   # executor memory — migration planned Q3
    - SPL-D07-001   # CSV format — legacy pipeline, tracked in JIRA-1234
```

---

### Monorepo with per-team configs

In a monorepo, place a base config at the repo root and per-team overrides in
subdirectories. The linter loads the first config found by walking up from the
scanned path, so team configs override the root config for their subtree only.

```
repo-root/
├── .spark-perf-lint.yaml          ← base config (conservative defaults)
├── teams/
│   ├── data-engineering/
│   │   ├── .spark-perf-lint.yaml  ← strict mode for senior team
│   │   └── jobs/
│   └── ml-platform/
│       ├── .spark-perf-lint.yaml  ← relaxed UDF rules for ML team
│       └── pipelines/
```

Root config:

```yaml
# repo-root/.spark-perf-lint.yaml
general:
  severity_threshold: WARNING
  fail_on: [CRITICAL]
```

ML team override (only changes what differs):

```yaml
# teams/ml-platform/.spark-perf-lint.yaml
severity_override:
  SPL-D09-001: INFO   # Python UDFs are expected in ML pipelines
  SPL-D09-002: INFO   # UDF-replaceability check is too noisy here
```

---

### Organisation-wide config enforcement

Use `spark_config_audit` to enforce that all jobs in a codebase set required
Spark configurations to approved values:

```yaml
spark_config_audit:
  enabled: true

  configs:
    spark.serializer:
      expected_value: "org.apache.spark.serializer.KryoSerializer"
      severity: CRITICAL
      note: "Required by platform policy; Java serializer is banned"

    spark.sql.adaptive.enabled:
      expected_value: "true"
      severity: CRITICAL
      note: "AQE must be enabled in all production jobs"

    spark.executor.memory:
      expected_value: null    # must be present; any value accepted
      severity: WARNING
      note: "Executor memory must be set explicitly; no default allowed"

    spark.driver.memory:
      expected_value: null
      severity: WARNING
```

---

## 7. Ignoring Files and Rules

### Excluding whole directories

```yaml
ignore:
  directories:
    - "notebooks"     # exploratory notebooks
    - "archive"       # legacy code not under active development
    - "vendor"        # vendored third-party code
    - "generated"     # auto-generated code
```

### Excluding files by pattern

```yaml
ignore:
  files:
    - "**/*_test.py"           # test files (anti-patterns are expected)
    - "**/conftest.py"         # pytest configuration
    - "**/migrations/*.py"     # schema migration scripts
    - "src/bootstrap.py"       # exact filename match
    - "src/legacy/**/*.py"     # entire legacy subtree
    - "**/*_generated.py"      # auto-generated files by suffix
```

Patterns are matched against both the full path and the filename alone, so
`conftest.py` matches `tests/unit/conftest.py` without needing a `**/` prefix.

### Suppressing specific rules globally

```yaml
ignore:
  rules:
    - SPL-D11-001   # we use DataDog, not SparkListener
    - SPL-D11-002   # Spark listener registered via deployment config
```

### Downgrading rules instead of suppressing them

If a rule is firing correctly but you don't want it to block builds,
use `severity_override` to demote it to `INFO` rather than silencing it
entirely. This keeps the finding visible in reports:

```yaml
severity_override:
  SPL-D07-001: INFO   # CSV format — acknowledged, migration in backlog
  SPL-D09-001: INFO   # Python UDFs — acceptable for this team's workloads
```

### Inline suppression (per-line)

To suppress a specific finding for a single line of code without affecting
other files or the global config, add a `# noqa: SPL-DXXX-XXX` comment on
the offending line:

```python
# Intentional cross join for Cartesian label assignment
labels = spark.range(1000).crossJoin(label_df)  # noqa: SPL-D03-001
```

Multiple rule IDs can be comma-separated:

```python
result = df.groupBy("status").count()  # noqa: SPL-D05-001, SPL-D02-001
```

### Verifying your ignore rules work

Use `--verbose` to see which files were scanned and which were skipped:

```bash
spark-perf-lint scan . --verbose 2>&1 | grep -E "(Scanning|Skipping)"
```

Or check that a specific rule is disabled for a path:

```bash
# Should exit 0 with no findings if the ignore is working
spark-perf-lint scan src/legacy/ --rule SPL-D07-001 --quiet
```
