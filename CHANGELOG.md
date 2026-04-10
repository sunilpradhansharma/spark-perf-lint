# Changelog

All notable changes to spark-perf-lint are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] — 2026-04-10

Initial release. Enterprise-grade Apache Spark performance linter with
three-tier architecture: pre-commit hook (Tier 1), CI/PR analysis (Tier 2),
and deep runtime audit (Tier 3).

### Rule engine — 93 rules across 11 dimensions

| Dimension | Rules | Description |
|-----------|------:|-------------|
| D01 · Cluster Configuration | 10 | Executor/driver memory, cores, parallelism, serializer |
| D02 · Shuffle | 7 | `groupByKey`, shuffle partitions, wide-transformation chains |
| D03 · Joins | 9 | Cross joins, broadcast hints, skewed joins, join ordering |
| D04 · Partitioning | 9 | Partition counts, `coalesce(1)`, `repartition` misuse |
| D05 · Data Skew | 7 | Salt key detection, `skewedJoin`, uneven partition ratios |
| D06 · Caching | 8 | `cache()` in loops, redundant caching, missing `unpersist()` |
| D07 · I/O and File Format | 10 | CSV/JSON anti-patterns, missing Parquet, small-files problem |
| D08 · AQE | 7 | `adaptive.enabled=false`, skew join config, coalescing config |
| D09 · UDF and Code Quality | 12 | Python UDFs, `collect()` without limit, `toPandas()` misuse |
| D10 · Catalyst Optimizer | 6 | `withColumn` in loops, schema inference, plan explosion |
| D11 · Monitoring | 5 | Missing listeners, hardcoded paths, unguarded actions |

### Static analysis engine (Tier 1)

- Python AST-based analysis — zero PySpark dependency at scan time
- `PatternMatcher`: 25+ detection methods covering method calls, loops,
  configuration assignments, DataFrame lineage, and RDD patterns
- `FileScanner`: recursive discovery with `.gitignore`-style exclude patterns
  and PySpark import detection (non-PySpark files fast-skipped)
- `ScanOrchestrator`: file-level parallelism, finding sort/cap, noqa suppression
- `# noqa: SPL-DXX-XXX` inline suppression on any finding line

### CLI — five commands

```
spark-perf-lint scan PATH...   # scan files/directories
spark-perf-lint rules          # list all 93 rules
spark-perf-lint explain RULE   # full rule documentation (text/json/markdown)
spark-perf-lint init           # scaffold .spark-perf-lint.yaml
spark-perf-lint version        # print version
```

- `--format terminal|json|markdown|github-pr` (repeatable for multi-output)
- `--severity-threshold CRITICAL|WARNING|INFO`
- `--fail-on CRITICAL|WARNING|INFO` (repeatable; drives exit code 1)
- `--dimension D03,D08` / `--rule SPL-D03-001` for targeted scans
- `--quiet` / `--verbose` / `--no-fix`

### Configuration

- YAML config file (`.spark-perf-lint.yaml`) with four-layer merge stack:
  defaults → YAML → environment variables → CLI flags
- Per-dimension enable/disable, per-rule severity overrides, ignore patterns
- 10 environment variable overrides (`SPL_SEVERITY_THRESHOLD`, etc.)
- `spark_config_audit` section for org-standard Spark config enforcement
- Full reference: `docs/CONFIGURATION.md`

### Reporters

- **Terminal** — Rich-formatted output with colour-coded severity, before/after
  code panels, effort/impact labels
- **JSON** — machine-readable findings array with all metadata fields
- **Markdown** — GitHub-renderable tables; embeds cleanly in PR comments
- **GitHub PR** — annotated diff comments via GitHub REST API

### LLM analysis (Tier 2)

- Claude API integration (Anthropic SDK) for context-aware recommendations
- Finding enrichment: cross-file pattern analysis, root-cause explanation,
  effort estimation
- `--llm` / `SPL_LLM_ENABLED=true` to activate; `llm.max_calls` budget cap
- Reusable GitHub Action at `.github/actions/spark-perf-lint/action.yml`
- Provider interface (`LLMProvider`) for future model substitution

### Deep audit (Tier 3)

- `PlanAnalyzer`: Spark physical plan parser (tree depth, join strategies,
  exchange operators, broadcast nodes)
- `DataGenerators`: synthetic DataFrames for controlled benchmarks
- `Benchmarks`: `benchmark_join_strategies`, `benchmark_partition_counts`,
  `benchmark_cache_strategies`
- Interactive Jupyter notebook at `notebooks/deep_audit.ipynb`

### Observability

- `BaseTracer` / `NullTracer` provider-agnostic interface
- `FileTracer`: JSON trace documents per run in `.spark-perf-lint-traces/`
- `LangSmithTracer`: stub ready for LangSmith SDK wiring
- `TraceViewer`: self-contained HTML dashboard (no external JS dependencies)
- Three verbosity levels: `minimal`, `standard`, `verbose`

### Pre-commit hook

- `.pre-commit-hooks.yaml` registers `spark-perf-lint scan` as a `python`
  language hook with `pass_filenames: true`
- `language: system` local config supported for monorepo setups
- Hook blocks commits on any finding at or above `--fail-on` severity
- `git commit --no-verify` bypass documented and tested

### Documentation

- `README.md` — full project overview, quick-start, architecture diagram
- `docs/RULES_REFERENCE.md` — all 93 rules with examples (6 300+ lines)
- `docs/CONFIGURATION.md` — complete configuration guide
- `docs/KNOWLEDGE_BASE.md` — Spark internals reference
- `docs/PRE_COMMIT_SETUP.md` — pre-commit integration guide
- `docs/CONTRIBUTING.md` — development setup and contribution workflow

### Test suite

- 1 377 tests across 24 test modules
- 84% line coverage on `src/`
- Fixtures: 10 bad-code files (311 findings), 10 good-code files (0 findings)
- `TestRealGitWorkflow`: end-to-end subprocess tests of the git hook
- Performance tests, edge-case tests, CLI integration tests

---

[0.1.0]: https://github.com/sucandra-dasa/spark-perf-lint/releases/tag/v0.1.0
