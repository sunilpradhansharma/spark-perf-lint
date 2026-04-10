---
layout: default
title: Architecture
nav_order: 3
---

# Architecture
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Three-tier overview

spark-perf-lint is designed around three independent tiers that can be used
separately or together. Each tier adds depth at the cost of additional
dependencies and runtime.

```
┌─────────────────────────────────────────────────────────────────┐
│  Tier 1 — Pre-commit (offline, zero Spark dep)                  │
│  Python AST → Pattern Matching → 93 Rules → Rich Terminal       │
├─────────────────────────────────────────────────────────────────┤
│  Tier 2 — CI / LLM  (requires Anthropic API key)               │
│  Full file context → Claude API → Context-aware recommendations │
├─────────────────────────────────────────────────────────────────┤
│  Tier 3 — Deep audit (requires running Spark cluster)           │
│  Physical plan analysis → Benchmark comparisons → Profiling     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tier 1 — Static analysis engine

Tier 1 runs in milliseconds with no external dependencies. It is the only tier
that runs as a pre-commit hook.

```
git commit
    │
    ▼
FileScanner
  ├─ discover .py files
  ├─ detect PySpark imports (skip non-Spark files)
  └─ respect .gitignore + config ignore patterns
    │
    ▼
ASTAnalyzer
  ├─ parse Python AST (stdlib ast module)
  ├─ extract Spark facts:
  │   ├─ SparkSession configs (.config("key", "val"))
  │   ├─ DataFrame operations (.join, .groupBy, .cache, …)
  │   ├─ UDF definitions (@udf, @pandas_udf)
  │   └─ Action calls (.collect, .count, .toPandas)
  └─ build structured fact set per file
    │
    ▼
PatternMatcher
  ├─ 25+ high-level detectors built on AST facts
  ├─ cross-operation analysis (e.g. cache-then-once-use)
  └─ configurable thresholds (broadcast_mb, partition bounds, …)
    │
    ▼
RuleEngine  (93 rules across 11 dimensions)
  ├─ each rule: CodeRule or ConfigRule subclass
  ├─ @register_rule auto-discovery decorator
  ├─ is_enabled() checks config disable list + severity overrides
  └─ create_finding() produces Finding dataclass
    │
    ▼
Orchestrator
  ├─ merge findings from all rules
  ├─ apply # noqa: SPL-DXX-XXX inline suppressions
  └─ route to reporter
    │
    ▼
Reporter  (terminal / json / markdown / github)
```

### Key source files

| File | Responsibility |
|------|---------------|
| `engine/ast_analyzer.py` | Python AST parsing; Spark-aware fact extraction |
| `engine/pattern_matcher.py` | 25+ high-level anti-pattern detectors |
| `engine/file_scanner.py` | Recursive file discovery; PySpark import detection |
| `engine/orchestrator.py` | Ties engine + rules + reporters; noqa suppression |
| `rules/base.py` | `BaseRule` / `CodeRule` / `ConfigRule` with metadata validation |
| `rules/registry.py` | Singleton registry; `@register_rule` decorator |
| `rules/d01_*.py … d11_*.py` | One module per dimension (93 rules total) |

---

## Tier 2 — LLM analysis

Tier 2 sends the full file content to the Claude API alongside all Tier 1
findings and asks for context-aware explanations and refactoring suggestions.

```
Tier 1 findings
    +
Full file source
    │
    ▼
LLMAnalyzer  (src/spark_perf_lint/llm/analyzer.py)
  ├─ build prompt from PromptTemplates
  ├─ call Anthropic Claude API
  ├─ parse structured JSON response
  └─ augment each Finding with llm_explanation + llm_recommendation
    │
    ▼
EnrichedReport → markdown_reporter → GitHub PR comment
```

### Prompt design

Prompts are templated in `llm/prompts.py`. The system prompt positions Claude
as a senior Spark performance engineer. Each user prompt includes:

- The file being reviewed (full source)
- The rule IDs and messages already found by Tier 1
- A request for explanation, recommendation, and a concrete before/after diff

### Observability

All LLM calls are traced through the `Tracer` interface
(`observability/tracer.py`). The default `FileTracer` writes JSON trace records
to `.spark-perf-lint-traces/`. A `LangSmithTracer` stub is ready for future
LangSmith integration.

To generate an HTML report from traces:

```bash
spark-perf-lint traces --input .spark-perf-lint-traces/ --output report.html
```

---

## Tier 3 — Runtime analysis

Tier 3 requires a running Spark cluster and uses the JVM bridge to inspect
physical query plans.

```
SparkSession (live cluster)
    │
    ▼
PlanAnalyzer  (engine/plan_analyzer.py)
  ├─ df._jdf.queryExecution().simpleString()
  ├─ detect plan anti-patterns: FileScan without pushdown,
  │   BroadcastHashJoin vs SortMergeJoin regression, etc.
  └─ produce PlanFinding list
    │
    ▼
Benchmarks  (tier3/benchmarks.py)
  ├─ run baseline vs optimised queries
  ├─ compare duration, shuffle bytes, GC time
  └─ produce BenchmarkResult with speedup factor

DataGenerators  (tier3/data_generators.py)
  └─ synthetic skewed / uniform / join datasets for reproducible benchmarks
```

The interactive [deep_audit.ipynb](https://github.com/sunilpradhansharma/spark-perf-lint/blob/main/notebooks/deep_audit.ipynb)
notebook walks through a full Tier 3 audit.

---

## Rule system

### Rule anatomy

Every rule is a Python class decorated with `@register_rule`:

```python
@register_rule
class MissingKryoSerializer(ConfigRule):
    rule_id     = "SPL-D01-001"
    name        = "Missing Kryo serializer"
    dimension   = Dimension.D01_CLUSTER_CONFIG
    default_severity = Severity.WARNING
    description = "spark.serializer is not set to KryoSerializer."
    explanation = "..."
    recommendation_template = "..."
    before_example = "SparkSession.builder.appName('job').getOrCreate()"
    after_example  = '.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")'
    estimated_impact = "10× serialization speedup; 3–5× reduction in shuffle data size"
    effort_level = EffortLevel.CONFIG_ONLY
    references   = ["https://spark.apache.org/docs/latest/tuning.html"]

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:
        if not self.is_enabled(config):
            return []
        if not analyzer.has_spark_config("spark.serializer"):
            return [self.create_finding(
                file_path=analyzer.filename,
                line_number=analyzer.spark_session_line or 1,
                message="spark.serializer is not set to KryoSerializer",
                config=config,
            )]
        return []
```

### Registry

`RuleRegistry` is a singleton. `@register_rule` adds the class at import time.
All 11 dimension modules are imported on first access via
`rules/__init__.py`, making all 93 rules available automatically.

```python
from spark_perf_lint.rules.registry import RuleRegistry
rules = RuleRegistry.instance().get_all_rules()        # all 93
d03   = RuleRegistry.instance().get_rules_for_dimension(Dimension.D03_JOINS)
rule  = RuleRegistry.instance().get_rule_by_id("SPL-D03-001")
```

---

## Configuration stack

```
Priority (lowest → highest)

 1. Built-in defaults
 2. .spark-perf-lint.yaml   (walked up from scan target to git root)
 3. SPARK_PERF_LINT_* env vars
 4. CLI flags
```

Each layer is merged recursively — a YAML file that sets one key does not wipe
the others. See [Configuration]({{ site.baseurl }}/configuration) for the full
reference.

---

## Reporter interface

Each reporter receives an `AuditReport` dataclass and returns a string:

| Reporter | Format | Use case |
|----------|--------|----------|
| `terminal.py` | Rich colour table | Interactive CLI |
| `json_reporter.py` | JSON array | Machine processing, CI artefacts |
| `markdown_reporter.py` | GitHub-flavoured Markdown | PR comment bodies |
| `github_pr.py` | GitHub annotations JSON | `github-actions` problem matchers |
