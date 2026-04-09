# CLAUDE.md — spark-perf-lint

## Project Overview
Enterprise-grade Apache Spark performance linter. Three tiers:
- Tier 1: Pre-commit hook (fast, offline, pure Python AST analysis)
- Tier 2: CI/PR analysis (Claude API for context-aware recommendations)
- Tier 3: Deep audit (Spark runtime + physical plan analysis)

## Architecture
- Pure Python, zero Spark dependency for Tier 1
- AST-based static analysis using Python's `ast` module
- Rule engine with pluggable rule modules per dimension
- Provider-agnostic observability interface (file-based now, LangSmith later)

## Tech Stack
- Python 3.10+
- ast (stdlib) for code parsing
- PyYAML for config
- Click for CLI
- pytest for testing
- pre-commit framework for hook integration
- anthropic SDK for Tier 2 LLM analysis (optional)

## Code Style
- Black formatter, 100 char line length
- Ruff linter
- Type hints on all public functions
- Docstrings on all classes and public methods (Google style)
- No PySpark dependency in core linter (Tier 1 must work without Spark installed)

## Directory Structure
spark-perf-lint/
├── CLAUDE.md
├── README.md
├── LICENSE
├── pyproject.toml
├── setup.cfg
├── .pre-commit-config.yaml
├── .spark-perf-lint.yaml          # Default project config
├── src/
│   └── spark_perf_lint/
│       ├── __init__.py
│       ├── cli.py                  # Click CLI entry point
│       ├── config.py               # Config loader
│       ├── types.py                # Core data classes
│       ├── engine/
│       │   ├── __init__.py
│       │   ├── ast_analyzer.py     # Python AST parsing
│       │   ├── pattern_matcher.py  # Rule matching framework
│       │   ├── file_scanner.py     # File discovery & staging
│       │   ├── orchestrator.py     # Main scan orchestration
│       │   └── plan_analyzer.py    # Spark plan parser (Tier 3)
│       ├── rules/
│       │   ├── __init__.py
│       │   ├── base.py             # Base rule class
│       │   ├── registry.py         # Rule registration & discovery
│       │   ├── d01_cluster_config.py
│       │   ├── d02_shuffle.py
│       │   ├── d03_joins.py
│       │   ├── d04_partitioning.py
│       │   ├── d05_skew.py
│       │   ├── d06_caching.py
│       │   ├── d07_io_format.py
│       │   ├── d08_aqe.py
│       │   ├── d09_udf_code.py
│       │   ├── d10_catalyst.py
│       │   └── d11_monitoring.py
│       ├── knowledge/
│       │   ├── __init__.py
│       │   ├── recommendations.yaml
│       │   ├── patterns.yaml
│       │   ├── decision_matrices.yaml
│       │   └── spark_configs.yaml
│       ├── reporters/
│       │   ├── __init__.py
│       │   ├── terminal.py
│       │   ├── json_reporter.py
│       │   ├── markdown_reporter.py
│       │   └── github_pr.py
│       ├── llm/
│       │   ├── __init__.py
│       │   ├── analyzer.py         # Claude API integration
│       │   ├── prompts.py          # Prompt templates
│       │   └── provider.py         # LLM provider interface
│       ├── observability/
│       │   ├── __init__.py
│       │   ├── tracer.py           # Provider-agnostic tracer interface
│       │   ├── file_tracer.py      # JSON file-based tracer (default)
│       │   └── langsmith_tracer.py # LangSmith tracer (future)
│       └── tier3/
│           ├── __init__.py
│           ├── data_generators.py  # Synthetic Spark data generators
│           └── benchmarks.py       # Performance benchmarks
├── tests/
│   ├── __init__.py
│   ├── conftest.py                 # Shared fixtures & test utilities
│   ├── fixtures/
│   │   ├── bad_code/               # Anti-pattern examples (10 files)
│   │   ├── good_code/              # Correct pattern examples (10 files)
│   │   └── code_generator.py       # Synthetic PySpark code generator
│   ├── test_ast_analyzer.py
│   ├── test_pattern_matcher.py
│   ├── test_file_scanner.py
│   ├── test_rules_d01_config.py
│   ├── test_rules_d02_shuffle.py
│   ├── test_rules_d03_joins.py
│   ├── test_rules_d04_partitioning.py
│   ├── test_rules_d05_skew.py
│   ├── test_rules_d06_caching.py
│   ├── test_rules_d07_io.py
│   ├── test_rules_d08_aqe.py
│   ├── test_rules_d09_udf.py
│   ├── test_rules_d10_catalyst.py
│   ├── test_edge_cases.py
│   ├── test_code_generator.py
│   ├── test_integration.py
│   ├── test_precommit_integration.py
│   ├── test_performance.py
│   ├── test_cli.py
│   └── test_llm_analyzer.py
├── notebooks/
│   └── deep_audit.ipynb            # Tier 3 interactive audit
├── examples/
│   ├── .pre-commit-config.yaml.example
│   └── .spark-perf-lint.yaml.example
├── .github/
│   ├── workflows/
│   │   ├── ci.yml
│   │   └── release.yml
│   └── actions/
│       └── spark-perf-lint/
│           └── action.yml
└── docs/
    ├── KNOWLEDGE_BASE.md
    ├── CONFIGURATION.md
    ├── RULES_REFERENCE.md
    ├── PRE_COMMIT_SETUP.md
    └── CONTRIBUTING.md

## Key Conventions
- Every rule has a unique ID: SPL-{dimension}{number} e.g., SPL-D03-001
- Every rule returns a Finding dataclass with: rule_id, severity, file, line, message, recommendation, before_code, after_code
- Severity levels: CRITICAL, WARNING, INFO
- All rules must have at least 2 test cases: one positive (should fire), one negative (should not fire)