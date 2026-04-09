"""D11 — Monitoring and observability rules for spark-perf-lint.

Production Spark jobs that lack instrumentation are hard to diagnose when they
slow down or fail silently.  These rules detect the most common observability
gaps: missing query-plan validation in tests, absent Spark listeners, no
metrics logging, unguarded action calls, and hardcoded storage paths that
resist parameterisation.

Rule IDs: SPL-D11-001 through SPL-D11-005
"""

from __future__ import annotations

import ast
import os

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.base import CodeRule
from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.types import Dimension, EffortLevel, Finding, Severity

_DIM = Dimension.D11_MONITORING

# Config keys used by Spark listener registration
_LISTENER_KEYS: frozenset[str] = frozenset(
    {"spark.extraListeners", "spark.sql.queryExecutionListeners"}
)

# Spark action method names that trigger a job
_SPARK_ACTIONS: frozenset[str] = frozenset(
    {"count", "collect", "take", "first", "head", "show", "foreach", "foreachPartition"}
)

# DataFrame operations that add meaningful complexity to a query plan
_HEAVY_OPS: frozenset[str] = frozenset(
    {"join", "groupBy", "agg", "distinct", "orderBy", "sort", "crossJoin"}
)

# URI schemes that identify storage paths (not table names)
_PATH_SCHEMES: tuple[str, ...] = (
    "s3://", "s3a://", "gs://", "abfs://", "wasbs://", "hdfs://", "file://",
)

# Minimum number of write operations that classify a job as "long-running"
_WRITE_ACTIONS_THRESHOLD = 2

# Standard Python logging method names
_LOG_METHODS: frozenset[str] = frozenset(
    {"info", "debug", "warning", "error", "critical", "exception"}
)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _is_test_file(filename: str) -> bool:
    """Return ``True`` if *filename* looks like a pytest test module."""
    base = os.path.basename(filename)
    return base.startswith("test_") or base.endswith("_test.py")


def _try_covered_lines(tree: ast.AST) -> set[int]:
    """Return the set of line numbers that fall inside any try/except block."""
    covered: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            end = getattr(node, "end_lineno", node.lineno)
            covered.update(range(node.lineno, end + 1))
    return covered


def _has_logging(analyzer: ASTAnalyzer) -> bool:
    """Return ``True`` if the file imports or uses a Python logger."""
    for node in ast.walk(analyzer.tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and node.module.startswith("logging"):
                return True
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("logging"):
                    return True
    # Also accept: calls to info/debug/warning on something named *log*/*logger*
    for method in _LOG_METHODS:
        for call in analyzer.find_method_calls(method):
            rcv = call.receiver_src.lower()
            if "log" in rcv:
                return True
    return False


def _is_storage_path(path: str) -> bool:
    """Return ``True`` if *path* looks like a hardcoded storage location."""
    if any(path.startswith(scheme) for scheme in _PATH_SCHEMES):
        return True
    # Absolute POSIX or Windows paths, or relative paths that include a slash
    return "/" in path or "\\" in path


# =============================================================================
# SPL-D11-001 — No explain() in test files
# =============================================================================


@register_rule
class NoExplainInTestsRule(CodeRule):
    """SPL-D11-001: Test file has complex Spark operations but no explain() calls."""

    rule_id = "SPL-D11-001"
    name = "No explain() for plan validation in test file"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "Test file contains complex Spark operations (join/groupBy/agg) but no"
        " explain() call — physical plan quality is not validated."
    )
    explanation = (
        "Adding ``df.explain()`` (or ``explain(mode='formatted')``) inside tests "
        "allows you to assert on the physical query plan, catching performance "
        "regressions early:\n\n"
        "• **Join strategy validation**: assert that a BroadcastHashJoin is chosen "
        "  for small lookup tables — a regression to SortMergeJoin doubles shuffle "
        "  cost.\n"
        "• **Predicate pushdown**: assert that filters appear before scan nodes.\n"
        "• **Partition pruning**: assert that only expected partitions are read.\n\n"
        "Capture the plan string and assert on substrings:\n\n"
        "``plan = df._jdf.queryExecution().explainString('formatted')``\n"
        "``assert 'BroadcastHashJoin' in plan``\n\n"
        "This pattern makes query-plan regressions as visible as logic regressions "
        "and is especially valuable after Spark version upgrades or config changes."
    )
    recommendation_template = (
        "Add explain() assertions: "
        "plan = df._jdf.queryExecution().explainString('formatted'); "
        "assert 'BroadcastHashJoin' in plan."
    )
    before_example = (
        "# test_etl.py\n"
        "def test_join():\n"
        "    result = users.join(events, 'user_id')\n"
        "    assert result.count() == expected"
    )
    after_example = (
        "# test_etl.py\n"
        "def test_join():\n"
        "    result = users.join(events, 'user_id')\n"
        "    plan = result._jdf.queryExecution().explainString('formatted')\n"
        "    assert 'BroadcastHashJoin' in plan  # validate plan strategy\n"
        "    assert result.count() == expected"
    )
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.explain.html",
    ]
    estimated_impact = "Plan regressions (join strategy, pushdown) invisible until production"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        if not _is_test_file(analyzer.filename):
            return []

        # Only relevant when the file tests complex operations worth validating
        has_heavy_op = any(
            analyzer.find_method_calls(op) for op in _HEAVY_OPS
        )
        if not has_heavy_op:
            return []

        if analyzer.find_method_calls("explain"):
            return []

        # Report at the first heavy operation found
        first_line = min(
            call.line
            for op in _HEAVY_OPS
            for call in analyzer.find_method_calls(op)
        )
        return [
            self.create_finding(
                analyzer.filename,
                first_line,
                "Test file has complex Spark operations but no explain() call"
                " — add plan assertions to catch strategy regressions",
                config=config,
            )
        ]


# =============================================================================
# SPL-D11-002 — No Spark listener configured
# =============================================================================


@register_rule
class NoSparkListenerRule(CodeRule):
    """SPL-D11-002: SparkSession built without registering any job/query listeners."""

    rule_id = "SPL-D11-002"
    name = "No Spark listener configured"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "SparkSession is created without registering a listener"
        " — job metrics and query events are not captured."
    )
    explanation = (
        "Spark's listener framework (``SparkListener`` / ``QueryExecutionListener``) "
        "provides hooks for every significant event in a job's lifecycle: task "
        "start/end, stage completion, SQL query execution, shuffle read/write bytes, "
        "and more.  Without a listener, the only observability is what Spark itself "
        "logs to stdout.\n\n"
        "Listeners can be registered two ways:\n\n"
        "1. **Config**: "
        "``spark.conf.set('spark.extraListeners', 'com.example.MyListener')``\n"
        "2. **Programmatic**: "
        "``spark.sparkContext.addSparkListener(MyListener())``\n\n"
        "Common use cases:\n\n"
        "• Emit per-job duration, shuffle bytes, and spill metrics to a metrics "
        "  backend (Datadog, Prometheus, Grafana).\n"
        "• Forward SQL query events to an audit log.\n"
        "• Detect slow stages and alert on-call.\n\n"
        "Without listeners, SLA breaches are only discovered retroactively via "
        "Spark History Server — often after users have already noticed degradation."
    )
    recommendation_template = (
        "Register a listener: "
        "spark.conf.set('spark.extraListeners', 'com.example.JobMetricsListener') "
        "or spark.sparkContext.addSparkListener(listener)."
    )
    before_example = "spark = SparkSession.builder.appName('etl').getOrCreate()"
    after_example = (
        "spark = (\n"
        "    SparkSession.builder\n"
        "    .appName('etl')\n"
        "    .config('spark.extraListeners', 'com.example.MetricsListener')\n"
        "    .getOrCreate()\n"
        ")"
    )
    references = [
        "https://spark.apache.org/docs/latest/monitoring.html"
        "#sparklistener-interface",
    ]
    estimated_impact = "Blind to job performance degradation; metrics only available post-mortem"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        get_or_create_calls = analyzer.find_method_calls("getOrCreate")
        if not get_or_create_calls:
            return []

        # Check for any listener key in static config
        for key in _LISTENER_KEYS:
            if analyzer.get_config_value(key) is not None:
                return []

        # Check for programmatic addSparkListener call
        if analyzer.find_method_calls("addSparkListener"):
            return []

        return [
            self.create_finding(
                analyzer.filename,
                get_or_create_calls[0].line,
                "SparkSession created without 'spark.extraListeners' or"
                " 'spark.sql.queryExecutionListeners' — register a listener"
                " to capture job metrics",
                config=config,
            )
        ]


# =============================================================================
# SPL-D11-003 — No metrics logging in long-running jobs
# =============================================================================


@register_rule
class NoMetricsLoggingRule(CodeRule):
    """SPL-D11-003: File has multiple write operations but no Python logging calls."""

    rule_id = "SPL-D11-003"
    name = "No metrics logging in long-running job"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        f"File has {_WRITE_ACTIONS_THRESHOLD}+ Spark write operations but no"
        " Python logging — job progress and row counts are not recorded."
    )
    explanation = (
        "Multi-stage Spark jobs that write results in several steps benefit greatly "
        "from intermediate logging: recording row counts, elapsed time, and key "
        "metrics after each stage makes it trivial to pinpoint where a slow or "
        "incorrect run diverged from expectation.\n\n"
        "Without logging:\n\n"
        "• A job that runs for 3 hours and produces wrong output requires re-running "
        "  with added instrumentation to diagnose.\n"
        "• Silent data drops (a filter removes all rows) are invisible.\n"
        "• Capacity planning is guesswork without recorded row counts over time.\n\n"
        "Recommended pattern:\n\n"
        "``n = df.count()``\n"
        "``logger.info('stage enriched: %d rows', n)``\n\n"
        "Use Python's built-in ``logging`` module rather than ``print()`` so that "
        "log level, timestamps, and destination can be controlled at runtime."
    )
    recommendation_template = (
        "Add import logging; logger = logging.getLogger(__name__) and log row "
        "counts after each major transformation: "
        "logger.info('step X: %d rows', df.count())."
    )
    before_example = (
        "enriched = raw.join(meta, 'id').withColumn('score', compute(col('v')))\n"
        "enriched.write.mode('overwrite').parquet('s3://bucket/enriched')\n"
        "summary = enriched.groupBy('cat').agg(count('*'))\n"
        "summary.write.mode('overwrite').parquet('s3://bucket/summary')"
    )
    after_example = (
        "import logging\n"
        "logger = logging.getLogger(__name__)\n\n"
        "enriched = raw.join(meta, 'id').withColumn('score', compute(col('v')))\n"
        "logger.info('enriched rows: %d', enriched.count())\n"
        "enriched.write.mode('overwrite').parquet('s3://bucket/enriched')\n"
        "summary = enriched.groupBy('cat').agg(count('*'))\n"
        "logger.info('summary rows: %d', summary.count())\n"
        "summary.write.mode('overwrite').parquet('s3://bucket/summary')"
    )
    references = [
        "https://docs.python.org/3/library/logging.html",
    ]
    estimated_impact = (
        "Silent data drops and slow stages invisible without post-mortem log analysis"
    )
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        writes = analyzer.find_writes()
        if len(writes) < _WRITE_ACTIONS_THRESHOLD:
            return []

        if _has_logging(analyzer):
            return []

        return [
            self.create_finding(
                analyzer.filename,
                writes[0].line,
                f"File has {len(writes)} write operation(s) but no Python logging"
                " — add logger.info() calls to record row counts and timing",
                config=config,
            )
        ]


# =============================================================================
# SPL-D11-004 — Missing error handling around Spark actions
# =============================================================================


@register_rule
class MissingErrorHandlingRule(CodeRule):
    """SPL-D11-004: Spark action calls found outside any try/except block."""

    rule_id = "SPL-D11-004"
    name = "Missing error handling around Spark actions"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "Spark action calls (count, collect, write, …) are not wrapped in"
        " try/except — driver crashes or executor failures propagate unhandled."
    )
    explanation = (
        "Spark actions — ``count()``, ``collect()``, write operations — submit jobs "
        "to the cluster.  Failures from executors surface as Python exceptions on the "
        "driver: ``AnalysisException``, ``SparkException``, ``IOException``, etc.  "
        "Without a ``try/except`` block, these exceptions propagate as unhandled "
        "errors and crash the driver process.\n\n"
        "In production pipelines this means:\n\n"
        "• **Silent failure**: the job exits with a non-zero code and downstream "
        "  jobs proceed with stale/missing data.\n"
        "• **No context**: the stack trace does not capture job parameters, input "
        "  sizes, or the specific partition that failed.\n"
        "• **No cleanup**: temp tables, locks, or partial writes are not rolled back.\n\n"
        "Wrap each logical pipeline stage in a ``try/except`` and log the exception "
        "with enough context to reproduce the failure:\n\n"
        "``except Exception as e:``\n"
        "``    logger.error('stage failed: input=%s rows=%d', path, n, exc_info=True)``\n"
        "``    raise``"
    )
    recommendation_template = (
        "Wrap Spark action calls in try/except Exception and log with exc_info=True "
        "before re-raising."
    )
    before_example = (
        "df = spark.read.parquet(input_path)\n"
        "result = df.join(other, 'id')\n"
        "result.write.mode('overwrite').parquet(output_path)"
    )
    after_example = (
        "try:\n"
        "    df = spark.read.parquet(input_path)\n"
        "    result = df.join(other, 'id')\n"
        "    result.write.mode('overwrite').parquet(output_path)\n"
        "except Exception as e:\n"
        "    logger.error('pipeline failed', exc_info=True)\n"
        "    raise"
    )
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.write.html",
    ]
    estimated_impact = "Unhandled driver crashes; partial writes; no error context in logs"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        # Gather all action calls in the file
        action_calls = [
            call
            for action in _SPARK_ACTIONS
            for call in analyzer.find_method_calls(action)
        ]
        # Include write-terminal calls surfaced as SparkIOInfo
        write_lines = {io.line for io in analyzer.find_writes()}

        if not action_calls and not write_lines:
            return []

        covered = _try_covered_lines(analyzer.tree)

        unprotected = [c for c in action_calls if c.line not in covered]
        unprotected_writes = [ln for ln in write_lines if ln not in covered]

        if not unprotected and not unprotected_writes:
            return []

        # Report once — at the first unprotected action line
        first_line = min(
            [c.line for c in unprotected] + unprotected_writes,
            default=1,
        )
        total = len(unprotected) + len(unprotected_writes)
        return [
            self.create_finding(
                analyzer.filename,
                first_line,
                f"{total} Spark action call(s) found outside any try/except block"
                " — wrap in try/except to capture executor failures",
                config=config,
            )
        ]


# =============================================================================
# SPL-D11-005 — Hardcoded paths (non-parameterized)
# =============================================================================


@register_rule
class HardcodedPathRule(CodeRule):
    """SPL-D11-005: Spark read/write uses a hardcoded string literal as path."""

    rule_id = "SPL-D11-005"
    name = "Hardcoded storage path"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "A Spark read/write uses a hardcoded string path — paths should be"
        " passed as parameters to allow environment-specific configuration."
    )
    explanation = (
        "Hardcoded paths (``s3://prod-bucket/...``, ``/data/input.parquet``) make "
        "jobs difficult to:\n\n"
        "• **Run in multiple environments**: dev, staging, and prod point to "
        "  different buckets/paths; hardcoded values require file edits per env.\n"
        "• **Test safely**: tests must not read from or write to production storage.\n"
        "• **Audit**: paths embedded in code bypass infrastructure-as-code reviews.\n\n"
        "Replace hardcoded paths with:\n\n"
        "• **CLI arguments** via ``argparse`` or ``click``.\n"
        "• **Environment variables**: ``os.environ.get('INPUT_PATH')``.\n"
        "• **Config files** loaded at runtime (YAML/JSON).\n"
        "• **Spark job parameters**: ``--conf spark.myapp.inputPath=...``.\n\n"
        "This pattern separates code from configuration and is a prerequisite for "
        "environment promotion without code changes."
    )
    recommendation_template = (
        "Replace the literal path with a parameter: "
        "input_path = args.input_path or os.environ['INPUT_PATH']. "
        "Then: spark.read.parquet(input_path)."
    )
    before_example = "df = spark.read.parquet('s3://prod-data/events/2024-01/')"
    after_example = (
        "import argparse\n"
        "parser = argparse.ArgumentParser()\n"
        "parser.add_argument('--input-path')\n"
        "args = parser.parse_args()\n"
        "df = spark.read.parquet(args.input_path)"
    )
    references = [
        "https://12factor.net/config",
    ]
    estimated_impact = "Environment coupling; production paths in source control; untestable"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        all_io = analyzer.find_reads() + analyzer.find_writes()
        for io in all_io:
            if io.path is None:
                continue
            if not _is_storage_path(io.path):
                continue
            op = io.operation
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    io.line,
                    f"Hardcoded {op} path '{io.path}'"
                    " — replace with a runtime parameter or environment variable",
                    config=config,
                )
            )
        return findings
