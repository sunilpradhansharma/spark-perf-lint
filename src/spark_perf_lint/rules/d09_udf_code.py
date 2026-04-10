"""D09 — UDF and code pattern rules for spark-perf-lint.

Poor code patterns inside Spark jobs are among the most common sources of
avoidable performance degradation: row-at-a-time Python UDFs bypass the JVM
vectorisation pipeline, collecting unbounded DataFrames to the driver causes
OOM, and rebuilding the query plan in a loop multiplies Catalyst planning
overhead.  These rules detect the most impactful code-level anti-patterns.

Rule IDs: SPL-D09-001 through SPL-D09-012
"""

from __future__ import annotations

import ast

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer, FuncDefInfo
from spark_perf_lint.rules.base import CodeRule
from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.types import Dimension, EffortLevel, Finding, Severity

_DIM = Dimension.D09_UDF_CODE

# Methods that make collect()/toPandas() safer by bounding the result set
_COLLECT_SAFE: frozenset[str] = frozenset(
    {"filter", "where", "limit", "sample", "head", "first", "take"}
)

# String methods that have direct Spark SQL function equivalents
_REPLACEABLE_STRING_METHODS: frozenset[str] = frozenset(
    {"lower", "upper", "strip", "lstrip", "rstrip", "title"}
)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _call_chain(node: ast.expr) -> list[str]:
    """Return the ordered list of method/attribute names in a chained expression.

    Given ``df.filter('x').limit(10).collect()`` returns
    ``["filter", "limit", "collect"]``.
    """
    chain: list[str] = []
    current: ast.expr = node
    while True:
        if isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
            chain.append(current.func.attr)
            current = current.func.value
        elif isinstance(current, ast.Attribute):
            chain.append(current.attr)
            current = current.value
        else:
            break
    chain.reverse()
    return chain


def _trivial_string_udf_method(func_def: FuncDefInfo) -> str | None:
    """Return the string method name if this is a trivial single-method UDF, else ``None``.

    Matches functions of the form::

        @udf(returnType=StringType())
        def my_udf(s):
            return s.lower()
    """
    node = func_def.node
    if not node.args.args:
        return None
    first_arg = node.args.args[0].arg
    if len(node.body) != 1:
        return None
    stmt = node.body[0]
    if not isinstance(stmt, ast.Return) or stmt.value is None:
        return None
    val = stmt.value
    if not isinstance(val, ast.Call):
        return None
    if not isinstance(val.func, ast.Attribute):
        return None
    if not isinstance(val.func.value, ast.Name):
        return None
    if val.func.value.id != first_arg:
        return None
    if val.func.attr in _REPLACEABLE_STRING_METHODS:
        return val.func.attr
    return None


def _has_type_hints(func_def: FuncDefInfo) -> bool:
    """Return ``True`` when the function has at least one type annotation."""
    node = func_def.node
    if node.returns is not None:
        return True
    return any(a.annotation is not None for a in node.args.args)


# =============================================================================
# SPL-D09-001 — Python UDF (row-at-a-time) detected
# =============================================================================


@register_rule
class PythonUdfDetectedRule(CodeRule):
    """SPL-D09-001: Row-at-a-time Python @udf detected."""

    rule_id = "SPL-D09-001"
    name = "Python UDF (row-at-a-time) detected"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "@udf functions execute row-at-a-time, serialising every row between"
        " the JVM and Python interpreter."
    )
    explanation = (
        "A Python UDF registered with ``@udf`` (or ``udf(lambda ...)``) is called "
        "once per row.  For every row Spark must:\n\n"
        "1. Serialise the row from JVM objects to Python pickle format.\n"
        "2. Deserialise in the Python worker process.\n"
        "3. Execute the Python function.\n"
        "4. Serialise the result back to JVM format.\n\n"
        "This per-row round-trip typically adds 2–10× overhead compared to using "
        "native Spark SQL functions.  Worse, the JVM cannot apply predicate pushdown, "
        "column pruning, or whole-stage code generation across a UDF boundary.\n\n"
        "Alternatives in order of preference:\n\n"
        "• **Native Spark SQL functions** (``from pyspark.sql.functions import *``): "
        "  cover most common transformations.\n"
        "• **``@pandas_udf``**: processes a full partition as a ``pd.Series``/"
        "  ``pd.DataFrame``, batch-serialising rows instead of one at a time.\n"
        "• **SQL expressions via ``expr()``**: leverage Catalyst optimisation."
    )
    recommendation_template = (
        "Replace with a native Spark function (e.g., ``lower(col('x'))``). "
        "If business logic requires Python, use ``@pandas_udf`` for vectorised "
        "batch processing instead of row-at-a-time execution."
    )
    before_example = (
        "@udf(returnType=StringType())\n" "def normalise(s):\n" "    return s.lower().strip()"
    )
    after_example = (
        "# Native SQL function — no serialisation overhead\n"
        "df.withColumn('normalised', lower(trim(col('s'))))"
    )
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.functions.pandas_udf.html",
    ]
    estimated_impact = "2–10× slowdown per row; blocks Catalyst code-generation pipeline"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for func_def in analyzer.find_function_definitions():
            if func_def.udf_type == "udf":
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        func_def.line,
                        f"@udf function '{func_def.name}' executes row-at-a-time"
                        " — consider a native Spark function or @pandas_udf",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D09-002 — Python UDF replaceable with native function
# =============================================================================


@register_rule
class UdfReplaceableRule(CodeRule):
    """SPL-D09-002: @udf function body is a single built-in string operation."""

    rule_id = "SPL-D09-002"
    name = "Python UDF replaceable with native Spark function"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "@udf body applies a built-in string method that has a direct Spark SQL equivalent."
    )
    explanation = (
        "This UDF's entire body is a single call to a Python string method "
        "(``lower``, ``upper``, ``strip``, etc.) that has an identical Spark SQL "
        "built-in function.  Using the UDF invokes the full Python serialisation "
        "round-trip for every row; using the native function executes inside the JVM "
        "without any data movement.\n\n"
        "Spark SQL equivalents:\n\n"
        "• ``s.lower()``  →  ``lower(col('s'))``\n"
        "• ``s.upper()``  →  ``upper(col('s'))``\n"
        "• ``s.strip()``  →  ``trim(col('s'))``\n"
        "• ``s.lstrip()`` →  ``ltrim(col('s'))``\n"
        "• ``s.rstrip()`` →  ``rtrim(col('s'))``\n"
        "• ``s.title()``  →  ``initcap(col('s'))``"
    )
    recommendation_template = (
        "Delete the UDF and use the native Spark function: "
        "``df.withColumn('col', lower(col('col')))`` "
        "instead of ``df.withColumn('col', my_lower_udf(col('col')))``."
    )
    before_example = (
        "@udf(returnType=StringType())\n"
        "def my_lower(s):\n"
        "    return s.lower()\n\n"
        "df.withColumn('name', my_lower(col('name')))"
    )
    after_example = "df.withColumn('name', lower(col('name')))"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/functions/string.html",
    ]
    estimated_impact = "Per-row JVM↔Python serialisation for a zero-logic transformation"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for func_def in analyzer.find_function_definitions():
            if func_def.udf_type != "udf":
                continue
            method_name = _trivial_string_udf_method(func_def)
            if method_name is None:
                continue
            sql_equiv = {
                "lower": "lower(col(...))",
                "upper": "upper(col(...))",
                "strip": "trim(col(...))",
                "lstrip": "ltrim(col(...))",
                "rstrip": "rtrim(col(...))",
                "title": "initcap(col(...))",
            }.get(method_name, f"a native SQL equivalent of .{method_name}()")
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    func_def.line,
                    f"@udf '{func_def.name}' calls .{method_name}()"
                    f" — replace with native {sql_equiv}",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D09-003 — withColumn() inside loop
# =============================================================================


@register_rule
class WithColumnInLoopRule(CodeRule):
    """SPL-D09-003: withColumn() called inside a for/while loop."""

    rule_id = "SPL-D09-003"
    name = "withColumn() inside loop"
    dimension = _DIM
    default_severity = Severity.CRITICAL
    description = (
        "withColumn() inside a loop rebuilds the full query plan on every iteration,"
        " causing O(N²) planning time."
    )
    explanation = (
        "Every call to ``withColumn()`` creates a new ``Project`` node in Spark's "
        "logical query plan, wrapping all previous nodes.  Inside a loop of N "
        "iterations this produces a plan with N nested projections — planning and "
        "analysis time grows quadratically.  On large loops (> 100 iterations) this "
        "can take minutes just in the driver before any data is processed.\n\n"
        "The fix is to build a list of column expressions first and apply them in a "
        "single ``select()`` call outside the loop:\n\n"
        "``df.select(*existing_cols, *[expr.alias(name) for name, expr in new_cols])``\n\n"
        "A single ``select()`` with N expressions produces one plan node, "
        "eliminating the quadratic blowup entirely."
    )
    recommendation_template = (
        "Build column expressions in a list and apply with one select() outside the loop:\n"
        "``exprs = [col(c) for c in columns]\n"
        "df = df.select(*df.columns, *exprs)``"
    )
    before_example = (
        "for col_name in column_list:\n" "    df = df.withColumn(col_name, compute(col(col_name)))"
    )
    after_example = (
        "exprs = [compute(col(c)).alias(c) for c in column_list]\n"
        "df = df.select(*df.columns, *exprs)"
    )
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.withColumn.html",
    ]
    estimated_impact = "O(N²) plan building; driver hangs for N > 100 columns"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for loop, count in analyzer.find_method_calls_in_loops("withColumn"):
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    loop.line,
                    f"withColumn() called {count} time(s) inside a {loop.loop_type} loop"
                    " — use select() with a list of expressions outside the loop",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D09-004 — Row-by-row iteration over DataFrame
# =============================================================================


@register_rule
class RowByRowIterationRule(CodeRule):
    """SPL-D09-004: for loop iterates over df.collect() or df.toLocalIterator()."""

    rule_id = "SPL-D09-004"
    name = "Row-by-row iteration over DataFrame"
    dimension = _DIM
    default_severity = Severity.CRITICAL
    description = (
        "Iterating over df.collect() or df.toLocalIterator() forces all data"
        " to the driver and processes it one row at a time."
    )
    explanation = (
        "Iterating over a Spark DataFrame in a Python ``for`` loop — via "
        "``df.collect()``, ``df.toLocalIterator()``, or direct DataFrame iteration "
        "— materialises every row as a Python ``Row`` object on the driver.  "
        "This has two serious problems:\n\n"
        "1. **Driver OOM**: the entire dataset must fit in the driver's heap.\n"
        "2. **No parallelism**: processing is single-threaded on the driver; the "
        "   cluster sits idle.\n\n"
        "Replace with distributed operations:\n\n"
        "• **Filter/aggregation**: express as SQL expressions on the DataFrame.\n"
        "• **Lookup/enrichment**: use a join instead of iterating to look up values.\n"
        "• **Side effects per row**: use ``df.foreach()`` / ``df.foreachPartition()`` "
        "  to distribute side-effecting work across executors.\n"
        "• **Sampling**: use ``df.limit(N).collect()`` only when a small sample "
        "  is intentionally needed."
    )
    recommendation_template = (
        "Replace the loop with a distributed operation: "
        "aggregation, join, or df.foreach()/df.foreachPartition()."
    )
    before_example = "for row in df.collect():\n" "    process(row)"
    after_example = "# Distribute processing — no data movement to driver\n" "df.foreach(process)"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.foreach.html",
    ]
    estimated_impact = "Driver OOM; entire cluster idle while driver processes rows sequentially"
    effort_level = EffortLevel.MAJOR_REFACTOR

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for node in ast.walk(analyzer.tree):
            if not isinstance(node, ast.For):
                continue
            chain = _call_chain(node.iter)
            if "collect" in chain or "toLocalIterator" in chain:
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        node.lineno,
                        "for loop iterates over DataFrame rows via"
                        f" .{chain[-1]}() — moves all data to driver; use foreach()",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D09-005 — .collect() without prior filter or limit
# =============================================================================


@register_rule
class CollectWithoutLimitRule(CodeRule):
    """SPL-D09-005: collect() called on an unbounded DataFrame."""

    rule_id = "SPL-D09-005"
    name = ".collect() without prior filter or limit"
    dimension = _DIM
    default_severity = Severity.CRITICAL
    description = (
        ".collect() without filter()/limit() pulls the entire DataFrame to the"
        " driver — risks OOM for large datasets."
    )
    explanation = (
        "``collect()`` materialises the entire DataFrame as a Python list on the "
        "driver.  Without a prior ``filter()`` or ``limit()``, the result set is "
        "unbounded: a 100 GB dataset will attempt to fit in driver heap, causing "
        "``java.lang.OutOfMemoryError`` or driver restart.\n\n"
        "Legitimate uses of ``collect()``:\n\n"
        "• Collecting a small aggregated result: "
        "  ``df.groupBy('key').count().collect()``.\n"
        "• Collecting with a hard limit: ``df.limit(1000).collect()``.\n"
        "• Collecting a single value: ``df.select('val').first()``.\n\n"
        "For large datasets that must be written to Python, use "
        "``df.write.parquet()`` + a downstream reader, or "
        "``df.toLocalIterator()`` with streaming processing."
    )
    recommendation_template = (
        "Add a limit() or filter() before collect(), or use df.first()/df.take(N). "
        "For writing results, use df.write.parquet() instead."
    )
    before_example = "rows = df.join(other, 'id').collect()"
    after_example = (
        "# Collect a bounded sample\n"
        "rows = df.join(other, 'id').limit(1000).collect()\n"
        "# Or write to storage instead\n"
        "df.join(other, 'id').write.mode('overwrite').parquet('output')"
    )
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.collect.html",
    ]
    estimated_impact = "Driver OOM for datasets > available driver heap; job failure"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("collect"):
            if not any(m in call.chain for m in _COLLECT_SAFE):
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        "collect() on unbounded DataFrame — add limit() or filter()"
                        " before collecting to prevent driver OOM",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D09-006 — .toPandas() on large DataFrame
# =============================================================================


@register_rule
class ToPandasWithoutLimitRule(CodeRule):
    """SPL-D09-006: toPandas() without a prior limit() or filter()."""

    rule_id = "SPL-D09-006"
    name = ".toPandas() on potentially large DataFrame"
    dimension = _DIM
    default_severity = Severity.CRITICAL
    description = (
        ".toPandas() without limit()/filter() materialises the entire DataFrame"
        " in driver memory as a pandas DataFrame."
    )
    explanation = (
        "``toPandas()`` fetches every row from every executor and assembles them "
        "into a single ``pandas.DataFrame`` in the driver's heap.  Without a prior "
        "``limit()`` or ``filter()``, this is identical in effect to ``collect()`` "
        "but with the additional overhead of pandas object creation.\n\n"
        "Common legitimate patterns:\n\n"
        "• Small aggregated result: "
        "  ``df.groupBy('k').count().toPandas()`` — aggregate first.\n"
        "• Bounded sample for plotting/exploration: "
        "  ``df.sample(0.01).limit(10_000).toPandas()``.\n\n"
        "For large-scale DataFrame-to-pandas conversion, use "
        "``pyspark.pandas`` (Koalas API) which keeps the data distributed."
    )
    recommendation_template = (
        "Add limit() or filter() before toPandas(): "
        "``df.filter(cond).limit(10_000).toPandas()``. "
        "For full dataset access, use pyspark.pandas or write to storage."
    )
    before_example = "pdf = df.join(other, 'id').toPandas()"
    after_example = "pdf = df.join(other, 'id').limit(50_000).toPandas()"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.toPandas.html",
    ]
    estimated_impact = "Driver OOM proportional to total dataset size; entire data in one process"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("toPandas"):
            if not any(m in call.chain for m in _COLLECT_SAFE):
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        "toPandas() on unbounded DataFrame — add limit() or filter()"
                        " before converting to prevent driver OOM",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D09-007 — .count() for emptiness check
# =============================================================================


@register_rule
class CountForEmptinessRule(CodeRule):
    """SPL-D09-007: count() used in a comparison — use isEmpty() or limit(1).count() instead."""

    rule_id = "SPL-D09-007"
    name = ".count() used for emptiness check"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "Using df.count() in a comparison triggers a full table scan;"
        " use df.isEmpty() or df.limit(1).count() == 0 to short-circuit."
    )
    explanation = (
        "``df.count()`` scans and counts every row in the DataFrame — an O(N) "
        "distributed operation.  When used only to check whether a DataFrame is "
        "empty (``if df.count() == 0``, ``if df.count() > 0``), the full scan is "
        "wasteful: the answer is known as soon as the first row is found.\n\n"
        "Alternatives:\n\n"
        "• **``df.isEmpty``** (Spark 3.3+): dedicated method, stops after finding "
        "  any row.\n"
        "• **``df.limit(1).count() == 0``**: cap at one row before counting — "
        "  O(1) once any executor finds a row.\n"
        "• **``df.first() is None``**: returns ``None`` if the DataFrame is empty.\n\n"
        "For DataFrames backed by cheap sources (in-memory or already cached), "
        "``count()`` may be acceptable — suppress the finding if so."
    )
    recommendation_template = (
        "Replace ``df.count() == 0`` with ``df.isEmpty`` (Spark 3.3+) "
        "or ``df.limit(1).count() == 0``."
    )
    before_example = "if df.count() == 0:\n" "    raise ValueError('No data found')"
    after_example = (
        "if df.isEmpty:  # Spark 3.3+ — short-circuits on first row\n"
        "    raise ValueError('No data found')"
    )
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.isEmpty.html",
    ]
    estimated_impact = "Full table scan to answer a yes/no question; O(N) instead of O(1)"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for node in ast.walk(analyzer.tree):
            if not isinstance(node, ast.Compare):
                continue
            if isinstance(node.left, ast.Call):
                chain = _call_chain(node.left)
                if "count" in chain:
                    findings.append(
                        self.create_finding(
                            analyzer.filename,
                            node.lineno,
                            "count() used in comparison — use df.isEmpty or"
                            " df.limit(1).count() == 0 to avoid full table scan",
                            config=config,
                        )
                    )
        return findings


# =============================================================================
# SPL-D09-008 — .show() in production code
# =============================================================================


@register_rule
class ShowInProductionRule(CodeRule):
    """SPL-D09-008: .show() prints to stdout — should not appear in production pipelines."""

    rule_id = "SPL-D09-008"
    name = ".show() in production code"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        ".show() triggers a materialisation and prints to stdout — remove before production."
    )
    explanation = (
        "``df.show()`` materialises the top N rows and prints them to stdout.  In a "
        "production pipeline this is problematic for two reasons:\n\n"
        "1. **Wasted computation**: materialising rows solely to print them triggers "
        "   the full upstream query plan.\n"
        "2. **Silent in logs**: production jobs typically redirect stdout; the output "
        "   is lost while the computation cost remains.\n\n"
        "``show()`` is a development/debugging tool.  Replace with:\n\n"
        "• Structured logging via the Python ``logging`` module.\n"
        "• Writing a sample to storage: ``df.limit(100).write.csv('debug/')``. \n"
        "• A proper data quality assertion."
    )
    recommendation_template = (
        "Remove show() calls before production deployment, " "or replace with structured logging."
    )
    before_example = "df.groupBy('status').count().show()"
    after_example = (
        "result = df.groupBy('status').count()\n"
        "# Log summary instead\n"
        "logger.info('status counts: %s', result.collect())"
    )
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.show.html",
    ]
    estimated_impact = "Wasted query execution; output lost in production log redirection"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("show"):
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    "show() triggers materialisation and prints to stdout"
                    " — remove before production deployment",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D09-009 — .explain() or .printSchema() in production code
# =============================================================================


@register_rule
class ExplainInProductionRule(CodeRule):
    """SPL-D09-009: .explain() / .printSchema() are debugging tools, not for production."""

    rule_id = "SPL-D09-009"
    name = ".explain() or .printSchema() in production code"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        ".explain() and .printSchema() are debugging tools that print to stdout"
        " — remove from production pipelines."
    )
    explanation = (
        "``df.explain()`` and ``df.printSchema()`` print query plan / schema "
        "information to stdout.  In production:\n\n"
        "• Output goes to a log file or is discarded entirely.\n"
        "• ``explain()`` can trigger partial query analysis which adds driver overhead.\n"
        "• Their presence is a sign that debugging code was not removed before merge.\n\n"
        "For ongoing query plan visibility in production, use structured logging "
        "with ``df.explain(mode='formatted')`` and write the output to a logging "
        "framework rather than stdout."
    )
    recommendation_template = (
        "Remove explain()/printSchema() calls from production code. "
        "If query plan debugging is needed in production, capture the output: "
        "``logging.debug(df._jdf.queryExecution().toString())``."
    )
    before_example = "df.join(other, 'id').explain(True)"
    after_example = "# Remove — or log via logging.debug() if needed"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.explain.html",
    ]
    estimated_impact = "Debugging output in production logs; extra driver analysis overhead"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for method in ("explain", "printSchema"):
            for call in analyzer.find_method_calls(method):
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        f"{method}() is a debugging tool — remove from" " production pipelines",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D09-010 — .rdd conversion
# =============================================================================


@register_rule
class RddConversionRule(CodeRule):
    """SPL-D09-010: .rdd attribute access drops out of the DataFrame/Dataset API."""

    rule_id = "SPL-D09-010"
    name = ".rdd conversion dropping out of DataFrame API"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "Accessing .rdd deserialises columnar DataFrame rows to Python Row objects,"
        " bypassing Catalyst and Tungsten optimisations."
    )
    explanation = (
        "Spark DataFrames execute through the Catalyst optimiser and Tungsten "
        "whole-stage code generation engine, which keep data in an efficient binary "
        "format throughout the pipeline.  Accessing ``.rdd`` forces a full "
        "deserialisation of each partition into Python ``Row`` objects — immediately "
        "dropping all columnar efficiency.\n\n"
        "Common patterns that use ``.rdd`` and their DataFrame API equivalents:\n\n"
        "• ``.rdd.map(f)`` → ``df.select(expr(...))`` or ``df.withColumn(...)``\n"
        "• ``.rdd.filter(f)`` → ``df.filter(...)``\n"
        "• ``.rdd.flatMap(f)`` → ``df.select(explode(...))``\n"
        "• ``.rdd.reduce(f)`` → ``df.agg(...)``\n"
        "• ``.rdd.count()`` → ``df.count()``\n\n"
        "If the operation genuinely cannot be expressed in the DataFrame API, "
        "consider using ``@pandas_udf`` or ``mapInPandas``/``mapInArrow`` which "
        "batch-process partitions without full deserialisation."
    )
    recommendation_template = (
        "Replace .rdd operations with DataFrame API equivalents. "
        "For complex transformations, use df.mapInPandas() or @pandas_udf."
    )
    before_example = "result = df.rdd.map(lambda r: (r['id'], r['val'] * 2)).toDF()"
    after_example = "result = df.select(col('id'), (col('val') * 2).alias('val'))"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.rdd.html",
    ]
    estimated_impact = "Columnar→row deserialisation; disables Catalyst and Tungsten optimisations"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for attr in analyzer.find_attribute_accesses("rdd"):
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    attr.line,
                    ".rdd access deserialises columnar rows to Python Row objects"
                    " — use DataFrame API or @pandas_udf instead",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D09-011 — pandas_udf without type hints
# =============================================================================


@register_rule
class PandasUdfWithoutTypeHintsRule(CodeRule):
    """SPL-D09-011: @pandas_udf function has no type annotations."""

    rule_id = "SPL-D09-011"
    name = "pandas_udf without type annotations"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "@pandas_udf without parameter/return type hints relies on runtime type"
        " inference and may not vectorise correctly."
    )
    explanation = (
        "Spark 3.0+ recommends using Python type hints on ``@pandas_udf`` functions "
        "to explicitly declare the vectorised execution mode and output type.  "
        "Without hints, Spark falls back to runtime introspection to determine the "
        "UDF's return type, which:\n\n"
        "• Can produce incorrect vectorisation if the return type is ambiguous.\n"
        "• Requires the legacy ``returnType`` keyword argument, which is deprecated.\n"
        "• Makes the function's contract opaque to code reviewers and IDEs.\n\n"
        "Use Python type hints to declare ``pd.Series``, ``pd.DataFrame``, or "
        "``Iterator[pd.Series]`` return types.  The decorator infers the Spark "
        "execution mode (SCALAR, GROUPED_AGG, etc.) from the annotation automatically."
    )
    recommendation_template = (
        "Add pd.Series type hints: " "``def my_udf(s: pd.Series) -> pd.Series:``."
    )
    before_example = (
        "@pandas_udf(returnType=DoubleType())\n" "def my_udf(col):\n" "    return col * 2"
    )
    after_example = (
        "import pandas as pd\n\n"
        "@pandas_udf('double')\n"
        "def my_udf(col: pd.Series) -> pd.Series:\n"
        "    return col * 2"
    )
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.functions.pandas_udf.html",
    ]
    estimated_impact = "Potential type inference errors; deprecated legacy API"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for func_def in analyzer.find_function_definitions():
            if func_def.udf_type != "pandas_udf":
                continue
            if _has_type_hints(func_def):
                continue
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    func_def.line,
                    f"@pandas_udf '{func_def.name}' has no type annotations"
                    " — add pd.Series hints for correct vectorisation",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D09-012 — Nested UDF calls
# =============================================================================


@register_rule
class NestedUdfCallRule(CodeRule):
    """SPL-D09-012: A @udf / @pandas_udf function calls another UDF in its body."""

    rule_id = "SPL-D09-012"
    name = "Nested UDF calls"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "A UDF calls another UDF function in its body — each UDF adds a"
        " separate serialisation boundary."
    )
    explanation = (
        "When a UDF calls another UDF function directly from its body, Spark "
        "executes the inner UDF as a plain Python function call (not as a registered "
        "Spark UDF).  While this technically works, it is a code-smell because:\n\n"
        "• The outer UDF already pays the full Python serialisation cost; the inner "
        "  function should be a plain helper, not a Spark-registered UDF.\n"
        "• Registering the inner function as a ``@udf`` implies it is intended for "
        "  independent use in ``withColumn()``/``select()``, but calling it inside "
        "  another UDF bypasses Spark's execution planner entirely.\n"
        "• It signals possible confusion between DataFrame-level and row-level logic.\n\n"
        "Refactor: turn the inner UDF into a plain Python helper function (no "
        "``@udf`` decorator) and keep the outer UDF as the single Spark boundary."
    )
    recommendation_template = (
        "Remove the @udf decorator from the inner function and call it as a"
        " plain Python helper: "
        "``def _helper(s): return s.lower()`` "
        "called inside the outer UDF."
    )
    before_example = (
        "@udf(returnType=StringType())\n"
        "def inner(s):\n"
        "    return s.strip()\n\n"
        "@udf(returnType=StringType())\n"
        "def outer(s):\n"
        "    return inner(s).lower()  # nested UDF call"
    )
    after_example = (
        "def _strip(s):  # plain helper — no @udf overhead\n"
        "    return s.strip()\n\n"
        "@udf(returnType=StringType())\n"
        "def outer(s):\n"
        "    return _strip(s).lower()"
    )
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.functions.udf.html",
    ]
    estimated_impact = "Confusing code; inner UDF bypasses Spark planner when called inside outer"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        udf_names: set[str] = {f.name for f in analyzer.find_function_definitions() if f.is_udf}
        if len(udf_names) < 2:
            return []

        findings: list[Finding] = []
        for func_def in analyzer.find_function_definitions():
            if not func_def.is_udf:
                continue
            for node in ast.walk(func_def.node):
                if not isinstance(node, ast.Call):
                    continue
                if not isinstance(node.func, ast.Name):
                    continue
                called = node.func.id
                if called in udf_names and called != func_def.name:
                    findings.append(
                        self.create_finding(
                            analyzer.filename,
                            func_def.line,
                            f"UDF '{func_def.name}' calls UDF '{called}' in its body"
                            " — remove @udf from the inner function",
                            config=config,
                        )
                    )
                    break  # one finding per outer UDF
        return findings
