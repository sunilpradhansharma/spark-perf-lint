"""D10 — Catalyst optimizer rules for spark-perf-lint.

The Catalyst optimizer rewrites logical query plans into efficient physical
plans.  Several common coding patterns prevent Catalyst from applying its most
impactful optimisations: predicate pushdown, cost-based join reordering, and
whole-stage code generation.  The rules here detect these patterns statically
from the source file.

Rule IDs: SPL-D10-001 through SPL-D10-006
"""

from __future__ import annotations

import ast

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.base import CodeRule
from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.types import Dimension, EffortLevel, Finding, Severity

_DIM = Dimension.D10_CATALYST

# Spark config keys
_CBO_ENABLED_KEY = "spark.sql.cbo.enabled"
_JOIN_REORDER_KEY = "spark.sql.cbo.joinReorder.enabled"

# Thresholds
_CBO_JOIN_MIN = 2  # need ≥2 joins for CBO to make a difference
_JOIN_REORDER_MIN = 3  # need ≥3 joins for reorder to help
_CHAIN_DEPTH_THRESHOLD = 20  # method chain ops count that signals a deep plan

# Non-deterministic PySpark SQL functions that cause surprising rewrites in filters
_NON_DETERMINISTIC: frozenset[str] = frozenset(
    {"rand", "randn", "random", "uuid", "monotonically_increasing_id"}
)

_ANALYZE_MARKER = "ANALYZE TABLE"


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _has_nondeterministic_in_args(args: list[ast.expr]) -> str | None:
    """Return the first non-deterministic function name found in *args*, else ``None``."""
    for arg in args:
        for node in ast.walk(arg):
            if not isinstance(node, ast.Call):
                continue
            fname: str | None = None
            if isinstance(node.func, ast.Name):
                fname = node.func.id
            elif isinstance(node.func, ast.Attribute):
                fname = node.func.attr
            if fname in _NON_DETERMINISTIC:
                return fname
    return None


def _chain_depth(node: ast.expr) -> int:
    """Count how many chained ``obj.method(...)`` calls deep *node* goes."""
    depth = 0
    current: ast.expr = node
    while isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
        depth += 1
        current = current.func.value
    return depth


def _has_analyze_table(analyzer: ASTAnalyzer) -> bool:
    """Return ``True`` if any ``spark.sql(...)`` call contains ANALYZE TABLE."""
    for call in analyzer.find_method_calls("sql"):
        if call.args:
            arg = call.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                if _ANALYZE_MARKER in arg.value.upper():
                    return True
    return False


# =============================================================================
# SPL-D10-001 — UDF blocks predicate pushdown
# =============================================================================


@register_rule
class UdfBlocksPredicatePushdownRule(CodeRule):
    """SPL-D10-001: @udf function present alongside filter/where — pushdown blocked."""

    rule_id = "SPL-D10-001"
    name = "UDF blocks predicate pushdown"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "@udf creates an opaque boundary that prevents Catalyst from pushing"
        " filters below the UDF, forcing a full scan before filtering."
    )
    explanation = (
        "Catalyst's predicate pushdown optimisation moves ``filter()``/``where()`` "
        "expressions as close to the data source as possible — ideally into the "
        "reader itself (Parquet/ORC column pruning, partition elimination).\n\n"
        "A ``@udf`` is a black-box from Catalyst's perspective: it cannot inspect "
        "the Python function body, so it cannot safely push filters past it.  Any "
        "``filter()`` applied to a column produced by a UDF must execute *after* "
        "the UDF has run on every row.  This prevents:\n\n"
        "• Parquet/ORC predicate pushdown (physical plan skips row groups early).\n"
        "• Partition pruning at the file reader level.\n"
        "• Whole-stage code generation across the UDF boundary.\n\n"
        "Replacing the UDF with a native Spark SQL expression restores all of "
        "these optimisations because Catalyst can then inspect and reorder the "
        "predicate freely."
    )
    recommendation_template = (
        "Replace the @udf with a native Spark SQL expression to restore predicate "
        "pushdown. If Python logic is unavoidable, use @pandas_udf which has a "
        "narrower optimisation boundary."
    )
    before_example = (
        "@udf(returnType=StringType())\n"
        "def normalise(s):\n"
        "    return s.lower().strip()\n\n"
        "df.withColumn('n', normalise(col('s'))).filter(col('n') == 'target')"
    )
    after_example = (
        "# Native functions — Catalyst can push filter to the source\n"
        "df.filter(trim(lower(col('s'))) == 'target')"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#predicate-pushdown-filtering",
    ]
    estimated_impact = "Full table scan before filter; no partition/row-group elimination"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        has_filter = bool(
            analyzer.find_method_calls("filter") or analyzer.find_method_calls("where")
        )
        if not has_filter:
            return []

        findings: list[Finding] = []
        for func_def in analyzer.find_function_definitions():
            if func_def.udf_type != "udf":
                continue
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    func_def.line,
                    f"@udf '{func_def.name}' blocks Catalyst predicate pushdown"
                    " — replace with a native SQL function to restore optimisation",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D10-002 — CBO not enabled for complex queries
# =============================================================================


@register_rule
class CboNotEnabledRule(CodeRule):
    """SPL-D10-002: File has 2+ joins but spark.sql.cbo.enabled is not set to true."""

    rule_id = "SPL-D10-002"
    name = "CBO not enabled for complex queries"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        f"File contains {_CBO_JOIN_MIN}+ joins but Cost-Based Optimisation is not"
        " enabled — Catalyst uses rule-based estimates instead of real table stats."
    )
    explanation = (
        "Spark's Cost-Based Optimiser (CBO) uses table and column statistics — "
        "collected via ``ANALYZE TABLE`` — to estimate the cardinality and size of "
        "intermediate results.  With CBO enabled, Catalyst can:\n\n"
        "• Choose the correct join strategy (broadcast vs sort-merge vs shuffle-hash).\n"
        "• Re-order joins to process smaller tables first.\n"
        "• Push down filters based on column statistics (min/max ranges).\n\n"
        "Without CBO, Catalyst falls back to heuristic rules that assume worst-case "
        "sizes, often choosing sort-merge joins where a broadcast join would be far "
        "cheaper.  On queries with 2+ joins the difference can be 5–20× in plan "
        "quality.  Enable CBO globally in the session config, then collect statistics "
        "with ``ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS``."
    )
    recommendation_template = (
        "Enable CBO: spark.conf.set('spark.sql.cbo.enabled', 'true'). "
        "Then collect table statistics: ANALYZE TABLE t COMPUTE STATISTICS."
    )
    before_example = (
        "spark = SparkSession.builder.getOrCreate()\n" "result = a.join(b, 'id').join(c, 'key')"
    )
    after_example = (
        "spark = (\n"
        "    SparkSession.builder\n"
        "    .config('spark.sql.cbo.enabled', 'true')\n"
        "    .config('spark.sql.cbo.joinReorder.enabled', 'true')\n"
        "    .getOrCreate()\n"
        ")\n"
        "spark.sql('ANALYZE TABLE a COMPUTE STATISTICS FOR ALL COLUMNS')\n"
        "result = a.join(b, 'id').join(c, 'key')"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#cost-based-query-optimization-cbo-in-spark-sql",
    ]
    estimated_impact = "Sub-optimal join strategies; 5–20× slower plans for multi-join queries"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        join_calls = analyzer.find_method_calls("join")
        if len(join_calls) < _CBO_JOIN_MIN:
            return []

        if analyzer.get_config_value(_CBO_ENABLED_KEY) == "true":
            return []

        first_join = join_calls[0]
        return [
            self.create_finding(
                analyzer.filename,
                first_join.line,
                f"File has {len(join_calls)} join(s) but '{_CBO_ENABLED_KEY}' is"
                " not 'true' — enable CBO for cost-based join strategy selection",
                config=config,
            )
        ]


# =============================================================================
# SPL-D10-003 — Join reordering disabled for multi-table joins
# =============================================================================


@register_rule
class JoinReorderDisabledRule(CodeRule):
    """SPL-D10-003: File has 3+ joins but CBO join reordering is not enabled."""

    rule_id = "SPL-D10-003"
    name = "Join reordering disabled for multi-table joins"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        f"File contains {_JOIN_REORDER_MIN}+ joins but join reordering (CBO) is not"
        " enabled — Catalyst executes joins in source order."
    )
    explanation = (
        "With ``spark.sql.cbo.joinReorder.enabled = true`` and CBO enabled, Catalyst "
        "uses a dynamic-programming algorithm (DP-based bushy join enumeration) to "
        "find the lowest-cost ordering of N-way joins.  The optimal order can reduce "
        "the sizes of intermediate DataFrames dramatically — e.g., joining a small "
        "lookup table first shrinks the result before the expensive join.\n\n"
        "Without join reordering, Catalyst executes joins in the left-to-right order "
        "written in the source, which may produce large intermediate results that make "
        "subsequent joins slow or trigger out-of-memory errors.\n\n"
        "Requirements: ``spark.sql.cbo.enabled = true`` must also be set, and table "
        "statistics must be collected via ``ANALYZE TABLE``."
    )
    recommendation_template = (
        "Enable join reordering: "
        "spark.conf.set('spark.sql.cbo.joinReorder.enabled', 'true'). "
        "Also ensure spark.sql.cbo.enabled is true and stats are collected."
    )
    before_example = "result = a.join(huge, 'id').join(b, 'key').join(small, 'ref')"
    after_example = (
        "spark.conf.set('spark.sql.cbo.enabled', 'true')\n"
        "spark.conf.set('spark.sql.cbo.joinReorder.enabled', 'true')\n"
        "# Catalyst will reorder to process small tables first\n"
        "result = a.join(huge, 'id').join(b, 'key').join(small, 'ref')"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#join-strategy-hints-for-sql-queries",
    ]
    estimated_impact = "Sub-optimal join order; large intermediate DataFrames; possible OOM"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        join_calls = analyzer.find_method_calls("join")
        if len(join_calls) < _JOIN_REORDER_MIN:
            return []

        if analyzer.get_config_value(_JOIN_REORDER_KEY) == "true":
            return []

        first_join = join_calls[0]
        return [
            self.create_finding(
                analyzer.filename,
                first_join.line,
                f"File has {len(join_calls)} join(s) but '{_JOIN_REORDER_KEY}'"
                " is not 'true' — enable join reordering with CBO",
                config=config,
            )
        ]


# =============================================================================
# SPL-D10-004 — Table statistics not collected
# =============================================================================


@register_rule
class TableStatsNotCollectedRule(CodeRule):
    """SPL-D10-004: File has join operations but no ANALYZE TABLE call found."""

    rule_id = "SPL-D10-004"
    name = "Table statistics not collected"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "File performs joins but contains no ANALYZE TABLE statement —"
        " CBO and predicate pushdown lack accurate cardinality estimates."
    )
    explanation = (
        "Spark's Cost-Based Optimiser and runtime adaptive strategies (AQE) rely "
        "on per-table and per-column statistics to make accurate estimates.  These "
        "statistics are populated by running ``ANALYZE TABLE t COMPUTE STATISTICS`` "
        "(or the column-level variant) before the query.\n\n"
        "Without statistics:\n\n"
        "• Join strategy selection defaults to heuristics (size thresholds).\n"
        "• AQE skew detection uses partition byte counts rather than row counts.\n"
        "• CBO's cardinality estimates are off, leading to poor join ordering.\n\n"
        "Run ``ANALYZE TABLE`` once after data is written, or schedule it as part "
        "of the pipeline's post-write step.  For Delta tables, statistics are "
        "maintained automatically when ``delta.dataSkippingNumIndexedCols`` is set."
    )
    recommendation_template = (
        "Add: spark.sql('ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS') "
        "before the join, or run it as a post-write step in the pipeline."
    )
    before_example = "result = spark.table('events').join(spark.table('users'), 'user_id')"
    after_example = (
        "spark.sql('ANALYZE TABLE events COMPUTE STATISTICS FOR ALL COLUMNS')\n"
        "spark.sql('ANALYZE TABLE users COMPUTE STATISTICS FOR ALL COLUMNS')\n"
        "result = spark.table('events').join(spark.table('users'), 'user_id')"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-ref-syntax-aux-analyze-table.html",
    ]
    estimated_impact = (
        "CBO operates without accurate stats; poor join strategies; missed optimisations"
    )
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        join_calls = analyzer.find_method_calls("join")
        if not join_calls:
            return []

        if _has_analyze_table(analyzer):
            return []

        return [
            self.create_finding(
                analyzer.filename,
                join_calls[0].line,
                f"File has {len(join_calls)} join(s) but no ANALYZE TABLE statement"
                " found — collect statistics to improve CBO estimates",
                config=config,
            )
        ]


# =============================================================================
# SPL-D10-005 — Non-deterministic function in filter
# =============================================================================


@register_rule
class NondeterministicInFilterRule(CodeRule):
    """SPL-D10-005: filter()/where() argument contains a non-deterministic function."""

    rule_id = "SPL-D10-005"
    name = "Non-deterministic function in filter"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "filter()/where() contains a non-deterministic function (rand, uuid, …) —"
        " Catalyst may evaluate it multiple times or reorder it unexpectedly."
    )
    explanation = (
        "Non-deterministic functions such as ``rand()``, ``randn()``, ``uuid()``, "
        "and ``monotonically_increasing_id()`` return a different value each time "
        "Catalyst evaluates them.  When used inside ``filter()`` or ``where()``, "
        "Catalyst's rule-based optimiser may:\n\n"
        "• Evaluate the expression multiple times per row during plan rewrites.\n"
        "• Move the filter to a different position in the plan during predicate "
        "  pushdown, changing the effective sample size.\n"
        "• Produce non-reproducible results across re-executions or retries.\n\n"
        "If random sampling is the goal, use ``df.sample(fraction)`` which has "
        "well-defined semantics and consistent optimiser behaviour.  If a random "
        "column is needed for downstream use, materialise it *before* filtering:\n\n"
        "``df.withColumn('r', rand()).filter(col('r') < 0.1)``\n\n"
        "Spark 3+ marks non-deterministic expressions and may prevent some "
        "optimisations automatically, but the intent is still unclear and fragile."
    )
    recommendation_template = (
        "Use df.sample(fraction) for random sampling, or materialise the random "
        "value first: df.withColumn('r', rand()).filter(col('r') < threshold)."
    )
    before_example = "sample = df.filter(rand() < 0.1)"
    after_example = (
        "# Clear semantics — Catalyst treats sample() specially\n"
        "sample = df.sample(fraction=0.1, seed=42)"
    )
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.sample.html",
    ]
    estimated_impact = "Non-reproducible results; potential double-evaluation by Catalyst"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for method in ("filter", "where"):
            for call in analyzer.find_method_calls(method):
                fname = _has_nondeterministic_in_args(call.args)
                if fname is None:
                    continue
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        f"{method}() uses non-deterministic '{fname}()'"
                        " — use df.sample() or materialise the column first",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D10-006 — Complex nested query plan risk (>20 chained ops)
# =============================================================================


@register_rule
class DeepMethodChainRule(CodeRule):
    """SPL-D10-006: A single expression chains more than 20 DataFrame operations."""

    rule_id = "SPL-D10-006"
    name = f"Deep method chain (>{_CHAIN_DEPTH_THRESHOLD} chained ops)"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        f"A single expression chains >{_CHAIN_DEPTH_THRESHOLD} DataFrame operations,"
        " producing a deeply nested logical plan that is slow to analyse and optimise."
    )
    explanation = (
        "Every chained DataFrame operation adds a node to Spark's logical query plan "
        "tree.  Catalyst must traverse, analyse, and optimise this tree before "
        "generating the physical plan.  With a very deep chain:\n\n"
        f"• Plans with >{_CHAIN_DEPTH_THRESHOLD} nodes take noticeably longer for the "
        "  driver to analyse — pure driver-side overhead before any data is processed.\n"
        "• Stack overflow in recursive plan visitors (documented Spark bug for chains "
        "  of hundreds of nodes).\n"
        "• Harder to debug: exception stack traces span the full plan tree.\n\n"
        "Break very long chains by materialising intermediate DataFrames at logical "
        "checkpoints.  This does not add Spark stages; it only simplifies the plan "
        "tree, making analysis faster and errors easier to locate."
    )
    recommendation_template = (
        "Break the chain into named intermediate DataFrames at logical checkpoints: "
        "``filtered = df.filter(...).dropDuplicates(...)\\n"
        "joined = filtered.join(other, 'id')``."
    )
    before_example = (
        "result = (\n"
        "    df.filter('a > 0').dropDuplicates(['id']).join(b, 'id')\n"
        "      .join(c, 'key').groupBy('cat').agg(sum('val'))\n"
        "      # ... 15 more operations ...\n"
        ")"
    )
    after_example = (
        "clean = df.filter('a > 0').dropDuplicates(['id'])\n"
        "enriched = clean.join(b, 'id').join(c, 'key')\n"
        "result = enriched.groupBy('cat').agg(sum('val'))\n"
        "# ... split remaining ops into named steps"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html",
    ]
    estimated_impact = "Driver-side plan analysis overhead; risk of plan visitor stack overflow"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        seen_lines: set[int] = set()

        for node in ast.walk(analyzer.tree):
            expr: ast.expr | None = None
            line: int = 0

            if isinstance(node, ast.Assign):
                expr = node.value
                line = node.lineno
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                expr = node.value
                line = node.lineno
            elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                expr = node.value
                line = node.lineno

            if expr is None or line in seen_lines:
                continue

            depth = _chain_depth(expr)
            if depth > _CHAIN_DEPTH_THRESHOLD:
                seen_lines.add(line)
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        line,
                        f"Method chain depth {depth} exceeds {_CHAIN_DEPTH_THRESHOLD}"
                        " — break into named intermediate DataFrames",
                        config=config,
                    )
                )
        return findings
