"""D03 — Join rules for spark-perf-lint.

Joins are among the most expensive Spark operations: every non-broadcast join
triggers a full shuffle to co-locate matching keys.  Cross-products, missing
broadcast hints, self-joins, and joins inside loops are common performance
killers.  These rules detect each pattern so developers can address them early.

Rule IDs: SPL-D03-001 through SPL-D03-010
"""

from __future__ import annotations

import ast

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.base import CodeRule, ConfigRule
from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.types import Dimension, EffortLevel, Finding, Severity

_DIM = Dimension.D03_JOINS

# how= values that indicate a left-outer join
_LEFT_JOIN_TYPES = frozenset({"left", "left_outer", "leftouter"})

# Methods that handle null values — suppress Rule 009 when present
_NULL_HANDLER_METHODS = frozenset({"fillna", "dropna", "coalesce", "na"})

# Read-terminal methods — presence in a join chain triggers Rule 004
_READ_TERMINALS = frozenset({"parquet", "csv", "json", "orc", "load", "table", "avro"})

# Filter/projection methods — presence suppresses Rule 004
_FILTER_METHODS = frozenset({"filter", "where", "select", "limit"})

# Materialisation methods that break a join sequence for Rule 006
_BREAK_METHODS = frozenset(
    {"cache", "persist", "checkpoint", "localCheckpoint", "repartition", "broadcast"}
)


# =============================================================================
# Private helpers
# =============================================================================


def _get_join_how(call) -> str | None:
    """Return the normalised ``how`` argument of a ``join()`` call.

    Handles both keyword (``how="left"``) and third positional argument forms.
    Returns ``None`` when ``how`` is not specified (Spark defaults to
    ``"inner"``).
    """
    how_node = call.kwargs.get("how")
    if how_node is not None:
        if isinstance(how_node, ast.Constant) and isinstance(how_node.value, str):
            return how_node.value.lower()
    # Positional: join(other, on, how)
    if len(call.args) >= 3:
        how_arg = call.args[2]
        if isinstance(how_arg, ast.Constant) and isinstance(how_arg.value, str):
            return how_arg.value.lower()
    return None


def _broadcast_lines(analyzer: ASTAnalyzer) -> set[int]:
    """Return line numbers where ``broadcast()`` or ``hint("broadcast")`` is used.

    Scans the full AST for both the ``broadcast()`` function (a ``Name`` call,
    not captured by the method-call visitor) and the ``df.hint("broadcast")``
    method call.
    """
    lines: set[int] = set()
    # broadcast() as a standalone function call
    for node in ast.walk(analyzer.tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id == "broadcast":
                lines.add(node.lineno)
    # df.hint("broadcast")
    for call in analyzer.find_method_calls("hint"):
        if call.args and isinstance(call.args[0], ast.Constant):
            if str(call.args[0].value).lower() == "broadcast":
                lines.add(call.line)
    return lines


# =============================================================================
# SPL-D03-001 — Cross join / cartesian product
# =============================================================================


@register_rule
class CrossJoinRule(CodeRule):
    """SPL-D03-001: crossJoin() or join(..., how='cross') creates a cartesian product."""

    rule_id = "SPL-D03-001"
    name = "Cross join / cartesian product"
    dimension = _DIM
    default_severity = Severity.CRITICAL
    description = "crossJoin() or join(how='cross') creates an O(n²) cartesian product."
    explanation = (
        "A cross join multiplies *every* row in the left DataFrame with *every* row in "
        "the right DataFrame.  Two DataFrames of 10 M rows each produce a 100 trillion-row "
        "result.  This pattern almost always indicates a missing join key or a logic error.\n\n"
        "Legitimate uses are rare and typically involve tiny reference DataFrames (fewer "
        "than a few thousand rows).  Even then, broadcasting the smaller side and using an "
        "explicit ``join()`` with a constant condition is safer and more self-documenting.\n\n"
        "Spark does not warn when you accidentally omit a join key — it silently upgrades "
        "the plan to a cartesian product, which can run for hours or OOM the cluster."
    )
    recommendation_template = (
        "Add the correct join key to ``join(other, on='key_column')``. "
        "If a cross product is genuinely required, use a very small right-side DataFrame "
        "wrapped in ``broadcast()`` and document the intent explicitly."
    )
    before_example = "df.crossJoin(other)  # O(n²) rows!"
    after_example = "df.join(other, on='id', how='inner')  # explicit key"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.crossJoin.html",
    ]
    estimated_impact = "Unbounded row explosion; can OOM or run indefinitely"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []

        for call in analyzer.find_method_calls("crossJoin"):
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    "crossJoin() creates an O(n²) cartesian product — add a join key",
                    config=config,
                )
            )

        for call in analyzer.find_method_calls("join"):
            how = _get_join_how(call)
            if how == "cross":
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        "join(how='cross') creates an O(n²) cartesian product — add a join key",
                        config=config,
                    )
                )

        return findings


# =============================================================================
# SPL-D03-002 — Missing broadcast hint on small DataFrame
# =============================================================================


@register_rule
class MissingBroadcastHintRule(CodeRule):
    """SPL-D03-002: join() without broadcast() hint on small DataFrame."""

    rule_id = "SPL-D03-002"
    name = "Missing broadcast hint on small DataFrame"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "join() without any broadcast hint — small lookup tables should be broadcast."
    explanation = (
        "When one side of a join is significantly smaller than the other (a typical "
        "fact-dimension or fact-lookup pattern), wrapping it in ``broadcast()`` tells "
        "Spark to send the small table to every executor as a hash map, eliminating the "
        "shuffle entirely.\n\n"
        "Without a broadcast hint, Spark falls back to a sort-merge join, which shuffles "
        "**both** DataFrames across the network — often orders of magnitude more I/O.\n\n"
        "Spark's ``spark.sql.autoBroadcastJoinThreshold`` auto-broadcasts tables below "
        "a size limit (default 10 MB).  For tables slightly above the threshold, or when "
        "auto-broadcast is disabled, explicit hints are the only way to trigger a broadcast."
    )
    recommendation_template = (
        "Wrap the smaller DataFrame in ``broadcast()``: "
        "``df.join(broadcast(small_df), 'id')``. "
        "For AQE-managed queries in Spark 3+, also set "
        "``spark.sql.adaptive.autoBroadcastJoinThreshold`` to a reasonable value."
    )
    before_example = "df.join(lookup, 'product_id')"
    after_example = "df.join(broadcast(lookup), 'product_id')"
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#broadcast-hint-for-sql-queries",
    ]
    estimated_impact = "Eliminates shuffle on both sides; 10–100× faster for small lookups"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        # If autoBroadcastJoinThreshold is set to a positive value, AQE/Spark
        # may auto-broadcast — only suppress if it's explicitly a positive number.
        threshold = analyzer.get_config_value("spark.sql.autoBroadcastJoinThreshold")
        if threshold is not None:
            try:
                if int(threshold.strip()) > 0:
                    return []
            except ValueError:
                pass

        bcast_lines = _broadcast_lines(analyzer)

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("join"):
            how = _get_join_how(call)
            if how == "cross":
                continue  # covered by SPL-D03-001

            # Check if broadcast appears in the join's own AST subtree
            if any(abs(bl - call.line) <= 2 for bl in bcast_lines):
                continue

            # Walk the call's AST node for inline broadcast usage
            has_inline_bcast = False
            for node in ast.walk(call.node):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    if node.func.id == "broadcast":
                        has_inline_bcast = True
                        break
            if has_inline_bcast:
                continue

            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    "join() without broadcast() hint — wrap small DataFrames in broadcast()",
                    config=config,
                )
            )

        return findings


# =============================================================================
# SPL-D03-003 — Broadcast threshold disabled (-1)
# =============================================================================


@register_rule
class BroadcastThresholdDisabledRule(ConfigRule):
    """SPL-D03-003: spark.sql.autoBroadcastJoinThreshold = -1 disables auto-broadcast."""

    rule_id = "SPL-D03-003"
    name = "Broadcast threshold disabled"
    dimension = _DIM
    default_severity = Severity.CRITICAL
    description = "spark.sql.autoBroadcastJoinThreshold = -1 disables all broadcast joins."
    explanation = (
        "Setting ``spark.sql.autoBroadcastJoinThreshold`` to ``-1`` completely disables "
        "Spark's automatic broadcast join optimisation.  Every join — including those on "
        "tiny lookup tables with a few hundred rows — will fall back to a full sort-merge "
        "join, shuffling both sides of every join across the network.\n\n"
        "This setting is sometimes set to ``-1`` to work around a race condition in older "
        "Spark versions where broadcasted tables were garbage-collected mid-query.  That "
        "bug was fixed in Spark 2.4+.  In modern deployments there is almost never a "
        "valid reason to disable broadcast joins entirely.\n\n"
        "If specific joins are causing OOM errors due to over-eager broadcasting, use "
        "``df.hint('merge')`` to force sort-merge for those joins rather than disabling "
        "broadcast globally."
    )
    recommendation_template = (
        "Remove the ``-1`` value or set a reasonable threshold "
        "(e.g. ``spark.sql.autoBroadcastJoinThreshold = 10485760`` for 10 MB). "
        "Use per-join ``hint('merge')`` to opt specific joins out of broadcasting."
    )
    before_example = '.config("spark.sql.autoBroadcastJoinThreshold", "-1")'
    after_example = '.config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB'
    references = [
        "https://spark.apache.org/docs/latest/configuration.html#execution-behavior",
    ]
    estimated_impact = "Every join shuffles both sides; disables all broadcast optimisations"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        raw = analyzer.get_config_value("spark.sql.autoBroadcastJoinThreshold")
        if raw is None:
            return []

        try:
            if int(raw.strip()) != -1:
                return []
        except ValueError:
            return []

        configs = [
            c
            for c in analyzer.find_spark_session_configs()
            if c.key == "spark.sql.autoBroadcastJoinThreshold"
        ]
        line = configs[-1].end_line if configs else 1
        return [
            self.create_finding(
                analyzer.filename,
                line,
                "spark.sql.autoBroadcastJoinThreshold = -1 disables all broadcast joins",
                config=config,
            )
        ]


# =============================================================================
# SPL-D03-004 — Join without prior filter/select
# =============================================================================


@register_rule
class JoinWithoutFilterRule(CodeRule):
    """SPL-D03-004: join() on a DataFrame read from storage without prior filter."""

    rule_id = "SPL-D03-004"
    name = "Join without prior filter/select"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "join() on an unfiltered read — apply filter()/select() first to reduce data."
    explanation = (
        "Joining full tables without projecting or filtering first forces Spark to "
        "shuffle *all* rows and *all* columns of both DataFrames across the network.\n\n"
        "Two 500 GB tables joined without any filtering = 1 TB of shuffle traffic just "
        "to materialise the join keys.  A simple ``filter()`` that removes 90 % of rows "
        "before the join reduces shuffle data by the same fraction.\n\n"
        "``select()`` before join is equally valuable: omitting wide columns that are "
        "not needed in the join result prevents those bytes from being serialised, "
        "transferred, and deserialised on every shuffle node.  On wide tables with "
        "hundreds of columns this is often the single biggest win available."
    )
    recommendation_template = (
        "Apply ``filter()`` and ``select()`` *before* ``join()`` to reduce shuffle payload. "
        "Push predicates as close to the data source as possible."
    )
    before_example = (
        "df = spark.read.parquet('s3://bucket/events')\n" "result = df.join(users, 'user_id')"
    )
    after_example = (
        "df = spark.read.parquet('s3://bucket/events').filter('date >= \"2024-01-01\"')\n"
        "result = df.select('user_id', 'event_type').join(users, 'user_id')"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html",
    ]
    estimated_impact = "Up to 10–100× shuffle reduction when large fractions of data are pruned"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("join"):
            chain = call.chain
            try:
                join_idx = chain.index("join")
            except ValueError:
                continue

            prefix = set(chain[:join_idx])
            if not (_READ_TERMINALS & prefix):
                continue  # no read in chain — can't determine if filtered elsewhere

            if _FILTER_METHODS & prefix:
                continue  # filter/select precedes join in the chain

            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    "join() on unfiltered read — apply filter()/select() first"
                    " to reduce shuffle payload",
                    config=config,
                )
            )

        return findings


# =============================================================================
# SPL-D03-005 — Join key type mismatch risk
# =============================================================================


@register_rule
class JoinKeyTypeMismatchRule(CodeRule):
    """SPL-D03-005: join condition compares columns with different names — type mismatch risk."""

    rule_id = "SPL-D03-005"
    name = "Join key type mismatch risk"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "join() condition compares different column names — verify the types match."
    explanation = (
        "When joining on a condition like ``df1.order_id == df2.id``, the two columns "
        "are implicitly cast to a common type before comparison.  If ``order_id`` is "
        "``LongType`` and ``id`` is ``StringType``, Spark silently coerces one side, "
        "which can produce wrong results (integer ``123`` never equals string ``'123'``) "
        "or cause the join to produce zero rows.\n\n"
        "The risk is highest when:\n"
        "• One column originates from a JSON/CSV source with inferred schema\n"
        "• Column names differ between tables (common in star-schema models)\n"
        "• One side was loaded with ``inferSchema=True``\n\n"
        "Spark's type coercion rules are non-obvious and change between versions.  "
        "Explicit ``cast()`` is always safer than relying on implicit promotion."
    )
    recommendation_template = (
        "Add explicit ``cast()`` to align types: "
        "``df1.join(df2, df1.order_id.cast('long') == df2.id.cast('long'))``. "
        "Or rename one column so a simple string key can be used: "
        "``df2 = df2.withColumnRenamed('id', 'order_id'); df1.join(df2, 'order_id')``."
    )
    before_example = "df1.join(df2, df1.order_id == df2.id)"
    after_example = "df1.join(df2, df1.order_id.cast('long') == df2.id.cast('long'))"
    references = [
        "https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html",
    ]
    estimated_impact = "Silent wrong-result bugs or empty join output due to type mismatch"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("join"):
            if len(call.args) < 2:
                continue
            on_node = call.args[1]

            # Only flag explicit column-equality conditions (df1.col == df2.col)
            if not isinstance(on_node, ast.Compare):
                continue
            if not (len(on_node.ops) == 1 and isinstance(on_node.ops[0], ast.Eq)):
                continue
            if not on_node.comparators:
                continue

            left = on_node.left
            right = on_node.comparators[0]

            # Extract the attribute name from each side
            left_col = left.attr if isinstance(left, ast.Attribute) else None
            right_col = right.attr if isinstance(right, ast.Attribute) else None

            if left_col is None or right_col is None:
                continue
            if left_col == right_col:
                continue  # same name → likely same type; use string key instead

            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    f"join condition '{left_col} == {right_col}' uses different column"
                    " names — verify types match or cast explicitly",
                    config=config,
                )
            )

        return findings


# =============================================================================
# SPL-D03-006 — Multiple joins without intermediate repartition
# =============================================================================


@register_rule
class MultipleJoinsWithoutRepartitionRule(CodeRule):
    """SPL-D03-006: Three or more join() calls in sequence without broadcast or repartition."""

    rule_id = "SPL-D03-006"
    name = "Multiple joins without intermediate repartition"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "Three or more join() calls in sequence without any broadcast or repartition hint."
    )
    explanation = (
        "In a star-schema join pattern (fact table joined to multiple dimension tables), "
        "every non-broadcast join triggers a full shuffle.  Three consecutive sort-merge "
        "joins on different keys produce three independent shuffle stages, each reading "
        "and writing the full intermediate dataset.\n\n"
        "Strategies to reduce cost:\n"
        "• **Broadcast small dimensions**: wrap any table under the broadcast threshold "
        "  in ``broadcast()`` to eliminate its shuffle entirely.\n"
        "• **Repartition once by the most selective key**: if all joins are on the same "
        "  key, one upfront ``repartition('key')`` can make subsequent joins local.\n"
        "• **Enable CBO** (``spark.sql.cbo.enabled = true``) so Catalyst can re-order "
        "  joins to minimise intermediate result size.\n"
        "• **Filter aggressively before the first join** to reduce the base row count "
        "  that flows through every subsequent join stage."
    )
    recommendation_template = (
        "Broadcast small dimensions with ``broadcast(dim_df)``. "
        "Enable CBO to auto-reorder joins: "
        "``spark.sql.cbo.enabled = true`` and ``spark.sql.statistics.histogram.enabled = true``. "
        "Repartition by the join key before the first join when all joins share a common key."
    )
    before_example = (
        "result = (facts\n"
        "    .join(dim_a, 'key_a')\n"
        "    .join(dim_b, 'key_b')\n"
        "    .join(dim_c, 'key_c'))"
    )
    after_example = (
        "result = (facts\n"
        "    .join(broadcast(dim_a), 'key_a')\n"
        "    .join(broadcast(dim_b), 'key_b')\n"
        "    .join(broadcast(dim_c), 'key_c'))"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#join-strategy-hints-for-sql-queries",
    ]
    estimated_impact = "3+ full shuffles; each shuffle writes/reads entire intermediate dataset"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    _MIN_JOIN_COUNT = 3

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        join_calls = sorted(
            [c for c in analyzer.find_method_calls("join") if _get_join_how(c) != "cross"],
            key=lambda c: c.line,
        )
        if len(join_calls) < self._MIN_JOIN_COUNT:
            return []

        bcast_lines = _broadcast_lines(analyzer)
        break_lines = {
            c.line for c in analyzer.find_all_method_calls() if c.method_name in _BREAK_METHODS
        }
        break_lines |= bcast_lines

        # Build runs of consecutive joins (within 15 lines of each other) with no break
        runs: list[list] = []
        current: list = [join_calls[0]]

        for prev, cur in zip(join_calls, join_calls[1:], strict=False):
            gap_too_large = (cur.line - prev.line) > 15
            # Use prev.line <= bl (inclusive) so cache() on the same line as
            # the preceding join (e.g. df.join(...).cache()) counts as a break.
            has_break = any(prev.line <= bl < cur.line for bl in break_lines)
            if gap_too_large or has_break:
                if len(current) >= self._MIN_JOIN_COUNT:
                    runs.append(current)
                current = [cur]
            else:
                current.append(cur)

        if len(current) >= self._MIN_JOIN_COUNT:
            runs.append(current)

        findings: list[Finding] = []
        for run in runs:
            third = run[self._MIN_JOIN_COUNT - 1]
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    third.line,
                    f"{len(run)} join() calls in sequence without broadcast or repartition"
                    " — consider broadcasting small dimensions",
                    config=config,
                )
            )

        return findings


# =============================================================================
# SPL-D03-007 — Self-join that could be a window function
# =============================================================================


@register_rule
class SelfJoinRule(CodeRule):
    """SPL-D03-007: join() where both sides reference the same DataFrame."""

    rule_id = "SPL-D03-007"
    name = "Self-join that could be window function"
    dimension = _DIM
    default_severity = Severity.INFO
    description = "join() where the left and right sides reference the same DataFrame."
    explanation = (
        "A self-join (joining a DataFrame to itself) doubles the shuffle cost: both "
        "the left and right sides of the join are the same data, so Spark must "
        "serialise, shuffle, and deserialise the same dataset twice.\n\n"
        "Most self-join patterns can be rewritten as a single ``Window`` function pass:\n"
        "• **Running totals / moving averages**: ``Window.partitionBy().orderBy()``\n"
        "• **Lead/lag comparisons**: ``lag(col, 1).over(window)``\n"
        "• **Rank within group**: ``rank().over(Window.partitionBy().orderBy())``\n"
        "• **Deduplication**: ``row_number().over(...) == 1``\n\n"
        "Window functions operate within a single partition pass and avoid both the "
        "shuffle and the row duplication that a self-join produces."
    )
    recommendation_template = (
        "Replace the self-join with a ``Window`` function. "
        "Import with ``from pyspark.sql.window import Window`` and use "
        "``rank() / lag() / sum() ... .over(Window.partitionBy(...).orderBy(...)``."
    )
    before_example = (
        "# self-join to get previous row\n"
        "df.join(df.withColumnRenamed('value', 'prev_value'), ...)"
    )
    after_example = (
        "from pyspark.sql.window import Window\n"
        "w = Window.partitionBy('key').orderBy('ts')\n"
        "df.withColumn('prev_value', lag('value', 1).over(w))"
    )
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.functions.lag.html",
    ]
    estimated_impact = "2× shuffle data volume; window functions need zero cross-node transfer"
    effort_level = EffortLevel.MAJOR_REFACTOR

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("join"):
            if not call.args:
                continue

            other_src = ast.unparse(call.args[0]).strip()
            receiver_src = call.receiver_src.strip()

            if other_src == receiver_src:
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        f"self-join detected: '{receiver_src}'.join('{other_src}') "
                        "— consider Window functions instead",
                        config=config,
                    )
                )

        return findings


# =============================================================================
# SPL-D03-008 — Join inside loop
# =============================================================================


@register_rule
class JoinInsideLoopRule(CodeRule):
    """SPL-D03-008: join() called inside a for/while loop creates a new shuffle each iteration."""

    rule_id = "SPL-D03-008"
    name = "Join inside loop"
    dimension = _DIM
    default_severity = Severity.CRITICAL
    description = "join() inside a loop creates a new shuffle stage on every iteration."
    explanation = (
        "Calling ``join()`` inside a Python ``for`` or ``while`` loop does *not* "
        "accumulate rows iteratively — it builds a new query plan on each iteration "
        "and triggers a full shuffle every time the loop body executes.\n\n"
        "With 100 loop iterations each joining a 10 GB table, the total shuffle I/O is "
        "1 TB even if the final result is tiny.  The query plan also grows linearly with "
        "the loop count, causing plan serialisation overhead and sometimes stack overflows "
        "in Catalyst for large iteration counts.\n\n"
        "The correct pattern is to union all the right-side DataFrames first and perform "
        "a single join, or to read all data with a filter and join once."
    )
    recommendation_template = (
        "Collect the loop values into a list and build a single DataFrame: "
        "``right_df = spark.createDataFrame(values, schema); df.join(right_df, 'key')``. "
        "For date ranges, use ``filter(col('date').between(start, end))`` rather than "
        "looping over individual dates."
    )
    before_example = (
        "for date in date_range:\n"
        "    result = result.join(daily_df.filter(f'date = \"{date}\"'), 'id')"
    )
    after_example = (
        "# Load all dates at once and join once\n"
        "all_dates = spark.createDataFrame([(d,) for d in date_range], ['date'])\n"
        "result = result.join(daily_df, 'id')"
    )
    references = [
        "https://spark.apache.org/docs/latest/rdd-programming-guide.html"
        "#resilient-distributed-datasets-rdds",
    ]
    estimated_impact = "N shuffles where N = loop iterations; plan explosion for large loops"
    effort_level = EffortLevel.MAJOR_REFACTOR

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for loop, count in analyzer.find_method_calls_in_loops("join"):
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    loop.line,
                    f"join() called {count}× inside a {loop.loop_type}-loop"
                    " — each iteration triggers a full shuffle",
                    config=config,
                )
            )

        return findings


# =============================================================================
# SPL-D03-009 — Left join without null handling
# =============================================================================


@register_rule
class LeftJoinWithoutNullHandlingRule(CodeRule):
    """SPL-D03-009: left join() without downstream null handling."""

    rule_id = "SPL-D03-009"
    name = "Left join without null handling"
    dimension = _DIM
    default_severity = Severity.INFO
    description = "left join() without null handling — null rows may propagate silently."
    explanation = (
        "A left join always produces ``null`` values for right-side columns when no "
        "matching row exists.  If those nulls are not handled explicitly, they silently "
        "propagate through every subsequent aggregation, filter, and write operation.\n\n"
        "Common failure modes:\n"
        "• ``count('column')`` returns fewer rows than ``count('*')`` due to null exclusion\n"
        "• ``sum('amount')`` returns a wrong total because nulls are skipped\n"
        "• ``filter(col > 0)`` silently drops all unmatched rows\n"
        "• Writing to Parquet with ``NOT NULL`` schema constraints fails at write time\n\n"
        "Handling nulls explicitly (``fillna`` / ``dropna`` / ``isNotNull`` filter / "
        "``coalesce``) makes the intent clear and prevents downstream data quality issues."
    )
    recommendation_template = (
        "After a left join, handle nulls explicitly: "
        "``df.join(other, 'id', 'left').fillna(0, subset=['amount'])`` "
        "or ``df.join(other, 'id', 'left').dropna(subset=['required_col'])``."
    )
    before_example = "result = df.join(lookup, 'id', 'left')"
    after_example = "result = df.join(lookup, 'id', 'left').fillna({'status': 'n/a', 'score': 0})"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.fillna.html",
    ]
    estimated_impact = "Silent null propagation; wrong aggregation results and data quality bugs"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        # Collect lines with null-handling calls
        null_lines: set[int] = set()
        for method in _NULL_HANDLER_METHODS:
            for c in analyzer.find_method_calls(method):
                null_lines.add(c.line)

        # Collect lines where a null handler is chained AFTER a join in the
        # same expression (e.g. df.join(...).fillna(0) — fillna's chain
        # contains "join", and both calls share the same line).
        chained_null_on_lines: set[int] = set()
        for method in _NULL_HANDLER_METHODS:
            for c in analyzer.find_method_calls(method):
                if "join" in c.chain:
                    chained_null_on_lines.add(c.line)

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("join"):
            how = _get_join_how(call)
            if how not in _LEFT_JOIN_TYPES:
                continue

            # Null handler is chained on the same line (comes after join)
            if call.line in chained_null_on_lines:
                continue

            # Check for null handling within 5 lines after the join
            nearby = any(call.line < nl <= call.line + 5 for nl in null_lines)
            if nearby:
                continue

            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    "left join() without null handling — add fillna() or dropna() "
                    "to prevent silent null propagation",
                    config=config,
                )
            )

        return findings


# =============================================================================
# SPL-D03-010 — CBO / statistics not enabled for complex joins
# =============================================================================


@register_rule
class CboNotEnabledRule(ConfigRule):
    """SPL-D03-010: Multiple joins without CBO enabled — Catalyst cannot reorder them."""

    rule_id = "SPL-D03-010"
    name = "CBO/statistics not enabled for complex joins"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "spark.sql.cbo.enabled not set; Catalyst cannot reorder joins by cost"
        " — enable CBO for multi-join queries."
    )
    explanation = (
        "Catalyst's default join ordering is *left-deep*: it joins tables in the order "
        "they appear in the query.  With cost-based optimisation (CBO) disabled, Catalyst "
        "has no statistics on table sizes and cannot reorder joins to minimise intermediate "
        "result size.\n\n"
        "For a three-table star join where the dimension tables vary from 1 K to 100 M rows, "
        "the difference between best and worst join order can be several orders of magnitude "
        "in shuffle data volume.  CBO uses column statistics (collected with "
        "``ANALYZE TABLE ... COMPUTE STATISTICS``) to pick the lowest-cost plan.\n\n"
        "CBO is especially impactful when:\n"
        "• The query has 3+ joins\n"
        "• Table sizes differ significantly\n"
        "• Some joins produce highly selective output (reducing the next join's input)\n\n"
        "Enable CBO and histogram statistics for the biggest gains."
    )
    recommendation_template = (
        "Enable CBO in the SparkSession config:\n"
        "``spark.sql.cbo.enabled = true``\n"
        "``spark.sql.statistics.histogram.enabled = true``\n"
        "Then analyse tables: ``spark.sql.ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS``."
    )
    before_example = "spark = SparkSession.builder.getOrCreate()  # CBO off by default"
    after_example = (
        "spark = (SparkSession.builder\n"
        '    .config("spark.sql.cbo.enabled", "true")\n'
        '    .config("spark.sql.statistics.histogram.enabled", "true")\n'
        "    .getOrCreate())"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#cost-based-optimizer-framework",
    ]
    estimated_impact = "Sub-optimal join ordering; large intermediate results on multi-join queries"
    effort_level = EffortLevel.CONFIG_ONLY

    _MIN_JOIN_COUNT = 3

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        # Only flag files that create a SparkSession (entry-point files)
        builder_calls = analyzer.find_method_calls("getOrCreate")
        if not builder_calls:
            return []

        non_cross_joins = [
            c for c in analyzer.find_method_calls("join") if _get_join_how(c) != "cross"
        ]
        if len(non_cross_joins) < self._MIN_JOIN_COUNT:
            return []

        cbo = analyzer.get_config_value("spark.sql.cbo.enabled")
        if cbo and cbo.strip().lower() == "true":
            return []

        line = builder_calls[0].line
        return [
            self.create_finding(
                analyzer.filename,
                line,
                f"spark.sql.cbo.enabled not set; {len(non_cross_joins)} joins detected"
                " — enable CBO for cost-based join reordering",
                config=config,
            )
        ]
