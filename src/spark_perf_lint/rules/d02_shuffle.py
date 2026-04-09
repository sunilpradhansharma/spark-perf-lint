"""D02 — Shuffle rules for spark-perf-lint.

Shuffles are the most expensive operations in Spark: they serialise every row,
write shuffle files to disk (or memory+disk), transfer data across the network,
and sort or hash-partition the result.  These rules detect code patterns that
cause unnecessary shuffles, worsen existing shuffles, or prevent Spark from
optimising them away.

Rule IDs: SPL-D02-001 through SPL-D02-008
"""

from __future__ import annotations

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.engine.pattern_matcher import PatternMatcher
from spark_perf_lint.rules.base import CodeRule, ConfigRule
from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.types import Dimension, EffortLevel, Finding, Severity

_DIM = Dimension.D02_SHUFFLE

# Wide transformations that each trigger a full shuffle stage.
_SHUFFLE_METHODS = frozenset(
    {"groupBy", "join", "orderBy", "sort", "distinct", "repartition", "groupByKey", "reduceByKey"}
)

# Methods that materialise intermediate results and break shuffle chains.
_BREAK_METHODS = frozenset({"cache", "persist", "checkpoint", "localCheckpoint"})


# =============================================================================
# SPL-D02-001 — groupByKey() instead of reduceByKey()
# =============================================================================


@register_rule
class GroupByKeyRule(CodeRule):
    """SPL-D02-001: groupByKey() forces full shuffle of all values to reducers."""

    rule_id = "SPL-D02-001"
    name = "groupByKey() instead of reduceByKey()"
    dimension = _DIM
    default_severity = Severity.CRITICAL
    description = "groupByKey() shuffles all values to reducers; use reduceByKey() instead."
    explanation = (
        "``groupByKey()`` collects *all* values for each key from every partition and "
        "ships them to a single reducer before any aggregation happens.  On a 10 GB "
        "dataset this means 10 GB of shuffle traffic even if the final aggregation "
        "reduces to a few kilobytes.\n\n"
        "``reduceByKey()`` / ``aggregateByKey()`` apply a *partial aggregation* on "
        "each partition first (a map-side combine), then shuffle only the reduced "
        "values.  For commutative, associative operations (sum, count, max, concat) "
        "this can reduce shuffle data by 10–100×.\n\n"
        "The same concern applies to the DataFrame API: prefer ``groupBy().agg()`` "
        "over ``groupBy().apply()`` with a Python function when possible."
    )
    recommendation_template = (
        "Replace ``groupByKey()`` with ``reduceByKey(func)`` or "
        "``aggregateByKey(zero, seqOp, combOp)`` so Spark can combine values "
        "map-side before the shuffle."
    )
    before_example = "rdd.groupByKey().mapValues(sum)"
    after_example = "rdd.reduceByKey(lambda a, b: a + b)"
    references = [
        "https://spark.apache.org/docs/latest/rdd-programming-guide.html"
        "#working-with-key-value-pairs",
        "https://databricks.gitbooks.io/databricks-spark-knowledge-base/content"
        "/best_practices/prefer_reducebykey_over_groupbykey.html",
    ]
    estimated_impact = "10–100× shuffle data reduction for aggregation workloads"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        pm = PatternMatcher(analyzer)
        return [
            self.create_finding(
                analyzer.filename,
                m.first_line or 1,
                "groupByKey() shuffles all values before aggregation — use reduceByKey()",
                config=config,
            )
            for m in pm.find_groupby_key_patterns()
        ]


# =============================================================================
# SPL-D02-002 — Default shuffle partitions (200) unchanged
# =============================================================================


@register_rule
class DefaultShufflePartitionsRule(CodeRule):
    """SPL-D02-002: spark.sql.shuffle.partitions not tuned from default 200."""

    rule_id = "SPL-D02-002"
    name = "Default shuffle partitions unchanged"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "spark.sql.shuffle.partitions is not set — shuffle stages use the default of 200."
    explanation = (
        "The default of 200 shuffle partitions was chosen for a small benchmark cluster "
        "circa 2014.  It is almost never correct:\n\n"
        "• **Too high for small data**: hundreds of tasks each reading a few KB produce "
        "  massive scheduler overhead and tiny output files that slow downstream readers.\n"
        "• **Too low for large data**: partitions grow to multiple GBs, spilling to disk "
        "  and overwhelming executor memory.\n\n"
        "The correct value depends on cluster size and data volume.  A practical starting "
        "point: ``2–3 × (total executor cores)`` or aim for 128–256 MB per post-shuffle "
        "partition.  With AQE enabled (``spark.sql.adaptive.enabled=true``) in Spark 3+, "
        "you can over-partition slightly and rely on AQE's coalesce-partitions to merge "
        "empty/small partitions automatically."
    )
    recommendation_template = (
        "Set ``spark.sql.shuffle.partitions`` explicitly. "
        "Rule of thumb: ``max(total_executor_cores * 2, dataset_size_gb * 8)``. "
        "Enable AQE to auto-tune at runtime: "
        "``spark.sql.adaptive.enabled = true``."
    )
    before_example = "spark = SparkSession.builder.getOrCreate()  # 200 partitions by default"
    after_example = (
        "spark = SparkSession.builder\n"
        '    .config("spark.sql.shuffle.partitions", "400")\n'
        "    .getOrCreate()"
    )
    references = [
        "https://spark.apache.org/docs/latest/configuration.html#execution-behavior",
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#adaptive-query-execution",
    ]
    estimated_impact = "Spill to disk on large datasets; task-scheduler overhead on small datasets"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        # Only flag when a SparkSession is actually created in this file.
        builder_calls = analyzer.find_method_calls("getOrCreate")
        if not builder_calls:
            return []

        raw = analyzer.get_config_value("spark.sql.shuffle.partitions")

        if raw is None:
            # Not configured → default 200 in effect
            return [
                self.create_finding(
                    analyzer.filename,
                    builder_calls[0].line,
                    "spark.sql.shuffle.partitions not set — Spark uses the default of 200",
                    config=config,
                )
            ]

        try:
            val = int(raw.strip())
        except ValueError:
            return []

        if val == 200:
            configs = [
                c
                for c in analyzer.find_spark_session_configs()
                if c.key == "spark.sql.shuffle.partitions"
            ]
            line = configs[-1].end_line if configs else builder_calls[0].line
            return [
                self.create_finding(
                    analyzer.filename,
                    line,
                    "spark.sql.shuffle.partitions = 200 (Spark default); tune for cluster size",
                    config=config,
                )
            ]

        return []


# =============================================================================
# SPL-D02-003 — Unnecessary repartition before join on same key
# =============================================================================


@register_rule
class RepartitionBeforeJoinRule(CodeRule):
    """SPL-D02-003: repartition() immediately before join() adds a redundant shuffle."""

    rule_id = "SPL-D02-003"
    name = "Unnecessary repartition before join"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "repartition() immediately before join() causes a redundant shuffle."
    explanation = (
        "``join()`` already performs a full shuffle to co-locate matching keys. "
        "Calling ``repartition()`` immediately before ``join()`` on the same key "
        "triggers *two* shuffles for the same data: one to repartition, then another "
        "inside the join.  Spark's exchange reuse optimisation (enabled by default in "
        "Spark 3+ with AQE) can sometimes merge these, but explicit pre-join "
        "repartitioning prevents that optimisation from kicking in and always costs "
        "at least one extra shuffle write.\n\n"
        "The only valid reason to repartition before a join is to change the partition "
        'count when Catalyst cannot infer it — and even then, ``hint("repartition")`` '
        "communicates intent more clearly."
    )
    recommendation_template = (
        "Remove the ``repartition()`` call before ``join()``. "
        "If you need to control partition count, set ``spark.sql.shuffle.partitions`` "
        'or use ``df.hint("repartition", n)`` instead.'
    )
    before_example = "df.repartition(200, 'key').join(other, 'key')"
    after_example = "df.join(other, 'key')  # join already shuffles on key"
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html",
    ]
    estimated_impact = "One extra full shuffle per join; 2× shuffle I/O for that stage"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []

        # Pattern 1 — chained: df.repartition(n).join(...)
        for call in analyzer.find_method_calls("join"):
            if "repartition" in call.chain:
                try:
                    repart_idx = call.chain.index("repartition")
                    join_idx = call.chain.index("join")
                    if repart_idx < join_idx:
                        findings.append(
                            self.create_finding(
                                analyzer.filename,
                                call.line,
                                "repartition() immediately before join() adds a redundant shuffle",
                                config=config,
                            )
                        )
                except ValueError:
                    pass

        # Pattern 2 — multi-statement: repartition on one line, join within 5 lines
        join_lines = {c.line for c in analyzer.find_method_calls("join")}
        seen_join_lines: set[int] = {f.line_number for f in findings}
        for repart_call in analyzer.find_method_calls("repartition"):
            nearby_joins = [
                jl for jl in join_lines if repart_call.line < jl <= repart_call.line + 5
            ]
            for jl in nearby_joins:
                if jl not in seen_join_lines:
                    seen_join_lines.add(jl)
                    findings.append(
                        self.create_finding(
                            analyzer.filename,
                            repart_call.line,
                            "repartition() immediately before join() adds a redundant shuffle",
                            config=config,
                        )
                    )

        return findings


# =============================================================================
# SPL-D02-004 — orderBy/sort on full dataset without limit
# =============================================================================


@register_rule
class OrderByWithoutLimitRule(CodeRule):
    """SPL-D02-004: orderBy()/sort() on full dataset forces a total-order sort."""

    rule_id = "SPL-D02-004"
    name = "orderBy/sort without limit"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "orderBy()/sort() on an unlimited dataset forces a full total-order sort."
    explanation = (
        "``orderBy()`` / ``sort()`` produce a *globally ordered* result, which requires "
        "a full shuffle followed by a range-partition sort across all executors.  On "
        "large datasets this is one of the most expensive operations possible — "
        "O(n log n) with n being the total row count.\n\n"
        "In almost every real-world use case you only want the *top N* rows, not a "
        "fully sorted dataset.  ``df.orderBy(col).limit(n)`` is optimised by Catalyst "
        "into a ``TakeOrderedAndProject`` physical plan that never materialises the "
        "full sorted output — it only tracks the top-N heap across all tasks.\n\n"
        "If you genuinely need a fully sorted output (e.g., for a downstream "
        "checkpoint file), use ``sortWithinPartitions()`` to sort each partition "
        "independently without the global shuffle cost."
    )
    recommendation_template = (
        "Add ``.limit(n)`` after ``orderBy()`` to trigger the "
        "``TakeOrderedAndProject`` optimisation. "
        "For partition-local sorting, use ``sortWithinPartitions()`` instead."
    )
    before_example = "df.orderBy('revenue', ascending=False)"
    after_example = "df.orderBy('revenue', ascending=False).limit(100)"
    references = [
        "https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-orderby.html",
    ]
    estimated_impact = "Full global sort: O(n log n) shuffle + sort vs O(n log k) for top-N"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        # Lines where a limit() call has orderBy/sort in its own chain
        # (limit comes after orderBy in the same expression: df.orderBy().limit())
        chained_limit_lines: set[int] = set()
        for lim in analyzer.find_method_calls("limit"):
            if "orderBy" in lim.chain or "sort" in lim.chain:
                chained_limit_lines.add(lim.line)

        limit_lines = {c.line for c in analyzer.find_method_calls("limit")}
        findings: list[Finding] = []

        for method in ("orderBy", "sort"):
            for call in analyzer.find_method_calls(method):
                # Check 1: a limit() that chains after this orderBy (same line)
                if call.line in chained_limit_lines:
                    continue
                # Check 2: a limit() call is nearby (within 3 lines after)
                nearby_limit = any(call.line < ll <= call.line + 3 for ll in limit_lines)
                if nearby_limit:
                    continue
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        f"{call.method_name}() without limit() forces a full global sort shuffle",
                        config=config,
                    )
                )

        return findings


# =============================================================================
# SPL-D02-005 — distinct() without prior filter
# =============================================================================


@register_rule
class DistinctWithoutFilterRule(CodeRule):
    """SPL-D02-005: distinct() on an unfiltered DataFrame triggers a full shuffle."""

    rule_id = "SPL-D02-005"
    name = "distinct() without prior filter"
    dimension = _DIM
    default_severity = Severity.INFO
    description = "distinct() triggers a full shuffle; ensure filters are applied first."
    explanation = (
        "``distinct()`` is equivalent to ``groupBy(*all_columns).count()`` — it "
        "shuffles every row to detect duplicates.  On a 100 M-row table with no "
        "prior filtering this is a full O(n) shuffle of the entire dataset.\n\n"
        "If you only need distinct values of a *subset* of columns, use "
        "``select('col').distinct()`` or ``dropDuplicates(['col'])`` to reduce "
        "the shuffle payload.  Applying filters *before* ``distinct()`` reduces "
        "both the shuffle payload and the number of rows the sort-based dedup must "
        "process.  On wide tables, ``select()`` before ``distinct()`` is especially "
        "impactful because it prevents serialising unused columns."
    )
    recommendation_template = (
        "Apply ``filter()``/``where()`` and ``select()`` *before* ``distinct()`` "
        "to reduce the shuffle payload. "
        "Use ``dropDuplicates(['col1', 'col2'])`` to deduplicate on specific columns."
    )
    before_example = "df.distinct()"
    after_example = "df.select('id', 'event_type').filter('date > \"2024-01-01\"').distinct()"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.distinct.html",
    ]
    estimated_impact = "Full shuffle of all rows and all columns; high memory pressure"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        filter_methods = {"filter", "where", "select"}
        findings: list[Finding] = []

        for call in analyzer.find_method_calls("distinct"):
            # If a filter/where/select appears in the chain before distinct, it's fine.
            chain = call.chain
            try:
                distinct_idx = chain.index("distinct")
            except ValueError:
                distinct_idx = len(chain)

            prior_chain = chain[:distinct_idx]
            if filter_methods & set(prior_chain):
                continue

            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    "distinct() without prior filter()/select() shuffles the full dataset",
                    config=config,
                )
            )

        return findings


# =============================================================================
# SPL-D02-006 — Shuffle followed by coalesce
# =============================================================================


@register_rule
class ShuffleFollowedByCoalesceRule(CodeRule):
    """SPL-D02-006: coalesce() after a shuffle creates unbalanced partitions."""

    rule_id = "SPL-D02-006"
    name = "Shuffle followed by coalesce"
    dimension = _DIM
    default_severity = Severity.INFO
    description = "coalesce() after a shuffle operation creates unbalanced partitions."
    explanation = (
        "``coalesce(n)`` merges partitions *without a shuffle* by packing multiple "
        "existing partitions onto fewer tasks.  When applied after a shuffle "
        "(``groupBy``, ``join``, ``orderBy``, ``distinct``, ``repartition``), this "
        "creates highly unbalanced tasks: some tasks process multiple original "
        "partitions while others process one, eliminating the data-parallelism the "
        "shuffle was designed to establish.\n\n"
        "If the goal is to reduce the number of output files written to storage, "
        "prefer ``repartition(n)`` — it shuffles data evenly — or set "
        "``spark.sql.shuffle.partitions`` to the desired target so the shuffle "
        "itself produces the right number of partitions."
    )
    recommendation_template = (
        "Replace ``coalesce(n)`` with ``repartition(n)`` to evenly redistribute "
        "data after the shuffle. "
        "Alternatively, tune ``spark.sql.shuffle.partitions`` so the shuffle "
        "already produces the target partition count."
    )
    before_example = "df.groupBy('col').agg(count('*')).coalesce(10)"
    after_example = "df.groupBy('col').agg(count('*')).repartition(10)"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.coalesce.html",
    ]
    estimated_impact = "Unbalanced tasks; long-tail executor stragglers after the shuffle"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    # Shuffle ops we look for in the chain leading up to coalesce
    _SHUFFLE_IN_CHAIN = frozenset({"groupBy", "join", "orderBy", "sort", "distinct", "repartition"})

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []

        # Pattern 1 — chained: df.groupBy(...).agg(...).coalesce(n)
        for call in analyzer.find_method_calls("coalesce"):
            shuffle_in_chain = self._SHUFFLE_IN_CHAIN & set(call.chain)
            if shuffle_in_chain:
                shuffles = ", ".join(sorted(shuffle_in_chain))
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        f"coalesce() after shuffle ({shuffles}) creates unbalanced partitions",
                        config=config,
                    )
                )

        # Pattern 2 — multi-statement: shuffle on line N, coalesce within 5 lines
        coalesce_lines = {c.line: c for c in analyzer.find_method_calls("coalesce")}
        seen_coalesce: set[int] = {f.line_number for f in findings}

        for call in analyzer.find_all_method_calls():
            if call.method_name not in self._SHUFFLE_IN_CHAIN:
                continue
            for cl, cc in coalesce_lines.items():
                if call.line < cl <= call.line + 5 and cl not in seen_coalesce:
                    seen_coalesce.add(cl)
                    findings.append(
                        self.create_finding(
                            analyzer.filename,
                            cc.line,
                            f"coalesce() after {call.method_name}() creates unbalanced partitions",
                            config=config,
                        )
                    )

        return findings


# =============================================================================
# SPL-D02-007 — Multiple shuffles in sequence without caching
# =============================================================================


@register_rule
class MultipleShufflesInSequenceRule(CodeRule):
    """SPL-D02-007: Multiple shuffle-wide ops in sequence without caching."""

    rule_id = "SPL-D02-007"
    name = "Multiple shuffles in sequence"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "Two or more shuffle-wide operations appear in sequence without intermediate caching."
    )
    explanation = (
        "Each wide transformation (``join``, ``groupBy``, ``orderBy``, ``distinct``, "
        "``repartition``) triggers a full shuffle stage.  When multiple such operations "
        "are chained without materialising the intermediate result "
        "(``cache()``/``checkpoint()``), Spark must re-execute the entire preceding "
        "lineage if any executor fails mid-way through the sequence, and the query "
        "plan grows deeply nested, making debugging difficult.\n\n"
        "Even without failures, each shuffle amplifies spill risk: shuffle data from "
        "stage 1 is immediately read and shuffled again in stage 2 without any "
        "opportunity to filter or compact it first.\n\n"
        "Consider whether filters or projections can be pushed between the shuffles to "
        "reduce data volume, and whether intermediate results should be checkpointed "
        "to truncate the lineage."
    )
    recommendation_template = (
        "Insert ``df.cache()`` or ``df.checkpoint()`` between shuffle operations to "
        "materialise the intermediate result and truncate the lineage. "
        "Also check whether a ``filter()`` can be applied between the shuffles to "
        "reduce the data volume entering the second shuffle."
    )
    before_example = (
        "df2 = df.groupBy('a').agg(sum('b'))\n"
        "df3 = df2.join(lookup, 'a')\n"
        "df4 = df3.orderBy('b')"
    )
    after_example = (
        "df2 = df.groupBy('a').agg(sum('b')).cache()\n"
        "df3 = df2.join(lookup, 'a')\n"
        "df4 = df3.orderBy('b')"
    )
    references = [
        "https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence",
    ]
    estimated_impact = "Compounded shuffle I/O; full lineage re-execution on any task failure"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        all_calls = sorted(analyzer.find_all_method_calls(), key=lambda c: (c.line, c.col))
        shuffle_calls = [c for c in all_calls if c.method_name in _SHUFFLE_METHODS]

        if len(shuffle_calls) < 2:
            return []

        break_lines = {c.line for c in all_calls if c.method_name in _BREAK_METHODS}

        # Build runs of consecutive shuffles with no materialisation between them.
        runs: list[list] = []
        current: list = [shuffle_calls[0]]

        for prev, cur in zip(shuffle_calls, shuffle_calls[1:], strict=False):
            # Use prev.line <= bl (inclusive) so cache() on the same line as the
            # preceding shuffle (e.g. df.groupBy(...).cache()) counts as a break.
            has_break = any(prev.line <= bl < cur.line for bl in break_lines)
            if has_break:
                if len(current) >= 2:
                    runs.append(current)
                current = [cur]
            else:
                current.append(cur)

        if len(current) >= 2:
            runs.append(current)

        findings: list[Finding] = []
        for run in runs:
            # Flag at the second shuffle so the finding points to the problem point.
            second = run[1]
            names = [c.method_name for c in run]
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    second.line,
                    f"{len(run)} shuffle operations in sequence without caching: "
                    f"{', '.join(names)}",
                    config=config,
                )
            )

        return findings


# =============================================================================
# SPL-D02-008 — Shuffle file buffer too small
# =============================================================================


@register_rule
class ShuffleFileBufferTooSmallRule(ConfigRule):
    """SPL-D02-008: spark.shuffle.file.buffer set below recommended 1 MB."""

    rule_id = "SPL-D02-008"
    name = "Shuffle file buffer too small"
    dimension = _DIM
    default_severity = Severity.INFO
    description = "spark.shuffle.file.buffer is configured below the recommended 1 MB."
    explanation = (
        "``spark.shuffle.file.buffer`` controls the in-memory buffer used when writing "
        "shuffle output files.  Spark's default is 32 KB, which was set conservatively "
        "for memory-constrained environments and causes a large number of small I/O "
        "syscalls during shuffle write.\n\n"
        "Increasing the buffer to 1 MB (``1m``) reduces the number of ``write()`` "
        "syscalls by ~32× with negligible memory overhead "
        "(one buffer per shuffle map task).  For shuffle-heavy workloads "
        "(many ``groupBy``/``join`` operations) this can meaningfully reduce the "
        "shuffle write time visible in the Spark UI stage details."
    )
    recommendation_template = (
        "Set ``spark.shuffle.file.buffer`` to ``1m`` in your SparkSession config. "
        "Also consider enabling ``spark.shuffle.io.preferDirectBufs = true`` "
        "to use off-heap buffers and reduce GC pressure."
    )
    before_example = '.config("spark.shuffle.file.buffer", "32k")'
    after_example = '.config("spark.shuffle.file.buffer", "1m")'
    references = [
        "https://spark.apache.org/docs/latest/configuration.html#shuffle-behavior",
    ]
    estimated_impact = "Up to 32× reduction in shuffle-write syscall count"
    effort_level = EffortLevel.CONFIG_ONLY

    _MIN_BUFFER_KB = 1024.0  # 1 MB in KB

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        raw = analyzer.get_config_value("spark.shuffle.file.buffer")
        if raw is None:
            return []

        kb = _parse_size_kb(raw)
        if kb is None or kb >= self._MIN_BUFFER_KB:
            return []

        configs = [
            c for c in analyzer.find_spark_session_configs() if c.key == "spark.shuffle.file.buffer"
        ]
        line = configs[-1].end_line if configs else 1
        return [
            self.create_finding(
                analyzer.filename,
                line,
                f"spark.shuffle.file.buffer = {raw!r} is below 1 MB "
                f"(parsed: {kb:.0f} KB); increases shuffle-write syscall count",
                config=config,
            )
        ]


# =============================================================================
# Private helpers
# =============================================================================


def _parse_size_kb(value: str) -> float | None:
    """Parse a Spark size string into kilobytes.

    Handles ``k``/``kb``, ``m``/``mb``, ``g``/``gb``, and bare integers
    (assumed bytes, matching Spark's ``Utils.byteStringAsKb`` behaviour).
    """
    import re

    v = value.strip().lower()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(g|gb|m|mb|k|kb|b)?", v)
    if not m:
        return None
    num = float(m.group(1))
    suffix = m.group(2) or "b"
    multipliers: dict[str, float] = {
        "g": 1024.0 * 1024.0,
        "gb": 1024.0 * 1024.0,
        "m": 1024.0,
        "mb": 1024.0,
        "k": 1.0,
        "kb": 1.0,
        "b": 1.0 / 1024.0,
    }
    return num * multipliers[suffix]
