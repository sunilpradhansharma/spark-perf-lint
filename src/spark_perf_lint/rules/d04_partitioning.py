"""D04 — Partitioning rules for spark-perf-lint.

Partition strategy determines how much parallelism Spark can exploit and how
efficiently it reads from and writes to storage.  Too few partitions under-use
the cluster; too many swamp the task scheduler.  Misaligned partition and join
keys cause redundant shuffles.  These rules detect each class of mistake at
static-analysis time.

Rule IDs: SPL-D04-001 through SPL-D04-010
"""

from __future__ import annotations

import ast

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer, MethodCallInfo
from spark_perf_lint.rules.base import CodeRule
from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.types import Dimension, EffortLevel, Finding, Severity

_DIM = Dimension.D04_PARTITIONING

# Partition count thresholds (rule defaults; can be overridden via config)
_HIGH_PARTITION_THRESHOLD = 10_000  # SPL-D04-003: above this → too many partitions
_LOW_PARTITION_THRESHOLD = 10  # SPL-D04-004: below this (and > 1) → too few partitions

# Read / write format support constants
_PARTITIONED_FORMATS = frozenset({"parquet", "orc", "delta"})
_WRITE_TERMINALS = frozenset(
    {"parquet", "csv", "json", "orc", "save", "saveAsTable", "avro", "delta"}
)
_WRITE_MARKERS = frozenset({"write", "writeStream"})
_READ_TERMINALS = frozenset({"parquet", "csv", "json", "orc", "load", "table", "avro"})

# Wide (shuffle-producing) operations used in Rule 005 chain inspection
_SHUFFLE_METHODS = frozenset(
    {"groupBy", "join", "orderBy", "sort", "distinct", "repartition", "groupByKey"}
)

# Expensive operations that benefit from partition pruning (Rule 009)
_EXPENSIVE_OPS = frozenset({"groupBy", "distinct", "orderBy", "sort", "agg"})
_FILTER_METHODS = frozenset({"filter", "where"})

# Column name patterns used to identify high-cardinality partition columns
_HIGH_CARDINALITY_NAMES = frozenset({"id", "uuid", "guid", "timestamp", "ts", "datetime"})
_HIGH_CARDINALITY_SUFFIXES = ("_id", "_uuid", "_guid", "_timestamp", "_ts", "_at")
_SAFE_PARTITION_NAMES = frozenset(
    {
        "date",
        "year",
        "month",
        "day",
        "hour",
        "week",
        "dt",
        "status",
        "type",
        "region",
        "country",
        "state",
        "category",
        "partition",
        "bucket",
        "shard",
    }
)


# =============================================================================
# Private helpers
# =============================================================================


def _int_arg(call: MethodCallInfo) -> int | None:
    """Return the integer value of the first positional argument, or ``None``."""
    if not call.args:
        return None
    arg = call.args[0]
    if isinstance(arg, ast.Constant) and isinstance(arg.value, int):
        return arg.value
    return None


def _is_high_cardinality_col(col: str) -> bool:
    """Return ``True`` when *col* looks like a high-cardinality partition column.

    Uses a name-based heuristic: columns whose names match common unique-ID or
    timestamp patterns (``_id``, ``_at``, ``timestamp``, ``uuid``, etc.) are
    poor partition choices because they produce too many small partitions.
    """
    c = col.lower().strip()
    if c in _SAFE_PARTITION_NAMES:
        return False
    if c in _HIGH_CARDINALITY_NAMES:
        return True
    return any(c.endswith(s) for s in _HIGH_CARDINALITY_SUFFIXES)


def _repartition_columns(call: MethodCallInfo) -> set[str]:
    """Extract string column names from a ``repartition()`` call's arguments.

    Skips integer arguments (partition-count positional arg).
    """
    cols: set[str] = set()
    for arg in call.args:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            cols.add(arg.value)
    return cols


def _join_keys(call: MethodCallInfo) -> set[str]:
    """Extract string join key(s) from a ``join()`` call's ``on`` argument."""
    keys: set[str] = set()
    if len(call.args) < 2:
        return keys
    on = call.args[1]
    if isinstance(on, ast.Constant) and isinstance(on.value, str):
        keys.add(on.value)
    elif isinstance(on, ast.List):
        for elt in on.elts:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                keys.add(elt.value)
    on_kwarg = call.kwargs.get("on")
    if on_kwarg is not None:
        if isinstance(on_kwarg, ast.Constant) and isinstance(on_kwarg.value, str):
            keys.add(on_kwarg.value)
    return keys


# =============================================================================
# SPL-D04-001 — repartition(1) bottleneck
# =============================================================================


@register_rule
class RepartitionToOneRule(CodeRule):
    """SPL-D04-001: repartition(1) collapses all data to a single task."""

    rule_id = "SPL-D04-001"
    name = "repartition(1) bottleneck"
    dimension = _DIM
    default_severity = Severity.CRITICAL
    description = "repartition(1) forces all data through a single task, eliminating parallelism."
    explanation = (
        "``repartition(1)`` triggers a full shuffle and then funnels the entire "
        "dataset through a single reducer task.  On any non-trivial dataset this "
        "means:\n\n"
        "• **One executor core handles all work** — the rest of the cluster sits idle.\n"
        "• **GC pressure** — the single partition must fit in one executor's memory.\n"
        "• **No recovery parallelism** — if that single task fails, the full dataset "
        "  must be re-shuffled from the beginning.\n\n"
        "``repartition(1)`` is occasionally legitimate for writing a single output file "
        "(e.g., a small reference table).  Even then, ``coalesce(1)`` is cheaper because "
        "it avoids the upfront shuffle.  For large data, use the cluster's parallelism: "
        "set the partition count to ``2-3 × total executor cores``."
    )
    recommendation_template = (
        "Remove ``repartition(1)`` and let Spark use the default partition count. "
        "If a single output file is required, use ``coalesce(1)`` after all "
        "transformations to avoid the extra shuffle cost."
    )
    before_example = "df.repartition(1).write.parquet('s3://bucket/out')"
    after_example = "df.write.parquet('s3://bucket/out')  # let Spark decide partition count"
    references = [
        "https://spark.apache.org/docs/latest/rdd-programming-guide.html"
        "#parallelized-collections",
    ]
    estimated_impact = "Full parallelism lost; single-core execution for the entire dataset"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("repartition"):
            if _int_arg(call) == 1:
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        "repartition(1) collapses all data to one task — parallelism eliminated",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D04-002 — coalesce(1) bottleneck
# =============================================================================


@register_rule
class CoalesceToOneRule(CodeRule):
    """SPL-D04-002: coalesce(1) produces a single partition without a shuffle."""

    rule_id = "SPL-D04-002"
    name = "coalesce(1) bottleneck"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "coalesce(1) reduces all data to one partition, serialising execution."
    explanation = (
        "``coalesce(1)`` merges all existing partitions onto a single task *without* "
        "a shuffle.  Unlike ``repartition(1)`` it does not pay the upfront shuffle "
        "cost, but the result is the same: one enormous partition that must be "
        "processed by a single executor core.\n\n"
        "When used before a write, ``coalesce(1)`` produces a single output file "
        "which is sometimes required (e.g., loading into a legacy system that "
        "cannot read multi-part output).  In that narrow case it is acceptable.\n\n"
        "When used before further transformations (filters, joins, aggregations), "
        "it destroys the parallelism of all subsequent stages and should always be "
        "removed or replaced with a smaller-but-not-one partition count."
    )
    recommendation_template = (
        "If a single output file is the goal, keep ``coalesce(1)`` but place it "
        "as the very last step before ``.write``. "
        "For transformations, remove it entirely or use ``coalesce(n)`` where "
        "``n`` aligns with cluster core count."
    )
    before_example = "df.coalesce(1).groupBy('key').count()  # single-partition agg"
    after_example = "df.groupBy('key').count()  # restore parallelism"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.coalesce.html",
    ]
    estimated_impact = "All subsequent stages run single-threaded; long-tail straggler"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("coalesce"):
            if _int_arg(call) == 1:
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        "coalesce(1) reduces all data to a single partition — serialises execution",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D04-003 — Repartition with very high partition count
# =============================================================================


@register_rule
class HighPartitionCountRule(CodeRule):
    """SPL-D04-003: repartition() with a partition count above the recommended limit."""

    rule_id = "SPL-D04-003"
    name = "Repartition with very high partition count"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        f"repartition() with a count above {_HIGH_PARTITION_THRESHOLD:,}"
        " causes excessive task-scheduler overhead."
    )
    explanation = (
        "Each Spark partition corresponds to one task.  Creating tens of thousands "
        "of tasks from a single ``repartition()`` call causes:\n\n"
        "• **Scheduler overhead**: the driver must track and schedule every task, "
        "  consuming significant CPU and memory even before any data is processed.\n"
        "• **Tiny shuffle files**: each task writes its own shuffle output file. "
        "  With 50 k tasks × 200 executors = 10 M shuffle files for a single stage.\n"
        "• **Slow speculation**: Spark's speculative execution scans all task "
        "  statuses every few seconds; huge task counts make this O(n) scan slow.\n\n"
        "A practical guideline: target 128–256 MB of input data per partition.  "
        "For most workloads that means partition counts in the hundreds to low "
        "thousands, not tens of thousands."
    )
    recommendation_template = (
        f"Set the partition count to no more than {_HIGH_PARTITION_THRESHOLD:,}. "
        "Target 128–256 MB per partition. "
        "Enable AQE (``spark.sql.adaptive.enabled = true``) to auto-coalesce "
        "small post-shuffle partitions."
    )
    before_example = "df.repartition(50_000)"
    after_example = "df.repartition(400)  # ~128 MB per partition at 50 GB data"
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#adaptive-query-execution",
    ]
    estimated_impact = "Driver OOM; millions of shuffle files; slow task scheduling"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("repartition"):
            n = _int_arg(call)
            if n is not None and n > _HIGH_PARTITION_THRESHOLD:
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        f"repartition({n:,}) is very high — target 128–256 MB per partition",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D04-004 — Repartition with very low partition count
# =============================================================================


@register_rule
class LowPartitionCountRule(CodeRule):
    """SPL-D04-004: repartition() with a very low partition count under-uses the cluster."""

    rule_id = "SPL-D04-004"
    name = "Repartition with very low partition count"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        f"repartition(n) where n < {_LOW_PARTITION_THRESHOLD}" " under-uses cluster parallelism."
    )
    explanation = (
        "Repartitioning to a very low number of partitions (2–9) caps parallelism "
        "to that partition count for all downstream stages.  On a 100-core cluster, "
        "``repartition(4)`` means at most 4 tasks run simultaneously — 96 cores sit "
        "idle.\n\n"
        "Low partition counts are sometimes intentional for:\n"
        "• **Very small DataFrames** (< 100 MB) where parallelism overhead exceeds "
        "  the computation cost.\n"
        "• **Final coalesce before a small write** — but ``coalesce()`` is cheaper "
        "  there (no shuffle).\n\n"
        "For anything but intentionally tiny data, partition to at least "
        "``2–3 × total executor cores``."
    )
    recommendation_template = (
        f"Increase the partition count to at least {_LOW_PARTITION_THRESHOLD}. "
        "A good starting point is ``2–3 × total executor cores``. "
        "If the DataFrame is genuinely small, remove the repartition entirely."
    )
    before_example = "df.repartition(4).groupBy('key').agg(sum('val'))"
    after_example = "df.repartition(200).groupBy('key').agg(sum('val'))"
    references = [
        "https://spark.apache.org/docs/latest/configuration.html#execution-behavior",
    ]
    estimated_impact = "Cluster cores sit idle; up to 25× slower than optimal parallelism"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("repartition"):
            n = _int_arg(call)
            # n == 1 is CRITICAL and handled by SPL-D04-001
            if n is not None and 1 < n < _LOW_PARTITION_THRESHOLD:
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        f"repartition({n}) is very low — caps all downstream parallelism",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D04-005 — Coalesce before write (unbalanced output files)
# =============================================================================


@register_rule
class CoalesceBeforeWriteRule(CodeRule):
    """SPL-D04-005: coalesce() before a write creates unbalanced output files."""

    rule_id = "SPL-D04-005"
    name = "Coalesce before write — unbalanced output files"
    dimension = _DIM
    default_severity = Severity.INFO
    description = "coalesce() before write() creates unbalanced output files — use repartition()."
    explanation = (
        "``coalesce(n)`` reduces partition count by *merging* existing partitions "
        "without redistributing data.  When used before a write, the resulting output "
        "files are unbalanced: the merged partitions produce a few very large files "
        "alongside smaller ones.\n\n"
        "This matters because:\n"
        "• **Downstream readers** (Spark, Presto, Athena) split files at block size "
        "  boundaries, not at coalesced boundaries.  A large file forces one task "
        "  to read the entire partition sequentially.\n"
        "• **S3/GCS listing performance**: a mix of huge and tiny files in the same "
        "  prefix degrades prefix-list performance.\n\n"
        "``repartition(n)`` is the correct choice when uniform output file sizes "
        "matter.  It costs one extra shuffle but guarantees evenly-sized output.\n\n"
        "Note: ``coalesce(1)`` (single output file) is covered by SPL-D04-002."
    )
    recommendation_template = (
        "Replace ``coalesce(n)`` with ``repartition(n)`` before the write "
        "to produce evenly-sized output files. "
        "Alternatively, set ``spark.sql.shuffle.partitions`` to the target file "
        "count so the shuffle itself produces the right number of partitions."
    )
    before_example = "df.coalesce(20).write.parquet('s3://bucket/out')"
    after_example = "df.repartition(20).write.parquet('s3://bucket/out')"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrame.coalesce.html",
    ]
    estimated_impact = "Unbalanced output files; downstream reader performance degradation"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        seen_lines: set[int] = set()

        for call in analyzer.find_all_method_calls():
            if call.method_name not in _WRITE_TERMINALS:
                continue
            # Confirm it's in a write chain (not a standalone method with same name)
            if not any(m in _WRITE_MARKERS for m in call.chain):
                continue
            if "coalesce" not in call.chain:
                continue
            if call.line in seen_lines:
                continue
            seen_lines.add(call.line)

            # Find the coalesce call on the same line to check its arg
            coalesce_calls = [
                c for c in analyzer.find_method_calls("coalesce") if c.line == call.line
            ]
            coalesce_n = _int_arg(coalesce_calls[0]) if coalesce_calls else None
            if coalesce_n == 1:
                continue  # SPL-D04-002 handles coalesce(1)

            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    "coalesce() before write() creates unbalanced output files"
                    " — use repartition() for uniform file sizes",
                    config=config,
                )
            )

        return findings


# =============================================================================
# SPL-D04-006 — Missing partitionBy on write
# =============================================================================


@register_rule
class MissingPartitionByOnWriteRule(CodeRule):
    """SPL-D04-006: Parquet/ORC/Delta write without partitionBy."""

    rule_id = "SPL-D04-006"
    name = "Missing partitionBy on write"
    dimension = _DIM
    default_severity = Severity.INFO
    description = "Writing to parquet/orc/delta without partitionBy — consider partition strategy."
    explanation = (
        "Writing analytics data (Parquet, ORC, Delta) without a ``partitionBy`` "
        "clause places all output in a single directory.  Readers that filter on "
        "common columns (date, region, status) must scan every file rather than "
        "pruning to a relevant subset.\n\n"
        "Partition pruning is one of the highest-impact Spark optimisations: "
        "partitioning a 10 TB table by ``date`` and filtering on a single day "
        "reduces the scan from 10 TB to ~27 GB (assuming 3 years of data).\n\n"
        "Good partition columns are:\n"
        "• **Low-cardinality** (hundreds to low thousands of distinct values): "
        "  date, year, month, region, status, event_type.\n"
        "• **Aligned with query filters**: the most common ``WHERE`` clause columns.\n"
        "• **Not too granular**: ``date`` is good; ``timestamp`` creates millions "
        "  of tiny files."
    )
    recommendation_template = (
        "Add ``partitionBy('date', 'region')`` (or your natural query dimensions) "
        "before the write terminal. "
        "Choose low-cardinality columns that align with how the data is queried."
    )
    before_example = "df.write.mode('overwrite').parquet('s3://bucket/events')"
    after_example = "df.write.partitionBy('date', 'region').mode('overwrite').parquet('s3://bucket/events')"  # noqa: E501
    references = [
        "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html" "#partition-discovery",
    ]
    estimated_impact = "Full table scans on every query; no partition pruning possible"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for io in analyzer.find_spark_read_writes():
            if io.operation != "write":
                continue
            if io.format not in _PARTITIONED_FORMATS:
                continue
            if io.partitioned_by:
                continue  # already partitioned
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    io.line,
                    f"write to {io.format} without partitionBy()"
                    " — add partition columns for query pruning",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D04-007 — Over-partitioning on write (high-cardinality column)
# =============================================================================


@register_rule
class HighCardinalityPartitionRule(CodeRule):
    """SPL-D04-007: partitionBy on a high-cardinality column creates millions of tiny files."""

    rule_id = "SPL-D04-007"
    name = "Over-partitioning — high-cardinality partition column"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "partitionBy() on a high-cardinality column (e.g. *_id, *_at)"
        " creates millions of tiny files."
    )
    explanation = (
        "Partitioning by a high-cardinality column (one with thousands to millions "
        "of distinct values) causes the *small files problem*:\n\n"
        "• Each distinct partition value becomes a directory.\n"
        "• With 10 M distinct ``user_id`` values × 200 Spark tasks = up to "
        "  2 billion output files (capped in practice by Spark's open-file limit "
        "  but still in the millions).\n"
        "• Object storage (S3, GCS, ADLS) charges per LIST/PUT operation; millions "
        "  of tiny files dramatically increase cost and latency.\n"
        "• Spark reads from partitioned data open one file per partition per task — "
        "  millions of tiny files cause OOM in the driver's partition listing.\n\n"
        "Partition columns should have at most a few thousand distinct values: "
        "dates, statuses, regions, categories — not IDs, UUIDs, or timestamps."
    )
    recommendation_template = (
        "Replace the high-cardinality column with a lower-cardinality equivalent: "
        "use ``date`` instead of ``timestamp``, or ``user_segment`` instead of ``user_id``. "
        "If the column cannot be replaced, bucket by it instead: "
        "``df.write.bucketBy(256, 'user_id').saveAsTable('events')``."
    )
    before_example = "df.write.partitionBy('user_id').parquet('s3://bucket/events')"
    after_example = "df.write.partitionBy('date', 'region').parquet('s3://bucket/events')"
    references = [
        "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html" "#partition-discovery",
    ]
    estimated_impact = "Millions of tiny files; driver OOM on partition listing; high storage cost"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for io in analyzer.find_spark_read_writes():
            if io.operation != "write":
                continue
            bad_cols = [c for c in io.partitioned_by if _is_high_cardinality_col(c)]
            if not bad_cols:
                continue
            cols_str = ", ".join(f"'{c}'" for c in bad_cols)
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    io.line,
                    f"partitionBy({cols_str}) uses high-cardinality column(s)"
                    " — creates millions of tiny files",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D04-008 — Missing bucketBy for repeatedly joined tables
# =============================================================================


@register_rule
class MissingBucketByRule(CodeRule):
    """SPL-D04-008: saveAsTable() without bucketBy when joins are present."""

    rule_id = "SPL-D04-008"
    name = "Missing bucketBy for repeatedly joined tables"
    dimension = _DIM
    default_severity = Severity.INFO
    description = "saveAsTable() without bucketBy — joins on this table cause repeated shuffles."
    explanation = (
        "When writing a table that will be repeatedly joined (a fact table, a "
        "frequently-used dimension, or a lookup table), bucketing pre-distributes "
        "rows by the join key so that every subsequent join on that key requires "
        "*zero shuffle*.\n\n"
        "``bucketBy(n, 'join_key')`` writes exactly ``n`` files per partition, "
        "sorted by hash of ``join_key``.  When two bucketed tables share the same "
        "bucket count and key, Spark detects the pre-sorted co-partitioning and "
        "replaces the sort-merge join plan with a bucket join — no shuffle, no "
        "sort stage.\n\n"
        "The upfront ``saveAsTable`` is more expensive than ``write.parquet()`` "
        "(must be written to a Hive-compatible location), but if the table is joined "
        "dozens of times per day the amortised shuffle savings dominate."
    )
    recommendation_template = (
        "Add ``bucketBy(n, 'join_key').sortBy('join_key')`` before ``saveAsTable()``. "
        "Use a bucket count that is a power of 2 and aligned with the other tables "
        "you will join against (e.g. all 256 buckets)."
    )
    before_example = "df.write.mode('overwrite').saveAsTable('fact_orders')"
    after_example = (
        "df.write.bucketBy(256, 'order_id').sortBy('order_id')"
        ".mode('overwrite').saveAsTable('fact_orders')"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html"
        "#bucketing-sorting-and-partitioning",
    ]
    estimated_impact = "Every join on this table requires a full shuffle; 0 shuffles with bucketing"
    effort_level = EffortLevel.MAJOR_REFACTOR

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        # Only flag when join() is also present in the file — suggests the table
        # will be joined, making bucketBy worthwhile.
        if not analyzer.find_method_calls("join"):
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("saveAsTable"):
            if "bucketBy" in call.chain:
                continue  # already bucketed
            # Extract table name for the message if available
            table_name = ""
            if (
                call.args
                and isinstance(call.args[0], ast.Constant)
                and isinstance(call.args[0].value, str)
            ):
                table_name = f" '{call.args[0].value}'"
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    f"saveAsTable({table_name.strip()!r}) without bucketBy"
                    " — every join on this table triggers a full shuffle",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D04-009 — Partition column not used in query filters
# =============================================================================


@register_rule
class MissingPartitionFilterRule(CodeRule):
    """SPL-D04-009: Reading from storage directly into expensive operations without a filter."""

    rule_id = "SPL-D04-009"
    name = "Partition column not used in query filters"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "Reading from storage directly into an expensive operation without a"
        " partition filter — full table scan likely."
    )
    explanation = (
        "Partitioned Parquet/ORC/Delta tables store their partition directory "
        "structure on disk.  When a query includes a ``filter()`` on the partition "
        "column (e.g. ``WHERE date = '2024-01-01'``), Spark's partition pruning "
        "skips all irrelevant directories and reads only the matching data.\n\n"
        "When no such filter is present — and the read flows directly into a "
        "``groupBy``, ``distinct``, ``orderBy``, or ``agg`` — Spark must scan "
        "the entire table.  On a multi-year history table this can mean scanning "
        "terabytes when only gigabytes are needed.\n\n"
        "Common causes:\n"
        "• Developer assumed Catalyst would infer the filter from context.\n"
        "• The filter was accidentally placed *after* the expensive operation.\n"
        "• The partition column is not the same as the query predicate column."
    )
    recommendation_template = (
        "Add ``filter()`` or ``where()`` on the partition column before the expensive "
        "operation: ``spark.read.parquet(path).filter('date = \"2024-01-01\"')``. "
        "Confirm partition pruning is active by checking the Spark UI 'Input' bytes "
        "in the stage details."
    )
    before_example = "spark.read.parquet('s3://datalake/events').groupBy('user_id').count()"
    after_example = (
        "spark.read.parquet('s3://datalake/events')"
        ".filter('date = \"2024-01-01\"').groupBy('user_id').count()"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html" "#partition-discovery",
    ]
    estimated_impact = "Full table scan instead of partition-pruned scan; 10–1000× more data read"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for method in _EXPENSIVE_OPS:
            for call in analyzer.find_method_calls(method):
                chain = call.chain
                try:
                    op_idx = chain.index(method)
                except ValueError:
                    continue

                prefix = set(chain[:op_idx])
                if not (_READ_TERMINALS & prefix):
                    continue  # no read in chain — can't determine scan scope
                if _FILTER_METHODS & prefix:
                    continue  # filter precedes expensive op → partition pruning likely

                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        f"{call.method_name}() on unfiltered read — add filter()"
                        " on partition column to enable partition pruning",
                        config=config,
                    )
                )

        return findings


# =============================================================================
# SPL-D04-010 — Repartition by column different from join key
# =============================================================================


@register_rule
class RepartitionJoinKeyMismatchRule(CodeRule):
    """SPL-D04-010: repartition on a column that differs from the subsequent join key."""

    rule_id = "SPL-D04-010"
    name = "Repartition by column different from join key"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "repartition() by a column that differs from the subsequent join key"
        " — the repartition is wasted."
    )
    explanation = (
        "``df.repartition('col_a').join(other, 'col_b')`` performs two full "
        "shuffles back-to-back:\n\n"
        "1. ``repartition('col_a')`` shuffles data by *col_a* hash.\n"
        "2. ``join('col_b')`` immediately re-shuffles data by *col_b* hash to "
        "   co-locate matching keys.\n\n"
        "The first shuffle is completely wasted because the join invalidates the "
        "partitioning before any computation benefits from it.  The only time "
        "a pre-join ``repartition`` saves work is when both the repartition key "
        "and the join key are the same column — and even then, Spark's exchange "
        "reuse (``spark.sql.exchange.reuse``) can achieve the same without an "
        "explicit ``repartition``."
    )
    recommendation_template = (
        "Remove the ``repartition()`` call — the join already shuffles on the join key. "
        "If pre-partitioning by the join key is the goal, use ``repartition('join_key')`` "
        "(same column as the join) and rely on Spark's exchange reuse to avoid a "
        "duplicate shuffle."
    )
    before_example = "df.repartition('user_id').join(other, 'order_id')"
    after_example = "df.join(other, 'order_id')  # join shuffles on order_id already"
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html",
    ]
    estimated_impact = "Two full shuffles instead of one; 2× shuffle I/O for that stage"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for join_call in analyzer.find_method_calls("join"):
            if "repartition" not in join_call.chain:
                continue

            # Find the repartition call on the same line (chained expression)
            repart_calls = [
                c for c in analyzer.find_method_calls("repartition") if c.line == join_call.line
            ]
            if not repart_calls:
                continue
            repart_call = repart_calls[0]

            repart_cols = _repartition_columns(repart_call)
            if not repart_cols:
                continue  # repartition(n) by count only — no column to mismatch

            join_keys = _join_keys(join_call)
            if not join_keys:
                continue  # join key not statically determinable

            if repart_cols & join_keys:
                continue  # at least one column overlaps → intentional pre-partitioning

            r_str = ", ".join(f"'{c}'" for c in sorted(repart_cols))
            j_str = ", ".join(f"'{k}'" for k in sorted(join_keys))
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    join_call.line,
                    f"repartition({r_str}) then join on {j_str}"
                    " — repartition key differs from join key, causing a wasted shuffle",
                    config=config,
                )
            )

        return findings
