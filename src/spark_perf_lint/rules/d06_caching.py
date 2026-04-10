"""D06 — Caching and persistence rules for spark-perf-lint.

Correct DataFrame caching is one of the highest-leverage Spark optimisations:
it eliminates redundant recomputation of expensive pipelines.  But incorrect
caching is equally costly — it wastes executor memory, causes evictions, or
bakes in more data than necessary.  These rules detect the most common caching
anti-patterns so they can be fixed before hitting production.

Rule IDs: SPL-D06-001 through SPL-D06-008
"""

from __future__ import annotations

import ast

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer, MethodCallInfo
from spark_perf_lint.rules.base import CodeRule
from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.types import Dimension, EffortLevel, Finding, Severity

_DIM = Dimension.D06_CACHING

# Method names that constitute a "cache" operation
_CACHE_METHODS: frozenset[str] = frozenset({"cache", "persist"})

# Method names that indicate a shuffle / expensive stage boundary
_EXPENSIVE_OPS: frozenset[str] = frozenset(
    {"join", "groupBy", "agg", "distinct", "orderBy", "sort", "crossJoin"}
)

# Filter methods that narrow rows — running these *after* a cache wastes memory
_FILTER_METHODS: frozenset[str] = frozenset({"filter", "where"})

# Write-chain markers used to exclude partitionBy in write contexts
_WRITE_MARKERS: frozenset[str] = frozenset({"write", "writeStream"})


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _cache_count(analyzer: ASTAnalyzer) -> int:
    """Return the total number of cache() / persist() calls in the file."""
    return sum(len(analyzer.find_method_calls(m)) for m in _CACHE_METHODS)


def _unpersist_count(analyzer: ASTAnalyzer) -> int:
    """Return the total number of unpersist() calls in the file."""
    return len(analyzer.find_method_calls("unpersist"))


def _receiver_use_count(analyzer: ASTAnalyzer, var_name: str, after_line: int) -> int:
    """Count how many times *var_name* appears as a method-call receiver after *after_line*.

    Only exact name matches are counted (``df2`` does not match ``df2_copy``).
    """
    count = 0
    for call in analyzer.find_all_method_calls():
        if call.line <= after_line:
            continue
        # receiver_src can be "df2", "df2.something", etc. — match exact prefix
        rs = call.receiver_src.strip()
        if rs == var_name or rs.startswith(var_name + "."):
            count += 1
    return count


def _storage_level_arg(call: MethodCallInfo) -> str | None:
    """Extract the StorageLevel argument from a persist() call.

    Handles:
    - ``persist(StorageLevel.MEMORY_ONLY)`` → ast.Attribute
    - ``persist("MEMORY_ONLY")`` → ast.Constant (string)
    - ``persist()`` / no args → None
    """
    if not call.args:
        return None
    arg = call.args[0]
    if isinstance(arg, ast.Attribute):
        return arg.attr.upper()
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return arg.value.upper()
    return None


# =============================================================================
# SPL-D06-001 — cache() / persist() without unpersist()
# =============================================================================


@register_rule
class CacheWithoutUnpersistRule(CodeRule):
    """SPL-D06-001: cache() or persist() called without a matching unpersist()."""

    rule_id = "SPL-D06-001"
    name = "cache() without unpersist()"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "cache() / persist() without unpersist() leaks executor memory."
    explanation = (
        "Every ``cache()`` or ``persist()`` call pins the DataFrame's RDD partitions in "
        "executor memory (and optionally disk).  If ``unpersist()`` is never called, those "
        "blocks remain pinned for the lifetime of the SparkContext.  In a long-running job "
        "or notebook this causes memory pressure, evictions of *other* cached DataFrames, "
        "and ultimately OOM errors.\n\n"
        "Best practice: always pair each ``cache()`` / ``persist()`` with an "
        "``unpersist()`` call once the DataFrame is no longer needed.  For short scripts "
        "where the SparkContext exits immediately this is less critical, but the habit "
        "prevents hard-to-debug memory issues when code is moved to shared clusters."
    )
    recommendation_template = (
        "Add ``df.unpersist()`` once the cached DataFrame is no longer needed.  "
        "For DataFrames used within a bounded scope, consider a context-manager wrapper."
    )
    before_example = "df_cached = df.join(other, 'id').cache()"
    after_example = (
        "df_cached = df.join(other, 'id').cache()\n"
        "# ... use df_cached ...\n"
        "df_cached.unpersist()"
    )
    references = [
        "https://spark.apache.org/docs/latest/rdd-programming-guide.html#which-storage-level-to-choose",
    ]
    estimated_impact = "Executor memory leak; evicts other cached data; OOM in long-running jobs"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        cache_calls = [c for m in _CACHE_METHODS for c in analyzer.find_method_calls(m)]
        if not cache_calls:
            return []

        if _unpersist_count(analyzer) > 0:
            return []  # At least some unpersist present — suppress global heuristic

        findings: list[Finding] = []
        for call in sorted(cache_calls, key=lambda c: c.line):
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    f"{call.method_name}() called without any unpersist() in this file"
                    " — executor memory will not be released",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D06-002 — cache() used only once
# =============================================================================


@register_rule
class CacheUsedOnceRule(CodeRule):
    """SPL-D06-002: A cached DataFrame is referenced only once after caching."""

    rule_id = "SPL-D06-002"
    name = "cache() used only once"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "Caching a DataFrame that is only read once adds overhead without benefit."
    explanation = (
        "``cache()`` / ``persist()`` is valuable when a DataFrame is consumed **multiple** "
        "times — it avoids recomputing the same pipeline on each action.  When a DataFrame "
        "is used only once after caching, the cache write itself (an extra pass through the "
        "data and a serialisation step) makes the job slower, not faster.\n\n"
        "Common causes:\n"
        "• Defensive caching of every intermediate step 'just in case'.\n"
        "• Caching in a refactor that reduced the number of downstream consumers.\n\n"
        "Remove the ``cache()`` call to save memory and eliminate the overhead write."
    )
    recommendation_template = (
        "Remove ``cache()`` — the DataFrame is only read once so caching adds overhead. "
        "Cache only DataFrames that are consumed 2+ times."
    )
    before_example = "df_cached = df.join(other, 'id').cache()\nresult = df_cached.count()"
    after_example = "result = df.join(other, 'id').count()  # no cache needed"
    references = [
        "https://spark.apache.org/docs/latest/rdd-programming-guide.html#which-storage-level-to-choose",
    ]
    estimated_impact = "Unnecessary serialisation pass; wastes executor memory for no benefit"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for assignment in analyzer.find_variable_assignments():
            if not any(m in assignment.rhs_method_calls for m in _CACHE_METHODS):
                continue
            # Only handle simple single-name assignment targets
            target = assignment.target.strip()
            if not target.isidentifier():
                continue
            uses = _receiver_use_count(analyzer, target, after_line=assignment.line)
            if uses <= 1:
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        assignment.line,
                        f"'{target}' is cached but used only {uses} time(s)"
                        " — caching adds overhead without benefit",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D06-003 — cache() inside a loop
# =============================================================================


@register_rule
class CacheInLoopRule(CodeRule):
    """SPL-D06-003: cache() or persist() called inside a for/while loop."""

    rule_id = "SPL-D06-003"
    name = "cache() inside loop"
    dimension = _DIM
    default_severity = Severity.CRITICAL
    description = "Calling cache()/persist() inside a loop fills executor memory on each iteration."
    explanation = (
        "Each call to ``cache()`` / ``persist()`` inside a loop creates a new cached "
        "RDD that is pinned in executor memory.  Without a matching ``unpersist()`` "
        "inside the *same* iteration, each loop pass accumulates more cached blocks "
        "until the executor runs out of memory.\n\n"
        "Even when ``unpersist()`` is present, the pattern is fragile: if the loop "
        "exits early (exception, ``break``), the last cached DataFrame leaks.  "
        "For iterative algorithms (ML, graph processing) prefer:\n\n"
        "• ``checkpoint()`` — truncates lineage so the DAG does not grow unboundedly.\n"
        "• Persisting outside the loop if the same DataFrame is reused across iterations.\n"
        "• Restructuring to avoid per-iteration caching entirely."
    )
    recommendation_template = (
        "Move cache()/persist() outside the loop, or use checkpoint() inside the loop "
        "to truncate lineage.  Call unpersist() at the start of each iteration to "
        "release the previous iteration's cached data."
    )
    before_example = (
        "for epoch in range(10):\n"
        "    df = model.transform(df).cache()\n"
        "    loss = df.agg(mean('loss')).collect()"
    )
    after_example = (
        "for epoch in range(10):\n"
        "    prev = df\n"
        "    df = model.transform(df).checkpoint()  # truncates lineage\n"
        "    prev.unpersist()"
    )
    references = [
        "https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence",
    ]
    estimated_impact = "OOM after O(N) iterations; executor memory accumulates without release"
    effort_level = EffortLevel.MAJOR_REFACTOR

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for method in _CACHE_METHODS:
            for loop, count in analyzer.find_method_calls_in_loops(method):
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        loop.line,
                        f"{method}() called {count} time(s) inside a {loop.loop_type} loop"
                        " — cached data accumulates each iteration; risks OOM",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D06-004 — cache() before filter
# =============================================================================


@register_rule
class CacheBeforeFilterRule(CodeRule):
    """SPL-D06-004: filter()/where() is chained after cache(), caching more data than needed."""

    rule_id = "SPL-D06-004"
    name = "cache() before filter"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "Filtering after cache() caches unnecessary rows; push filter before cache."
    explanation = (
        "When ``cache()`` / ``persist()`` is called before ``filter()`` / ``where()``, "
        "Spark caches the *full* (unfiltered) dataset and then applies the filter during "
        "each downstream action.  This wastes memory proportional to the rows removed by "
        "the filter.\n\n"
        "The fix is simple: apply the filter **before** caching so only the relevant "
        "subset is stored in memory:\n\n"
        "``df.filter('active = true').cache()``  ← correct\n"
        "``df.cache().filter('active = true')``  ← wasteful\n\n"
        "Applying filters early is one of the most impactful Spark optimisations — it "
        "reduces the data volume for every downstream operation, not just the cached copy."
    )
    recommendation_template = (
        "Apply filter()/where() before cache() to reduce the cached dataset size: "
        "``df.filter(...).cache()`` instead of ``df.cache().filter(...)``."
    )
    before_example = "df.cache().filter('active = true').groupBy('status').count()"
    after_example = "df.filter('active = true').cache().groupBy('status').count()"
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html",
    ]
    estimated_impact = "Caches N rows but only M < N are used; wastes (N-M) / N of cache memory"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for method in _FILTER_METHODS:
            for call in analyzer.find_method_calls(method):
                # Chain direction: call.chain = [root … this_call]
                # "cache before filter" means cache appears in chain before filter
                if any(m in call.chain for m in _CACHE_METHODS):
                    findings.append(
                        self.create_finding(
                            analyzer.filename,
                            call.line,
                            f"{method}() applied after cache() — push the filter before"
                            " cache() to avoid caching unnecessary rows",
                            config=config,
                        )
                    )
        return findings


# =============================================================================
# SPL-D06-005 — MEMORY_ONLY storage level for large datasets
# =============================================================================


@register_rule
class MemoryOnlyStorageLevelRule(CodeRule):
    """SPL-D06-005: persist(StorageLevel.MEMORY_ONLY) for datasets that may exceed heap."""

    rule_id = "SPL-D06-005"
    name = "MEMORY_ONLY storage level for potentially large datasets"
    dimension = _DIM
    default_severity = Severity.INFO
    description = "MEMORY_ONLY drops partitions when memory is full; prefer MEMORY_AND_DISK."
    explanation = (
        "``StorageLevel.MEMORY_ONLY`` (the default for RDD ``cache()``) evicts partitions "
        "when the executor's storage fraction is exhausted.  Evicted partitions are "
        "**recomputed from scratch** on the next access — silently, with no warning.  "
        "For large datasets this can turn a cache hit into a full recomputation, negating "
        "the benefit of caching entirely.\n\n"
        "``MEMORY_AND_DISK`` spills evicted partitions to disk instead of recomputing, "
        "providing a safety net at the cost of disk I/O.  For most production workloads "
        "this is the safer default.  ``MEMORY_AND_DISK_SER`` further reduces memory "
        "footprint by serialising objects.\n\n"
        "DataFrame ``cache()`` uses ``MEMORY_AND_DISK`` by default — this finding only "
        "fires when ``MEMORY_ONLY`` is set explicitly via ``persist()``."
    )
    recommendation_template = (
        "Use ``persist(StorageLevel.MEMORY_AND_DISK)`` or "
        "``persist(StorageLevel.MEMORY_AND_DISK_SER)`` "
        "to avoid silent partition eviction and full recomputation."
    )
    before_example = "df.persist(StorageLevel.MEMORY_ONLY)"
    after_example = "df.persist(StorageLevel.MEMORY_AND_DISK)  # safe fallback to disk"
    references = [
        "https://spark.apache.org/docs/latest/rdd-programming-guide.html#which-storage-level-to-choose",
    ]
    estimated_impact = "Silent partition eviction leads to full recomputation under memory pressure"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("persist"):
            level = _storage_level_arg(call)
            if level == "MEMORY_ONLY":
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        "persist(MEMORY_ONLY) evicts partitions under memory pressure"
                        " — prefer MEMORY_AND_DISK to avoid silent recomputation",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D06-006 — Reused DataFrame without cache
# =============================================================================


@register_rule
class ReusedDataFrameWithoutCacheRule(CodeRule):
    """SPL-D06-006: A variable holding an expensive DataFrame is used as a receiver 2+ times."""

    rule_id = "SPL-D06-006"
    name = "Reused DataFrame without cache"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "A DataFrame built from an expensive operation is used multiple times"
        " without caching, causing redundant recomputation."
    )
    explanation = (
        "When a DataFrame variable is used as the receiver of multiple actions or "
        "transformations, Spark recomputes its full lineage for each action.  "
        "If the lineage includes expensive operations — joins, aggregations, wide "
        "shuffles — the redundant recomputation can dominate job runtime.\n\n"
        "Example: ``df_agg = df.groupBy('id').agg(sum('v'))`` used in "
        "``df_agg.join(...)`` and ``df_agg.count()`` triggers two full aggregations "
        "instead of one.\n\n"
        "Calling ``cache()`` or ``persist()`` once materialises the result; subsequent "
        "uses read from the cached copy instead of re-executing the pipeline."
    )
    recommendation_template = (
        "Add ``.cache()`` after the expensive transformation: "
        "``df_agg = df.groupBy('id').agg(...).cache()``. "
        "Call ``df_agg.unpersist()`` when done."
    )
    before_example = (
        "df_agg = df.groupBy('id').agg(sum('v'))\n"
        "result1 = df_agg.join(lookup, 'id')\n"
        "total = df_agg.count()"
    )
    after_example = (
        "df_agg = df.groupBy('id').agg(sum('v')).cache()\n"
        "result1 = df_agg.join(lookup, 'id')\n"
        "total = df_agg.count()\n"
        "df_agg.unpersist()"
    )
    references = [
        "https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence",
    ]
    estimated_impact = "N downstream uses trigger N full pipeline recomputations instead of 1"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for assignment in analyzer.find_variable_assignments():
            # Skip if already cached / persisted
            if any(m in assignment.rhs_method_calls for m in _CACHE_METHODS):
                continue
            # Only flag assignments that involve at least one expensive op
            if not any(op in assignment.rhs_method_calls for op in _EXPENSIVE_OPS):
                continue
            target = assignment.target.strip()
            if not target.isidentifier():
                continue
            uses = _receiver_use_count(analyzer, target, after_line=assignment.line)
            if uses >= 2:
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        assignment.line,
                        f"'{target}' is used {uses} times but not cached"
                        " — each use recomputes the full pipeline",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D06-007 — cache() after repartition
# =============================================================================


@register_rule
class CacheAfterRepartitionRule(CodeRule):
    """SPL-D06-007: cache()/persist() immediately after repartition()."""

    rule_id = "SPL-D06-007"
    name = "cache() after repartition"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "cache() after repartition() is useful for multi-use DataFrames"
        " but wasted if the result is only used once."
    )
    explanation = (
        "``repartition()`` triggers a full shuffle — an expensive distributed sort.  "
        "Caching the result with ``cache()`` / ``persist()`` immediately afterward is "
        "beneficial **only** when the repartitioned DataFrame is consumed multiple times, "
        "because it avoids re-running the shuffle on each action.\n\n"
        "If the repartitioned DataFrame is used only once, the cache write is pure "
        "overhead: an extra serialisation pass and memory allocation on top of an "
        "already-expensive shuffle.\n\n"
        "This finding is INFO-level — verify that the DataFrame is indeed reused before "
        "acting on it.  If it is used multiple times, the pattern is correct and can be "
        "suppressed."
    )
    recommendation_template = (
        "Confirm the repartitioned DataFrame is used 2+ times. "
        "If used once, remove cache() to avoid serialisation overhead. "
        "If used multiple times, the cache() is correct — suppress this finding."
    )
    before_example = "df2 = df.repartition(200).cache()  # used once"
    after_example = (
        "df2 = df.repartition(200)  # no cache — used only once\n"
        "# OR, if reused:\n"
        "df2 = df.repartition(200).cache()  # cache is justified"
    )
    references = [
        "https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence",
    ]
    estimated_impact = "Unnecessary cache write after an already-expensive shuffle"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for method in _CACHE_METHODS:
            for call in analyzer.find_method_calls(method):
                if "repartition" in call.chain or "coalesce" in call.chain:
                    findings.append(
                        self.create_finding(
                            analyzer.filename,
                            call.line,
                            f"{call.method_name}() after repartition()"
                            " — verify the DataFrame is used 2+ times to justify caching",
                            config=config,
                        )
                    )
        return findings


# =============================================================================
# SPL-D06-008 — checkpoint() vs cache() misuse
# =============================================================================


@register_rule
class CheckpointVsCacheMisuseRule(CodeRule):
    """SPL-D06-008: checkpoint() used in non-iterative code where cache() would suffice."""

    rule_id = "SPL-D06-008"
    name = "checkpoint vs cache misuse"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "checkpoint() in non-iterative code writes to disk unnecessarily;"
        " use cache() unless DAG truncation is required."
    )
    explanation = (
        "``checkpoint()`` saves a DataFrame to HDFS / the checkpoint directory and "
        "**truncates the DAG lineage** up to that point.  It is designed for iterative "
        "algorithms (ML training loops, graph processing) where the lineage grows "
        "unboundedly across iterations and eventually causes stack overflows or OOM.\n\n"
        "In non-iterative code the DAG is bounded regardless — lineage truncation "
        "provides no benefit.  ``checkpoint()`` is strictly slower than ``cache()`` "
        "because it always writes to disk and requires a SparkContext checkpoint "
        "directory to be configured.  Using it outside an iterative context is "
        "unnecessary overhead.\n\n"
        "Use ``cache()`` / ``persist()`` in non-iterative pipelines; reserve "
        "``checkpoint()`` for loops where the DAG grows with each iteration."
    )
    recommendation_template = (
        "Replace checkpoint() with cache() for non-iterative pipelines. "
        "If this is part of an iterative algorithm (ML, PageRank, etc.) "
        "and is inside a loop, checkpoint() is correct — suppress this finding."
    )
    before_example = "df_intermediate = df.join(other, 'id').checkpoint()"
    after_example = "df_intermediate = df.join(other, 'id').cache()  # faster; no disk write"
    references = [
        "https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-checkpointing",
    ]
    estimated_impact = "Unnecessary disk write + checkpoint dir overhead in non-iterative pipelines"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        checkpoint_calls = analyzer.find_method_calls("checkpoint")
        if not checkpoint_calls:
            return []

        # Suppress if the file contains any loop — likely an iterative algorithm
        if analyzer.find_loop_patterns():
            return []

        findings: list[Finding] = []
        for call in checkpoint_calls:
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    "checkpoint() in non-iterative code writes to disk unnecessarily"
                    " — use cache() unless DAG lineage truncation is needed",
                    config=config,
                )
            )
        return findings
