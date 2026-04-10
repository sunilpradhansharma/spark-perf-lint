"""Tier 3 performance benchmarks for PySpark optimisation patterns.

Each benchmark function runs a controlled experiment — varying one parameter
while holding others constant — and returns a list of result dicts suitable
for display in the deep-audit notebook or for programmatic analysis.

**Tier 3 only** — this module imports PySpark directly and is *never*
imported by any Tier 1 (static analysis) code.

Benchmark summary
-----------------
``benchmark_join_strategies``
    Compares four join hint variants (auto, broadcast, sort-merge,
    shuffle-hash) on the same pair of DataFrames.  Returns elapsed wall
    time, shuffle-read bytes, and a one-line physical-plan summary for each.

``benchmark_partition_counts``
    Reruns a caller-supplied operation over a range of repartition counts.
    Returns elapsed time, actual output partition count, and skew ratio
    (max / mean rows per partition) for each configuration.

``benchmark_cache_strategies``
    Runs a multi-action pipeline (count + aggregate + count) against each of
    five caching strategies.  Returns per-action timings and total elapsed
    time for each strategy.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# =============================================================================
# Internal helpers
# =============================================================================


def _elapsed(fn: Callable[[], Any]) -> tuple[float, Any]:
    """Run *fn*, return ``(elapsed_seconds, return_value)``."""
    t0 = time.perf_counter()
    result = fn()
    return time.perf_counter() - t0, result


def _shuffle_bytes(spark: SparkSession) -> int:
    """Return total shuffle-read bytes from the most recent SQL execution.

    Reads the Spark metrics from ``spark.sparkContext.statusTracker`` via the
    REST API listener metrics.  Falls back to 0 if metrics are unavailable
    (e.g., local mode without history server).

    Args:
        spark: Active ``SparkSession``.

    Returns:
        Total shuffle-read bytes as an integer (0 when unavailable).
    """
    try:
        sc = spark.sparkContext
        # statusTracker only exposes stage metrics after execution
        stage_ids = sc.statusTracker().getActiveStageIds()
        total = 0
        for sid in stage_ids:
            info = sc.statusTracker().getStageInfo(sid)
            if info is not None:
                total += getattr(info, "shuffleReadBytes", 0) or 0
        # Also accumulate completed stages from the last SQL query
        for sid in sc.statusTracker().getActiveJobIds():
            job_info = sc.statusTracker().getJobInfo(sid)
            if job_info is not None:
                for stage_id in getattr(job_info, "stageIds", []):
                    info = sc.statusTracker().getStageInfo(stage_id)
                    if info is not None:
                        total += getattr(info, "shuffleReadBytes", 0) or 0
        return total
    except Exception:  # noqa: BLE001
        return 0


def _plan_summary(df: DataFrame) -> str:
    """Return a one-line summary of the physical plan for *df*.

    Extracts the first non-blank line from ``df.explain()`` output that
    contains a recognisable join/exchange operator keyword.

    Args:
        df: DataFrame whose physical plan to summarise.

    Returns:
        A short string identifying the dominant operator, e.g.
        ``"BroadcastHashJoin"`` or ``"SortMergeJoin"``.
    """
    # explain() prints to stdout; capture via explain(True) string form
    plan_text = df._jdf.queryExecution().simpleString()  # type: ignore[attr-defined]

    keywords = (
        "BroadcastHashJoin",
        "SortMergeJoin",
        "ShuffledHashJoin",
        "BroadcastNestedLoopJoin",
        "CartesianProduct",
    )
    for line in plan_text.splitlines():
        for kw in keywords:
            if kw in line:
                return kw
    return plan_text.splitlines()[0].strip() if plan_text.strip() else "unknown"


# =============================================================================
# Public benchmarks
# =============================================================================


def benchmark_join_strategies(
    spark: SparkSession,
    df_large: DataFrame,
    df_small: DataFrame,
    key_col: str,
) -> list[dict[str, Any]]:
    """Compare four join-hint variants on *df_large* ⋈ *df_small*.

    The benchmark materialises each join variant by calling ``.count()`` and
    records wall time, shuffle-read bytes, and the dominant physical-plan
    operator.

    The four strategies tested are:

    - ``"auto"``           — no hint; Spark's cost-based optimizer decides
    - ``"broadcast"``      — ``F.broadcast(df_small)`` hint forces BHJ
    - ``"sort_merge"``     — ``MERGE`` hint forces SortMergeJoin
    - ``"shuffle_hash"``   — ``SHUFFLE_HASH`` hint forces ShuffledHashJoin

    Args:
        spark: Active ``SparkSession``.
        df_large: The larger (probe) side of the join.
        df_small: The smaller (build) side of the join.  The broadcast
            strategy wraps this side with ``F.broadcast()``.
        key_col: Column name present in both DataFrames used as the join key.

    Returns:
        A list of dicts, one per strategy, each containing:

        - ``strategy`` (str): strategy label
        - ``elapsed_s`` (float): wall-clock seconds for ``.count()``
        - ``shuffle_read_bytes`` (int): shuffle read bytes (0 in local mode)
        - ``plan_summary`` (str): dominant physical-plan operator

        Example::

            [
                {"strategy": "auto",         "elapsed_s": 4.12, ...},
                {"strategy": "broadcast",     "elapsed_s": 0.83, ...},
                {"strategy": "sort_merge",    "elapsed_s": 5.60, ...},
                {"strategy": "shuffle_hash",  "elapsed_s": 3.40, ...},
            ]
    """
    results: list[dict[str, Any]] = []

    variants: list[tuple[str, DataFrame, DataFrame]] = [
        ("auto", df_large, df_small),
        ("broadcast", df_large, F.broadcast(df_small)),  # type: ignore[arg-type]
        ("sort_merge", df_large.hint("MERGE"), df_small),
        ("shuffle_hash", df_large.hint("SHUFFLE_HASH"), df_small),
    ]

    for strategy, left, right in variants:
        joined = left.join(right, on=key_col, how="inner")
        plan = _plan_summary(joined)

        elapsed, _ = _elapsed(lambda df=joined: df.count())
        shuffle = _shuffle_bytes(spark)

        results.append(
            {
                "strategy": strategy,
                "elapsed_s": round(elapsed, 3),
                "shuffle_read_bytes": shuffle,
                "plan_summary": plan,
            }
        )

    return results


def benchmark_partition_counts(
    spark: SparkSession,
    df: DataFrame,
    operation: Callable[[DataFrame], DataFrame],
    partition_counts: list[int],
) -> list[dict[str, Any]]:
    """Evaluate *operation* at multiple repartition counts.

    For each ``n`` in *partition_counts*, runs::

        result_df = operation(df.repartition(n))
        result_df.count()

    and records wall time, actual output partition count, and the skew ratio
    (``max_rows / mean_rows`` across output partitions).

    Skew ratio is computed by calling ``result_df.withColumn("_pid",
    F.spark_partition_id()).groupBy("_pid").count()`` and comparing the max
    to the mean count.  This adds one extra Spark job per configuration.

    Args:
        spark: Active ``SparkSession``.
        df: Base DataFrame to repartition.
        operation: A callable ``(DataFrame) -> DataFrame`` representing the
            workload to benchmark.  Typical examples::

                operation = lambda df: df.groupBy("key").agg(F.sum("value"))
                operation = lambda df: df.sort("key")

        partition_counts: List of target partition counts to test.  Each
            value is passed to ``df.repartition(n)``.

    Returns:
        A list of dicts, one per partition count, each containing:

        - ``n_partitions`` (int): the requested partition count
        - ``elapsed_s`` (float): wall-clock seconds for the ``.count()`` action
        - ``actual_partitions`` (int): actual output partition count
        - ``skew_ratio`` (float): max / mean rows per output partition
          (1.0 = perfectly balanced; higher values indicate skew)

        Example::

            [
                {"n_partitions": 50,  "elapsed_s": 2.1, "actual_partitions": 50,
                 "skew_ratio": 1.0},
                {"n_partitions": 200, "elapsed_s": 3.4, "actual_partitions": 200,
                 "skew_ratio": 1.2},
                {"n_partitions": 800, "elapsed_s": 6.8, "actual_partitions": 800,
                 "skew_ratio": 1.0},
            ]
    """
    results: list[dict[str, Any]] = []

    for n in partition_counts:
        repartitioned = df.repartition(n)
        result_df = operation(repartitioned)

        elapsed, count = _elapsed(lambda rdf=result_df: rdf.count())

        # Actual output partition count
        actual_partitions = result_df.rdd.getNumPartitions()

        # Skew ratio: max / mean rows per partition
        skew_ratio = 1.0
        try:
            partition_counts_df = (
                result_df.withColumn("_pid", F.spark_partition_id()).groupBy("_pid").count()
            )
            stats = partition_counts_df.agg(
                F.max("count").alias("max_count"),
                F.avg("count").alias("avg_count"),
            ).collect()[0]
            max_count = stats["max_count"] or 0
            avg_count = stats["avg_count"] or 1
            skew_ratio = round(max_count / avg_count, 2) if avg_count > 0 else 1.0
        except Exception:  # noqa: BLE001
            pass

        results.append(
            {
                "n_partitions": n,
                "elapsed_s": round(elapsed, 3),
                "actual_partitions": actual_partitions,
                "skew_ratio": skew_ratio,
            }
        )

    return results


def benchmark_cache_strategies(
    spark: SparkSession,
    df: DataFrame,
    strategies: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Compare caching strategies on a three-action pipeline.

    For each strategy, the benchmark runs a fixed pipeline of three actions:

    1. ``df.count()``                        — first pass (cold read or cache fill)
    2. ``df.agg(F.count("*")).collect()``    — second pass
    3. ``df.count()``                        — third pass

    For strategies that involve caching, the DataFrame is cached/persisted
    *before* action 1 and unpersisted after action 3.

    Supported strategy names:

    - ``"none"``                — no caching; reads from source every action
    - ``"cache"``               — ``df.cache()``  (MEMORY_AND_DISK)
    - ``"persist_memory"``      — ``df.persist(StorageLevel.MEMORY_ONLY)``
    - ``"persist_memory_disk"`` — ``df.persist(StorageLevel.MEMORY_AND_DISK)``
    - ``"checkpoint"``          — ``df.checkpoint()`` (requires checkpoint dir)

    The ``"checkpoint"`` strategy requires ``spark.sparkContext.setCheckpointDir``
    to have been called; if it has not, the strategy is skipped with a warning.

    Args:
        spark: Active ``SparkSession``.
        df: DataFrame to benchmark.
        strategies: Optional list of strategy names to test.  When ``None``,
            all five strategies are run in the order listed above.

    Returns:
        A list of dicts, one per strategy, each containing:

        - ``strategy`` (str): strategy label
        - ``action_1_s`` (float): elapsed seconds for the first action
        - ``action_2_s`` (float): elapsed seconds for the second action
        - ``action_3_s`` (float): elapsed seconds for the third action
        - ``total_s`` (float): sum of the three action times
        - ``skipped`` (bool): ``True`` when the strategy was skipped

        Example::

            [
                {"strategy": "none",    "action_1_s": 3.2, "action_2_s": 3.1,
                 "action_3_s": 3.2, "total_s": 9.5,  "skipped": False},
                {"strategy": "cache",   "action_1_s": 3.5, "action_2_s": 0.4,
                 "action_3_s": 0.4, "total_s": 4.3,  "skipped": False},
                {"strategy": "persist_memory", "action_1_s": 3.6, "action_2_s": 0.3,
                 "action_3_s": 0.3, "total_s": 4.2,  "skipped": False},
            ]
    """
    from pyspark import StorageLevel

    _ALL_STRATEGIES = [
        "none",
        "cache",
        "persist_memory",
        "persist_memory_disk",
        "checkpoint",
    ]

    if strategies is None:
        strategies = _ALL_STRATEGIES

    results: list[dict[str, Any]] = []

    for strategy in strategies:
        cached_df: DataFrame | None = None

        try:
            if strategy == "none":
                cached_df = df

            elif strategy == "cache":
                cached_df = df.cache()

            elif strategy == "persist_memory":
                cached_df = df.persist(StorageLevel.MEMORY_ONLY)

            elif strategy == "persist_memory_disk":
                cached_df = df.persist(StorageLevel.MEMORY_AND_DISK)

            elif strategy == "checkpoint":
                sc = spark.sparkContext
                cp_dir = sc._jvm.org.apache.spark.SparkContext.getOrCreate().getCheckpointDir()  # type: ignore[attr-defined]
                if not cp_dir.isDefined():
                    import warnings

                    warnings.warn(
                        "benchmark_cache_strategies: strategy='checkpoint' skipped — "
                        "no checkpoint directory configured.  Call "
                        "spark.sparkContext.setCheckpointDir('/tmp/checkpoints') first.",
                        stacklevel=2,
                    )
                    results.append(
                        {
                            "strategy": strategy,
                            "action_1_s": 0.0,
                            "action_2_s": 0.0,
                            "action_3_s": 0.0,
                            "total_s": 0.0,
                            "skipped": True,
                        }
                    )
                    continue
                cached_df = df.checkpoint()

            else:
                import warnings

                warnings.warn(
                    f"benchmark_cache_strategies: unknown strategy {strategy!r} — skipping.",
                    stacklevel=2,
                )
                results.append(
                    {
                        "strategy": strategy,
                        "action_1_s": 0.0,
                        "action_2_s": 0.0,
                        "action_3_s": 0.0,
                        "total_s": 0.0,
                        "skipped": True,
                    }
                )
                continue

            assert cached_df is not None  # mypy: always assigned above

            t1, _ = _elapsed(lambda d=cached_df: d.count())
            t2, _ = _elapsed(lambda d=cached_df: d.agg(F.count("*")).collect())
            t3, _ = _elapsed(lambda d=cached_df: d.count())

            results.append(
                {
                    "strategy": strategy,
                    "action_1_s": round(t1, 3),
                    "action_2_s": round(t2, 3),
                    "action_3_s": round(t3, 3),
                    "total_s": round(t1 + t2 + t3, 3),
                    "skipped": False,
                }
            )

        except Exception as exc:  # noqa: BLE001
            import warnings

            warnings.warn(
                f"benchmark_cache_strategies: strategy={strategy!r} failed: {exc}",
                stacklevel=2,
            )
            results.append(
                {
                    "strategy": strategy,
                    "action_1_s": 0.0,
                    "action_2_s": 0.0,
                    "action_3_s": 0.0,
                    "total_s": 0.0,
                    "skipped": True,
                }
            )

        finally:
            # Unpersist cached variants to free executor memory between runs
            if cached_df is not None and strategy not in ("none", "checkpoint"):
                try:
                    cached_df.unpersist()
                except Exception:  # noqa: BLE001
                    pass

    return results
