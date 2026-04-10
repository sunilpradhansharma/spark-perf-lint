"""Synthetic PySpark data generators for Tier 3 deep-audit benchmarks.

Each generator produces a ``DataFrame`` with a specific data distribution
designed to trigger or benchmark a known Spark performance pattern.  The
functions are intentionally self-contained: they accept a ``SparkSession``
and return a ``DataFrame`` without touching the filesystem.

**Tier 3 only** — this module imports PySpark directly and is *never*
imported by any Tier 1 (static analysis) code.  Import it only from the
deep-audit notebook or explicit Tier 3 test suites.

Generator summary
-----------------
``generate_skewed_data``
    Power-law key distribution calibrated to a target skew ratio.
    Stresses hash-partitioned shuffles, sort-merge joins, and groupBy.

``generate_join_test_data``
    Large fact table + small dimension table with configurable null-key
    fraction.  Designed for join-strategy and null-skew benchmarks.

``generate_partition_test_data``
    Controlled partition occupancy.  Optionally introduces 80/20 skew
    so that a minority of reducers hold the majority of rows.

``generate_high_cardinality_data``
    Near-unique key column (n_unique_keys ≈ n_rows / 2).  Tests Bloom
    filter join optimisation and string-hashing overhead.

``generate_wide_table``
    DataFrame with up to 500 columns.  Benchmarks Catalyst column-pruning
    and the cost of ``select('*')`` on wide schemas.

``generate_nested_schema_data``
    Deeply nested struct hierarchy.  Benchmarks ``explode``, field access,
    and schema-flattening patterns.
"""

from __future__ import annotations

import math

# PySpark is required at import time for this module.
# Importing this module without PySpark installed raises ImportError immediately,
# which is the intended behaviour for Tier 3 optional dependencies.
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
)

# =============================================================================
# Internal helpers
# =============================================================================

_DEFAULT_SEED: int = 42


def _calibrate_powerlaw_alpha(n_keys: int, skew_ratio: float) -> float:
    """Return the exponent *alpha* for a power-law key generator.

    The generator uses ``key = floor(U^alpha * n_keys)`` where ``U ~ Uniform(0,1)``.
    The expected ratio of ``P(key=0)`` to the mean probability is

        ratio = n_keys^(1 - 1/alpha)

    Solving for alpha given the target *skew_ratio*::

        alpha = log(n_keys) / (log(n_keys) - log(skew_ratio))

    Args:
        n_keys:     Number of distinct keys.
        skew_ratio: Desired ratio of hot-key frequency to mean key frequency.
                    Must satisfy ``1 < skew_ratio < n_keys``.

    Returns:
        Float exponent alpha > 1.

    Raises:
        ValueError: If *skew_ratio* is outside the valid range.
    """
    if skew_ratio <= 1:
        raise ValueError(f"skew_ratio must be > 1, got {skew_ratio}")
    if skew_ratio >= n_keys:
        raise ValueError(
            f"skew_ratio ({skew_ratio}) must be less than n_keys ({n_keys}). "
            "Try reducing skew_ratio or increasing n_rows."
        )
    return math.log(n_keys) / (math.log(n_keys) - math.log(skew_ratio))


def _build_nested_struct(depth: int, seed: int = 0) -> Column:
    """Recursively build a StructType column expression to *depth* levels.

    All values are derived from ``id`` (the column produced by
    ``spark.range()``) via hash functions, so no extra base columns are
    required.

    Args:
        depth: Nesting depth.  ``depth=0`` produces a leaf struct with three
               scalar fields; each increment wraps the inner struct in a new
               outer struct.
        seed:  Integer salt to vary hash outputs across depth levels.

    Returns:
        A ``Column`` expression whose runtime type is a ``StructType``.
    """
    if depth == 0:
        return F.struct(
            (F.abs(F.hash(F.col("id"), F.lit(seed))) % 1_000_000)
            .cast(DoubleType())
            .alias("leaf_float"),
            (F.abs(F.hash(F.col("id"), F.lit(seed + 1))) % 10_000).alias("leaf_int"),
            F.concat(F.lit("val_"), F.col("id").cast(StringType())).alias("leaf_str"),
        )
    return F.struct(
        (F.abs(F.hash(F.col("id"), F.lit(seed))) % 1_000_000).cast(DoubleType()).alias("value"),
        F.concat(F.lit(f"L{depth}_"), F.col("id").cast(StringType())).alias("tag"),
        _build_nested_struct(depth - 1, seed + 100).alias("nested"),
    )


# =============================================================================
# Public generators
# =============================================================================


def generate_skewed_data(
    spark: SparkSession,
    n_rows: int = 10_000_000,
    skew_ratio: float = 100.0,
    n_keys: int | None = None,
    seed: int = _DEFAULT_SEED,
) -> DataFrame:
    """Generate a DataFrame with a power-law key distribution.

    The join key follows ``P(key = k) ∝ k^(−s)`` (discrete Pareto) achieved
    via the inverse-CDF transform ``key = floor(U^α · n_keys)`` where *α* is
    calibrated so that the hot key (key 0) receives approximately
    ``skew_ratio × mean_count`` rows.

    Calibration formula::

        alpha = log(n_keys) / (log(n_keys) - log(skew_ratio))

    Schema::

        join_key  STRING   -- power-law distributed integer cast to string
        value     LONG     -- sequential id (useful as a payload column)
        amount    DOUBLE   -- uniform [0, 1000) random value
        category  STRING   -- low-cardinality label derived from join_key

    Args:
        spark:       Active ``SparkSession``.
        n_rows:      Total number of rows to generate.
        skew_ratio:  Approximate ratio of hot-key row count to mean row count.
                     Must satisfy ``1 < skew_ratio < n_keys``.
        n_keys:      Number of distinct key values.  Defaults to
                     ``max(200, n_rows // 1000)`` so there are always at
                     least a few hundred keys.
        seed:        Random seed for the ``amount`` column.

    Returns:
        A ``DataFrame`` with schema ``(join_key STRING, value LONG,
        amount DOUBLE, category STRING)``.

    Raises:
        ValueError: If *skew_ratio* is outside the valid range for *n_keys*.

    Example::

        df = generate_skewed_data(spark, n_rows=1_000_000, skew_ratio=50)
        # Key "0" holds ~50× the average per-key row count.
    """
    if n_keys is None:
        n_keys = max(200, n_rows // 1_000)

    alpha = _calibrate_powerlaw_alpha(n_keys, skew_ratio)
    n_partitions = max(10, min(200, n_rows // 50_000))

    return (
        spark.range(n_rows, numPartitions=n_partitions)
        .withColumn(
            "join_key",
            F.floor(F.pow(F.rand(seed), alpha) * n_keys).cast(LongType()).cast(StringType()),
        )
        .withColumnRenamed("id", "value")
        .withColumn("amount", (F.rand(seed + 1) * 1_000).cast(DoubleType()))
        .withColumn(
            "category",
            F.concat(
                F.lit("cat_"),
                (F.col("value") % 10).cast(StringType()),
            ),
        )
    )


def generate_join_test_data(
    spark: SparkSession,
    left_rows: int = 10_000_000,
    right_rows: int = 50_000,
    key_cardinality: int = 10_000,
    null_key_pct: float = 0.01,
    seed: int = _DEFAULT_SEED,
) -> tuple[DataFrame, DataFrame]:
    """Generate a (fact, dimension) table pair for join benchmarks.

    The *left* (fact) table is large with a foreign key that matches the
    *right* (dimension) table's primary key.  A configurable fraction of
    left rows carry a ``NULL`` foreign key to exercise null-skew behaviour
    (SPL-D05-002: join key contains null values).

    Left schema::

        fact_id      LONG    -- sequential id
        join_key     STRING  -- foreign key (null_key_pct fraction are NULL)
        amount       DOUBLE  -- transaction amount
        ts           LONG    -- synthetic Unix timestamp
        region       STRING  -- low-cardinality region label

    Right schema::

        join_key     STRING  -- primary key (no nulls, unique per row)
        category     STRING  -- product category
        discount     DOUBLE  -- discount rate [0, 0.5)
        is_active    INT     -- boolean flag (0 or 1)

    Args:
        spark:           Active ``SparkSession``.
        left_rows:       Row count for the fact table.
        right_rows:      Row count for the dimension table.
        key_cardinality: Number of distinct non-null join key values shared
                         between the two tables.  Must be ``<= right_rows``.
        null_key_pct:    Fraction of left rows whose ``join_key`` is ``NULL``
                         (e.g. ``0.01`` → 1 % of fact rows have no match).
        seed:            Random seed.

    Returns:
        ``(left_df, right_df)`` — fact and dimension DataFrames.

    Raises:
        ValueError: If *key_cardinality* exceeds *right_rows*, or if
                    *null_key_pct* is not in [0, 1).

    Example::

        fact, dim = generate_join_test_data(spark, left_rows=500_000, right_rows=5_000)
        result = fact.join(F.broadcast(dim), "join_key")
    """
    if key_cardinality > right_rows:
        raise ValueError(
            f"key_cardinality ({key_cardinality}) must be <= right_rows ({right_rows})"
        )
    if not 0.0 <= null_key_pct < 1.0:
        raise ValueError(f"null_key_pct must be in [0, 1), got {null_key_pct}")

    left_partitions = max(10, min(200, left_rows // 50_000))
    right_partitions = max(2, min(20, right_rows // 5_000))

    # ── Left (fact) table ────────────────────────────────────────────────────
    left_df = (
        spark.range(left_rows, numPartitions=left_partitions)
        .withColumnRenamed("id", "fact_id")
        .withColumn(
            "join_key",
            F.when(
                F.rand(seed) < null_key_pct,
                F.lit(None).cast(StringType()),
            ).otherwise((F.col("fact_id") % key_cardinality).cast(StringType())),
        )
        .withColumn("amount", (F.rand(seed + 1) * 10_000).cast(DoubleType()))
        .withColumn("ts", (F.col("fact_id") % 86_400 + 1_700_000_000).cast(LongType()))
        .withColumn(
            "region",
            F.element_at(
                F.array(
                    F.lit("US-EAST"),
                    F.lit("US-WEST"),
                    F.lit("EU"),
                    F.lit("APAC"),
                ),
                (F.col("fact_id") % 4 + 1).cast(IntegerType()),
            ),
        )
    )

    # ── Right (dimension) table ──────────────────────────────────────────────
    right_df = (
        spark.range(right_rows, numPartitions=right_partitions)
        .withColumn("join_key", (F.col("id") % key_cardinality).cast(StringType()))
        .withColumn(
            "category",
            F.concat(
                F.lit("cat_"),
                (F.col("id") % 20).cast(StringType()),
            ),
        )
        .withColumn("discount", (F.rand(seed + 2) * 0.5).cast(DoubleType()))
        .withColumn("is_active", (F.col("id") % 10 > 0).cast(IntegerType()))
        .drop("id")
        .dropDuplicates(["join_key"])
    )

    return left_df, right_df


def generate_partition_test_data(
    spark: SparkSession,
    n_rows: int = 10_000_000,
    n_partitions: int = 200,
    partition_skew: bool = False,
    skew_hot_fraction: float = 0.80,
    skew_hot_partition_fraction: float = 0.20,
    seed: int = _DEFAULT_SEED,
) -> DataFrame:
    """Generate a DataFrame with a controlled partition distribution.

    When *partition_skew* is ``False``, rows are distributed uniformly
    across all *n_partitions* — the balanced baseline.

    When *partition_skew* is ``True``, an 80/20-style imbalance is created:
    *skew_hot_fraction* of rows land in *skew_hot_partition_fraction* of
    partitions.  This reproduces the straggler pattern that AQE skew-join
    is designed to mitigate.

    Schema::

        id            LONG    -- sequential row id
        partition_key INT     -- explicit partition assignment column
        value         DOUBLE  -- uniform random payload

    Args:
        spark:                      Active ``SparkSession``.
        n_rows:                     Total row count.
        n_partitions:               Target partition count.
        partition_skew:             When ``True``, apply 80/20 partition imbalance.
        skew_hot_fraction:          Fraction of rows assigned to hot partitions.
                                    Only used when *partition_skew* is ``True``.
        skew_hot_partition_fraction:Fraction of partitions that are "hot".
                                    Only used when *partition_skew* is ``True``.
        seed:                       Random seed for value column.

    Returns:
        A ``DataFrame`` repartitioned to *n_partitions* by ``partition_key``.

    Example::

        balanced = generate_partition_test_data(spark, 1_000_000, 50)
        skewed   = generate_partition_test_data(spark, 1_000_000, 50, partition_skew=True)
    """
    n_hot = max(1, int(n_partitions * skew_hot_partition_fraction))
    n_cold = max(1, n_partitions - n_hot)

    base = spark.range(n_rows, numPartitions=max(10, n_partitions // 2))

    if partition_skew:
        # skew_hot_fraction of rows → keys in [0, n_hot)
        # remaining rows → keys in [n_hot, n_partitions)
        partition_key_col = F.when(
            F.rand(seed) < skew_hot_fraction,
            (F.abs(F.hash(F.col("id"), F.lit(1))) % n_hot).cast(IntegerType()),
        ).otherwise(
            (F.abs(F.hash(F.col("id"), F.lit(2))) % n_cold + n_hot).cast(IntegerType()),
        )
    else:
        partition_key_col = (F.abs(F.hash(F.col("id"), F.lit(3))) % n_partitions).cast(
            IntegerType()
        )

    return (
        base.withColumn("partition_key", partition_key_col)
        .withColumn("value", (F.rand(seed + 1) * 1_000).cast(DoubleType()))
        .repartition(n_partitions, "partition_key")
    )


def generate_high_cardinality_data(
    spark: SparkSession,
    n_rows: int = 10_000_000,
    n_unique_keys: int = 5_000_000,
    payload_length: int = 32,
    seed: int = _DEFAULT_SEED,
) -> DataFrame:
    """Generate a DataFrame with a near-unique string key column.

    High-cardinality string keys stress hash-join performance and expose the
    benefit of Bloom filter join optimization (SPL-D03-004).  The key is an
    uppercase hex string of *payload_length* characters derived from the row id
    so the distribution is deterministic and reproducible.

    Schema::

        id               LONG    -- sequential row id
        high_card_key    STRING  -- near-unique hex string (n_unique_keys distinct values)
        value            LONG    -- row id modulo n_unique_keys (numeric join key)
        payload          STRING  -- fixed-length string payload (simulates wide rows)
        metric           DOUBLE  -- uniform random metric value

    Args:
        spark:          Active ``SparkSession``.
        n_rows:         Total row count.
        n_unique_keys:  Number of distinct key values.  Should be <= *n_rows*.
                        Typical use: ``n_unique_keys = n_rows // 2``.
        payload_length: Character length of the ``payload`` string column.
                        Increase to simulate wide string-heavy rows.
        seed:           Random seed for ``metric`` column.

    Returns:
        A ``DataFrame`` with near-unique string keys.

    Example::

        df = generate_high_cardinality_data(spark, n_rows=500_000, n_unique_keys=250_000)
        # Join this with another high-cardinality table to benchmark Bloom filters.
    """
    n_partitions = max(10, min(200, n_rows // 50_000))
    payload_expr = F.lpad(
        (F.abs(F.hash(F.col("id"), F.lit(seed + 99))) % (10**payload_length)).cast(StringType()),
        payload_length,
        "0",
    )
    return (
        spark.range(n_rows, numPartitions=n_partitions)
        .withColumn(
            "high_card_key",
            F.upper(F.hex((F.col("id") % n_unique_keys).cast(LongType()))),
        )
        .withColumn("value", F.col("id") % n_unique_keys)
        .withColumn("payload", payload_expr)
        .withColumn("metric", (F.rand(seed) * 100_000).cast(DoubleType()))
    )


def generate_wide_table(
    spark: SparkSession,
    n_rows: int = 1_000_000,
    n_columns: int = 500,
    seed: int = _DEFAULT_SEED,
) -> DataFrame:
    """Generate a wide DataFrame with many numeric columns.

    Wide tables expose the cost of ``select('*')`` (SPL-D10-001: prevents
    column pruning) and benchmark Catalyst's ability to push down projections.
    All metric columns are ``DOUBLE``; identifiers are ``STRING`` and ``LONG``.

    Schema::

        id       LONG      -- sequential row id
        key      STRING    -- low-cardinality group key (id % 1000)
        col_001  DOUBLE    -- first metric column
        col_002  DOUBLE
        …
        col_{n}  DOUBLE    -- last metric column  (n = n_columns − 2)

    Args:
        spark:      Active ``SparkSession``.
        n_rows:     Total row count.
        n_columns:  Total column count including ``id`` and ``key``.
                    Must be >= 3.
        seed:       Base random seed.  Column ``i`` uses ``seed + i`` for
                    independence.

    Returns:
        A wide ``DataFrame`` with *n_columns* columns.

    Raises:
        ValueError: If *n_columns* < 3.

    Example::

        wide = generate_wide_table(spark, n_rows=100_000, n_columns=200)
        # Benchmarks: wide.select("*").count() vs wide.select("id","key").count()
    """
    if n_columns < 3:
        raise ValueError(f"n_columns must be >= 3, got {n_columns}")

    n_partitions = max(10, min(100, n_rows // 50_000))
    n_metric_cols = n_columns - 2  # subtract id and key

    base = spark.range(n_rows, numPartitions=n_partitions).withColumn(
        "key", (F.col("id") % 1_000).cast(StringType())
    )

    # Build all metric column expressions in one select to avoid
    # deeply nested logical plans (SPL-D10-005 anti-pattern).
    metric_exprs = [
        (F.rand(seed + i) * 1_000).cast(DoubleType()).alias(f"col_{i:03d}")
        for i in range(1, n_metric_cols + 1)
    ]
    return base.select(F.col("id"), F.col("key"), *metric_exprs)


def generate_nested_schema_data(
    spark: SparkSession,
    n_rows: int = 1_000_000,
    nesting_depth: int = 5,
    seed: int = _DEFAULT_SEED,
) -> DataFrame:
    """Generate a DataFrame with a deeply nested struct hierarchy.

    Deeply nested schemas arise from JSON ingestion or ORC/Parquet files that
    encode complex events.  They stress ``explode()`` without subsequent filter
    (SPL-D10-004), field-path access, and schema-flattening patterns.

    The struct hierarchy is::

        data : StructType(
            value  : DOUBLE,
            tag    : STRING,
            nested : StructType(
                value  : DOUBLE,
                tag    : STRING,
                nested : StructType(
                    …   (nesting_depth − 1 more levels)
                    nested : StructType(  ← leaf
                        leaf_float : DOUBLE,
                        leaf_int   : INT,
                        leaf_str   : STRING,
                    )
                )
            )
        )

    Schema (top level)::

        id     LONG        -- sequential row id
        key    STRING      -- low-cardinality group key
        data   StructType  -- nested struct to *nesting_depth* levels

    Args:
        spark:         Active ``SparkSession``.
        n_rows:        Total row count.
        nesting_depth: Number of struct nesting levels (minimum 1).
                       ``depth=1`` gives one outer struct containing a leaf struct.
        seed:          Random seed (not currently used; struct values are
                       deterministic hash functions of ``id``).

    Returns:
        A ``DataFrame`` with a nested struct column.

    Raises:
        ValueError: If *nesting_depth* < 1.

    Example::

        df = generate_nested_schema_data(spark, n_rows=100_000, nesting_depth=3)
        # Access leaf: df.select("data.nested.nested.leaf_float")
        # Flatten:     df.select(F.col("data.*"))
    """
    if nesting_depth < 1:
        raise ValueError(f"nesting_depth must be >= 1, got {nesting_depth}")

    n_partitions = max(5, min(100, n_rows // 50_000))

    return (
        spark.range(n_rows, numPartitions=n_partitions)
        .withColumn("key", (F.col("id") % 100).cast(StringType()))
        .withColumn("data", _build_nested_struct(nesting_depth, seed=seed))
    )
