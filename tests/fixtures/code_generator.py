"""Synthetic PySpark code generator for stress testing spark-perf-lint.

Public API
----------
generate_spark_file(...)          -> str
generate_multi_file_project(...)  -> dict[str, str]
ALL_ANTI_PATTERNS                 -> frozenset[str]

All generated PySpark files:
- Parse cleanly with ``ast.parse`` (syntactically valid Python).
- Import ``pyspark.sql`` so the scanner detects them as Spark files.
- Contain realistic-looking ETL structure (load → transform → write).

Anti-pattern injection inserts the exact code signatures that the
spark-perf-lint rules look for, so generated files can be used as
positive-case inputs in stress and fuzz tests.
"""

from __future__ import annotations

__all__ = [
    "ALL_ANTI_PATTERNS",
    "generate_spark_file",
    "generate_multi_file_project",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: The complete set of injectable anti-pattern names.
ALL_ANTI_PATTERNS: frozenset[str] = frozenset(
    [
        "aqe_disabled",  # spark.sql.adaptive.enabled = false  [D08]
        "broadcast_disabled",  # autoBroadcastJoinThreshold = -1     [D03]
        "cache_no_unpersist",  # .cache() without .unpersist()       [D06]
        "collect_no_filter",  # .collect() on unfiltered DF         [D09]
        "cross_join",  # .crossJoin(...)                     [D03]
        "csv_read",  # spark.read.csv(...)                 [D07]
        "groupByKey",  # rdd.groupByKey()                    [D02]
        "infer_schema",  # .option("inferSchema", "true")      [D07]
        "jdbc_no_partitions",  # jdbc read without partitionColumn   [D07]
        "python_udf",  # @F.udf(...)                        [D09]
        "repartition_one",  # .repartition(1)                    [D04]
        "select_star",  # .select("*")                       [D10]
        "show_in_prod",  # .show()                            [D11]
        "toPandas_no_limit",  # .toPandas() without .limit()       [D09]
        "withColumn_loop",  # for col in cols: df.withColumn(...) [D09]
    ]
)

# Default SparkSession config used when spark_config is not supplied.
_DEFAULT_SPARK_CONFIG: dict[str, str] = {
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.executor.memory": "4g",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
}


# ---------------------------------------------------------------------------
# Internal builder
# ---------------------------------------------------------------------------


class _SparkFileBuilder:
    """Accumulates source lines and builds a synthetic PySpark file.

    All state is held in ``self._lines``.  Sections are appended in order;
    ``build()`` joins them and returns the final string.
    """

    def __init__(
        self,
        n_lines: int,
        n_joins: int,
        n_group_bys: int,
        n_caches: int,
        n_udfs: int,
        n_writes: int,
        inject_anti_patterns: list[str],
        spark_config: dict[str, str],
    ) -> None:
        self._n_lines = n_lines
        self._n_joins = n_joins
        self._n_group_bys = n_group_bys
        self._n_caches = n_caches
        self._n_udfs = n_udfs
        self._n_writes = n_writes
        self._anti: set[str] = set(inject_anti_patterns)
        self._config: dict[str, str] = spark_config
        self._lines: list[str] = []
        # Track generated variable names for cross-section references.
        self._table_vars: list[str] = []
        self._udf_names: list[str] = []
        self._cached_vars: list[str] = []  # vars that need unpersist

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _add(self, line: str = "") -> None:
        self._lines.append(line)

    def _sep(self) -> None:
        """Add a section separator comment block."""
        self._add("# " + "-" * 75)

    # ------------------------------------------------------------------
    # Section: header (docstring + imports)
    # ------------------------------------------------------------------

    def _build_header(self) -> None:
        self._add('"""Generated PySpark ETL job — synthetic test fixture."""')
        self._add()
        self._add("import os")
        self._add("import logging")
        self._add("from pyspark.sql import SparkSession")
        self._add("from pyspark.sql import functions as F")
        if self._n_udfs > 0 or "python_udf" in self._anti:
            self._add("from pyspark.sql.types import StringType, DoubleType")
        self._add()
        self._add("logger = logging.getLogger(__name__)")
        self._add()

    # ------------------------------------------------------------------
    # Section: SparkSession
    # ------------------------------------------------------------------

    def _build_session(self) -> None:
        self._sep()
        self._add("# SparkSession")
        self._sep()
        self._add()
        self._add("spark = (")
        self._add("    SparkSession.builder")
        self._add('    .appName("generated_etl_job")')

        # Config-level anti-patterns go into the builder chain.
        if "aqe_disabled" in self._anti:
            self._add('    .config("spark.sql.adaptive.enabled", "false")')
        elif "spark.sql.adaptive.enabled" not in self._config:
            self._add('    .config("spark.sql.adaptive.enabled", "true")')

        if "broadcast_disabled" in self._anti:
            self._add('    .config("spark.sql.autoBroadcastJoinThreshold", "-1")')

        for key, val in self._config.items():
            # Skip adaptive.enabled if we already handled it via anti-pattern.
            if key == "spark.sql.adaptive.enabled" and "aqe_disabled" in self._anti:
                continue
            self._add(f'    .config("{key}", "{val}")')

        self._add("    .getOrCreate()")
        self._add(")")
        self._add()

    # ------------------------------------------------------------------
    # Section: data loading
    # ------------------------------------------------------------------

    def _build_table_loads(self) -> None:
        self._sep()
        self._add("# Load source tables")
        self._sep()
        self._add()

        # JDBC without partition params
        if "jdbc_no_partitions" in self._anti:
            self._add("jdbc_src = (")
            self._add("    spark.read")
            self._add('    .format("jdbc")')
            self._add('    .option("url", "jdbc:postgresql://localhost/app")')
            self._add('    .option("dbtable", "public.events")')
            self._add('    .option("user", os.environ.get("DB_USER", "spark"))')
            self._add("    .load()")
            self._add(")")
            self._table_vars.append("jdbc_src")
            self._add()

        # CSV read (with or without inferSchema)
        if "csv_read" in self._anti and "infer_schema" in self._anti:
            self._add("csv_src = (")
            self._add("    spark.read")
            self._add('    .option("inferSchema", "true")')
            self._add('    .option("header", "true")')
            self._add('    .csv("/data/source.csv")')
            self._add(")")
            self._table_vars.append("csv_src")
            self._add()
        elif "csv_read" in self._anti:
            self._add('csv_src = spark.read.option("header", "true").csv("/data/source.csv")')
            self._table_vars.append("csv_src")
            self._add()
        elif "infer_schema" in self._anti:
            self._add("infer_src = (")
            self._add("    spark.read")
            self._add('    .option("inferSchema", "true")')
            self._add('    .option("header", "true")')
            self._add('    .csv("/data/infer_src.csv")')
            self._add(")")
            self._table_vars.append("infer_src")
            self._add()

        # Parquet tables — enough for all joins
        n_parquet = max(2, self._n_joins + 1)
        for i in range(n_parquet):
            var = f"table_{i}"
            self._add(f'table_{i} = spark.read.parquet("/data/table_{i}")')
            self._table_vars.append(var)
        self._add()

    # ------------------------------------------------------------------
    # Section: UDF definitions
    # ------------------------------------------------------------------

    def _build_udfs(self) -> None:
        n = max(self._n_udfs, 1 if "python_udf" in self._anti else 0)
        if n == 0:
            return

        self._sep()
        self._add("# Python UDF definitions")
        self._sep()
        self._add()

        for i in range(n):
            name = f"udf_transform_{i}"
            self._udf_names.append(name)
            self._add("@F.udf(StringType())")
            self._add(f"def {name}(value: str) -> str:")
            self._add(f'    """Synthetic UDF {i} — opaque to Catalyst."""')
            self._add("    if value is None:")
            self._add('        return "unknown"')
            self._add('    return f"transformed_{value}"')
            self._add()

    # ------------------------------------------------------------------
    # Section: transformation pipeline
    # ------------------------------------------------------------------

    def _build_transforms(self) -> None:
        self._sep()
        self._add("# Transformation pipeline")
        self._sep()
        self._add()

        # Base DataFrame
        base = self._table_vars[0] if self._table_vars else "table_0"
        self._add(f"result = {base}")
        self._add()

        # Joins
        rhs_pool = [v for v in self._table_vars if v != base]
        for i in range(self._n_joins):
            rhs = rhs_pool[i] if i < len(rhs_pool) else f"table_{i + 1}"
            self._add(f'result = result.join({rhs}, on="id_{i}", how="left")')
        if self._n_joins > 0:
            self._add()

        # Cross join anti-pattern
        if "cross_join" in self._anti:
            rhs = rhs_pool[0] if rhs_pool else "table_1"
            self._add("# Anti-pattern: cartesian cross join")
            self._add(f"cross_result = result.crossJoin({rhs})")
            self._add()

        # Apply UDFs
        for udf_name in self._udf_names:
            self._add(f'result = result.withColumn("udf_out", {udf_name}(F.col("value")))')
        if self._udf_names:
            self._add()

        # withColumn loop anti-pattern vs normal transformation
        if "withColumn_loop" in self._anti:
            self._add("# Anti-pattern: withColumn inside a loop builds N separate plan nodes")
            self._add('_feature_cols = ["feat_a", "feat_b", "feat_c", "feat_d", "feat_e"]')
            self._add("for _col in _feature_cols:")
            self._add('    result = result.withColumn(_col, F.lit(0).cast("double"))')
            self._add()
        else:
            self._add(
                'result = result.withColumn("status_flag", '
                'F.when(F.col("status") == "active", 1).otherwise(0))'
            )
            self._add()

        # select("*") anti-pattern
        if "select_star" in self._anti:
            self._add('# Anti-pattern: select("*") prevents column pruning')
            self._add('result = result.select("*")')
            self._add()

        # Caches — track which ones need unpersist
        for i in range(self._n_caches):
            cache_var = f"cached_{i}"
            self._add(f"# Cache subset {i} for reuse")
            self._add(f'{cache_var} = result.filter(F.col("partition_id") == {i})')
            self._add(f"{cache_var}.cache()")
            # Only skip unpersist for the FIRST cache when testing the anti-pattern.
            if "cache_no_unpersist" in self._anti and i == 0:
                self._add("# (no unpersist — anti-pattern: cache_no_unpersist)")
            else:
                self._cached_vars.append(cache_var)
            self._add()

        # GroupBys
        for i in range(self._n_group_bys):
            self._add(f"agg_{i} = (")
            self._add("    result")
            self._add(f'    .groupBy("category_{i}", "region")')
            self._add("    .agg(")
            self._add(f'        F.count("*").alias("row_count_{i}"),')
            self._add(f'        F.sum("revenue").alias("total_revenue_{i}"),')
            self._add("    )")
            self._add(")")
            self._add()

        # RDD groupByKey anti-pattern
        if "groupByKey" in self._anti:
            self._add("# Anti-pattern: RDD.groupByKey shuffles all values before aggregating")
            self._add(
                "_rdd_counts = result.rdd"
                '.map(lambda r: (r["category"], r["revenue"]))'
                ".groupByKey()"
                ".mapValues(list)"
            )
            self._add()

        # repartition(1) anti-pattern
        if "repartition_one" in self._anti:
            self._add("# Anti-pattern: repartition(1) collapses data to a single task")
            self._add("result = result.repartition(1)")
            self._add()

        # collect() without filter anti-pattern
        if "collect_no_filter" in self._anti:
            self._add("# Anti-pattern: collect() on an unfiltered DataFrame")
            self._add("_all_rows = result.collect()")
            self._add()

        # toPandas() without limit anti-pattern
        if "toPandas_no_limit" in self._anti:
            self._add("# Anti-pattern: toPandas() without a preceding .limit()")
            self._add("_pandas_df = result.toPandas()")
            self._add()

        # show() in production code anti-pattern
        if "show_in_prod" in self._anti:
            self._add("# Anti-pattern: df.show() in production code")
            self._add("result.show()")
            self._add()

        # Unpersist all cached vars that are NOT part of the no-unpersist test.
        for cv in self._cached_vars:
            self._add(f"{cv}.unpersist()")
        if self._cached_vars:
            self._add()

    # ------------------------------------------------------------------
    # Section: write actions
    # ------------------------------------------------------------------

    def _build_writes(self) -> None:
        if self._n_writes == 0:
            return
        self._sep()
        self._add("# Write outputs")
        self._sep()
        self._add()
        for i in range(self._n_writes):
            # Prefer writing aggregation results; fall back to result.
            source_var = f"agg_{i}" if i < self._n_group_bys else "result"
            self._add(f'{source_var}.write.mode("overwrite").parquet("/output/out_{i}")')
        self._add()

    # ------------------------------------------------------------------
    # Padding to reach target line count
    # ------------------------------------------------------------------

    def _pad_to_n_lines(self) -> None:
        """Append filler lines until ``len(self._lines) >= self._n_lines``."""
        if len(self._lines) >= self._n_lines:
            return
        self._sep()
        self._add("# Additional normalisation stages (padding)")
        self._sep()
        self._add()
        pad_idx = 0
        while len(self._lines) < self._n_lines - 1:
            self._add(f"# Padding stage {pad_idx}")
            self._add(
                f"_pad_{pad_idx} = result"
                f'.withColumn("norm_{pad_idx}", F.col("value").cast("double"))'
            )
            pad_idx += 1
        self._add()  # trailing newline

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def build(self) -> str:
        self._build_header()
        self._build_session()
        self._build_table_loads()
        self._build_udfs()
        self._build_transforms()
        self._build_writes()
        self._pad_to_n_lines()
        return "\n".join(self._lines) + "\n"


# ---------------------------------------------------------------------------
# Plain-Python helper (non-Spark files for multi-file project)
# ---------------------------------------------------------------------------


def _generate_plain_python(index: int) -> str:
    """Return a plain Python utility module with no Spark imports."""
    return f'''\
"""Utility module {index} — no Spark dependency."""

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def load_config_{index}(path: str) -> dict[str, Any]:
    """Load a JSON configuration file."""
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def process_batch_{index}(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply light transformation to a batch of records."""
    return [{{**item, "processed_{index}": True}} for item in items if item.get("active")]


def write_results_{index}(data: list[dict[str, Any]], output_dir: str) -> None:
    """Persist processed records to *output_dir*."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    target = out / f"batch_{index}.json"
    with target.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    logger.info("Wrote %d records to %s", len(data), target)


if __name__ == "__main__":
    cfg_path = os.environ.get("CONFIG_PATH_{index}", "config.json")
    cfg = load_config_{index}(cfg_path)
    results = process_batch_{index}(cfg.get("items", []))
    write_results_{index}(results, cfg.get("output_dir", "/tmp/output_{index}"))
'''


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_spark_file(
    n_lines: int = 50,
    n_joins: int = 1,
    n_group_bys: int = 1,
    n_caches: int = 0,
    n_udfs: int = 0,
    n_writes: int = 1,
    inject_anti_patterns: list[str] | None = None,
    spark_config: dict[str, str] | None = None,
) -> str:
    """Generate a valid PySpark source file string.

    The returned string:

    * Parses without error under ``ast.parse``.
    * Imports ``pyspark.sql`` so the scanner detects it as a Spark file.
    * Contains exactly the requested structural elements (joins, aggregations,
      etc.) plus any injected anti-patterns.

    Args:
        n_lines: Minimum number of source lines.  The generator pads with
            synthetic normalisation stages until this threshold is reached.
            If the natural content already exceeds *n_lines*, no truncation
            occurs.
        n_joins: Number of ``DataFrame.join()`` calls to emit.
        n_group_bys: Number of ``DataFrame.groupBy().agg()`` blocks to emit.
        n_caches: Number of ``DataFrame.cache()`` calls (with matching
            ``unpersist()`` unless ``"cache_no_unpersist"`` is injected).
        n_udfs: Number of ``@F.udf`` definitions to include.  At least one
            is added automatically when ``"python_udf"`` is in
            *inject_anti_patterns*.
        n_writes: Number of ``DataFrame.write.parquet()`` calls to emit.
        inject_anti_patterns: Anti-pattern names to inject.  Every name must
            be a member of ``ALL_ANTI_PATTERNS``; an unknown name raises
            ``ValueError``.  ``None`` or an empty list produces clean code.
        spark_config: Extra ``SparkSession`` config entries.  Merged with
            the built-in defaults; caller-supplied values take precedence.
            Pass an empty dict ``{}`` to use no defaults.

    Returns:
        A syntactically-valid Python source string.

    Raises:
        ValueError: If *inject_anti_patterns* contains an unknown name.

    Examples:
        >>> code = generate_spark_file(n_joins=2, n_group_bys=1)
        >>> import ast; ast.parse(code)  # no exception

        >>> code = generate_spark_file(inject_anti_patterns=["cross_join"])
        >>> "crossJoin" in code
        True
    """
    anti = list(inject_anti_patterns or [])
    unknown = set(anti) - ALL_ANTI_PATTERNS
    if unknown:
        raise ValueError(
            f"Unknown anti-pattern(s): {sorted(unknown)!r}.  "
            f"Valid names: {sorted(ALL_ANTI_PATTERNS)!r}"
        )

    # Merge caller config over defaults.
    config: dict[str, str]
    if spark_config is None:
        config = dict(_DEFAULT_SPARK_CONFIG)
    else:
        config = spark_config

    builder = _SparkFileBuilder(
        n_lines=n_lines,
        n_joins=n_joins,
        n_group_bys=n_group_bys,
        n_caches=n_caches,
        n_udfs=n_udfs,
        n_writes=n_writes,
        inject_anti_patterns=anti,
        spark_config=config,
    )
    return builder.build()


def generate_multi_file_project(
    n_files: int = 10,
    spark_files_ratio: float = 0.6,
    anti_patterns_per_file: list[str] | None = None,
) -> dict[str, str]:
    """Generate a dict mapping filename to source code for a synthetic project.

    The project contains a mix of PySpark ETL scripts and plain Python
    utility modules.  The ratio of Spark files is controlled by
    *spark_files_ratio*.

    Args:
        n_files: Total number of files to generate.  Must be >= 1.
        spark_files_ratio: Fraction of files that should be PySpark files.
            ``0.0`` → all plain Python; ``1.0`` → all Spark.
            The count is rounded to the nearest integer.
        anti_patterns_per_file: Anti-pattern names injected into every
            generated Spark file.  ``None`` or ``[]`` produces clean files.

    Returns:
        ``dict[str, str]`` where keys are filenames like ``"spark_job_000.py"``
        or ``"utils_000.py"`` and values are the source code strings.

    Raises:
        ValueError: If *n_files* < 1 or *spark_files_ratio* is outside [0, 1].
    """
    if n_files < 1:
        raise ValueError(f"n_files must be >= 1, got {n_files}")
    if not (0.0 <= spark_files_ratio <= 1.0):
        raise ValueError(f"spark_files_ratio must be in [0.0, 1.0], got {spark_files_ratio}")

    anti = list(anti_patterns_per_file or [])
    n_spark = round(n_files * spark_files_ratio)
    n_plain = n_files - n_spark

    result: dict[str, str] = {}

    for i in range(n_spark):
        fname = f"spark_job_{i:03d}.py"
        result[fname] = generate_spark_file(
            n_lines=30,
            n_joins=1,
            n_group_bys=1,
            n_writes=1,
            inject_anti_patterns=anti,
        )

    for i in range(n_plain):
        fname = f"utils_{i:03d}.py"
        result[fname] = _generate_plain_python(i)

    return result
