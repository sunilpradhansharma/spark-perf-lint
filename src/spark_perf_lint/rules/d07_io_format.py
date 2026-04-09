"""D07 — I/O and file format rules for spark-perf-lint.

Poor I/O choices are among the most impactful Spark performance issues: reading
a row-oriented format for analytical queries, inferring schema at runtime, or
loading all columns when only a handful are needed can each add an order of
magnitude to job runtime.  These rules detect static I/O anti-patterns that
can be identified without running Spark.

Rule IDs: SPL-D07-001 through SPL-D07-010
"""

from __future__ import annotations

import ast

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.base import CodeRule
from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.types import Dimension, EffortLevel, Finding, Severity

_DIM = Dimension.D07_IO_FORMAT

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Row-oriented formats that are inefficient for analytical (columnar) workloads
_ROW_FORMATS: frozenset[str] = frozenset({"csv", "json", "text"})

# Columnar / efficient formats
_ANALYTICAL_FORMATS: frozenset[str] = frozenset({"parquet", "orc", "delta", "avro"})

# Parquet-family formats where compression is relevant
_PARQUET_FORMATS: frozenset[str] = frozenset({"parquet", "delta"})

# Compression-related option keys
_COMPRESSION_KEYS: frozenset[str] = frozenset({"compression", "codec"})

# Write-chain markers
_WRITE_MARKERS: frozenset[str] = frozenset({"write", "writeStream"})

# JDBC partition parameter names (as kwargs on jdbc() and as .option() keys)
_JDBC_PARTITION_KWARGS: frozenset[str] = frozenset(
    {"column", "partitionColumn", "numPartitions", "lowerBound", "upperBound"}
)
_JDBC_PARTITION_OPTION_KEYS: frozenset[str] = frozenset(
    {"partitionColumn", "numPartitions", "lowerBound", "upperBound"}
)

# Spark config key for global parquet schema merging
_MERGE_SCHEMA_CONFIG = "spark.sql.parquet.mergeSchema"

# Spark config key for parquet compression codec
_PARQUET_COMPRESSION_CONFIG = "spark.sql.parquet.compression.codec"


# =============================================================================
# SPL-D07-001 — CSV/JSON used for analytical workload
# =============================================================================


@register_rule
class CsvJsonAnalyticalRule(CodeRule):
    """SPL-D07-001: Reading or writing CSV/JSON for an analytical workload."""

    rule_id = "SPL-D07-001"
    name = "CSV/JSON used for analytical workload"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "CSV/JSON are row-oriented formats with no column pruning or predicate pushdown;"
        " use Parquet or ORC for analytical workloads."
    )
    explanation = (
        "CSV and JSON are row-oriented text formats designed for data exchange, not "
        "analytical query performance.  They have three major disadvantages compared to "
        "columnar formats (Parquet, ORC):\n\n"
        "• **No column pruning**: Spark must read and decode every column even when "
        "  only a few are needed.\n"
        "• **No predicate pushdown**: Filters cannot be evaluated at the storage layer "
        "  — every row must be decoded before filtering.\n"
        "• **No compression statistics**: Column-level min/max statistics used by "
        "  Parquet/ORC for partition elimination are absent.\n\n"
        "For large analytical datasets, switching from CSV to Parquet typically reduces "
        "scan time by 5–20× and storage size by 3–10×."
    )
    recommendation_template = (
        "Convert the dataset to Parquet or ORC: "
        "``df.write.mode('overwrite').parquet('path')`` and read with "
        "``spark.read.parquet('path')``. "
        "For streaming ingestion, use Delta Lake for ACID + columnar benefits."
    )
    before_example = "df = spark.read.csv('data/', header=True, inferSchema=True)"
    after_example = "df = spark.read.parquet('data.parquet')"
    references = [
        "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html",
    ]
    estimated_impact = "5–20× slower scans vs Parquet; no column pruning or predicate pushdown"
    effort_level = EffortLevel.MAJOR_REFACTOR

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for io in analyzer.find_spark_read_writes():
            if io.format in _ROW_FORMATS:
                op = io.operation
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        io.line,
                        f"{io.format!r} is a row-oriented format unsuitable for analytical"
                        f" {op}s — consider Parquet or ORC",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D07-002 — Schema inference enabled
# =============================================================================


@register_rule
class SchemaInferenceRule(CodeRule):
    """SPL-D07-002: inferSchema=True triggers an extra full scan of the input data."""

    rule_id = "SPL-D07-002"
    name = "Schema inference enabled"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "inferSchema=True reads the entire dataset twice — once to infer types,"
        " once for the actual query."
    )
    explanation = (
        "When ``inferSchema=true`` (or ``inferSchema=True`` in Python), Spark performs "
        "a full pass over the input data to detect column types before running the actual "
        "query.  This doubles I/O for every job execution.\n\n"
        "Beyond performance, inferred schemas are fragile: a single malformed row can "
        "change the detected type for a column, causing downstream type errors that are "
        "difficult to reproduce.\n\n"
        "Best practice: define the schema explicitly using ``StructType`` / ``StructField`` "
        "or a DDL string.  This eliminates the inference scan, makes type expectations "
        "explicit, and catches schema drift at read time."
    )
    recommendation_template = (
        "Define the schema explicitly: "
        "``schema = 'id LONG, name STRING, ts TIMESTAMP'`` "
        "and pass it to the reader: ``spark.read.schema(schema).csv('path')``."
    )
    before_example = "df = spark.read.option('inferSchema', 'true').csv('path')"
    after_example = (
        "schema = 'id LONG, name STRING, amount DOUBLE'\n"
        "df = spark.read.schema(schema).csv('path')"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-data-sources-csv.html",
    ]
    estimated_impact = "2× I/O per job; unstable schemas cause intermittent type errors"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for io in analyzer.find_reads():
            if io.infer_schema:
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        io.line,
                        "inferSchema=true doubles I/O for schema detection"
                        " — define the schema explicitly with StructType or DDL string",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D07-003 — select("*") or no column pruning
# =============================================================================


@register_rule
class SelectStarRule(CodeRule):
    """SPL-D07-003: select('*') reads all columns; prefer explicit column lists."""

    rule_id = "SPL-D07-003"
    name = 'select("*") — no column pruning'
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        'select("*") disables column pruning — all columns are decoded'
        " even when only a few are needed."
    )
    explanation = (
        "``select('*')`` is a no-op transformation that reads and retains every column "
        "in the DataFrame.  For columnar formats (Parquet, ORC) this defeats column "
        "pruning: the storage layer must deserialise every column even if only two or "
        "three are used downstream.\n\n"
        "Column pruning is one of Spark's most effective I/O optimisations: "
        "a 100-column Parquet table that only needs 5 columns can read 20× less data "
        "with explicit column selection.  ``select('*')`` prevents this.\n\n"
        "Replace ``select('*')`` with an explicit list of the columns actually needed "
        "downstream.  If all columns genuinely are needed, remove the ``select('*')`` "
        "call entirely — it is a no-op and adds plan complexity."
    )
    recommendation_template = (
        "Replace ``select('*')`` with explicit columns: "
        "``df.select('id', 'name', 'ts')``. "
        "If all columns are needed, omit the select call."
    )
    before_example = "df.select('*').groupBy('status').count()"
    after_example = "df.select('status').groupBy('status').count()"
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html",
    ]
    estimated_impact = "All columns decoded from storage; no columnar pruning benefit"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("select"):
            has_star = any(
                isinstance(arg, ast.Constant) and arg.value == "*"
                for arg in call.args
            )
            if has_star:
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        call.line,
                        "select('*') reads all columns — replace with an explicit"
                        " column list to enable column pruning",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D07-004 — Missing predicate pushdown opportunity
# =============================================================================


@register_rule
class PostJoinFilterRule(CodeRule):
    """SPL-D07-004: filter()/where() applied after join(); filter should precede the join."""

    rule_id = "SPL-D07-004"
    name = "Filter applied after join — missing predicate pushdown"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "filter()/where() applied after join() processes more data in the shuffle"
        " — push the filter to before the join to reduce shuffle volume."
    )
    explanation = (
        "Applying a filter *after* a join is correct but suboptimal: the join "
        "shuffles and processes all rows from both sides before the filter discards "
        "some of them.  If the filter is based on a column from one of the join inputs "
        "(not a computed column), applying it *before* the join reduces the shuffle "
        "data volume proportionally.\n\n"
        "Example: ``df.join(other, 'id').filter('status = \"active\"')`` — if "
        "``status`` is a column in ``df``, filtering ``df`` to active rows before the "
        "join sends fewer rows through the shuffle.\n\n"
        "Catalyst's predicate pushdown optimiser handles this automatically for simple "
        "cases, but UDF-based filters and complex expressions are opaque to Catalyst and "
        "will not be pushed down."
    )
    recommendation_template = (
        "Apply filter()/where() to each join input before the join: "
        "``df.filter('status = \"active\"').join(other.filter(...), 'id')``."
    )
    before_example = "df.join(other, 'id').filter('status = \"active\"').groupBy('x').count()"
    after_example = "df.filter('status = \"active\"').join(other, 'id').groupBy('x').count()"
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html",
    ]
    estimated_impact = "Shuffle includes rows that will be discarded; wasted network I/O"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for method in ("filter", "where"):
            for call in analyzer.find_method_calls(method):
                # chain direction: root → this call; join before filter = join in chain
                if "join" in call.chain:
                    findings.append(
                        self.create_finding(
                            analyzer.filename,
                            call.line,
                            f"{method}() applied after join()"
                            " — push the filter before join() to reduce shuffle volume",
                            config=config,
                        )
                    )
        return findings


# =============================================================================
# SPL-D07-005 — Small file problem on write
# =============================================================================


@register_rule
class SmallFileWriteRule(CodeRule):
    """SPL-D07-005: partitionBy() on write without prior coalesce/repartition."""

    rule_id = "SPL-D07-005"
    name = "Small file problem on write"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "Writing with partitionBy() without coalesce()/repartition() may produce"
        " many small files — one per (shuffle partition × partition value)."
    )
    explanation = (
        "``write.partitionBy('date').parquet(path)`` writes one file per Spark "
        "partition per unique ``date`` value.  With Spark's default of 200 shuffle "
        "partitions, a dataset spanning 365 days can produce up to "
        "200 × 365 = 73 000 files.  These tiny files degrade Spark read performance "
        "(each file requires a separate HDFS RPC / S3 request), overwhelm Hive Metastore "
        "with partition listing operations, and cause driver OOM during planning.\n\n"
        "The fix is to ``coalesce()`` or ``repartition()`` before writing so that each "
        "partition directory contains a bounded number of files:\n\n"
        "``df.coalesce(N).write.partitionBy('date').parquet(path)``\n\n"
        "``N`` should be chosen so that each output file is 128–256 MB."
    )
    recommendation_template = (
        "Add coalesce() or repartition() before writing: "
        "``df.coalesce(N).write.partitionBy('date').parquet(path)``. "
        "Target output files of 128–256 MB each."
    )
    before_example = "df.write.partitionBy('date').parquet('s3://bucket/data')"
    after_example = "df.coalesce(N).write.partitionBy('date').parquet('s3://bucket/data')"
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html",
    ]
    estimated_impact = (
        "O(partitions × partition_values) small files; slow reads and metastore overload"
    )
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for io in analyzer.find_writes():
            if not io.partitioned_by:
                continue  # no partitionBy → different problem
            # Find the terminal write call at this line to inspect the chain
            write_calls = [
                c
                for c in analyzer.find_all_method_calls()
                if c.line == io.line and c.method_name in {
                    "parquet", "orc", "csv", "json", "avro", "save", "saveAsTable", "delta"
                }
            ]
            if not write_calls:
                continue
            chain = write_calls[0].chain
            if "coalesce" not in chain and "repartition" not in chain:
                cols = ", ".join(f"'{c}'" for c in io.partitioned_by)
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        io.line,
                        f"partitionBy({cols}) without coalesce()/repartition()"
                        " — may produce many small files",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D07-006 — JDBC read without partition parameters
# =============================================================================


@register_rule
class JdbcWithoutPartitionsRule(CodeRule):
    """SPL-D07-006: JDBC read without partition parameters uses a single driver thread."""

    rule_id = "SPL-D07-006"
    name = "JDBC read without partition parameters"
    dimension = _DIM
    default_severity = Severity.CRITICAL
    description = (
        "Reading from JDBC without partitionColumn/numPartitions serialises the entire"
        " table through a single driver thread."
    )
    explanation = (
        "By default a Spark JDBC read creates a **single** partition and reads the "
        "entire database table through one JDBC connection on the driver.  This is "
        "effectively sequential I/O and does not scale — a 1 B row table will block "
        "the driver for hours.\n\n"
        "Parallelism is enabled by providing three parameters:\n\n"
        "• ``partitionColumn``: an integer/long/date column to split on.\n"
        "• ``lowerBound`` / ``upperBound``: the range of that column (can be approximate).\n"
        "• ``numPartitions``: number of parallel reader tasks.\n\n"
        "With these set, Spark creates ``numPartitions`` JDBC tasks, each reading a "
        "``[lower, upper]`` range slice in parallel, turning a sequential read into a "
        "distributed one.\n\n"
        "Alternative: use ``dbtable`` with a sub-query that includes a ``TABLESAMPLE`` "
        "or partition-based predicate, or use a custom ``predicates`` list."
    )
    recommendation_template = (
        "Add partition parameters: "
        "``spark.read.jdbc(url, table, column='id', "
        "lowerBound=0, upperBound=1_000_000, numPartitions=20)``."
    )
    before_example = "df = spark.read.jdbc(url, 'orders')"
    after_example = (
        "df = spark.read.jdbc(\n"
        "    url, 'orders',\n"
        "    column='order_id',\n"
        "    lowerBound=0,\n"
        "    upperBound=10_000_000,\n"
        "    numPartitions=20,\n"
        ")"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html",
    ]
    estimated_impact = "Sequential single-thread read; does not scale to large JDBC tables"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []

        # Pattern 1: spark.read.jdbc(url, table, [kwargs...])
        # Track every direct jdbc() call line so Pattern 2 skips them entirely.
        jdbc_call_lines: set[int] = set()
        for call in analyzer.find_method_calls("jdbc"):
            jdbc_call_lines.add(call.line)
            if any(k in call.kwargs for k in _JDBC_PARTITION_KWARGS):
                continue
            # 4+ positional args = predicates list form — skip
            if len(call.args) >= 4:
                continue
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    "jdbc() read without partition parameters"
                    " — all data flows through a single driver thread",
                    config=config,
                )
            )

        # Pattern 2: spark.read.format("jdbc").option(...).load()
        for io in analyzer.find_reads():
            if io.format != "jdbc":
                continue
            if io.line in jdbc_call_lines:
                continue  # already handled by Pattern 1
            if any(k in io.options for k in _JDBC_PARTITION_OPTION_KEYS):
                continue
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    io.line,
                    "JDBC read without partition parameters"
                    " — all data flows through a single driver thread",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D07-007 — Parquet compression not set
# =============================================================================


@register_rule
class ParquetCompressionNotSetRule(CodeRule):
    """SPL-D07-007: Writing Parquet without an explicit compression codec."""

    rule_id = "SPL-D07-007"
    name = "Parquet compression not set"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "Writing Parquet without explicit compression relies on the cluster default;"
        " set compression to snappy or zstd explicitly."
    )
    explanation = (
        "Parquet's default compression codec depends on the Spark version and cluster "
        "configuration (``spark.sql.parquet.compression.codec``).  In older versions "
        "the default was ``uncompressed``; in Spark 2.0+ it is ``snappy``.  Relying on "
        "the default makes job behaviour cluster-dependent.\n\n"
        "Recommendations by use case:\n\n"
        "• **Snappy**: fast compression/decompression, moderate ratio — good default "
        "  for hot data accessed frequently.\n"
        "• **Zstd**: better ratio than snappy with comparable speed — prefer for cold "
        "  data or when storage cost matters.\n"
        "• **Gzip/brotli**: best ratio but slower — use for archival.\n\n"
        "Set it explicitly on the write call so the codec is visible in code review."
    )
    recommendation_template = (
        "Add a compression option: "
        "``df.write.option('compression', 'snappy').parquet('path')`` "
        "or ``df.write.option('compression', 'zstd').parquet('path')``."
    )
    before_example = "df.write.mode('overwrite').parquet('s3://bucket/data')"
    after_example = "df.write.mode('overwrite').option('compression', 'snappy').parquet('s3://bucket/data')"
    references = [
        "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html"
        "#configuration",
    ]
    estimated_impact = "Cluster-dependent codec; may write uncompressed on some environments"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        # Suppress if a global codec is set for the session
        if analyzer.get_config_value(_PARQUET_COMPRESSION_CONFIG):
            return []

        findings: list[Finding] = []
        for io in analyzer.find_writes():
            if io.format not in _PARQUET_FORMATS:
                continue
            if any(k in io.options for k in _COMPRESSION_KEYS):
                continue
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    io.line,
                    "Parquet write without explicit compression codec"
                    " — set option('compression', 'snappy') or 'zstd'",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D07-008 — Write mode not specified
# =============================================================================


@register_rule
class WriteModeNotSpecifiedRule(CodeRule):
    """SPL-D07-008: DataFrame write without .mode() defaults to 'error' if path exists."""

    rule_id = "SPL-D07-008"
    name = "Write mode not specified"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "write without .mode() defaults to 'error' and fails if the output path"
        " already exists — specify mode explicitly."
    )
    explanation = (
        "Spark's default write mode is ``'error'`` (or ``'errorifexists'``), which "
        "raises an ``AnalysisException`` if the output path already exists.  This "
        "catches accidental overwrites during development but is almost always wrong "
        "in production pipelines where the output is intended to be refreshed.\n\n"
        "Common production modes:\n\n"
        "• ``'overwrite'``: delete existing data and write new data — safest for "
        "  idempotent batch jobs.\n"
        "• ``'append'``: add data without deleting existing files — use for "
        "  incremental loads.\n"
        "• ``'ignore'``: skip the write if data already exists — idempotent re-runs.\n\n"
        "Always set ``.mode()`` explicitly so the write behaviour is self-documenting "
        "and does not depend on whether the path exists."
    )
    recommendation_template = (
        "Add ``.mode('overwrite')`` (or 'append' / 'ignore') before the format call: "
        "``df.write.mode('overwrite').parquet('path')``."
    )
    before_example = "df.write.parquet('s3://bucket/output')"
    after_example = "df.write.mode('overwrite').parquet('s3://bucket/output')"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.DataFrameWriter.mode.html",
    ]
    estimated_impact = "Job fails on re-run if output path exists; silent data loss on 'append'"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for io in analyzer.find_writes():
            if io.write_mode is None:
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        io.line,
                        "write without .mode() — defaults to 'error'; add"
                        " .mode('overwrite') or .mode('append')",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D07-009 — No format specified on read/write
# =============================================================================


@register_rule
class NoFormatSpecifiedRule(CodeRule):
    """SPL-D07-009: spark.read.load() / df.write.save() without .format() relies on default."""

    rule_id = "SPL-D07-009"
    name = "No format specified on read/write"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "read.load() / write.save() without .format() relies on the cluster default"
        " format — specify the format explicitly."
    )
    explanation = (
        "``spark.read.load('path')`` without ``.format(...)`` uses the format configured "
        "in ``spark.sql.sources.default`` (default: ``'parquet'``).  This makes the "
        "code's behaviour depend on the cluster configuration, which may differ between "
        "local development, staging, and production.\n\n"
        "Explicit format specification:\n\n"
        "• Makes the code self-documenting.\n"
        "• Prevents silent format mismatches when ``spark.sql.sources.default`` is "
        "  overridden by platform teams.\n"
        "• Enables IDE/linter format validation.\n\n"
        "Prefer format-specific methods (``spark.read.parquet()``, ``spark.read.csv()``) "
        "or the ``.format()`` + ``.load()`` pattern over bare ``.load()``."
    )
    recommendation_template = (
        "Use a format-specific method: ``spark.read.parquet('path')`` "
        "or ``spark.read.format('parquet').load('path')``."
    )
    before_example = "df = spark.read.load('path')"
    after_example = "df = spark.read.parquet('path')"
    references = [
        "https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html",
    ]
    estimated_impact = "Format depends on cluster config; silent mismatches across environments"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for io in analyzer.find_spark_read_writes():
            if io.format is None:
                op = io.operation
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        io.line,
                        f"{op} without explicit .format()"
                        " — relies on spark.sql.sources.default; specify format explicitly",
                        config=config,
                    )
                )
        return findings


# =============================================================================
# SPL-D07-010 — mergeSchema enabled without necessity
# =============================================================================


@register_rule
class MergeSchemaRule(CodeRule):
    """SPL-D07-010: mergeSchema=true reads every file's footer to merge schemas."""

    rule_id = "SPL-D07-010"
    name = "mergeSchema enabled without necessity"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "mergeSchema=true reads the footer of every Parquet file to reconcile schemas"
        " — O(files) overhead; disable unless schema evolution is required."
    )
    explanation = (
        "``mergeSchema=true`` (or ``spark.sql.parquet.mergeSchema=true``) instructs "
        "Spark to read the schema footer from every Parquet file in the dataset and "
        "merge them into a unified schema.  For datasets with thousands of files this "
        "is an O(files) metadata scan that occurs on every read — regardless of how "
        "few rows are actually processed.\n\n"
        "Schema merging is intended for schema-evolution scenarios where different "
        "files in the same table have different column sets (e.g., new columns added "
        "over time).  It is unnecessary when:\n\n"
        "• All files were written with the same schema.\n"
        "• The schema is explicitly provided via ``.schema()``.\n"
        "• The table is managed by a schema-aware format (Delta Lake handles this "
        "  automatically without mergeSchema).\n\n"
        "Disable ``mergeSchema`` and provide an explicit schema to eliminate the "
        "metadata scan."
    )
    recommendation_template = (
        "Remove mergeSchema=true and provide an explicit schema: "
        "``spark.read.schema(explicit_schema).parquet('path')``. "
        "If schema evolution is required, use Delta Lake instead."
    )
    before_example = "df = spark.read.option('mergeSchema', 'true').parquet('path')"
    after_example = "df = spark.read.schema(explicit_schema).parquet('path')"
    references = [
        "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html"
        "#schema-merging",
    ]
    estimated_impact = "O(files) metadata scan on every read; O(10 s) for tables with many files"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []

        # Per-read option: .option("mergeSchema", "true")
        for io in analyzer.find_reads():
            if io.options.get("mergeSchema", "").lower() == "true":
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        io.line,
                        "mergeSchema=true reads every file footer for schema reconciliation"
                        " — disable unless schema evolution is required",
                        config=config,
                    )
                )

        # Global config: spark.sql.parquet.mergeSchema = true
        merge_val = analyzer.get_config_value(_MERGE_SCHEMA_CONFIG)
        if merge_val is not None and merge_val.strip().lower() == "true":
            cfgs = [
                c
                for c in analyzer.find_spark_session_configs()
                if c.key == _MERGE_SCHEMA_CONFIG
            ]
            line = cfgs[-1].end_line if cfgs else 1
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    line,
                    f"{_MERGE_SCHEMA_CONFIG} = true enables global mergeSchema"
                    " — O(files) metadata scan on every Parquet read",
                    config=config,
                )
            )

        return findings
