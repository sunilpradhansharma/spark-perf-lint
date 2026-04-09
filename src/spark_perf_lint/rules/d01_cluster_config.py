"""D01 — Cluster Configuration rules for spark-perf-lint.

These rules analyse ``SparkSession.builder.config()`` and ``spark.conf.set()``
calls and flag common cluster configuration anti-patterns that degrade
performance or stability.

Rule IDs: SPL-D01-001 through SPL-D01-010
"""

from __future__ import annotations

import re

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.engine.pattern_matcher import PatternMatcher
from spark_perf_lint.rules.base import ConfigRule
from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.types import Dimension, EffortLevel, Finding, Severity

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DIM = Dimension.D01_CLUSTER_CONFIG


def _first_line(match_line: int | None) -> int:
    """Return *match_line* or 1 when the config is absent (line is None or 0)."""
    return match_line if (match_line is not None and match_line > 0) else 1


def _parse_memory_mb(value: str) -> float | None:
    """Parse a Spark memory string into megabytes.

    Handles suffixes: ``g``/``gb``, ``m``/``mb``, ``k``/``kb``, and bare
    integers (assumed to be bytes when no suffix is given, matching Spark's
    own ``Utils.byteStringAsMb`` behaviour).

    Returns ``None`` when the value cannot be parsed.
    """
    v = value.strip().lower()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(g|gb|m|mb|k|kb|b)?", v)
    if not m:
        return None
    num = float(m.group(1))
    suffix = m.group(2) or "b"
    multipliers = {
        "g": 1024.0,
        "gb": 1024.0,
        "m": 1.0,
        "mb": 1.0,
        "k": 1 / 1024.0,
        "kb": 1 / 1024.0,
        "b": 1 / (1024 * 1024),
    }
    return num * multipliers[suffix]


def _parse_seconds(value: str) -> float | None:
    """Parse a Spark time string into seconds.

    Handles suffixes: ``s``/``sec``/``second``/``seconds``,
    ``ms``/``milliseconds``, ``m``/``min``/``minutes``, bare integers
    (assumed milliseconds — Spark's default for ``spark.network.timeout``).
    """
    v = value.strip().lower()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|sec(?:onds?)?|m|min(?:utes?)?)?", v)
    if not m:
        return None
    num = float(m.group(1))
    suffix = m.group(2) or "ms"
    if suffix in ("ms", "millisecond", "milliseconds"):
        return num / 1000.0
    if suffix in ("s", "sec", "second", "seconds"):
        return num
    if suffix in ("m", "min", "minute", "minutes"):
        return num * 60.0
    return None  # unknown suffix


# =============================================================================
# SPL-D01-001 — Missing Kryo serializer
# =============================================================================


@register_rule
class MissingKryoSerializerRule(ConfigRule):
    """SPL-D01-001: spark.serializer not set to KryoSerializer."""

    rule_id = "SPL-D01-001"
    name = "Missing Kryo serializer"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "spark.serializer is not set to KryoSerializer."
    explanation = (
        "Spark's default serializer is Java serialization, which is significantly slower "
        "and produces larger payloads than Kryo for most data types. "
        "Kryo is typically 10× faster and produces 3-5× smaller serialized objects. "
        "Java serialization also requires all objects to implement ``java.io.Serializable``, "
        "which forces defensive copies and inhibits caching optimisations inside the "
        "Spark block manager."
    )
    recommendation_template = (
        'Add ``.config("spark.serializer", '
        '"org.apache.spark.serializer.KryoSerializer")`` to your SparkSession builder. '
        "Optionally also set ``spark.kryo.registrationRequired = false`` during migration."
    )
    before_example = 'SparkSession.builder.appName("job").getOrCreate()'
    after_example = (
        "SparkSession.builder\n"
        '    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\n'
        "    .getOrCreate()"
    )
    references = [
        "https://spark.apache.org/docs/latest/tuning.html#data-serialization",
        "https://spark.apache.org/docs/latest/configuration.html#compression-and-serialization",
    ]
    estimated_impact = "10× serialization speedup; 3–5× reduction in shuffle data size"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        pm = PatternMatcher(analyzer)
        matches = pm.find_missing_config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer",
        )
        return [
            self.create_finding(
                analyzer.filename,
                _first_line(m.first_line),
                "spark.serializer is not set to KryoSerializer (Java serialization is ~10× slower)",
                config=config,
            )
            for m in matches
        ]


# =============================================================================
# SPL-D01-002 — Executor memory not configured
# =============================================================================


@register_rule
class ExecutorMemoryNotConfiguredRule(ConfigRule):
    """SPL-D01-002: spark.executor.memory absent from builder config."""

    rule_id = "SPL-D01-002"
    name = "Executor memory not configured"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "spark.executor.memory is not explicitly set."
    explanation = (
        "When ``spark.executor.memory`` is not set, Spark uses a deployment-mode default "
        "(typically 1 GB on YARN/K8s). This is almost always insufficient for production "
        "workloads, causing excessive spilling to disk, GC pressure, and executor OOM. "
        "Untuned executor memory is one of the most common causes of slow or failing jobs "
        "in shared cluster environments."
    )
    recommendation_template = (
        'Add ``.config("spark.executor.memory", "4g")`` (or higher) to your builder. '
        "Size based on: (task count per executor) × (data per task) + 20% overhead. "
        "See the Spark tuning guide for a methodology."
    )
    before_example = 'SparkSession.builder.appName("job").getOrCreate()'
    after_example = (
        "SparkSession.builder\n" '    .config("spark.executor.memory", "4g")\n' "    .getOrCreate()"
    )
    references = [
        "https://spark.apache.org/docs/latest/configuration.html#application-properties",
        "https://spark.apache.org/docs/latest/tuning.html#memory-management-overview",
    ]
    estimated_impact = "Executor OOM or excessive GC on any dataset > JVM heap size"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        pm = PatternMatcher(analyzer)
        matches = pm.find_missing_config("spark.executor.memory")
        return [
            self.create_finding(
                analyzer.filename,
                _first_line(m.first_line),
                "spark.executor.memory is not set — defaults to 1 GB, rarely sufficient",
                config=config,
            )
            for m in matches
        ]


# =============================================================================
# SPL-D01-003 — Driver memory not configured
# =============================================================================


@register_rule
class DriverMemoryNotConfiguredRule(ConfigRule):
    """SPL-D01-003: spark.driver.memory absent from builder config."""

    rule_id = "SPL-D01-003"
    name = "Driver memory not configured"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "spark.driver.memory is not explicitly set."
    explanation = (
        "The driver accumulates task metadata, broadcast variables, and results from "
        "``collect()`` / ``toPandas()`` calls. Spark's default driver memory (1 GB) is "
        "routinely exhausted by jobs that collect even moderately sized result sets or "
        "use large broadcast joins. Driver OOM causes the entire application to fail, "
        "wasting all executor work already done."
    )
    recommendation_template = (
        'Add ``.config("spark.driver.memory", "2g")`` (minimum) to your builder. '
        "Increase to 4 GB or more if the job collects large result sets or uses "
        "many broadcast joins."
    )
    before_example = 'SparkSession.builder.appName("job").getOrCreate()'
    after_example = (
        "SparkSession.builder\n" '    .config("spark.driver.memory", "2g")\n' "    .getOrCreate()"
    )
    references = [
        "https://spark.apache.org/docs/latest/configuration.html#application-properties",
    ]
    estimated_impact = "Driver OOM when collecting results or using large broadcast variables"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        pm = PatternMatcher(analyzer)
        matches = pm.find_missing_config("spark.driver.memory")
        return [
            self.create_finding(
                analyzer.filename,
                _first_line(m.first_line),
                "spark.driver.memory is not set — defaults to 1 GB, may OOM on collect()",
                config=config,
            )
            for m in matches
        ]


# =============================================================================
# SPL-D01-004 — Dynamic allocation not enabled
# =============================================================================


@register_rule
class DynamicAllocationNotEnabledRule(ConfigRule):
    """SPL-D01-004: spark.dynamicAllocation.enabled not set to true."""

    rule_id = "SPL-D01-004"
    name = "Dynamic allocation not enabled"
    dimension = _DIM
    default_severity = Severity.INFO
    description = "spark.dynamicAllocation.enabled is not set to true."
    explanation = (
        "Without dynamic allocation, Spark holds all requested executors for the entire "
        "lifetime of the application, even during idle phases (e.g., the driver is "
        "computing the next query plan). On shared clusters this wastes resources and "
        "increases queue wait times for other jobs. Dynamic allocation returns idle "
        "executors to the cluster manager, improving throughput and cost on YARN/K8s."
        "\n\nNote: dynamic allocation is incompatible with ``spark.streaming.dynamicAllocation`` "
        "and should not be used with Spark Structured Streaming without the shuffle service."
    )
    recommendation_template = (
        'Add ``.config("spark.dynamicAllocation.enabled", "true")`` and also '
        "``spark.dynamicAllocation.shuffleTracking.enabled = true`` (Spark 3+) or "
        "configure an external shuffle service."
    )
    before_example = 'SparkSession.builder.config("spark.executor.instances", "10").getOrCreate()'
    after_example = (
        "SparkSession.builder\n"
        '    .config("spark.dynamicAllocation.enabled", "true")\n'
        '    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")\n'
        "    .getOrCreate()"
    )
    references = [
        "https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation",
        "https://spark.apache.org/docs/latest/configuration.html#dynamic-allocation",
    ]
    estimated_impact = "Idle executor waste; reduced cluster throughput for concurrent jobs"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        pm = PatternMatcher(analyzer)
        matches = pm.find_missing_config("spark.dynamicAllocation.enabled", "true")
        return [
            self.create_finding(
                analyzer.filename,
                _first_line(m.first_line),
                "spark.dynamicAllocation.enabled is not true — executors held for entire job",
                config=config,
            )
            for m in matches
        ]


# =============================================================================
# SPL-D01-005 — Executor cores set too high
# =============================================================================


@register_rule
class ExecutorCoresTooHighRule(ConfigRule):
    """SPL-D01-005: spark.executor.cores > 5."""

    rule_id = "SPL-D01-005"
    name = "Executor cores set too high"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "spark.executor.cores is set above the recommended maximum of 5."
    explanation = (
        "Each executor core runs one task concurrently; all cores in an executor share "
        "the executor's JVM heap and HDFS connections. Setting too many cores per executor "
        "causes: (1) HDFS throughput contention — the HDFS client supports ~5 concurrent "
        "streams per node efficiently; (2) GC pressure from many tasks competing for "
        "heap; (3) larger shuffle write files per executor, increasing sort memory demand. "
        "The Cloudera/Databricks rule of thumb is 4–5 cores per executor."
    )
    recommendation_template = (
        "Set ``spark.executor.cores`` to 4 or 5. "
        "If you need more parallelism, increase the number of executors instead. "
        "Use the config threshold ``max_executor_cores`` (default 5) to tune the limit."
    )
    before_example = '.config("spark.executor.cores", "10")'
    after_example = '.config("spark.executor.cores", "4")'
    references = [
        "https://spark.apache.org/docs/latest/hardware-provisioning.html",
        "https://blog.cloudera.com/how-to-tune-your-apache-spark-jobs-part-2/",
    ]
    estimated_impact = "HDFS throughput degradation; increased GC pause frequency"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        raw = analyzer.get_config_value("spark.executor.cores")
        if raw is None:
            return []
        try:
            cores = int(raw.strip())
        except ValueError:
            return []

        max_cores = int(config.get_threshold("max_executor_cores"))
        if cores <= max_cores:
            return []

        configs = [
            c for c in analyzer.find_spark_session_configs() if c.key == "spark.executor.cores"
        ]
        line = configs[-1].end_line if configs else 1
        return [
            self.create_finding(
                analyzer.filename,
                line,
                f"spark.executor.cores = {cores} exceeds max_executor_cores ({max_cores})",
                config=config,
            )
        ]


# =============================================================================
# SPL-D01-006 — Memory overhead too low
# =============================================================================


@register_rule
class MemoryOverheadTooLowRule(ConfigRule):
    """SPL-D01-006: spark.executor.memoryOverhead is set but below 384 MB."""

    rule_id = "SPL-D01-006"
    name = "Memory overhead too low"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "spark.executor.memoryOverhead is configured below the recommended 384 MB."
    explanation = (
        "``memoryOverhead`` covers off-heap memory used by the JVM process itself: "
        "JVM metadata, thread stacks, NIO direct buffers, and native PySpark worker "
        "memory when ``pyspark`` is involved. Spark's internal default is "
        "``max(384 MB, 0.10 × executor.memory)``; explicitly setting it below this "
        "floor causes container kill events on YARN/K8s (the container manager sees "
        "RSS memory exceed the allocation and sends SIGKILL), which manifests as "
        "mysterious executor loss without an OutOfMemoryError in the logs."
    )
    recommendation_template = (
        "Set ``spark.executor.memoryOverhead`` to at least 384m, or preferably "
        "``max(512m, 0.10 × executor.memory)``. "
        "For PySpark jobs with UDFs, increase to 1g–2g."
    )
    before_example = '.config("spark.executor.memoryOverhead", "128m")'
    after_example = '.config("spark.executor.memoryOverhead", "512m")'
    references = [
        "https://spark.apache.org/docs/latest/configuration.html#application-properties",
        "https://spark.apache.org/docs/latest/running-on-yarn.html#resource-allocation",
    ]
    estimated_impact = "Executor container killed by YARN/K8s without OOM log entry"
    effort_level = EffortLevel.CONFIG_ONLY

    _MIN_OVERHEAD_MB = 384.0

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        raw = analyzer.get_config_value("spark.executor.memoryOverhead")
        if raw is None:
            # Not set → Spark uses its own default; not our concern here.
            return []
        mb = _parse_memory_mb(raw)
        if mb is None or mb >= self._MIN_OVERHEAD_MB:
            return []

        configs = [
            c
            for c in analyzer.find_spark_session_configs()
            if c.key == "spark.executor.memoryOverhead"
        ]
        line = configs[-1].end_line if configs else 1
        return [
            self.create_finding(
                analyzer.filename,
                line,
                f"spark.executor.memoryOverhead = {raw!r} is below the 384 MB minimum "
                f"(parsed: {mb:.0f} MB); executor containers may be killed by YARN/K8s",
                config=config,
            )
        ]


# =============================================================================
# SPL-D01-007 — Missing PySpark memory config when UDFs detected
# =============================================================================


@register_rule
class MissingPySparkWorkerMemoryRule(ConfigRule):
    """SPL-D01-007: UDFs detected but spark.python.worker.memory is not set."""

    rule_id = "SPL-D01-007"
    name = "Missing PySpark worker memory config"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "Python UDFs are present but spark.python.worker.memory is not configured."
    explanation = (
        "Each Python UDF invocation launches a separate Python worker subprocess per "
        "executor task slot. Each worker's memory is bounded by "
        "``spark.python.worker.memory`` (default 512 MB). When UDFs process large "
        "pandas DataFrames or load ML models, workers easily exhaust their allocation "
        "and are killed, causing tasks to fail with a confusing ``WorkerLostFailureReason``. "
        "Explicitly setting this config documents the intent and prevents accidental "
        "resource exhaustion during data volume growth."
    )
    recommendation_template = (
        'Add ``.config("spark.python.worker.memory", "1g")`` (adjust based on '
        "UDF memory footprint). For pandas UDFs processing wide DataFrames, "
        "2 GB or more may be needed."
    )
    before_example = "@udf(returnType=StringType())\n" "def transform(x): return x.upper()"
    after_example = (
        "spark = SparkSession.builder\n"
        '    .config("spark.python.worker.memory", "1g")\n'
        "    .getOrCreate()\n\n"
        "@udf(returnType=StringType())\n"
        "def transform(x): return x.upper()"
    )
    references = [
        "https://spark.apache.org/docs/latest/configuration.html#execution-behavior",
        "https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html",
    ]
    estimated_impact = "Python worker OOM when UDFs process large inputs; silent task failures"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        udfs = analyzer.find_udf_definitions()
        if not udfs:
            return []
        if analyzer.get_config_value("spark.python.worker.memory") is not None:
            return []

        # Report at the first UDF definition
        line = udfs[0].line
        return [
            self.create_finding(
                analyzer.filename,
                line,
                f"Python UDF(s) detected ({len(udfs)} found) but spark.python.worker.memory "
                "is not set (default 512 MB may be insufficient)",
                config=config,
            )
        ]


# =============================================================================
# SPL-D01-008 — Network timeout too low for large shuffles
# =============================================================================


@register_rule
class NetworkTimeoutTooLowRule(ConfigRule):
    """SPL-D01-008: spark.network.timeout is configured below 120 seconds."""

    rule_id = "SPL-D01-008"
    name = "Network timeout too low"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "spark.network.timeout is set below the recommended 120 seconds."
    explanation = (
        "``spark.network.timeout`` is the global timeout for all network interactions "
        "between Spark components (executors, block manager, shuffle service). During "
        "heavy shuffle phases or GC pauses, executors can legitimately go silent for "
        "30–90 seconds. If the timeout fires before GC completes, Spark marks the "
        "executor as dead, kills its tasks, and re-schedules them — wasting significant "
        "compute. The Spark default (120 s) is deliberately conservative for this reason. "
        "Lowering it increases false-positive executor evictions."
    )
    recommendation_template = (
        "Remove the ``spark.network.timeout`` override or set it to at least ``120s``. "
        "If you need responsive failure detection, tune ``spark.executor.heartbeatInterval`` "
        "separately (default 10 s)."
    )
    before_example = '.config("spark.network.timeout", "30s")'
    after_example = '.config("spark.network.timeout", "120s")'
    references = [
        "https://spark.apache.org/docs/latest/configuration.html#networking",
    ]
    estimated_impact = "False-positive executor evictions during GC or shuffle; wasted recompute"
    effort_level = EffortLevel.CONFIG_ONLY

    _MIN_TIMEOUT_S = 120.0

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        raw = analyzer.get_config_value("spark.network.timeout")
        if raw is None:
            return []
        seconds = _parse_seconds(raw)
        if seconds is None or seconds >= self._MIN_TIMEOUT_S:
            return []

        configs = [
            c for c in analyzer.find_spark_session_configs() if c.key == "spark.network.timeout"
        ]
        line = configs[-1].end_line if configs else 1
        return [
            self.create_finding(
                analyzer.filename,
                line,
                f"spark.network.timeout = {raw!r} is below 120 s "
                f"(parsed: {seconds:.0f} s); GC pauses may trigger false executor evictions",
                config=config,
            )
        ]


# =============================================================================
# SPL-D01-009 — Speculation not enabled
# =============================================================================


@register_rule
class SpeculationNotEnabledRule(ConfigRule):
    """SPL-D01-009: spark.speculation is not set to true."""

    rule_id = "SPL-D01-009"
    name = "Speculation not enabled"
    dimension = _DIM
    default_severity = Severity.INFO
    description = "spark.speculation is not enabled."
    explanation = (
        "Speculative execution detects straggler tasks (tasks running significantly "
        "slower than the median for their stage) and launches duplicate copies on other "
        "executors, using whichever finishes first. Without it, a single slow node "
        "(due to hardware degradation, noisy neighbour, or data skew) can hold up the "
        "entire stage. This is particularly valuable on heterogeneous cloud hardware "
        "where instance performance varies by 20–30%.\n\n"
        "Note: speculation is disabled by default because it can cause duplicate side "
        "effects for non-idempotent operations (e.g., writing to an external DB). "
        "Enable it only for idempotent workloads."
    )
    recommendation_template = (
        'Add ``.config("spark.speculation", "true")`` if your tasks are idempotent. '
        "Also set ``spark.speculation.multiplier = 1.5`` and "
        "``spark.speculation.quantile = 0.9`` to tune the straggler detection threshold."
    )
    before_example = 'SparkSession.builder.appName("etl").getOrCreate()'
    after_example = (
        "SparkSession.builder\n"
        '    .config("spark.speculation", "true")\n'
        '    .config("spark.speculation.multiplier", "1.5")\n'
        "    .getOrCreate()"
    )
    references = [
        "https://spark.apache.org/docs/latest/configuration.html#scheduling",
    ]
    estimated_impact = "Slow nodes can stall entire stages; 2–5× job slowdown on degraded hardware"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        pm = PatternMatcher(analyzer)
        matches = pm.find_missing_config("spark.speculation", "true")
        return [
            self.create_finding(
                analyzer.filename,
                _first_line(m.first_line),
                "spark.speculation is not enabled — straggler tasks will stall the entire stage",
                config=config,
            )
            for m in matches
        ]


# =============================================================================
# SPL-D01-010 — Default shuffle partitions unchanged (200)
# =============================================================================


@register_rule
class DefaultShufflePartitionsRule(ConfigRule):
    """SPL-D01-010: spark.sql.shuffle.partitions is absent or left at 200."""

    rule_id = "SPL-D01-010"
    name = "Default shuffle partitions unchanged"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "spark.sql.shuffle.partitions is not set (or left at the default of 200)."
    explanation = (
        "``spark.sql.shuffle.partitions`` controls the number of partitions produced by "
        "wide transformations (joins, aggregations, window functions). The default of 200 "
        "is chosen for a 'medium' dataset on a small cluster and is almost always wrong: "
        "too high for small datasets (hundreds of tiny files, shuffle overhead dominates) "
        "or too low for large datasets (GBs of data per partition, spill to disk, OOM). "
        "The rule of thumb is 2–3 partitions per CPU core available for the stage, or "
        "target 128–256 MB per partition. "
        "With AQE enabled (Spark 3+), the coalesce-partitions feature can automatically "
        "merge small post-shuffle partitions, reducing the impact of over-partitioning."
    )
    recommendation_template = (
        "Set ``spark.sql.shuffle.partitions`` based on your cluster size. "
        "A starting formula: ``(total_executor_cores × 2) or (dataset_size_gb × 8)``. "
        "With AQE enabled, prefer a slightly higher value and let AQE coalesce down."
    )
    before_example = 'SparkSession.builder.appName("job").getOrCreate()  # default: 200 partitions'
    after_example = '.config("spark.sql.shuffle.partitions", "400")  # tuned for cluster'
    references = [
        "https://spark.apache.org/docs/latest/configuration.html#execution-behavior",
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html",
    ]
    estimated_impact = "Spill to disk on large datasets; small-file explosion on small datasets"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        raw = analyzer.get_config_value("spark.sql.shuffle.partitions")

        # Not set at all → still using the default 200
        if raw is None:
            # Only flag if the file contains a SparkSession creation (builder pattern)
            builder_calls = analyzer.find_method_calls("getOrCreate")
            if not builder_calls:
                return []
            return [
                self.create_finding(
                    analyzer.filename,
                    _first_line(builder_calls[0].line if builder_calls else 0),
                    "spark.sql.shuffle.partitions is not set — default of 200 is rarely correct",
                    config=config,
                )
            ]

        # Set explicitly to "200" — still the default, still wrong
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
            line = configs[-1].end_line if configs else 1
            return [
                self.create_finding(
                    analyzer.filename,
                    line,
                    "spark.sql.shuffle.partitions = 200 (the Spark default); tune for your cluster",
                    config=config,
                )
            ]

        return []
