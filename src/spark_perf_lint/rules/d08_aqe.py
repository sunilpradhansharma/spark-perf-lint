"""D08 — Adaptive Query Execution (AQE) rules for spark-perf-lint.

Spark 3.0 introduced Adaptive Query Execution, which re-optimises a query
at runtime using live shuffle statistics: it coalesces small output partitions,
converts sort-merge joins to broadcast joins when one side turns out to be small,
and splits skewed partitions automatically.  These rules detect when AQE is
disabled, misconfigured, or in conflict with manual settings.

Rule IDs: SPL-D08-001 through SPL-D08-007
"""

from __future__ import annotations

import ast
import re

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.base import ConfigRule
from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.types import Dimension, EffortLevel, Finding, Severity

_DIM = Dimension.D08_AQE

# ---------------------------------------------------------------------------
# Config key constants
# ---------------------------------------------------------------------------

_AQE_ENABLED_KEY = "spark.sql.adaptive.enabled"
_AQE_COALESCE_KEY = "spark.sql.adaptive.coalescePartitions.enabled"
_AQE_ADVISORY_SIZE_KEY = "spark.sql.adaptive.advisoryPartitionSizeInBytes"
_AQE_SKEW_JOIN_KEY = "spark.sql.adaptive.skewJoin.enabled"
_AQE_SKEW_FACTOR_KEY = "spark.sql.adaptive.skewJoin.skewedPartitionFactor"
_AQE_LOCAL_READER_KEY = "spark.sql.adaptive.localShuffleReader.enabled"
_SHUFFLE_PARTITIONS_KEY = "spark.sql.shuffle.partitions"

# Advisory partition size thresholds
_ADVISORY_SIZE_MIN_BYTES = 16 * 1024 * 1024  # 16 MB — below this is too small

# Skew factor thresholds
_SKEW_FACTOR_MIN = 2.0  # below this splits nearly all partitions

# shuffle.partitions threshold above which manual setting conflicts with AQE
_SHUFFLE_PARTITIONS_HIGH = 400  # 2× default of 200

# ---------------------------------------------------------------------------
# Skew-prone column heuristics (for D08-004)
# ---------------------------------------------------------------------------

_SKEW_PRONE_NAMES: frozenset[str] = frozenset(
    {"status", "flag", "type", "active", "enabled", "deleted", "archived", "gender"}
)
_SKEW_PRONE_PREFIXES: tuple[str, ...] = ("is_", "has_", "flag_")
_SKEW_ID_NAMES: frozenset[str] = frozenset(
    {
        "user_id",
        "customer_id",
        "product_id",
        "account_id",
        "session_id",
        "seller_id",
        "buyer_id",
    }
)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _parse_bytes(value: str) -> int | None:
    """Parse a Spark size string into bytes.

    Handles ``g``/``gb``, ``m``/``mb``, ``k``/``kb``, and bare integers
    (assumed bytes).  Returns ``None`` on parse failure.
    """
    v = value.strip().lower()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(g|gb|m|mb|k|kb|b)?", v)
    if not m:
        return None
    num = float(m.group(1))
    suffix = m.group(2) or "b"
    mult: dict[str, float] = {
        "g": 1024**3,
        "gb": 1024**3,
        "m": 1024**2,
        "mb": 1024**2,
        "k": 1024,
        "kb": 1024,
        "b": 1,
    }
    return int(num * mult[suffix])


def _config_line(analyzer: ASTAnalyzer, key: str, fallback: int) -> int:
    """Return the source line of the last occurrence of *key* in the session config."""
    cfgs = [c for c in analyzer.find_spark_session_configs() if c.key == key]
    return cfgs[-1].end_line if cfgs else fallback


def _is_skew_prone_col(col: str) -> bool:
    """Return ``True`` when *col* is a low-cardinality or power-law distributed key."""
    c = col.lower().strip()
    if c in _SKEW_PRONE_NAMES or c in _SKEW_ID_NAMES:
        return True
    return any(c.startswith(p) for p in _SKEW_PRONE_PREFIXES)


def _get_join_string_keys(call) -> list[str]:
    """Extract string join keys from a ``join()`` call's ``on`` argument."""
    keys: list[str] = []
    on: ast.expr | None = None
    if len(call.args) >= 2:
        on = call.args[1]
    else:
        on = call.kwargs.get("on")
    if on is None:
        return keys
    if isinstance(on, ast.Constant) and isinstance(on.value, str):
        keys.append(on.value)
    elif isinstance(on, ast.List):
        for elt in on.elts:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                keys.append(elt.value)
    return keys


def _has_skew_prone_joins(analyzer: ASTAnalyzer) -> bool:
    """Return ``True`` when any ``join()`` uses a statically detectable skew-prone key."""
    for call in analyzer.find_method_calls("join"):
        for k in _get_join_string_keys(call):
            if _is_skew_prone_col(k):
                return True
    return False


def _requires_spark_session(analyzer: ASTAnalyzer) -> bool:
    """Return ``True`` only when the file contains a SparkSession builder."""
    return bool(analyzer.find_method_calls("getOrCreate"))


# =============================================================================
# SPL-D08-001 — AQE disabled
# =============================================================================


@register_rule
class AqeDisabledRule(ConfigRule):
    """SPL-D08-001: spark.sql.adaptive.enabled = false disables all AQE optimisations."""

    rule_id = "SPL-D08-001"
    name = "AQE disabled"
    dimension = _DIM
    default_severity = Severity.CRITICAL
    description = (
        "spark.sql.adaptive.enabled = false disables all AQE optimisations:"
        " dynamic partition coalescing, skew join splitting, and join strategy adaptation."
    )
    explanation = (
        "Adaptive Query Execution (AQE, Spark 3.0+) re-optimises a query plan at runtime "
        "using the actual shuffle statistics collected after each stage.  It provides three "
        "major automatic optimisations at zero developer cost:\n\n"
        "1. **Partition coalescing**: merges small output partitions into larger ones to "
        "   eliminate the task scheduling overhead of thousands of tiny tasks.\n"
        "2. **Skew join handling**: detects and automatically splits skewed shuffle "
        "   partitions so one task does not process disproportionate data.\n"
        "3. **Dynamic join strategy**: downgrades a planned sort-merge join to a broadcast "
        "   hash join when one side proves small enough after shuffle statistics are known.\n\n"
        "Setting ``spark.sql.adaptive.enabled = false`` disables all three features.  "
        "AQE has been stable since Spark 3.1 and is enabled by default from Spark 3.2 "
        "onwards.  There is almost never a valid reason to disable it."
    )
    recommendation_template = (
        "Remove the ``false`` override: "
        "``spark.sql.adaptive.enabled = true`` (or remove the config entry entirely "
        "in Spark 3.2+ where it defaults to ``true``)."
    )
    before_example = '.config("spark.sql.adaptive.enabled", "false")'
    after_example = "# Remove the line — AQE is enabled by default in Spark 3.2+"
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#adaptive-query-execution",
    ]
    estimated_impact = (
        "Loses automatic partition coalescing, skew handling, and join strategy adaptation"
    )
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        if not _requires_spark_session(analyzer):
            return []

        val = analyzer.get_config_value(_AQE_ENABLED_KEY)
        if val is None or val.strip().lower() != "false":
            return []

        line = _config_line(analyzer, _AQE_ENABLED_KEY, 1)
        return [
            self.create_finding(
                analyzer.filename,
                line,
                "spark.sql.adaptive.enabled = false — all AQE optimisations are disabled;"
                " remove this override",
                config=config,
            )
        ]


# =============================================================================
# SPL-D08-002 — AQE coalesce partitions disabled
# =============================================================================


@register_rule
class AqeCoalesceDisabledRule(ConfigRule):
    """SPL-D08-002: spark.sql.adaptive.coalescePartitions.enabled = false."""

    rule_id = "SPL-D08-002"
    name = "AQE coalesce partitions disabled"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "spark.sql.adaptive.coalescePartitions.enabled = false prevents AQE from"
        " merging small post-shuffle partitions."
    )
    explanation = (
        "After a shuffle, Spark creates ``spark.sql.shuffle.partitions`` output partitions "
        "(default 200).  For a small dataset this produces hundreds of tiny tasks — each "
        "adding scheduling overhead and Executor RPC cost.  AQE's partition coalescing "
        "merges adjacent small partitions into ones close to "
        "``advisoryPartitionSizeInBytes`` (default 64 MB), reducing the number of tasks "
        "to a reasonable level automatically.\n\n"
        "Disabling coalescing forces all downstream stages to run with the full 200 "
        "(or manually set) number of partitions regardless of how small each one is.  "
        "For datasets much smaller than 200 × 64 MB = 12.8 GB this is pure overhead."
    )
    recommendation_template = (
        "Remove the ``false`` override: "
        "``spark.sql.adaptive.coalescePartitions.enabled = true`` "
        "(or remove the config entry entirely)."
    )
    before_example = '.config("spark.sql.adaptive.coalescePartitions.enabled", "false")'
    after_example = "# Remove the line — coalescing is enabled by default with AQE"
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#coalescing-post-shuffle-partitions",
    ]
    estimated_impact = "Hundreds of tiny tasks per stage; driver scheduling overhead"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        if not _requires_spark_session(analyzer):
            return []

        val = analyzer.get_config_value(_AQE_COALESCE_KEY)
        if val is None or val.strip().lower() != "false":
            return []

        line = _config_line(analyzer, _AQE_COALESCE_KEY, 1)
        return [
            self.create_finding(
                analyzer.filename,
                line,
                "spark.sql.adaptive.coalescePartitions.enabled = false"
                " — AQE cannot merge small post-shuffle partitions",
                config=config,
            )
        ]


# =============================================================================
# SPL-D08-003 — AQE advisory partition size too small
# =============================================================================


@register_rule
class AqeAdvisoryPartitionSizeTooSmallRule(ConfigRule):
    """SPL-D08-003: advisoryPartitionSizeInBytes set below 16 MB."""

    rule_id = "SPL-D08-003"
    name = "AQE advisory partition size too small"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "advisoryPartitionSizeInBytes below 16 MB causes AQE to coalesce into"
        " too many small partitions."
    )
    explanation = (
        "``spark.sql.adaptive.advisoryPartitionSizeInBytes`` (default 64 MB) is the "
        "target size for partitions after AQE coalescing.  AQE merges adjacent shuffle "
        "output partitions until each merged partition is close to this target.\n\n"
        "Setting this value very small (below 16 MB) causes AQE to produce many small "
        "partitions, which:\n\n"
        "• Increases task scheduling overhead (one JVM task per partition).\n"
        "• Increases the number of shuffle fetch requests.\n"
        "• May produce more output files than downstream readers can efficiently handle.\n\n"
        "The optimal value depends on your cluster (executor memory, data volume), but "
        "64 MB is a robust default.  Values below 16 MB are rarely beneficial."
    )
    recommendation_template = (
        "Increase advisoryPartitionSizeInBytes to at least 64 MB (the default): "
        "``spark.sql.adaptive.advisoryPartitionSizeInBytes = 67108864`` "
        "or remove the override entirely."
    )
    before_example = '.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "4m")'
    after_example = (
        "# Use the default (64 MB) or set explicitly:\n"
        '.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")'
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#coalescing-post-shuffle-partitions",
    ]
    estimated_impact = "Too many small post-coalesce partitions; task scheduling overhead"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        if not _requires_spark_session(analyzer):
            return []

        raw = analyzer.get_config_value(_AQE_ADVISORY_SIZE_KEY)
        if raw is None:
            return []

        size_bytes = _parse_bytes(raw)
        if size_bytes is None or size_bytes >= _ADVISORY_SIZE_MIN_BYTES:
            return []

        line = _config_line(analyzer, _AQE_ADVISORY_SIZE_KEY, 1)
        size_mb = size_bytes / (1024 * 1024)
        return [
            self.create_finding(
                analyzer.filename,
                line,
                f"advisoryPartitionSizeInBytes = {raw!r} ({size_mb:.0f} MB)"
                " is below 16 MB — AQE will produce many small partitions",
                config=config,
            )
        ]


# =============================================================================
# SPL-D08-004 — AQE skew join disabled with skew-prone joins
# =============================================================================


@register_rule
class AqeSkewJoinDisabledWithJoinsRule(ConfigRule):
    """SPL-D08-004: skewJoin.enabled = false while skew-prone joins exist in this file."""

    rule_id = "SPL-D08-004"
    name = "AQE skew join disabled with skew-prone joins detected"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "spark.sql.adaptive.skewJoin.enabled = false is set while this file contains"
        " joins on skew-prone columns."
    )
    explanation = (
        "AQE's skew join optimisation automatically detects shuffle partitions that are "
        "disproportionately large compared to the median and splits them across multiple "
        "tasks.  This is the lowest-effort fix for join-induced data skew.\n\n"
        "This finding fires when ``spark.sql.adaptive.skewJoin.enabled = false`` is "
        "set in the same file that contains joins on columns that commonly carry "
        "power-law or low-cardinality distributions (``user_id``, ``product_id``, "
        "``status``, ``is_active`` …).  These joins are the primary beneficiaries of "
        "AQE skew handling, so disabling the feature here is particularly impactful.\n\n"
        "Re-enabling skew join handling (the default) allows Spark to automatically "
        "split the dominant partitions without any code changes."
    )
    recommendation_template = (
        "Remove the ``false`` override: "
        "``spark.sql.adaptive.skewJoin.enabled = true`` "
        "(or remove the config entry entirely)."
    )
    before_example = (
        '.config("spark.sql.adaptive.skewJoin.enabled", "false")\n'
        "# ...\n"
        "df.join(events, 'user_id')"
    )
    after_example = (
        "# Remove the skewJoin.enabled = false line\n"
        "df.join(events, 'user_id')  # AQE now auto-splits skewed partitions"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#skew-join-optimization",
    ]
    estimated_impact = "Skewed partitions on joins not auto-split; straggler tasks persist"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        if not _requires_spark_session(analyzer):
            return []

        val = analyzer.get_config_value(_AQE_SKEW_JOIN_KEY)
        if val is None or val.strip().lower() != "false":
            return []

        # Only fire when the file also contains detectable skew-prone joins
        if not _has_skew_prone_joins(analyzer):
            return []

        line = _config_line(analyzer, _AQE_SKEW_JOIN_KEY, 1)
        return [
            self.create_finding(
                analyzer.filename,
                line,
                "spark.sql.adaptive.skewJoin.enabled = false while skew-prone joins"
                " are present — re-enable to auto-split skewed partitions",
                config=config,
            )
        ]


# =============================================================================
# SPL-D08-005 — AQE skew factor too aggressive
# =============================================================================


@register_rule
class AqeSkewFactorTooAggressiveRule(ConfigRule):
    """SPL-D08-005: skewedPartitionFactor set below 2 — nearly every partition is split."""

    rule_id = "SPL-D08-005"
    name = "AQE skew factor too aggressive"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "skewedPartitionFactor below 2 treats nearly every above-median partition"
        " as skewed, causing excessive partition splitting."
    )
    explanation = (
        "AQE's skew detection uses two thresholds: a partition is considered skewed "
        "when it is **both** larger than ``skewedPartitionThresholdInBytes`` (default "
        "256 MB) **and** larger than ``skewedPartitionFactor × median partition size`` "
        "(default factor = 5, i.e. 5× the median).\n\n"
        "Setting the factor below 2 (e.g. 1.1) causes AQE to treat almost any "
        "partition above the median as skewed and split it.  This leads to:\n\n"
        "• Many unnecessary partition splits, increasing total task count.\n"
        "• Higher driver overhead for tracking additional tasks.\n"
        "• Potential regression for balanced datasets where no splitting is needed.\n\n"
        "The default factor of 5 is well-calibrated.  Values below 2 are rarely "
        "justified outside of extreme skew research scenarios."
    )
    recommendation_template = (
        "Increase skewedPartitionFactor to at least 2 (recommended: 5, the default): "
        "``spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5``."
    )
    before_example = '.config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "1")'
    after_example = (
        "# Use the default (5) or set explicitly:\n"
        '.config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")'
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#skew-join-optimization",
    ]
    estimated_impact = "Excessive partition splits for balanced data; higher task count"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        if not _requires_spark_session(analyzer):
            return []

        raw = analyzer.get_config_value(_AQE_SKEW_FACTOR_KEY)
        if raw is None:
            return []

        try:
            factor = float(raw.strip())
        except ValueError:
            return []

        if factor >= _SKEW_FACTOR_MIN:
            return []

        line = _config_line(analyzer, _AQE_SKEW_FACTOR_KEY, 1)
        return [
            self.create_finding(
                analyzer.filename,
                line,
                f"skewedPartitionFactor = {factor:.1f} is below 2"
                " — nearly every above-median partition will be split unnecessarily",
                config=config,
            )
        ]


# =============================================================================
# SPL-D08-006 — AQE local shuffle reader disabled
# =============================================================================


@register_rule
class AqeLocalShuffleReaderDisabledRule(ConfigRule):
    """SPL-D08-006: spark.sql.adaptive.localShuffleReader.enabled = false."""

    rule_id = "SPL-D08-006"
    name = "AQE local shuffle reader disabled"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "spark.sql.adaptive.localShuffleReader.enabled = false forces remote shuffle"
        " reads even when data is already local."
    )
    explanation = (
        "AQE's local shuffle reader optimisation avoids remote shuffle data fetches "
        "when all the shuffle blocks needed by a task are already on the same executor "
        "node.  Instead of going over the network, the task reads the shuffle data "
        "locally — reducing shuffle read latency and network congestion.\n\n"
        "This optimisation is particularly beneficial on clusters where:\n\n"
        "• The broadcast join threshold is reached (data stays local post-broadcast).\n"
        "• Executors are co-located with HDFS data nodes.\n"
        "• Network bandwidth is a bottleneck.\n\n"
        "Disabling it forces every shuffle read through the network layer, even when "
        "a local read would suffice.  The default (``true``) is almost always correct."
    )
    recommendation_template = (
        "Remove the ``false`` override — "
        "``spark.sql.adaptive.localShuffleReader.enabled = true`` "
        "is the default and is safe for all workloads."
    )
    before_example = '.config("spark.sql.adaptive.localShuffleReader.enabled", "false")'
    after_example = "# Remove the line — local shuffle reader is enabled by default"
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#converting-sort-merge-join-to-local-sort-merge-join",
    ]
    estimated_impact = "Unnecessary remote shuffle reads; extra network I/O even for local data"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        if not _requires_spark_session(analyzer):
            return []

        val = analyzer.get_config_value(_AQE_LOCAL_READER_KEY)
        if val is None or val.strip().lower() != "false":
            return []

        line = _config_line(analyzer, _AQE_LOCAL_READER_KEY, 1)
        return [
            self.create_finding(
                analyzer.filename,
                line,
                "spark.sql.adaptive.localShuffleReader.enabled = false"
                " — forces remote shuffle reads even for locally available data",
                config=config,
            )
        ]


# =============================================================================
# SPL-D08-007 — Manual shuffle partitions set high with AQE enabled
# =============================================================================


@register_rule
class ManualShufflePartitionsWithAqeRule(ConfigRule):
    """SPL-D08-007: Large spark.sql.shuffle.partitions with AQE enabled."""

    rule_id = "SPL-D08-007"
    name = "Manual shuffle partition count set high with AQE enabled"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "spark.sql.shuffle.partitions set high (> 400) with AQE enabled;"
        " tune advisoryPartitionSizeInBytes instead."
    )
    explanation = (
        "``spark.sql.shuffle.partitions`` controls the initial number of shuffle output "
        "partitions (default 200).  With AQE enabled, this becomes the *maximum* count "
        "from which AQE coalesces downward based on actual data size.\n\n"
        "Setting this very high (e.g. 2000) causes Spark to create 2000 initial "
        "partitions and then spend executor time coalescing them.  The overhead is "
        "proportional to the initial count: more shuffle map tasks, more metadata, "
        "more driver bookkeeping — most of which is discarded during coalescing.\n\n"
        "With AQE, the correct lever is "
        "``spark.sql.adaptive.advisoryPartitionSizeInBytes`` (target output file size), "
        "not a raw partition count.  Set the advisory size to your desired partition "
        "size (e.g. 128 MB) and AQE will compute the appropriate count automatically.\n\n"
        "Exception: setting a high value is reasonable if AQE coalescing is disabled "
        "or if you intentionally want many initial map tasks for fine-grained parallelism."
    )
    recommendation_template = (
        "Instead of a high spark.sql.shuffle.partitions, set the target partition size: "
        "``spark.sql.adaptive.advisoryPartitionSizeInBytes = 128m``. "
        "AQE will determine the right count from the actual shuffle statistics."
    )
    before_example = '.config("spark.sql.shuffle.partitions", "2000")'
    after_example = (
        '.config("spark.sql.shuffle.partitions", "200")  # reasonable initial count\n'
        '.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")'
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#coalescing-post-shuffle-partitions",
    ]
    estimated_impact = "Unnecessary map-side overhead proportional to initial partition count"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []
        if not _requires_spark_session(analyzer):
            return []

        raw = analyzer.get_config_value(_SHUFFLE_PARTITIONS_KEY)
        if raw is None:
            return []

        try:
            partitions = int(raw.strip())
        except ValueError:
            return []

        if partitions <= _SHUFFLE_PARTITIONS_HIGH:
            return []

        # Suppress when AQE is explicitly disabled — manual count is then intentional
        aqe_val = analyzer.get_config_value(_AQE_ENABLED_KEY)
        if aqe_val is not None and aqe_val.strip().lower() == "false":
            return []

        line = _config_line(analyzer, _SHUFFLE_PARTITIONS_KEY, 1)
        return [
            self.create_finding(
                analyzer.filename,
                line,
                f"spark.sql.shuffle.partitions = {partitions} with AQE enabled"
                " — set advisoryPartitionSizeInBytes instead of a high fixed count",
                config=config,
            )
        ]
