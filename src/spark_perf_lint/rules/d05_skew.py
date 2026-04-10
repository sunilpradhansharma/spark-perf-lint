"""D05 — Data Skew rules for spark-perf-lint.

Data skew occurs when one or a few tasks receive a disproportionate share of the
data, causing those tasks to run for minutes while the rest of the stage completes
in seconds.  Skew manifests as one "straggler" task blocking an entire stage.  It
is most commonly triggered by low-cardinality join/groupBy keys, null-heavy join
keys, or power-law distributed IDs without salting.  These rules detect each
pattern statically so they can be addressed before hitting production.

Rule IDs: SPL-D05-001 through SPL-D05-007
"""

from __future__ import annotations

import ast
import re

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer, MethodCallInfo
from spark_perf_lint.rules.base import CodeRule, ConfigRule
from spark_perf_lint.rules.registry import register_rule
from spark_perf_lint.types import Dimension, EffortLevel, Finding, Severity

_DIM = Dimension.D05_SKEW

# ---------------------------------------------------------------------------
# Column-name heuristics
# ---------------------------------------------------------------------------

# Column names / prefixes that imply low-cardinality data → bad join/groupBy keys
_SKEW_PRONE_NAMES: frozenset[str] = frozenset(
    {
        "status",
        "flag",
        "type",
        "active",
        "enabled",
        "deleted",
        "archived",
        "gender",
    }
)
_SKEW_PRONE_PREFIXES: tuple[str, ...] = ("is_", "has_", "flag_")

# Known analytics-domain IDs that often have power-law distributions
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

# Column patterns that frequently contain nulls (nullable foreign keys)
_NULL_PRONE_PATTERNS: tuple[str, ...] = ("parent_", "manager_", "referred_", "optional_")
_NULL_PRONE_NAMES: frozenset[str] = frozenset({"parent_id", "manager_id", "referred_by"})

# Write-chain markers — partitionBy inside a write chain is D04 territory
_WRITE_MARKERS: frozenset[str] = frozenset({"write", "writeStream"})

# AQE skew thresholds: above these the threshold is considered "too high"
_SKEW_THRESHOLD_MAX_BYTES = 1_073_741_824  # 1 GB
_SKEW_FACTOR_MAX = 10.0


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _is_skew_prone_col(col: str) -> bool:
    """Return ``True`` when *col* is a low-cardinality column likely to cause skew.

    Matches exact names (``status``, ``flag``, ``gender``…) and common
    boolean-style prefixes (``is_``, ``has_``, ``flag_``).
    """
    c = col.lower().strip()
    if c in _SKEW_PRONE_NAMES:
        return True
    return any(c.startswith(p) for p in _SKEW_PRONE_PREFIXES)


def _get_string_args(call: MethodCallInfo) -> list[str]:
    """Extract all string-literal positional arguments from a method call.

    Also unpacks ``ast.List`` arguments so that ``groupBy(['a', 'b'])`` is
    handled identically to ``groupBy('a', 'b')``.
    """
    result: list[str] = []
    for arg in call.args:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            result.append(arg.value)
        elif isinstance(arg, ast.List):
            for elt in arg.elts:
                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                    result.append(elt.value)
    return result


def _get_join_string_keys(call: MethodCallInfo) -> list[str]:
    """Extract string join keys from a ``join()`` call's ``on`` argument.

    Handles ``join(other, 'key')``, ``join(other, ['k1', 'k2'])``, and
    ``join(other, on='key')`` keyword forms.
    """
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


def _file_uses_salting(analyzer: ASTAnalyzer) -> bool:
    """Return ``True`` when the file contains ``rand()``, ``randn()``, or ``explode()``
    function calls that suggest a salting pattern is in use.
    """
    for node in ast.walk(analyzer.tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in {"rand", "randn", "explode"}:
                return True
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr in {"rand", "randn", "explode"}:
                return True
    return False


# =============================================================================
# SPL-D05-001 — Join on low-cardinality column
# =============================================================================


@register_rule
class LowCardinalityJoinRule(CodeRule):
    """SPL-D05-001: join() on a low-cardinality column concentrates all matching rows."""

    rule_id = "SPL-D05-001"
    name = "Join on low-cardinality column"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "join() on a low-cardinality column (e.g. status, type, is_*)"
        " sends matching rows to a single task."
    )
    explanation = (
        "When a join key has very few distinct values (``status``, ``type``, ``is_active``…), "
        "all rows sharing a value must be processed by the same reduce task.  If 80 % of "
        "records have ``status = 'active'``, one task handles 80 % of the data while the "
        "other tasks finish quickly — a classic straggler pattern.\n\n"
        "Unlike ID-based skew (which salting solves), low-cardinality join skew is structural: "
        "the key definition itself causes the imbalance.  Remediation strategies:\n\n"
        "• **Add a secondary key**: ``join(other, ['status', 'user_id'])`` breaks ties within "
        "  each status value, distributing load across more tasks.\n"
        "• **Filter before joining**: reduce the dominant group "
        "  (e.g. ``filter('status != \"active\"')``) if not all groups are needed.\n"
        "• **Enable AQE skew join**: Spark 3+ AQE can automatically split skewed partitions "
        "  when ``spark.sql.adaptive.skewJoin.enabled = true``."
    )
    recommendation_template = (
        "Add a secondary high-cardinality key to the join: "
        "``join(other, ['status', 'user_id'])``. "
        "Enable AQE skew join: ``spark.sql.adaptive.skewJoin.enabled = true``."
    )
    before_example = "df.join(other, 'status')"
    after_example = "df.join(other, ['status', 'user_id'])  # user_id breaks ties"
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#adaptive-query-execution",
    ]
    estimated_impact = "One task processes the dominant group; rest of cluster sits idle"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("join"):
            keys = _get_join_string_keys(call)
            skew_keys = [k for k in keys if _is_skew_prone_col(k)]
            if not skew_keys:
                continue
            key_list = ", ".join(f"'{k}'" for k in skew_keys)
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    f"join() on low-cardinality key {key_list}"
                    " — risks task skew; add a secondary high-cardinality key",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D05-002 — GroupBy on low-cardinality column without secondary key
# =============================================================================


@register_rule
class LowCardinalityGroupByRule(CodeRule):
    """SPL-D05-002: groupBy() on only low-cardinality columns produces skewed reducers."""

    rule_id = "SPL-D05-002"
    name = "GroupBy on low-cardinality column without secondary key"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "groupBy() using only low-cardinality columns (status, type, is_*)"
        " creates unbalanced reducer tasks."
    )
    explanation = (
        "``groupBy('status')`` routes all rows with the same status to a single "
        "partition.  If 90 % of rows are ``status = 'active'``, that partition "
        "holds 90 % of the data — one task takes 90 % of the stage's wall time.\n\n"
        "This is distinct from join skew in that there is no secondary key to add "
        "for correctness — the aggregation must see all rows for each group.  "
        "Mitigation options:\n\n"
        "• **Two-phase aggregation**: aggregate by ``(status, rand_bucket)`` first "
        "  to distribute, then aggregate the partial results by ``status`` alone.\n"
        "• **AQE skew join**: Spark 3.2+ AQE can split skewed groupBy partitions "
        "  when ``spark.sql.adaptive.skewJoin.enabled = true``.\n"
        "• **Filter dominant group separately** and union the results if the "
        "  aggregation is simple."
    )
    recommendation_template = (
        "Consider a two-phase aggregation to avoid the skewed partition: "
        "``df.withColumn('bucket', (rand()*10).cast('int'))"
        ".groupBy('status', 'bucket').agg(...)"
        ".groupBy('status').agg(...)``."
    )
    before_example = "df.groupBy('status').agg(count('*'))"
    after_example = (
        "# Two-phase: distribute within status first\n"
        "df.withColumn('b', (rand()*10).cast('int'))\n"
        "  .groupBy('status', 'b').agg(sum('amount').alias('partial'))\n"
        "  .groupBy('status').agg(sum('partial'))"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#adaptive-query-execution",
    ]
    estimated_impact = "Dominant group task runs 10–100× longer than median; straggler stage"
    effort_level = EffortLevel.MAJOR_REFACTOR

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("groupBy"):
            str_keys = _get_string_args(call)
            if not str_keys:
                continue  # column objects or variables — can't determine cardinality
            skew_keys = [k for k in str_keys if _is_skew_prone_col(k)]
            non_skew_keys = [k for k in str_keys if not _is_skew_prone_col(k)]
            # Fire only when ALL keys are low-cardinality (no secondary key breaks ties)
            if not skew_keys or non_skew_keys:
                continue
            key_list = ", ".join(f"'{k}'" for k in skew_keys)
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    f"groupBy({key_list}) uses only low-cardinality keys"
                    " — risks unbalanced reduce partitions",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D05-003 — AQE skew join handling disabled
# =============================================================================


@register_rule
class AqeSkewJoinDisabledRule(ConfigRule):
    """SPL-D05-003: AQE skew join handling is explicitly disabled."""

    rule_id = "SPL-D05-003"
    name = "AQE skew join handling disabled"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = "spark.sql.adaptive.skewJoin.enabled = false disables automatic skew mitigation."
    explanation = (
        "Spark 3.0+ Adaptive Query Execution (AQE) includes an automatic skew join "
        "optimisation that detects partitions larger than "
        "``skewedPartitionThresholdInBytes × skewedPartitionFactor`` and automatically "
        "splits them across multiple tasks.  This is the lowest-effort fix for many "
        "skew problems.\n\n"
        "Disabling AQE entirely (``spark.sql.adaptive.enabled = false``) or the skew "
        "join feature specifically "
        "(``spark.sql.adaptive.skewJoin.enabled = false``) removes this safety net, "
        "turning every skewed join into a long-running straggler that blocks the stage.\n\n"
        "The feature was disabled in early Spark 3.0 betas due to occasional incorrect "
        "plan rewrites, but has been stable since Spark 3.1.  There is almost never a "
        "valid reason to disable it in Spark 3.1+."
    )
    recommendation_template = (
        "Remove the ``false`` override or set both: "
        "``spark.sql.adaptive.enabled = true`` and "
        "``spark.sql.adaptive.skewJoin.enabled = true``."
    )
    before_example = '.config("spark.sql.adaptive.skewJoin.enabled", "false")'
    after_example = (
        '.config("spark.sql.adaptive.enabled", "true")\n'
        '.config("spark.sql.adaptive.skewJoin.enabled", "true")'
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#skew-join-optimization",
    ]
    estimated_impact = "Skewed partitions are not split; one task processes majority of data"
    effort_level = EffortLevel.CONFIG_ONLY

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        builder_calls = analyzer.find_method_calls("getOrCreate")
        if not builder_calls:
            return []

        findings: list[Finding] = []

        def _config_line(key: str) -> int:
            cfgs = [c for c in analyzer.find_spark_session_configs() if c.key == key]
            return cfgs[-1].end_line if cfgs else builder_calls[0].line

        aqe = analyzer.get_config_value("spark.sql.adaptive.enabled")
        if aqe is not None and aqe.strip().lower() == "false":
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    _config_line("spark.sql.adaptive.enabled"),
                    "spark.sql.adaptive.enabled = false disables AQE skew join protection",
                    config=config,
                )
            )

        skew = analyzer.get_config_value("spark.sql.adaptive.skewJoin.enabled")
        if skew is not None and skew.strip().lower() == "false":
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    _config_line("spark.sql.adaptive.skewJoin.enabled"),
                    "spark.sql.adaptive.skewJoin.enabled = false disables automatic"
                    " skew partition splitting",
                    config=config,
                )
            )

        return findings


# =============================================================================
# SPL-D05-004 — AQE skew threshold too high
# =============================================================================


@register_rule
class AqeSkewThresholdTooHighRule(ConfigRule):
    """SPL-D05-004: AQE skew detection thresholds set so high that skew goes undetected."""

    rule_id = "SPL-D05-004"
    name = "AQE skew threshold too high"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "AQE skew detection threshold exceeds 1 GB" " — large skewed partitions may not be split."
    )
    explanation = (
        "AQE's skew join optimisation uses two thresholds to detect skewed partitions:\n\n"
        "• ``spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`` (default 256 MB): "
        "  a partition is skewed if it is **both** above this size threshold…\n"
        "• ``spark.sql.adaptive.skewJoin.skewedPartitionFactor`` (default 5): "
        "  …**and** larger than ``factor × median partition size``.\n\n"
        "Setting the byte threshold above 1 GB means only partitions over 1 GB are "
        "considered, missing moderate skew (e.g. 800 MB vs 50 MB median) that already "
        "causes significant straggler behaviour.  Setting the factor above 10 requires "
        "extreme relative size differences before AQE intervenes."
    )
    recommendation_template = (
        "Keep ``skewedPartitionThresholdInBytes`` at or below 256 MB (default) and "
        "``skewedPartitionFactor`` at or below 5 (default). "
        "Lower values make AQE more aggressive at splitting skewed partitions."
    )
    before_example = '.config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "2g")'
    after_example = (
        '.config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")  # default'
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#skew-join-optimization",
    ]
    estimated_impact = "Moderate skew (< threshold) silently degrades stage performance"
    effort_level = EffortLevel.CONFIG_ONLY

    _THRESHOLD_KEY = "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"
    _FACTOR_KEY = "spark.sql.adaptive.skewJoin.skewedPartitionFactor"

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []

        raw_threshold = analyzer.get_config_value(self._THRESHOLD_KEY)
        if raw_threshold is not None:
            size_bytes = _parse_bytes(raw_threshold)
            if size_bytes is not None and size_bytes > _SKEW_THRESHOLD_MAX_BYTES:
                cfgs = [
                    c for c in analyzer.find_spark_session_configs() if c.key == self._THRESHOLD_KEY
                ]
                line = cfgs[-1].end_line if cfgs else 1
                size_gb = size_bytes / (1024**3)
                findings.append(
                    self.create_finding(
                        analyzer.filename,
                        line,
                        f"skewedPartitionThresholdInBytes = {raw_threshold!r}"
                        f" ({size_gb:.1f} GB) — only extreme skew is detected",
                        config=config,
                    )
                )

        raw_factor = analyzer.get_config_value(self._FACTOR_KEY)
        if raw_factor is not None:
            try:
                factor = float(raw_factor.strip())
                if factor > _SKEW_FACTOR_MAX:
                    cfgs = [
                        c
                        for c in analyzer.find_spark_session_configs()
                        if c.key == self._FACTOR_KEY
                    ]
                    line = cfgs[-1].end_line if cfgs else 1
                    findings.append(
                        self.create_finding(
                            analyzer.filename,
                            line,
                            f"skewedPartitionFactor = {factor:.0f}"
                            " — requires extreme size difference; moderate skew goes undetected",
                            config=config,
                        )
                    )
            except ValueError:
                pass

        return findings


# =============================================================================
# SPL-D05-005 — Missing salting pattern for known skewed keys
# =============================================================================


@register_rule
class MissingSaltingPatternRule(CodeRule):
    """SPL-D05-005: join() on a power-law distributed ID without any salting pattern."""

    rule_id = "SPL-D05-005"
    name = "Missing salting pattern for known skewed keys"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "join() on a potentially skewed ID column without rand()/explode() salting pattern."
    )
    explanation = (
        "Columns like ``user_id``, ``customer_id``, and ``product_id`` often follow "
        "power-law distributions in production: a handful of 'whale' users generate "
        "millions of events, and a handful of popular products receive most orders.  "
        "When joining a fact table on such a column, the reducer tasks for those popular "
        "IDs receive far more rows than average, causing straggler tasks.\n\n"
        "The classic remedy is *salting*: append a random bucket number (0–N) to the "
        "join key so that one popular ID is distributed across N tasks instead of one:\n\n"
        "1. Add ``salt = (rand() * N).cast('int')`` to the larger table.\n"
        "2. Explode the smaller table into N copies, each tagged with a salt value.\n"
        "3. Join on ``(original_key, salt)`` instead of ``original_key`` alone.\n\n"
        "AQE's skew join optimisation (``spark.sql.adaptive.skewJoin.enabled = true``) "
        "handles many cases automatically in Spark 3+ and should be the first option."
    )
    recommendation_template = (
        "Enable AQE skew join first: ``spark.sql.adaptive.skewJoin.enabled = true``. "
        "If skew persists, implement salting: add ``(rand()*N).cast('int')`` as a salt "
        "column and join on ``(key, salt)``."
    )
    before_example = "df.join(events, 'user_id')  # user_id may be highly skewed"
    after_example = (
        "# Salted join\n"
        "N = 10\n"
        "df_salted = df.withColumn('salt', (rand()*N).cast('int'))\n"
        "events_salted = events.withColumn('salt', explode(array([lit(i) for i in range(N)])))\n"
        "df_salted.join(events_salted, ['user_id', 'salt'])"
    )
    references = [
        "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
        "#skew-join-optimization",
    ]
    estimated_impact = "'Whale' keys cause one reducer to handle O(N) more rows than average"
    effort_level = EffortLevel.MAJOR_REFACTOR

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        join_calls = analyzer.find_method_calls("join")
        skewed_joins = [
            call
            for call in join_calls
            if any(k in _SKEW_ID_NAMES for k in _get_join_string_keys(call))
        ]
        if not skewed_joins:
            return []

        # Suppress entirely if the file already uses salting primitives
        if _file_uses_salting(analyzer):
            return []

        findings: list[Finding] = []
        for call in skewed_joins:
            skewed_keys = [k for k in _get_join_string_keys(call) if k in _SKEW_ID_NAMES]
            key_list = ", ".join(f"'{k}'" for k in skewed_keys)
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    f"join() on {key_list} may skew in production"
                    " — enable AQE skew join or implement salting",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D05-006 — Window function partitioned by skew-prone column
# =============================================================================


@register_rule
class WindowPartitionSkewRule(CodeRule):
    """SPL-D05-006: Window.partitionBy() on a low-cardinality column concentrates rows."""

    rule_id = "SPL-D05-006"
    name = "Window function partitioned by skew-prone column"
    dimension = _DIM
    default_severity = Severity.WARNING
    description = (
        "Window.partitionBy() on a low-cardinality column"
        " routes most rows to one task — risks OOM."
    )
    explanation = (
        "``Window.partitionBy('status')`` sends all rows with the same status to the "
        "same executor task.  If 90 % of data is ``status = 'active'``, that single "
        "task must buffer 90 % of the dataset in memory to compute the window function "
        "(rank, lag, sum, etc.).\n\n"
        "Unlike a skewed join (where AQE can split the skewed partition), window "
        "functions cannot be parallelised within a partition — the entire partition "
        "must be sorted and held in memory at once.  Skewed window partitions frequently "
        "cause executor OOM errors that are difficult to reproduce in staging.\n\n"
        "Remediation: add a secondary high-cardinality key to the ``partitionBy`` clause "
        "so that each window partition covers a smaller, bounded subset of the data."
    )
    recommendation_template = (
        "Add a secondary high-cardinality column to the ``partitionBy``: "
        "``Window.partitionBy('status', 'user_id').orderBy('ts')``. "
        "This limits each window partition to one user's rows rather than all rows "
        "with a given status."
    )
    before_example = "w = Window.partitionBy('status').orderBy('ts')"
    after_example = "w = Window.partitionBy('status', 'user_id').orderBy('ts')"
    references = [
        "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql"
        "/api/pyspark.sql.Window.partitionBy.html",
    ]
    estimated_impact = "Executor OOM for dominant partition; entire stage blocked by straggler"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("partitionBy"):
            # Skip write-context partitionBy (covered by D04 rules)
            if any(m in call.chain for m in _WRITE_MARKERS):
                continue

            str_keys = _get_string_args(call)
            if not str_keys:
                continue

            skew_keys = [k for k in str_keys if _is_skew_prone_col(k)]
            non_skew_keys = [k for k in str_keys if not _is_skew_prone_col(k)]
            if not skew_keys or non_skew_keys:
                # No skew-prone keys, or at least one high-cardinality key → OK
                continue

            key_list = ", ".join(f"'{k}'" for k in skew_keys)
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    f"Window.partitionBy({key_list}) on low-cardinality column"
                    " — risks OOM; add a secondary high-cardinality key",
                    config=config,
                )
            )
        return findings


# =============================================================================
# SPL-D05-007 — Null-heavy join key
# =============================================================================


@register_rule
class NullHeavyJoinKeyRule(CodeRule):
    """SPL-D05-007: join() on a nullable foreign-key column without prior null filtering."""

    rule_id = "SPL-D05-007"
    name = "Null-heavy join key"
    dimension = _DIM
    default_severity = Severity.INFO
    description = (
        "join() on a column name that commonly contains nulls"
        " without prior dropna()/isNotNull() filtering."
    )
    explanation = (
        "In Spark, ``NULL`` values are not equal to each other in most join types "
        "(they do not match in inner joins), but they *do* accumulate in the same "
        "hash partition because the hash of ``NULL`` is a constant.  When a join "
        "key column contains millions of null rows, all those rows are routed to the "
        "same reducer bucket, causing skew.\n\n"
        "Common nullable foreign keys include: ``parent_id`` (root records have no "
        "parent), ``manager_id`` (top-level managers), ``referred_by`` (organic users).  "
        "These often appear harmless in small datasets but cause severe skew at scale.\n\n"
        "Filtering out nulls **before** the join is the most effective fix:\n"
        "``df.filter(col('parent_id').isNotNull()).join(other, 'parent_id')``."
    )
    recommendation_template = (
        "Filter null join keys before the join: "
        "``df.filter(col('parent_id').isNotNull()).join(other, 'parent_id')``. "
        "If null keys must be preserved, use a left join and handle nulls in the result."
    )
    before_example = "df.join(other, 'parent_id')  # parent_id is null for root records"
    after_example = "df.filter(col('parent_id').isNotNull()).join(other, 'parent_id')"
    references = [
        "https://spark.apache.org/docs/latest/sql-ref-null-semantics.html",
    ]
    estimated_impact = "All null-key rows hash to same partition; straggler task on null bucket"
    effort_level = EffortLevel.MINOR_CODE_CHANGE

    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:  # noqa: D102
        if not analyzer.has_spark_imports():
            return []

        # Collect lines where null handling occurs (dropna, fillna, isNotNull)
        null_handled_lines: set[int] = set()
        for method in ("dropna", "fillna"):
            for c in analyzer.find_method_calls(method):
                null_handled_lines.add(c.line)
        for c in analyzer.find_method_calls("isNotNull"):
            null_handled_lines.add(c.line)
        # filter() calls that reference isNotNull in their receiver
        for c in analyzer.find_method_calls("filter"):
            if "isNotNull" in c.receiver_src:
                null_handled_lines.add(c.line)

        findings: list[Finding] = []
        for call in analyzer.find_method_calls("join"):
            keys = _get_join_string_keys(call)
            null_prone = [
                k
                for k in keys
                if k in _NULL_PRONE_NAMES or any(k.startswith(p) for p in _NULL_PRONE_PATTERNS)
            ]
            if not null_prone:
                continue

            # Suppress if null handling appears within 5 lines before the join
            has_handling = any(call.line - 5 <= nl < call.line for nl in null_handled_lines)
            if has_handling:
                continue

            key_list = ", ".join(f"'{k}'" for k in null_prone)
            findings.append(
                self.create_finding(
                    analyzer.filename,
                    call.line,
                    f"join() on {key_list} — nullable foreign key may cause"
                    " null-bucket skew; filter isNotNull() before joining",
                    config=config,
                )
            )
        return findings
