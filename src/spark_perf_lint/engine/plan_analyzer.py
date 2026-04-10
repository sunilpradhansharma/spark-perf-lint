"""Spark physical plan analyzer for Tier 3 deep-audit workflows.

Parses the text output of ``df.explain(True)`` (the extended physical plan)
into a structured tree of ``PlanNode`` objects, then extracts actionable
metrics and supports before/after optimization diffs.

**Tier 3 use only** — this module does *not* import PySpark and can run
offline against any plan text, but it is intended for use inside the
deep-audit notebook where a ``SparkSession`` is available.

Quickstart::

    from spark_perf_lint.engine.plan_analyzer import PlanAnalyzer

    plan_text = df.explain(extended=True, mode="formatted")
    analysis  = PlanAnalyzer.from_explain_text(plan_text)

    print(analysis.shuffle_count)          # number of Exchange nodes
    print(analysis.join_strategies)        # list of join type strings
    print(analysis.pushdown_filters)       # list of pushed-down predicates
    print(analysis.aqe_enabled)            # True/False
    print(analysis.codegen_stages)         # number of WholeStageCodegen nodes

    # Side-by-side diff of two plans
    diff = PlanAnalyzer.compare(before_text, after_text)
    print(diff.summary)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


# =============================================================================
# Regex catalogue — compiled once at module load
# =============================================================================

# Node header: optional leading whitespace / tree-drawing chars, then the
# operator name (word chars + spaces up to the first parenthesis or newline).
# Spark uses `:`, `:-`, `:  `, `+`, `-`, `|`, `\` as tree-drawing characters.
_RE_NODE_HEADER = re.compile(
    r"^(?P<indent>[\s+\-\\|:]*)"
    r"(?P<operator>[A-Z][A-Za-z0-9_]+)"
    r"(?P<rest>.*?)$"
)

# Exchange (shuffle): covers both plain Exchange and AQEShuffleRead wrappers.
_RE_EXCHANGE = re.compile(
    r"\b(Exchange|AQEShuffleRead|ShuffleQueryStage)\b", re.IGNORECASE
)

# Join type keywords (order matters — more specific first).
_RE_JOIN_TYPE = re.compile(
    r"\b("
    r"BroadcastHashJoin"
    r"|BroadcastNestedLoopJoin"
    r"|ShuffledHashJoin"
    r"|SortMergeJoin"
    r"|CartesianProduct"
    r"|HashJoin"
    r")\b"
)

# FileScan / Scan lines that list pushed filters.
_RE_FILESCAN = re.compile(r"\b(FileScan|Scan parquet|Scan orc|Scan csv|Scan json)\b", re.IGNORECASE)
_RE_PUSHED_FILTERS = re.compile(r"PushedFilters:\s*\[([^\]]*)\]")
_RE_PARTITION_FILTERS = re.compile(r"PartitionFilters:\s*\[([^\]]*)\]")
_RE_DATA_FILTERS = re.compile(r"DataFilters:\s*\[([^\]]*)\]")
_RE_OUTPUT_COLUMNS = re.compile(r"Output:\s*\[([^\]]*)\]")
_RE_READ_SCHEMA = re.compile(r"ReadSchema:\s*struct<([^>]*)>")

# Codegen stage marker.
_RE_CODEGEN = re.compile(r"\bWholeStageCodegen\b")

# AQE markers.
_RE_AQE = re.compile(
    r"\b(AdaptiveSparkPlan|AQEShuffleRead|QueryStageExec|ShuffleQueryStage|BroadcastQueryStage)\b"
)

# Repartition / coalesce nodes.
_RE_REPARTITION = re.compile(r"\b(Repartition|RepartitionByExpression|Coalesce)\b")

# Sort nodes.
_RE_SORT = re.compile(r"\bSort\b")

# Aggregate nodes.
_RE_AGGREGATE = re.compile(r"\b(HashAggregate|ObjectHashAggregate|SortAggregate)\b")

# Projection nodes.
_RE_PROJECT = re.compile(r"\bProject\b")

# Filter nodes.
_RE_FILTER = re.compile(r"\bFilter\b")

# Broadcast exchange specifically (used to separate broadcast from hash shuffle).
_RE_BROADCAST_EXCHANGE = re.compile(r"\bBroadcastExchange\b")

# Skew join indicator emitted by AQE.
_RE_SKEW_JOIN = re.compile(r"\bSkewJoin\b|\bskewJoin\s*=\s*true\b", re.IGNORECASE)

# Section delimiters in extended explain output.
_RE_SECTION_HEADER = re.compile(
    r"^==\s+(Parsed Logical Plan|Analyzed Logical Plan|Optimized Logical Plan|Physical Plan)\s+==",
    re.IGNORECASE,
)


# =============================================================================
# PlanNode — one line / operator in the parsed plan tree
# =============================================================================


@dataclass
class PlanNode:
    """A single operator node extracted from a Spark physical plan.

    Attributes:
        operator: Operator class name, e.g. ``"SortMergeJoin"``.
        indent_level: Depth in the tree (0 = root).  Derived from the number
            of leading tree-drawing characters on the source line.
        raw_line: The original (stripped) text line this node was parsed from.
        attributes: Key/value pairs parsed from the operator's argument text,
            e.g. ``{"PushedFilters": ["IsNotNull(id)"]}``.
        children: Child nodes in the plan tree (populated by the tree builder).
    """

    operator: str
    indent_level: int
    raw_line: str
    attributes: dict[str, Any] = field(default_factory=dict)
    children: list[PlanNode] = field(default_factory=list)

    def __repr__(self) -> str:
        return f"PlanNode(op={self.operator!r}, depth={self.indent_level}, children={len(self.children)})"


# =============================================================================
# PlanAnalysis — structured metrics derived from one plan
# =============================================================================


@dataclass
class PlanAnalysis:
    """Structured metrics derived from a single Spark physical plan.

    Attributes:
        nodes: All ``PlanNode`` objects in the plan, in source order.
        root: The root (top) node of the plan tree, or ``None`` if the plan
            could not be parsed.

        shuffle_count: Total number of ``Exchange`` / ``AQEShuffleRead`` nodes.
            Each represents a full shuffle across all executors.
        broadcast_exchange_count: Number of ``BroadcastExchange`` nodes.
            Does **not** count toward ``shuffle_count``.

        join_strategies: List of join-type strings found in the plan, e.g.
            ``["SortMergeJoin", "BroadcastHashJoin"]``.  Duplicates are kept
            so the caller can count occurrences.
        has_skew_join: ``True`` when AQE emitted a skew-join indicator.

        pushdown_filters: Predicates that were pushed down into the scan
            (``PushedFilters`` attribute on FileScan nodes).
        partition_filters: Partition-level filters applied at the scan.
        data_filters: Row-level filters applied inside the data source.
        column_pruning_ratio: Fraction of schema columns actually read.
            ``None`` when ``ReadSchema`` is absent from the plan.

        codegen_stages: Number of ``WholeStageCodegen`` blocks.
        aqe_enabled: ``True`` when any AQE node is present in the plan.

        repartition_count: Number of explicit ``Repartition`` /
            ``RepartitionByExpression`` nodes.
        sort_count: Number of ``Sort`` nodes.
        aggregate_operators: List of aggregate operator names found.

        plan_text: The physical-plan section of the original explain output.
        full_explain_text: The complete explain string passed to the parser.
    """

    nodes: list[PlanNode] = field(default_factory=list)
    root: PlanNode | None = None

    shuffle_count: int = 0
    broadcast_exchange_count: int = 0

    join_strategies: list[str] = field(default_factory=list)
    has_skew_join: bool = False

    pushdown_filters: list[str] = field(default_factory=list)
    partition_filters: list[str] = field(default_factory=list)
    data_filters: list[str] = field(default_factory=list)
    column_pruning_ratio: float | None = None

    codegen_stages: int = 0
    aqe_enabled: bool = False

    repartition_count: int = 0
    sort_count: int = 0
    aggregate_operators: list[str] = field(default_factory=list)

    plan_text: str = ""
    full_explain_text: str = ""

    # ------------------------------------------------------------------
    # Convenience summaries
    # ------------------------------------------------------------------

    @property
    def dominant_join(self) -> str | None:
        """Return the most frequently appearing join strategy, or ``None``.

        When multiple join types exist, the one with the highest count wins.
        Ties are broken by the order they first appear in the plan.

        Returns:
            Join type string such as ``"SortMergeJoin"``, or ``None`` when
            the plan contains no joins.
        """
        if not self.join_strategies:
            return None
        return max(set(self.join_strategies), key=self.join_strategies.count)

    @property
    def has_full_shuffle(self) -> bool:
        """``True`` when at least one non-broadcast Exchange is present."""
        return self.shuffle_count > 0

    def summary(self) -> dict[str, Any]:
        """Return a concise, JSON-serialisable summary of the plan metrics.

        Returns:
            A flat dictionary covering all key metrics.
        """
        return {
            "shuffle_count": self.shuffle_count,
            "broadcast_exchange_count": self.broadcast_exchange_count,
            "join_strategies": self.join_strategies,
            "dominant_join": self.dominant_join,
            "has_skew_join": self.has_skew_join,
            "codegen_stages": self.codegen_stages,
            "aqe_enabled": self.aqe_enabled,
            "repartition_count": self.repartition_count,
            "sort_count": self.sort_count,
            "aggregate_operators": self.aggregate_operators,
            "pushdown_filter_count": len(self.pushdown_filters),
            "partition_filter_count": len(self.partition_filters),
            "column_pruning_ratio": self.column_pruning_ratio,
            "node_count": len(self.nodes),
        }


# =============================================================================
# PlanDiff — comparison of two PlanAnalysis objects
# =============================================================================


@dataclass
class PlanDiff:
    """Side-by-side comparison of a *before* and *after* plan analysis.

    Attributes:
        before: Analysis of the original (unoptimised) plan.
        after: Analysis of the optimised plan.
        shuffle_delta: Change in shuffle count (negative = improvement).
        join_strategy_changed: ``True`` when the dominant join type differs.
        codegen_delta: Change in WholeStageCodegen stage count.
        aqe_introduced: ``True`` when AQE was absent before and present after.
        new_pushdown_filters: Filters pushed down *after* but not before.
        removed_pushdown_filters: Filters present before but absent after.
        improvements: Human-readable list of improvements detected.
        regressions: Human-readable list of potential regressions detected.
    """

    before: PlanAnalysis
    after: PlanAnalysis
    shuffle_delta: int = 0
    join_strategy_changed: bool = False
    codegen_delta: int = 0
    aqe_introduced: bool = False
    new_pushdown_filters: list[str] = field(default_factory=list)
    removed_pushdown_filters: list[str] = field(default_factory=list)
    improvements: list[str] = field(default_factory=list)
    regressions: list[str] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        """Return a JSON-serialisable summary of the diff.

        Returns:
            A flat dictionary with before/after metrics and delta fields.
        """
        return {
            "shuffle_before": self.before.shuffle_count,
            "shuffle_after": self.after.shuffle_count,
            "shuffle_delta": self.shuffle_delta,
            "join_before": self.before.dominant_join,
            "join_after": self.after.dominant_join,
            "join_strategy_changed": self.join_strategy_changed,
            "codegen_before": self.before.codegen_stages,
            "codegen_after": self.after.codegen_stages,
            "codegen_delta": self.codegen_delta,
            "aqe_introduced": self.aqe_introduced,
            "new_pushdown_filters": self.new_pushdown_filters,
            "removed_pushdown_filters": self.removed_pushdown_filters,
            "improvements": self.improvements,
            "regressions": self.regressions,
        }


# =============================================================================
# PlanAnalyzer — the main entry point
# =============================================================================


class PlanAnalyzer:
    """Parses and analyses Spark physical plan text.

    Designed to be called with the string output of ``df.explain(True)`` or
    ``df.explain(extended=True, mode="formatted")``.

    The parser is intentionally lenient — it degrades gracefully on plan
    formats it does not fully understand rather than raising exceptions.

    Usage::

        analysis = PlanAnalyzer.from_explain_text(df.explain(True))
        diff     = PlanAnalyzer.compare(before_explain, after_explain)
    """

    # ------------------------------------------------------------------
    # Public factory methods
    # ------------------------------------------------------------------

    @classmethod
    def from_explain_text(cls, explain_text: str) -> PlanAnalysis:
        """Parse an ``explain(True)`` string and return a ``PlanAnalysis``.

        Handles both the legacy multi-section format (``== Physical Plan ==``
        header) and the newer ``mode="formatted"`` output.

        Args:
            explain_text: The full string returned by ``df.explain(True)`` or
                captured from the console.  May include the logical plan
                sections — only the physical plan section is analysed.

        Returns:
            A fully populated ``PlanAnalysis`` instance.
        """
        analysis = PlanAnalysis(full_explain_text=explain_text)
        plan_text = cls._extract_physical_plan(explain_text)
        analysis.plan_text = plan_text

        lines = [ln for ln in plan_text.splitlines() if ln.strip()]
        nodes = cls._parse_nodes(lines)
        analysis.nodes = nodes
        analysis.root = nodes[0] if nodes else None

        # ── Accumulate metrics ──────────────────────────────────────────
        for node in nodes:
            cls._accumulate_node_metrics(node, analysis)

        # ── Column pruning ratio (requires ReadSchema + Output) ─────────
        analysis.column_pruning_ratio = cls._compute_pruning_ratio(plan_text)

        return analysis

    @classmethod
    def from_dataframe(cls, df: Any) -> PlanAnalysis:
        """Capture the plan directly from a PySpark ``DataFrame``.

        Args:
            df: A ``pyspark.sql.DataFrame`` instance.

        Returns:
            ``PlanAnalysis`` built from ``df.explain(extended=True)``.
        """
        import io
        import contextlib

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            df.explain(extended=True)
        return cls.from_explain_text(buf.getvalue())

    @classmethod
    def compare(
        cls,
        before_text: str,
        after_text: str,
    ) -> PlanDiff:
        """Compare two explain texts and return a structured diff.

        Args:
            before_text: ``explain(True)`` output from the original query.
            after_text: ``explain(True)`` output from the optimised query.

        Returns:
            ``PlanDiff`` with delta metrics and human-readable improvement /
            regression notes.
        """
        before = cls.from_explain_text(before_text)
        after = cls.from_explain_text(after_text)
        return cls._build_diff(before, after)

    @classmethod
    def compare_dataframes(cls, before_df: Any, after_df: Any) -> PlanDiff:
        """Compare plans from two PySpark DataFrames.

        Args:
            before_df: The original (unoptimised) ``DataFrame``.
            after_df: The optimised ``DataFrame``.

        Returns:
            ``PlanDiff`` produced by comparing their explain outputs.
        """
        before = cls.from_dataframe(before_df)
        after = cls.from_dataframe(after_df)
        return cls._build_diff(before, after)

    # ------------------------------------------------------------------
    # Plan text extraction
    # ------------------------------------------------------------------

    @classmethod
    def _extract_physical_plan(cls, explain_text: str) -> str:
        """Isolate the physical plan section from a full explain output.

        For extended explain output the physical plan follows the
        ``== Physical Plan ==`` header.  For ``mode="formatted"`` output the
        text does not have section headers, so the whole text is used.

        Args:
            explain_text: Raw explain string.

        Returns:
            Just the physical plan portion, stripped of section headers.
        """
        lines = explain_text.splitlines()
        physical_start: int | None = None
        next_section_after: int | None = None

        for i, line in enumerate(lines):
            m = _RE_SECTION_HEADER.match(line.strip())
            if m:
                section = m.group(1).lower()
                if "physical plan" in section:
                    physical_start = i + 1
                elif physical_start is not None:
                    # A section header after the physical plan section
                    next_section_after = i
                    break

        if physical_start is None:
            # No section headers found — assume entire text is the physical plan
            return explain_text.strip()

        end = next_section_after if next_section_after is not None else len(lines)
        return "\n".join(lines[physical_start:end]).strip()

    # ------------------------------------------------------------------
    # Node parsing
    # ------------------------------------------------------------------

    @classmethod
    def _parse_nodes(cls, lines: list[str]) -> list[PlanNode]:
        """Convert plan text lines into a flat list of ``PlanNode`` objects.

        Each line is matched against ``_RE_NODE_HEADER``.  Lines that do not
        match an operator are attached as ``raw_line`` metadata to the most
        recently seen node (capturing multi-line attribute blocks).

        Args:
            lines: Non-empty lines from the physical plan section.

        Returns:
            Flat list of ``PlanNode`` objects in source order.
        """
        nodes: list[PlanNode] = []
        last_node: PlanNode | None = None

        for raw in lines:
            m = _RE_NODE_HEADER.match(raw)
            if not m:
                # Continuation line — append raw text to last node
                if last_node is not None:
                    last_node.raw_line += " " + raw.strip()
                continue

            indent_str = m.group("indent")
            operator = m.group("operator")
            rest = m.group("rest")

            # Indent level heuristic: count tree-drawing characters
            indent_level = len(indent_str.replace("|", " ").rstrip())

            node = PlanNode(
                operator=operator,
                indent_level=indent_level,
                raw_line=raw.strip(),
                attributes=cls._parse_attributes(rest),
            )
            nodes.append(node)
            last_node = node

        cls._build_tree(nodes)
        return nodes

    @classmethod
    def _build_tree(cls, nodes: list[PlanNode]) -> None:
        """Wire up parent→child relationships in the flat node list.

        Uses a stack to track open parent nodes: when a node has a greater
        indent level than the stack top, it becomes a child; otherwise the
        stack is popped until the correct parent depth is found.

        The tree structure is embedded into each node's ``children`` list
        in-place; no new nodes are created.

        Args:
            nodes: Flat list of ``PlanNode`` in source order.
        """
        # Stack holds (indent_level, node) tuples
        stack: list[tuple[int, PlanNode]] = []

        for node in nodes:
            # Pop nodes at the same or greater indent level
            while stack and stack[-1][0] >= node.indent_level:
                stack.pop()

            if stack:
                stack[-1][1].children.append(node)

            stack.append((node.indent_level, node))

    @classmethod
    def _parse_attributes(cls, text: str) -> dict[str, Any]:
        """Extract key-value attributes from the argument portion of a plan line.

        Parses patterns like:
        - ``[PushedFilters: [IsNotNull(id), ...], ReadSchema: struct<...>]``
        - ``(id#0L, ...) [codegen id : 3]``

        Args:
            text: Everything on the plan line after the operator name.

        Returns:
            Dictionary of extracted attributes.  Values are strings or
            lists of strings.
        """
        attrs: dict[str, Any] = {}

        # Pushed / partition / data filters
        for pattern, key in (
            (_RE_PUSHED_FILTERS, "PushedFilters"),
            (_RE_PARTITION_FILTERS, "PartitionFilters"),
            (_RE_DATA_FILTERS, "DataFilters"),
        ):
            m = pattern.search(text)
            if m:
                raw = m.group(1).strip()
                attrs[key] = [f.strip() for f in raw.split(",") if f.strip()] if raw else []

        # Output columns
        m = _RE_OUTPUT_COLUMNS.search(text)
        if m:
            raw = m.group(1).strip()
            attrs["Output"] = [c.strip() for c in raw.split(",") if c.strip()] if raw else []

        # ReadSchema
        m = _RE_READ_SCHEMA.search(text)
        if m:
            raw = m.group(1).strip()
            attrs["ReadSchema"] = [f.strip() for f in raw.split(",") if f.strip()] if raw else []

        # Codegen id
        codegen_match = re.search(r"\[codegen\s+id\s*:\s*(\d+)\]", text)
        if codegen_match:
            attrs["codegen_id"] = int(codegen_match.group(1))

        return attrs

    # ------------------------------------------------------------------
    # Metric accumulation
    # ------------------------------------------------------------------

    @classmethod
    def _accumulate_node_metrics(cls, node: PlanNode, analysis: PlanAnalysis) -> None:
        """Update *analysis* counters/lists based on a single *node*.

        Args:
            node: A ``PlanNode`` whose operator and attributes are inspected.
            analysis: The ``PlanAnalysis`` being built (mutated in-place).
        """
        op = node.operator
        raw = node.raw_line

        # Shuffle exchanges (exclude broadcast)
        if _RE_EXCHANGE.search(op) and not _RE_BROADCAST_EXCHANGE.search(op):
            analysis.shuffle_count += 1

        # Broadcast exchanges
        if _RE_BROADCAST_EXCHANGE.search(op):
            analysis.broadcast_exchange_count += 1

        # Join strategies
        jm = _RE_JOIN_TYPE.search(raw)
        if jm:
            analysis.join_strategies.append(jm.group(1))

        # Skew join
        if _RE_SKEW_JOIN.search(raw):
            analysis.has_skew_join = True

        # AQE
        if _RE_AQE.search(op):
            analysis.aqe_enabled = True

        # WholeStageCodegen
        if _RE_CODEGEN.search(op):
            analysis.codegen_stages += 1

        # Repartition
        if _RE_REPARTITION.search(op):
            analysis.repartition_count += 1

        # Sort
        if _RE_SORT.search(op):
            analysis.sort_count += 1

        # Aggregates
        am = _RE_AGGREGATE.search(op)
        if am:
            analysis.aggregate_operators.append(am.group(1))

        # FileScan — collect filter metadata
        if _RE_FILESCAN.search(raw):
            pf = node.attributes.get("PushedFilters", [])
            analysis.pushdown_filters.extend(pf)
            partf = node.attributes.get("PartitionFilters", [])
            analysis.partition_filters.extend(partf)
            dataf = node.attributes.get("DataFilters", [])
            analysis.data_filters.extend(dataf)

    # ------------------------------------------------------------------
    # Column pruning ratio
    # ------------------------------------------------------------------

    # Inline projection columns: e.g. "FileScan parquet [id#1L, value#3]"
    _RE_SCAN_PROJECTION = re.compile(r"(?:FileScan|Scan\s+\w+)\s+[^\[]*\[([^\]]+)\]")

    @classmethod
    def _compute_pruning_ratio(cls, plan_text: str) -> float | None:
        """Estimate the fraction of schema columns actually read.

        Compares the number of fields in ``ReadSchema`` against the number
        of output columns projected from the scan.  The projected columns are
        taken from the inline bracket notation (``FileScan parquet [col1,col2]``)
        or, when present, an explicit ``Output: [...]`` attribute.  When
        multiple scans are present, the per-scan ratios are averaged.

        Args:
            plan_text: The physical plan section (not the full explain).

        Returns:
            A float in ``[0.0, 1.0]`` (1.0 = all columns read; lower = better
            pruning), or ``None`` when neither attribute is found.
        """
        ratios: list[float] = []

        for line in plan_text.splitlines():
            if not _RE_FILESCAN.search(line):
                continue

            schema_m = _RE_READ_SCHEMA.search(line)
            if not schema_m:
                continue

            schema_cols = len(
                [c for c in schema_m.group(1).split(",") if c.strip()]
            )
            if schema_cols == 0:
                continue

            # Try explicit "Output: [...]" first, then inline "[col#id, ...]"
            output_m = _RE_OUTPUT_COLUMNS.search(line)
            if output_m:
                output_cols = len(
                    [c for c in output_m.group(1).split(",") if c.strip()]
                )
            else:
                proj_m = cls._RE_SCAN_PROJECTION.search(line)
                if not proj_m:
                    continue
                output_cols = len(
                    [c for c in proj_m.group(1).split(",") if c.strip()]
                )

            ratios.append(min(1.0, output_cols / schema_cols))

        if not ratios:
            return None
        return round(sum(ratios) / len(ratios), 3)

    # ------------------------------------------------------------------
    # Diff builder
    # ------------------------------------------------------------------

    @classmethod
    def _build_diff(cls, before: PlanAnalysis, after: PlanAnalysis) -> PlanDiff:
        """Compute a ``PlanDiff`` from two ``PlanAnalysis`` objects.

        Args:
            before: Analysis of the original plan.
            after: Analysis of the optimised plan.

        Returns:
            Populated ``PlanDiff`` with delta fields and human-readable notes.
        """
        diff = PlanDiff(
            before=before,
            after=after,
            shuffle_delta=after.shuffle_count - before.shuffle_count,
            join_strategy_changed=before.dominant_join != after.dominant_join,
            codegen_delta=after.codegen_stages - before.codegen_stages,
            aqe_introduced=(not before.aqe_enabled and after.aqe_enabled),
        )

        # Pushdown filter changes
        before_filters = set(before.pushdown_filters)
        after_filters = set(after.pushdown_filters)
        diff.new_pushdown_filters = sorted(after_filters - before_filters)
        diff.removed_pushdown_filters = sorted(before_filters - after_filters)

        # ── Improvements ────────────────────────────────────────────────
        if diff.shuffle_delta < 0:
            diff.improvements.append(
                f"Shuffle count reduced by {abs(diff.shuffle_delta)} "
                f"({before.shuffle_count} → {after.shuffle_count})"
            )

        if diff.join_strategy_changed:
            before_j = before.dominant_join or "none"
            after_j = after.dominant_join or "none"
            # BroadcastHashJoin is generally faster than SortMergeJoin
            if "Broadcast" in after_j and "Broadcast" not in before_j:
                diff.improvements.append(
                    f"Join strategy improved: {before_j} → {after_j} "
                    f"(broadcast avoids shuffle)"
                )
            else:
                diff.improvements.append(
                    f"Join strategy changed: {before_j} → {after_j}"
                )

        if diff.aqe_introduced:
            diff.improvements.append("AQE enabled: plan can adapt to runtime statistics")

        if diff.new_pushdown_filters:
            diff.improvements.append(
                f"{len(diff.new_pushdown_filters)} new filter(s) pushed down to scan"
            )

        pruning_before = before.column_pruning_ratio
        pruning_after = after.column_pruning_ratio
        if (
            pruning_before is not None
            and pruning_after is not None
            and pruning_after < pruning_before - 0.05
        ):
            diff.improvements.append(
                f"Column pruning improved: "
                f"{pruning_before:.1%} → {pruning_after:.1%} columns read"
            )

        if diff.codegen_delta > 0:
            diff.improvements.append(
                f"WholeStageCodegen stages increased by {diff.codegen_delta} "
                f"(more operations JIT-compiled)"
            )

        # ── Regressions ─────────────────────────────────────────────────
        if diff.shuffle_delta > 0:
            diff.regressions.append(
                f"Shuffle count increased by {diff.shuffle_delta} "
                f"({before.shuffle_count} → {after.shuffle_count})"
            )

        if diff.removed_pushdown_filters:
            diff.regressions.append(
                f"{len(diff.removed_pushdown_filters)} filter(s) no longer pushed down to scan"
            )

        if diff.codegen_delta < 0:
            diff.regressions.append(
                f"WholeStageCodegen stages decreased by {abs(diff.codegen_delta)} "
                f"(fewer operations JIT-compiled)"
            )

        after_sort = after.sort_count
        before_sort = before.sort_count
        if after_sort > before_sort:
            diff.regressions.append(
                f"Sort count increased by {after_sort - before_sort} "
                f"({before_sort} → {after_sort})"
            )

        return diff

    # ------------------------------------------------------------------
    # Convenience repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:  # pragma: no cover
        return "PlanAnalyzer()"
