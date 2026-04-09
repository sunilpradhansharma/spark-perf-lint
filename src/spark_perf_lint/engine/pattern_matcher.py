"""High-level pattern matching over pre-analysed AST facts.

``PatternMatcher`` sits between ``ASTAnalyzer`` (raw AST facts) and
individual rule modules (domain logic).  It answers questions like:

- "Does a join ever appear after a filter on the same receiver?"
- "Is ``withColumn`` called inside a loop?"
- "Is this DataFrame cached more than once without an unpersist?"
- "Is ``spark.sql.adaptive.enabled`` missing or set to the wrong value?"

Rules import ``PatternMatcher`` and call the high-level match methods
rather than writing their own AST traversal logic.

Architecture notes
------------------
- ``PatternMatcher`` accepts a fully-initialised ``ASTAnalyzer`` and adds
  *no* re-traversal — it only filters and correlates the pre-built result
  sets.
- Every public method returns a ``list[PatternMatch]``.  An empty list
  means no match.
- ``PatternMatch`` intentionally holds both the matched ``MethodCallInfo``
  nodes *and* a free-form ``context`` dict so rules can embed rich
  information in their ``Finding`` without needing to re-query the
  analyser.
- DataFrame lineage tracking is **best-effort**: it operates on
  variable-name strings, not dataflow graphs.  It will miss assignments
  through comprehensions, function returns, or conditional branches — that
  is an acceptable trade-off for a static linter.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from typing import Any

from spark_perf_lint.engine.ast_analyzer import (
    AssignmentInfo,
    ASTAnalyzer,
    MethodCallInfo,
    SparkConfigInfo,
)

# =============================================================================
# PatternMatch — result container
# =============================================================================


@dataclass
class PatternMatch:
    """The result of a single pattern-matcher check.

    Attributes:
        matched: Whether the pattern was found.
        calls: The ``MethodCallInfo`` nodes involved in the match, in source
            order.  Empty when ``matched`` is ``False``.
        assignments: ``AssignmentInfo`` nodes involved (e.g. for lineage
            matches).
        configs: ``SparkConfigInfo`` entries involved (for config matches).
        context: Free-form dictionary of additional information the rule can
            use when building its ``Finding``.  Keys are strings; values are
            anything JSON-serialisable.
    """

    matched: bool
    calls: list[MethodCallInfo] = field(default_factory=list)
    assignments: list[AssignmentInfo] = field(default_factory=list)
    configs: list[SparkConfigInfo] = field(default_factory=list)
    context: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Convenience constructors
    # ------------------------------------------------------------------

    @classmethod
    def yes(
        cls,
        calls: list[MethodCallInfo] | None = None,
        assignments: list[AssignmentInfo] | None = None,
        configs: list[SparkConfigInfo] | None = None,
        **ctx: Any,
    ) -> PatternMatch:
        """Return a matched result with optional nodes and context."""
        return cls(
            matched=True,
            calls=calls or [],
            assignments=assignments or [],
            configs=configs or [],
            context=dict(ctx),
        )

    @classmethod
    def no(cls) -> PatternMatch:
        """Return a non-matched result (canonical false result)."""
        return cls(matched=False)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def first_line(self) -> int | None:
        """Line number of the first matched call, or ``None``."""
        return self.calls[0].line if self.calls else None

    def __bool__(self) -> bool:
        return self.matched


# =============================================================================
# DataFrame lineage node
# =============================================================================


@dataclass
class DataFrameNode:
    """One node in the best-effort DataFrame lineage graph.

    Attributes:
        name: Variable name (e.g. ``"df2"``).
        created_by: Method that produced this DataFrame (e.g. ``"join"``).
        source_names: Variable names on the RHS of the assignment
            (best-effort; populated from ``AssignmentInfo.value_src``).
        line: Source line where the assignment appears.
        use_count: Number of times this variable name appears as a
            receiver in subsequent method calls.
        cached: Whether ``.cache()`` or ``.persist()`` was called on this
            variable.
        unpersisted: Whether ``.unpersist()`` was called.
        cache_line: Line of the cache/persist call, or ``None``.
    """

    name: str
    created_by: str  # the producing method, e.g. "join", "filter", "read"
    source_names: list[str]
    line: int
    use_count: int = 0
    cached: bool = False
    unpersisted: bool = False
    cache_line: int | None = None


# =============================================================================
# PatternMatcher
# =============================================================================


class PatternMatcher:
    """High-level pattern matching over ``ASTAnalyzer`` results.

    Instantiate once per file and call the match methods as needed.
    All methods are pure reads; nothing is mutated.

    Example::

        analyzer = ASTAnalyzer.from_file("etl.py")
        matcher  = PatternMatcher(analyzer)

        for m in matcher.find_method_in_loop("withColumn"):
            print(m.context["loop_line"], m.calls[0].line)

        for m in matcher.find_missing_config("spark.sql.adaptive.enabled", "true"):
            print("AQE not enabled")
    """

    def __init__(self, analyzer: ASTAnalyzer) -> None:
        self._a = analyzer
        # Build lineage graph lazily on first use
        self._lineage: dict[str, DataFrameNode] | None = None

    # ------------------------------------------------------------------
    # 1. Chained method sequence patterns
    # ------------------------------------------------------------------

    def find_chained_sequence(
        self,
        first: str,
        second: str,
    ) -> list[PatternMatch]:
        """Find calls to *second* that appear directly after *first* in a chain.

        Detects ``df.first(...).second(...)`` by checking whether the
        ``MethodCallInfo.chain`` for *second* contains *first* immediately
        before it.

        Args:
            first: Method name that should precede *second*.
            second: Method name to search for.

        Returns:
            One ``PatternMatch`` per occurrence of the ``first→second``
            sequence, containing the ``second`` call as ``calls[0]`` and
            context keys ``first_method``, ``second_method``.
        """
        results: list[PatternMatch] = []
        for call in self._a.find_method_calls(second):
            chain = call.chain
            try:
                idx = chain.index(second)
            except ValueError:
                continue
            if idx > 0 and chain[idx - 1] == first:
                results.append(
                    PatternMatch.yes(
                        calls=[call],
                        first_method=first,
                        second_method=second,
                        chain=chain,
                    )
                )
        return results

    def find_method_before_without(
        self,
        target: str,
        required_prior: str,
        window: int = 50,
    ) -> list[PatternMatch]:
        """Find calls to *target* that are NOT preceded by *required_prior*.

        Scans source-order calls.  For each occurrence of *target*, looks
        back up to *window* lines for any call to *required_prior* on the
        same or a related receiver.

        Useful for rules like "join without prior filter" or "cache without
        prior repartition".

        Args:
            target: Method the rule fires on.
            required_prior: Method that should precede *target*.
            window: Number of source lines to look back.

        Returns:
            Matches where *required_prior* was absent in the lookback window.
        """
        all_prior_lines = {c.line for c in self._a.find_method_calls(required_prior)}
        results: list[PatternMatch] = []
        for call in self._a.find_method_calls(target):
            lookback_start = max(1, call.line - window)
            found_prior = any(lookback_start <= line < call.line for line in all_prior_lines)
            if not found_prior:
                results.append(
                    PatternMatch.yes(
                        calls=[call],
                        target_method=target,
                        required_prior=required_prior,
                        window_lines=window,
                    )
                )
        return results

    def find_method_after(
        self,
        trigger: str,
        following: str,
        window: int = 50,
    ) -> list[PatternMatch]:
        """Find calls to *trigger* that are followed by *following* nearby.

        Useful for rules like "join followed by filter" (filter should come
        first) or "cache followed by write" (pointless cache).

        Args:
            trigger: Method that should trigger the rule.
            following: Method that appears *after* the trigger.
            window: Number of source lines to look forward.

        Returns:
            Matches where *following* was found after *trigger*.
        """
        following_calls = self._a.find_method_calls(following)
        results: list[PatternMatch] = []
        for trig_call in self._a.find_method_calls(trigger):
            nearby = [
                c for c in following_calls if trig_call.line < c.line <= trig_call.line + window
            ]
            for follow_call in nearby:
                results.append(
                    PatternMatch.yes(
                        calls=[trig_call, follow_call],
                        trigger_method=trigger,
                        following_method=following,
                        trigger_line=trig_call.line,
                        following_line=follow_call.line,
                    )
                )
        return results

    def find_cache_without_unpersist(self) -> list[PatternMatch]:
        """Find DataFrames that are cached but never unpersisted.

        Uses lineage data to correlate cache/persist calls with their
        corresponding unpersist calls.  A DataFrame that is cached but
        has no unpersist call in the same module is flagged.

        Returns:
            One match per variable that is cached without unpersist.
        """
        cache_calls = self._a.find_method_calls("cache") + self._a.find_method_calls("persist")
        unpersist_calls = {c.receiver_src for c in self._a.find_method_calls("unpersist")}

        results: list[PatternMatch] = []
        for call in cache_calls:
            receiver = call.receiver_src
            if receiver not in unpersist_calls:
                results.append(
                    PatternMatch.yes(
                        calls=[call],
                        cached_receiver=receiver,
                        cache_method=call.method_name,
                    )
                )
        return results

    def find_cache_used_once(self) -> list[PatternMatch]:
        """Find DataFrames that are cached but their receiver appears only once.

        A cache call is flagged when the receiver expression appears as a
        receiver in exactly one subsequent method call (the data would be
        computed and immediately evicted).

        Returns:
            One match per cache/persist call whose receiver is only used once.
        """
        all_calls = self._a.find_all_method_calls()
        results: list[PatternMatch] = []

        for cache_call in self._a.find_method_calls("cache") + self._a.find_method_calls("persist"):
            receiver = cache_call.receiver_src
            # Count calls on the same receiver after the cache line
            subsequent_uses = [
                c for c in all_calls if c.receiver_src == receiver and c.line >= cache_call.line
            ]
            if len(subsequent_uses) <= 1:
                results.append(
                    PatternMatch.yes(
                        calls=[cache_call],
                        cached_receiver=receiver,
                        use_count=len(subsequent_uses),
                    )
                )
        return results

    # ------------------------------------------------------------------
    # 2. Config patterns
    # ------------------------------------------------------------------

    def find_missing_config(
        self,
        key: str,
        expected_value: str | None = None,
    ) -> list[PatternMatch]:
        """Check whether a Spark config key is absent or set to a wrong value.

        Args:
            key: Spark configuration key, e.g.
                ``"spark.sql.adaptive.enabled"``.
            expected_value: If given, also flag when the key is present but
                its value differs (case-insensitive comparison).
                Pass ``None`` to check for presence only.

        Returns:
            A single-element list containing a match if the config is
            missing or wrong; an empty list if the config is correctly set.
        """
        all_configs = self._a.find_spark_session_configs()
        found = [c for c in all_configs if c.key == key]

        if not found:
            return [
                PatternMatch.yes(
                    configs=[],
                    config_key=key,
                    expected_value=expected_value,
                    actual_value=None,
                    reason="missing",
                )
            ]

        if expected_value is not None:
            last = found[-1]  # last assignment wins
            if last.value is None or last.value.lower() != expected_value.lower():
                return [
                    PatternMatch.yes(
                        configs=[last],
                        config_key=key,
                        expected_value=expected_value,
                        actual_value=last.value,
                        reason="wrong_value",
                    )
                ]

        return []

    def find_conflicting_configs(
        self,
        key: str,
    ) -> list[PatternMatch]:
        """Find cases where *key* is set multiple times with different values.

        Args:
            key: Spark configuration key to inspect.

        Returns:
            A single-element list with all occurrences if conflicting values
            are found; an empty list otherwise.
        """
        all_configs = [c for c in self._a.find_spark_session_configs() if c.key == key]
        if len(all_configs) < 2:
            return []

        values = {c.value for c in all_configs}
        if len(values) > 1:
            return [
                PatternMatch.yes(
                    configs=all_configs,
                    config_key=key,
                    distinct_values=sorted(str(v) for v in values),
                    occurrences=len(all_configs),
                )
            ]
        return []

    def find_config_below_threshold(
        self,
        key: str,
        min_value: float,
        unit_multipliers: dict[str, float] | None = None,
    ) -> list[PatternMatch]:
        """Find a numeric config key whose value is below *min_value*.

        Supports common Spark size suffixes: ``"g"``/``"gb"``, ``"m"``/``"mb"``,
        ``"k"``/``"kb"``.  The comparison is done in the *base unit* implied
        by *min_value*.

        Args:
            key: Spark configuration key.
            min_value: Minimum acceptable value (in the base unit).
            unit_multipliers: Custom suffix → multiplier mapping.  If
                ``None``, the default Spark size suffixes are used.

        Returns:
            A single-element match list if the value is below the threshold.
        """
        _default_multipliers: dict[str, float] = {
            "g": 1024.0,
            "gb": 1024.0,
            "m": 1.0,
            "mb": 1.0,
            "k": 1.0 / 1024.0,
            "kb": 1.0 / 1024.0,
        }
        multipliers = unit_multipliers or _default_multipliers

        config_value = self._a.get_config_value(key)
        if config_value is None:
            return []

        numeric = _parse_size_value(config_value, multipliers)
        if numeric is not None and numeric < min_value:
            configs = [c for c in self._a.find_spark_session_configs() if c.key == key]
            return [
                PatternMatch.yes(
                    configs=configs[-1:],
                    config_key=key,
                    actual_value=config_value,
                    min_value=min_value,
                    parsed_value=numeric,
                )
            ]
        return []

    # ------------------------------------------------------------------
    # 3. Anti-pattern templates
    # ------------------------------------------------------------------

    def find_method_in_loop(self, method_name: str) -> list[PatternMatch]:
        """Find calls to *method_name* that occur inside a for/while loop.

        Args:
            method_name: The method to search for inside loop bodies.

        Returns:
            One ``PatternMatch`` per loop that contains a call to
            *method_name*, with context keys ``loop_type``, ``loop_line``,
            ``occurrence_count``.
        """
        results: list[PatternMatch] = []
        for loop, count in self._a.find_method_calls_in_loops(method_name):
            # Find the actual MethodCallInfo nodes inside the loop body
            loop_start = loop.line
            loop_end = getattr(loop.node, "end_lineno", loop.line + 9999)
            inner_calls = [
                c
                for c in self._a.find_method_calls(method_name)
                if loop_start <= c.line <= loop_end
            ]
            results.append(
                PatternMatch.yes(
                    calls=inner_calls,
                    loop_type=loop.loop_type,
                    loop_line=loop.line,
                    occurrence_count=count,
                )
            )
        return results

    def find_method_on_result_of(
        self,
        outer_method: str,
        inner_method: str,
    ) -> list[PatternMatch]:
        """Find calls to *outer_method* whose receiver was produced by *inner_method*.

        Matches the pattern ``df.inner_method(...).outer_method(...)`` by
        checking whether *inner_method* is in the ``chain`` of the
        *outer_method* call.

        Example: ``find_method_on_result_of("collect", "groupBy")`` catches
        ``df.groupBy(...).agg(...).collect()``.

        Args:
            outer_method: The method the rule fires on.
            inner_method: The method that should appear earlier in the chain.

        Returns:
            One match per occurrence of the pattern.
        """
        results: list[PatternMatch] = []
        for call in self._a.find_method_calls(outer_method):
            if inner_method in call.chain:
                results.append(
                    PatternMatch.yes(
                        calls=[call],
                        outer_method=outer_method,
                        inner_method=inner_method,
                        chain=call.chain,
                    )
                )
        return results

    def find_repeated_action_without_cache(
        self,
        action_methods: list[str] | None = None,
    ) -> list[PatternMatch]:
        """Find DataFrames that have multiple action calls without caching.

        An "action" is any method that triggers Spark execution:
        ``collect``, ``count``, ``show``, ``take``, ``first``, ``write``,
        ``toPandas``, etc.

        Args:
            action_methods: Override the default set of action method names.

        Returns:
            One match per receiver that has 2+ action calls without a
            cache/persist call on the same receiver.
        """
        _default_actions = {
            "collect",
            "count",
            "show",
            "take",
            "first",
            "toPandas",
            "write",
            "save",
            "saveAsTable",
        }
        actions = set(action_methods or _default_actions)
        cached_receivers = {
            c.receiver_src
            for c in self._a.find_method_calls("cache") + self._a.find_method_calls("persist")
        }

        # Group action calls by receiver
        by_receiver: dict[str, list[MethodCallInfo]] = {}
        for method in actions:
            for call in self._a.find_method_calls(method):
                by_receiver.setdefault(call.receiver_src, []).append(call)

        results: list[PatternMatch] = []
        for receiver, calls in by_receiver.items():
            if len(calls) >= 2 and receiver not in cached_receivers:
                results.append(
                    PatternMatch.yes(
                        calls=sorted(calls, key=lambda c: c.line),
                        receiver=receiver,
                        action_count=len(calls),
                        action_methods=[c.method_name for c in calls],
                    )
                )
        return results

    def find_collect_without_limit(self) -> list[PatternMatch]:
        """Find ``collect()`` or ``toPandas()`` calls not preceded by ``limit()``.

        These calls pull all data to the driver and can cause OOM on large
        datasets.

        Returns:
            One match per collect/toPandas call that has no ``limit()`` in
            its chain or in the preceding 20 lines.
        """
        limit_lines = {c.line for c in self._a.find_method_calls("limit")}
        results: list[PatternMatch] = []

        for method in ("collect", "toPandas"):
            for call in self._a.find_method_calls(method):
                # Check chain first (df.limit(n).collect())
                if "limit" in call.chain:
                    continue
                # Check nearby preceding lines
                lookback = range(max(1, call.line - 20), call.line)
                if any(ln in limit_lines for ln in lookback):
                    continue
                results.append(
                    PatternMatch.yes(
                        calls=[call],
                        method=method,
                    )
                )
        return results

    # ------------------------------------------------------------------
    # 4. DataFrame lineage tracking
    # ------------------------------------------------------------------

    def build_lineage(self) -> dict[str, DataFrameNode]:
        """Build (or return cached) the best-effort DataFrame lineage graph.

        Scans variable assignments and correlates them with method calls to
        determine:

        - Which operation produced each named DataFrame.
        - How many times each named DataFrame is used as a receiver.
        - Whether each named DataFrame is cached and/or unpersisted.

        The graph is keyed by variable name.  Only simple single-name
        assignments are tracked (``df2 = df1.join(...)``);  tuple
        unpacking, augmented assignment, and comprehension targets are
        skipped.

        Returns:
            Dictionary mapping variable name → ``DataFrameNode``.
        """
        if self._lineage is not None:
            return self._lineage

        lineage: dict[str, DataFrameNode] = {}

        # Step 1: record assignments
        for assign in self._a.find_variable_assignments():
            # Only track simple single-name targets
            target = assign.target.strip()
            if "," in target or " " in target:
                continue  # tuple unpack or complex target

            producing_method = assign.rhs_method_calls[0] if assign.rhs_method_calls else "unknown"
            # Best-effort: extract referenced variable names from RHS src
            source_names = _extract_names_from_src(assign.value_src)

            lineage[target] = DataFrameNode(
                name=target,
                created_by=producing_method,
                source_names=source_names,
                line=assign.line,
            )

        # Step 2: mark cache/persist/unpersist (must run before use-count so
        # we can exclude the cache call itself from the use count).
        cache_persist_methods = {"cache", "persist"}
        cache_lines: dict[str, int] = {}  # name → line of first cache call

        for call in self._a.find_method_calls("cache") + self._a.find_method_calls("persist"):
            receiver = call.receiver_src.strip()
            if receiver in lineage:
                lineage[receiver].cached = True
                if lineage[receiver].cache_line is None:
                    lineage[receiver].cache_line = call.line
                cache_lines[receiver] = call.line

        for call in self._a.find_method_calls("unpersist"):
            receiver = call.receiver_src.strip()
            if receiver in lineage:
                lineage[receiver].unpersisted = True

        # Step 3: count usages after the cache call.  Exclude the cache/
        # persist call itself so "df.cache(); df.show()" registers use_count=1,
        # not 2.  For uncached DataFrames, count all calls.
        all_calls = self._a.find_all_method_calls()
        for call in all_calls:
            receiver = call.receiver_src.strip()
            if receiver not in lineage:
                continue
            # Skip the cache/persist call itself
            if call.method_name in cache_persist_methods:
                continue
            node = lineage[receiver]
            # For cached DataFrames, only count uses at or after cache line
            if node.cached and node.cache_line is not None:
                if call.line < node.cache_line:
                    continue
            node.use_count += 1

        self._lineage = lineage
        return lineage

    def find_dataframes_used_multiple_times_without_cache(
        self,
        min_uses: int = 2,
    ) -> list[PatternMatch]:
        """Find DataFrames used ≥ *min_uses* times but never cached.

        Args:
            min_uses: Minimum number of method calls on the same receiver
                before flagging.  Default is 2.

        Returns:
            One match per uncached DataFrame that meets the threshold.
        """
        lineage = self.build_lineage()
        results: list[PatternMatch] = []

        for name, node in lineage.items():
            if node.use_count >= min_uses and not node.cached:
                # Gather the actual call objects for context
                calls = [
                    c for c in self._a.find_all_method_calls() if c.receiver_src.strip() == name
                ]
                results.append(
                    PatternMatch.yes(
                        calls=sorted(calls, key=lambda c: c.line),
                        dataframe_name=name,
                        use_count=node.use_count,
                        created_by=node.created_by,
                        created_line=node.line,
                    )
                )
        return results

    def find_cached_dataframes_used_once(self) -> list[PatternMatch]:
        """Find DataFrames that are cached but used only once afterward.

        A cache is wasteful if the DataFrame is only consumed once: it adds
        memory pressure without any reuse benefit.

        Returns:
            One match per cached DataFrame whose ``use_count`` is ≤ 1 after
            the cache call.
        """
        lineage = self.build_lineage()
        results: list[PatternMatch] = []

        for name, node in lineage.items():
            if node.cached and node.use_count <= 1:
                cache_calls = [
                    c
                    for c in self._a.find_method_calls("cache")
                    + self._a.find_method_calls("persist")
                    if c.receiver_src.strip() == name
                ]
                results.append(
                    PatternMatch.yes(
                        calls=cache_calls,
                        dataframe_name=name,
                        use_count=node.use_count,
                        cache_line=node.cache_line,
                    )
                )
        return results

    def find_cached_without_unpersist(self) -> list[PatternMatch]:
        """Find named DataFrames that are cached but never unpersisted.

        Uses lineage (variable tracking) rather than receiver-string
        comparison, so it is more accurate than
        :meth:`find_cache_without_unpersist` for named variables.

        Returns:
            One match per cached DataFrame that has no unpersist call.
        """
        lineage = self.build_lineage()
        results: list[PatternMatch] = []

        for name, node in lineage.items():
            if node.cached and not node.unpersisted:
                cache_calls = [
                    c
                    for c in self._a.find_method_calls("cache")
                    + self._a.find_method_calls("persist")
                    if c.receiver_src.strip() == name
                ]
                results.append(
                    PatternMatch.yes(
                        calls=cache_calls,
                        dataframe_name=name,
                        cache_line=node.cache_line,
                        created_by=node.created_by,
                    )
                )
        return results

    # ------------------------------------------------------------------
    # Convenience / composite patterns
    # ------------------------------------------------------------------

    def find_join_before_filter(self) -> list[PatternMatch]:
        """Find joins that are followed by a filter on the join result.

        When a filter can be pushed before the join, data volume is reduced
        and shuffle cost is lower.

        Returns:
            Matches where ``join`` precedes ``filter``/``where`` in the
            same chain or within a short source window.
        """
        results: list[PatternMatch] = []

        # Pattern 1: chain — df.join(...).filter(...)
        results.extend(self.find_chained_sequence("join", "filter"))
        results.extend(self.find_chained_sequence("join", "where"))

        # Pattern 2: source-window — filter appears shortly after join
        results.extend(self.find_method_after("join", "filter", window=10))

        # Deduplicate by join-call line
        seen: set[int] = set()
        deduped: list[PatternMatch] = []
        for m in results:
            key = m.calls[0].line if m.calls else -1
            if key not in seen:
                seen.add(key)
                deduped.append(m)
        return deduped

    def find_repartition_before_write(self) -> list[PatternMatch]:
        """Find ``repartition()`` called immediately before a write.

        ``coalesce()`` is usually better before a write because it avoids
        a full shuffle.

        Spark's ``.write`` is a *property*, not a method call, so it does
        not appear in ``find_method_calls("write")``.  Instead this method
        inspects the ``chain`` of every IO terminal call (``parquet``,
        ``save``, etc.) and flags those that contain ``"repartition"``
        before ``"write"`` or ``"writeStream"`` in the chain.

        Returns:
            Matches where ``repartition`` precedes a write terminal in the
            call chain, or where a standalone ``repartition()`` call is
            followed by a write-terminal call within 5 source lines.
        """
        _write_terminals = {
            "parquet",
            "csv",
            "json",
            "orc",
            "avro",
            "text",
            "save",
            "saveAsTable",
            "insertInto",
            "delta",
        }
        _write_markers = {"write", "writeStream"}

        results: list[PatternMatch] = []

        # Pattern 1: chain — repartition appears before a write marker in
        # the chain of a write-terminal call, e.g.
        # df.repartition(200).write.parquet(...)
        for call in self._a.find_all_method_calls():
            if call.method_name not in _write_terminals:
                continue
            chain = call.chain
            has_write = any(m in _write_markers for m in chain)
            if not has_write:
                continue
            try:
                repart_idx = chain.index("repartition")
            except ValueError:
                continue
            write_idx = next((i for i, m in enumerate(chain) if m in _write_markers), len(chain))
            if repart_idx < write_idx:
                # Also grab the repartition call for context
                repart_calls = [
                    c for c in self._a.find_method_calls("repartition") if c.line <= call.line
                ]
                repart_call = repart_calls[-1] if repart_calls else None
                results.append(
                    PatternMatch.yes(
                        calls=[c for c in [repart_call, call] if c is not None],
                        repartition_in_chain=True,
                        write_terminal=call.method_name,
                    )
                )

        # Pattern 2: standalone repartition() on one line, write terminal
        # on a nearby following line (multi-statement pattern).
        write_terminal_lines = {
            c.line: c
            for c in self._a.find_all_method_calls()
            if c.method_name in _write_terminals and any(m in _write_markers for m in c.chain)
        }
        for repart_call in self._a.find_method_calls("repartition"):
            # Skip if already captured in Pattern 1 (repartition is in chain)
            for wline, wcall in write_terminal_lines.items():
                if "repartition" in wcall.chain:
                    continue
                if repart_call.line < wline <= repart_call.line + 5:
                    results.append(
                        PatternMatch.yes(
                            calls=[repart_call, wcall],
                            repartition_in_chain=False,
                            write_terminal=wcall.method_name,
                        )
                    )

        # Deduplicate by repartition call line
        seen: set[int] = set()
        deduped: list[PatternMatch] = []
        for m in results:
            key = m.calls[0].line if m.calls else -1
            if key not in seen:
                seen.add(key)
                deduped.append(m)
        return deduped

    def find_groupby_key_patterns(self) -> list[PatternMatch]:
        """Find ``groupByKey()`` calls (should use ``reduceByKey`` instead).

        Returns:
            One match per ``groupByKey`` call found.
        """
        return [
            PatternMatch.yes(calls=[c], receiver=c.receiver_src)
            for c in self._a.find_method_calls("groupByKey")
        ]

    def find_select_star(self) -> list[PatternMatch]:
        """Find ``select("*")`` calls that prevent column pruning.

        Returns:
            One match per ``select("*")`` call.
        """
        results: list[PatternMatch] = []
        for call in self._a.find_method_calls("select"):
            if not call.args:
                continue
            arg = call.args[0]
            if isinstance(arg, ast.Constant) and arg.value == "*":
                results.append(PatternMatch.yes(calls=[call], receiver=call.receiver_src))
        return results

    def find_withcolumn_in_loop(self) -> list[PatternMatch]:
        """Find ``withColumn`` called inside a loop (deeply nested plan risk).

        Returns:
            One match per loop that contains ``withColumn`` calls.
        """
        return self.find_method_in_loop("withColumn")

    def find_cross_join(self) -> list[PatternMatch]:
        """Find ``crossJoin()`` calls (cartesian product).

        Returns:
            One match per ``crossJoin`` call.
        """
        return [
            PatternMatch.yes(calls=[c], receiver=c.receiver_src)
            for c in self._a.find_method_calls("crossJoin")
        ]

    def find_coalesce_to_one(self) -> list[PatternMatch]:
        """Find ``coalesce(1)`` or ``repartition(1)`` calls.

        Collapsing to a single partition forces all data through one task
        and eliminates parallelism.

        Returns:
            Matches where the argument is the integer literal ``1``.
        """
        results: list[PatternMatch] = []
        for method in ("coalesce", "repartition"):
            for call in self._a.find_method_calls(method):
                if not call.args:
                    continue
                arg = call.args[0]
                if isinstance(arg, ast.Constant) and arg.value == 1:
                    results.append(
                        PatternMatch.yes(
                            calls=[call],
                            method=method,
                            receiver=call.receiver_src,
                        )
                    )
        return results


# =============================================================================
# Private helpers
# =============================================================================


def _parse_size_value(raw: str, multipliers: dict[str, float]) -> float | None:
    """Parse a Spark size string like ``"8g"`` or ``"512m"`` to a float.

    Args:
        raw: The raw config value string.
        multipliers: Suffix → multiplier mapping.

    Returns:
        Numeric value in the base unit, or ``None`` if unparseable.
    """
    s = raw.strip().lower()
    for suffix, mult in sorted(multipliers.items(), key=lambda x: -len(x[0])):
        if s.endswith(suffix):
            numeric_part = s[: -len(suffix)]
            try:
                return float(numeric_part) * mult
            except ValueError:
                return None
    try:
        return float(s)
    except ValueError:
        return None


def _extract_names_from_src(src: str) -> list[str]:
    """Extract simple variable name references from a source expression string.

    Uses ``ast.parse`` on the expression; falls back to splitting on common
    delimiters if parsing fails.

    Args:
        src: The ``value_src`` string from an ``AssignmentInfo``.

    Returns:
        List of simple name strings referenced in *src*.
    """
    try:
        tree = ast.parse(src, mode="eval")
        return [node.id for node in ast.walk(tree.body) if isinstance(node, ast.Name)]
    except SyntaxError:
        # crude fallback: split on non-identifier characters
        import re

        return re.findall(r"\b([a-zA-Z_]\w*)\b", src)
