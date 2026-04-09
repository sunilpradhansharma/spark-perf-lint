"""Abstract base classes for spark-perf-lint rule modules.

Every rule is a concrete subclass of either ``ConfigRule`` (for rules that
inspect SparkSession configuration) or ``CodeRule`` (for rules that detect
code-level anti-patterns).  Both inherit from ``BaseRule``, which provides:

- Required class-level metadata validated at class-definition time.
- ``create_finding()`` — a convenience factory that pre-populates a
  ``Finding`` with the rule's own metadata so each ``check()`` body only
  supplies the file-specific fields.
- ``is_enabled()`` / ``get_severity()`` — thin wrappers around
  ``LintConfig`` that rules can call before running expensive analysis.

Usage::

    class CrossJoinRule(CodeRule):
        rule_id = "SPL-D03-005"
        name = "Cross join detected"
        dimension = Dimension.D03_JOINS
        default_severity = Severity.CRITICAL
        description = "cross_join() produces a full Cartesian product."
        explanation = (
            "cross_join() multiplies row counts of both sides. "
            "A 1 M-row table joined with a 1 K-row lookup produces 1 B rows."
        )
        recommendation_template = "Add a join condition or filter one side first."
        before_example = "df.cross_join(lookup)"
        after_example = "df.join(lookup, df.id == lookup.id)"
        references = ["https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html"]
        estimated_impact = "Row count explosion; often causes OOM or job failure"

        def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:
            pm = PatternMatcher(analyzer)
            results = []
            for m in pm.find_cross_join():
                results.append(self.create_finding(
                    analyzer.filename, m.first_line, "cross_join() detected", config=config
                ))
            return results
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.types import Dimension, EffortLevel, Finding, Severity

# ---------------------------------------------------------------------------
# Required class-level attributes — validated by __init_subclass__
# ---------------------------------------------------------------------------

_REQUIRED_CLASS_ATTRS: tuple[str, ...] = (
    "rule_id",
    "name",
    "dimension",
    "default_severity",
    "description",
    "explanation",
    "recommendation_template",
)


# =============================================================================
# BaseRule
# =============================================================================


class BaseRule(ABC):
    """Abstract base for every spark-perf-lint linting rule.

    Concrete subclasses **must** define the class-level metadata listed in
    ``_REQUIRED_CLASS_ATTRS`` and implement ``check()``.  Missing required
    attributes are detected at class-definition time so misconfigured rules
    fail loudly at import rather than silently at scan time.

    Class attributes:
        rule_id: Unique rule identifier following the pattern
            ``SPL-D{dimension}-{number}``, e.g. ``"SPL-D03-001"``.
        name: Short display name shown in ``spark-perf-lint rules list``.
        dimension: The ``Dimension`` this rule belongs to.
        default_severity: ``Severity`` to use when no ``severity_override``
            is configured.
        description: One-line description of what the rule detects.
            Used as a table entry in CLI output.
        explanation: Multi-sentence explanation of *why* the detected
            pattern is harmful, referencing Spark internals where relevant.
        recommendation_template: Actionable fix text.  May contain
            ``{key}`` placeholders that callers substitute via
            ``create_finding(recommendation=...)``.
        before_example: Representative problematic code snippet shown in
            the ``explain`` command output.  Defaults to ``""``.
        after_example: Corrected version of ``before_example``.
            Defaults to ``""``.
        references: List of URLs (Spark docs, JIRA, blog posts).
            Defaults to ``[]``.
        effort_level: Developer effort required for remediation.
            Defaults to ``EffortLevel.MINOR_CODE_CHANGE``; overridden to
            ``EffortLevel.CONFIG_ONLY`` by ``ConfigRule``.
        estimated_impact: Quantified performance impact description, e.g.
            ``"3–10× slowdown on datasets > 1 M rows"``.  Defaults to ``""``.
    """

    # -- Required metadata (must be defined on every concrete subclass) ------
    rule_id: ClassVar[str]
    name: ClassVar[str]
    dimension: ClassVar[Dimension]
    default_severity: ClassVar[Severity]
    description: ClassVar[str]
    explanation: ClassVar[str]
    recommendation_template: ClassVar[str]

    # -- Optional metadata (sensible defaults provided) ----------------------
    before_example: ClassVar[str] = ""
    after_example: ClassVar[str] = ""
    references: ClassVar[list[str]] = []
    effort_level: ClassVar[EffortLevel] = EffortLevel.MINOR_CODE_CHANGE
    estimated_impact: ClassVar[str] = ""

    # ------------------------------------------------------------------
    # Subclass validation
    # ------------------------------------------------------------------

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Validate required metadata on every concrete rule subclass.

        Abstract intermediary classes (``ConfigRule``, ``CodeRule``, and any
        other class that still has un-implemented abstract methods) are
        exempt; only leaf rule classes are validated.

        Args:
            **kwargs: Forwarded to ``super().__init_subclass__``.

        Raises:
            TypeError: If a concrete rule class omits a required attribute.
        """
        super().__init_subclass__(**kwargs)

        # Skip abstract intermediary classes (ConfigRule, CodeRule, and any
        # user-defined abstract subclass) — they don't implement ``check`` in
        # their own __dict__, so they're still abstract and need no metadata.
        if "check" not in cls.__dict__:
            return

        for attr in _REQUIRED_CLASS_ATTRS:
            # Walk the MRO (excluding BaseRule and above) looking for the attr.
            found = any(
                attr in klass.__dict__
                for klass in cls.__mro__
                if klass not in (BaseRule, ABC, object)
            )
            if not found:
                raise TypeError(
                    f"{cls.__name__} must define class attribute {attr!r}. "
                    f"All concrete BaseRule subclasses must define: "
                    f"{', '.join(_REQUIRED_CLASS_ATTRS)}"
                )

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def check(self, analyzer: ASTAnalyzer, config: LintConfig) -> list[Finding]:
        """Run this rule against a parsed source file.

        Called once per scanned file.  Implementations should use
        ``PatternMatcher`` or direct ``ASTAnalyzer`` queries to detect the
        relevant anti-pattern and emit one ``Finding`` per occurrence via
        ``create_finding()``.

        Args:
            analyzer: ``ASTAnalyzer`` instance for the file being scanned.
                Provides access to all parsed AST facts.
            config: Resolved lint configuration.  Use ``get_severity(config)``
                and ``is_enabled(config)`` before expensive work.

        Returns:
            A (possibly empty) list of ``Finding`` instances.  Never return
            ``None``.
        """

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def create_finding(
        self,
        file_path: str,
        line_number: int,
        message: str,
        *,
        recommendation: str | None = None,
        before_code: str | None = None,
        after_code: str | None = None,
        column_offset: int | None = None,
        config_suggestion: dict[str, Any] | None = None,
        config: LintConfig | None = None,
        severity: Severity | None = None,
    ) -> Finding:
        """Create a ``Finding`` pre-populated with this rule's metadata.

        Reduces boilerplate in ``check()`` implementations: callers only
        supply the file-specific fields; rule-level metadata is filled in
        automatically.

        Severity resolution order (highest priority first):

        1. *severity* argument (explicit override for this specific finding)
        2. ``config`` severity override (from ``severity_override`` section)
        3. ``self.default_severity``

        Args:
            file_path: Absolute or repo-relative path to the source file.
            line_number: 1-based line number of the finding.
            message: Short human-readable description of what was found.
                Should complete the sentence "Found: …".
            recommendation: Actionable fix text for this specific occurrence.
                Falls back to ``self.recommendation_template`` when omitted.
            before_code: Problematic code extracted from the source.
                Falls back to ``self.before_example`` when omitted.
            after_code: Corrected version of the code.
                Falls back to ``self.after_example`` when omitted.
            column_offset: 0-based column offset; ``None`` when unavailable.
            config_suggestion: Optional mapping of Spark config keys to their
                recommended values, e.g.
                ``{"spark.sql.adaptive.enabled": "true"}``.
            config: ``LintConfig`` used to resolve any ``severity_override``.
                When ``None``, ``self.default_severity`` is used directly.
            severity: Explicit severity for this finding.  Takes precedence
                over both the config override and ``default_severity``.

        Returns:
            A fully populated ``Finding`` instance ready to be returned from
            ``check()``.
        """
        if severity is not None:
            effective_severity = severity
        elif config is not None:
            effective_severity = self.get_severity(config)
        else:
            effective_severity = self.default_severity

        return Finding(
            rule_id=self.rule_id,
            severity=effective_severity,
            dimension=self.dimension,
            file_path=file_path,
            line_number=line_number,
            message=message,
            explanation=self.explanation,
            recommendation=(
                recommendation if recommendation is not None else self.recommendation_template
            ),
            column_offset=column_offset,
            before_code=before_code if before_code is not None else (self.before_example or None),
            after_code=after_code if after_code is not None else (self.after_example or None),
            config_suggestion=config_suggestion or {},
            estimated_impact=self.estimated_impact,
            effort_level=self.effort_level,
            references=list(self.references),
        )

    def is_enabled(self, config: LintConfig) -> bool:
        """Return whether this rule is enabled in the given configuration.

        Delegates to ``config.is_rule_enabled(self.rule_id)``.

        Args:
            config: Resolved lint configuration.

        Returns:
            ``True`` if the rule should run for this scan.
        """
        return config.is_rule_enabled(self.rule_id)

    def get_severity(self, config: LintConfig) -> Severity:
        """Return the effective ``Severity`` for this rule.

        Checks the ``severity_override`` section of *config* first; falls
        back to ``self.default_severity`` when no override is configured.

        Args:
            config: Resolved lint configuration.

        Returns:
            The ``Severity`` to use when creating findings for this rule.
        """
        if config.has_severity_override(self.rule_id):
            return config.get_severity_for(self.rule_id)
        return self.default_severity

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"rule_id={self.rule_id!r}, "
            f"severity={self.default_severity.name})"
        )


# =============================================================================
# ConfigRule
# =============================================================================


class ConfigRule(BaseRule):
    """Base class for rules that inspect SparkSession configuration.

    ``ConfigRule`` subclasses examine ``SparkConfigInfo`` entries extracted
    by the AST analyzer and flag missing, incorrect, or conflicting
    configuration values.

    They typically delegate to:

    - ``PatternMatcher.find_missing_config()``
    - ``PatternMatcher.find_conflicting_configs()``
    - ``PatternMatcher.find_config_below_threshold()``

    The default ``effort_level`` is ``EffortLevel.CONFIG_ONLY`` because
    fixing a Spark config is purely additive — it never requires code edits.
    Subclasses may override this for configs that require code restructuring
    to apply correctly (rare).

    Example::

        class AQEEnabledRule(ConfigRule):
            rule_id = "SPL-D08-001"
            name = "AQE disabled"
            dimension = Dimension.D08_AQE
            default_severity = Severity.WARNING
            description = "Adaptive Query Execution is not enabled."
            explanation = "AQE lets Spark re-optimise a query at runtime ..."
            recommendation_template = (
                "Add spark.conf.set('spark.sql.adaptive.enabled', 'true') "
                "to your SparkSession builder."
            )
    """

    effort_level: ClassVar[EffortLevel] = EffortLevel.CONFIG_ONLY


# =============================================================================
# CodeRule
# =============================================================================


class CodeRule(BaseRule):
    """Base class for rules that detect code-level anti-patterns.

    ``CodeRule`` subclasses examine method-call chains, loop patterns,
    DataFrame lineage, and other structural issues detected by the AST
    analyzer and ``PatternMatcher``.

    They typically delegate to methods such as:

    - ``PatternMatcher.find_method_in_loop()``
    - ``PatternMatcher.find_collect_without_limit()``
    - ``PatternMatcher.find_join_before_filter()``
    - Direct ``ASTAnalyzer`` queries for structural checks

    The default ``effort_level`` is ``EffortLevel.MINOR_CODE_CHANGE``.
    Subclasses should override with ``EffortLevel.MAJOR_REFACTOR`` for
    rules that require significant restructuring (e.g., salting a skewed
    join).

    Example::

        class WithColumnInLoopRule(CodeRule):
            rule_id = "SPL-D10-001"
            name = "withColumn inside loop"
            dimension = Dimension.D10_CATALYST
            default_severity = Severity.CRITICAL
            description = "withColumn() called inside a Python loop."
            explanation = "Each withColumn() call rebuilds the full query plan ..."
            recommendation_template = (
                "Collect column expressions into a list and apply them in "
                "a single select() call outside the loop."
            )
            effort_level = EffortLevel.MINOR_CODE_CHANGE
    """

    effort_level: ClassVar[EffortLevel] = EffortLevel.MINOR_CODE_CHANGE
