"""Core data types for spark-perf-lint.

Defines the canonical data structures used throughout the linter:
enumerations for severity, effort, and Spark dimensions; the Finding
dataclass that every rule emits; and AuditReport that aggregates a full
scan run into a structured, serialisable result.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# =============================================================================
# Enumerations
# =============================================================================


class Severity(Enum):
    """Severity level for a linting finding.

    Levels are ordered so that CRITICAL > WARNING > INFO, enabling
    comparisons such as ``if finding.severity >= Severity.WARNING``.
    """

    INFO = 1
    WARNING = 2
    CRITICAL = 3

    # ------------------------------------------------------------------
    # Comparison support
    # ------------------------------------------------------------------

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Severity):
            return NotImplemented
        return self.value < other.value

    def __le__(self, other: object) -> bool:
        if not isinstance(other, Severity):
            return NotImplemented
        return self.value <= other.value

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, Severity):
            return NotImplemented
        return self.value > other.value

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, Severity):
            return NotImplemented
        return self.value >= other.value

    def __repr__(self) -> str:
        return f"Severity.{self.name}"


class EffortLevel(Enum):
    """Estimated developer effort required to remediate a finding.

    Attributes:
        CONFIG_ONLY: Fix is a SparkSession config change only — no code edits.
        MINOR_CODE_CHANGE: Fix requires a small, localised code change
            (e.g., swap ``groupByKey`` for ``reduceByKey``).
        MAJOR_REFACTOR: Fix requires significant restructuring
            (e.g., adding salting to a skewed join pipeline).
    """

    CONFIG_ONLY = "config_only"
    MINOR_CODE_CHANGE = "minor_code_change"
    MAJOR_REFACTOR = "major_refactor"

    def __repr__(self) -> str:
        return f"EffortLevel.{self.name}"


class Dimension(Enum):
    """The 11 performance dimensions analysed by spark-perf-lint.

    Each member maps to a rule module (``rules/d{nn}_*.py``) and a set of
    SPL-D{nn}-NNN rule IDs.

    The ``display_name`` property returns a human-readable label suitable
    for reports and terminal output.
    """

    D01_CLUSTER_CONFIG = "D01"
    D02_SHUFFLE = "D02"
    D03_JOINS = "D03"
    D04_PARTITIONING = "D04"
    D05_SKEW = "D05"
    D06_CACHING = "D06"
    D07_IO_FORMAT = "D07"
    D08_AQE = "D08"
    D09_UDF_CODE = "D09"
    D10_CATALYST = "D10"
    D11_MONITORING = "D11"

    @property
    def display_name(self) -> str:
        """Return a human-readable name for this dimension.

        Returns:
            A title-cased string, e.g. ``"D03 · Joins"``.
        """
        labels: dict[str, str] = {
            "D01": "D01 · Cluster Configuration",
            "D02": "D02 · Shuffle",
            "D03": "D03 · Joins",
            "D04": "D04 · Partitioning",
            "D05": "D05 · Data Skew",
            "D06": "D06 · Caching",
            "D07": "D07 · I/O Format",
            "D08": "D08 · Adaptive Query Execution",
            "D09": "D09 · UDF Code Quality",
            "D10": "D10 · Catalyst Optimizer",
            "D11": "D11 · Monitoring & Observability",
        }
        return labels[self.value]

    def __repr__(self) -> str:
        return f"Dimension.{self.name}"


# =============================================================================
# SparkConfigEntry
# =============================================================================


@dataclass
class SparkConfigEntry:
    """A single Spark configuration parameter audit result.

    Produced by the config auditor when a ``SparkSession.builder`` call
    sets (or omits) a parameter that differs from the project's expected
    value defined in ``.spark-perf-lint.yaml``.

    Attributes:
        parameter: The fully-qualified Spark config key,
            e.g. ``"spark.sql.adaptive.enabled"``.
        current_value: The value found in the source code, or ``None``
            if the parameter is absent.
        recommended_value: The value prescribed by the project config, or
            ``None`` if the audit only checks for presence.
        reason: Why this value is recommended — references Spark internals
            or operational experience.
        impact: The performance or reliability consequence of the current
            value, e.g. ``"Disables AQE; static plan cannot adapt to skew"``.
    """

    parameter: str
    current_value: Any
    recommended_value: Any
    reason: str
    impact: str

    def __repr__(self) -> str:
        return (
            f"SparkConfigEntry(parameter={self.parameter!r}, "
            f"current={self.current_value!r}, "
            f"recommended={self.recommended_value!r})"
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dictionary.

        Returns:
            A JSON-serialisable dictionary representation.
        """
        return {
            "parameter": self.parameter,
            "current_value": self.current_value,
            "recommended_value": self.recommended_value,
            "reason": self.reason,
            "impact": self.impact,
        }


# =============================================================================
# Finding
# =============================================================================


@dataclass
class Finding:
    """A single performance anti-pattern detected by a linting rule.

    Every rule module returns zero or more ``Finding`` instances. The
    fields are designed to give the developer everything they need to
    understand the problem, its impact, and how to fix it — without
    needing to look anything up.

    Attributes:
        rule_id: Unique rule identifier following the pattern
            ``SPL-D{dimension}-{number}``, e.g. ``"SPL-D03-001"``.
        severity: How urgently the issue should be addressed.
        dimension: The performance dimension this finding belongs to.
        file_path: Absolute or repo-relative path to the offending file.
        line_number: 1-based line number of the finding in the source file.
        column_offset: 0-based column offset; ``None`` when not determinable
            from the AST node.
        message: Short, human-readable description of what was found.
            Should complete the sentence "Found: …".
        explanation: Multi-sentence explanation of *why* this pattern is
            harmful, referencing Spark internals where relevant.
        recommendation: Specific, actionable fix. Not generic advice.
        before_code: The problematic code snippet extracted from the source,
            or a representative example. ``None`` when unavailable.
        after_code: The corrected version of ``before_code``.
            ``None`` when unavailable.
        config_suggestion: Optional mapping of Spark config keys to their
            recommended values, e.g.
            ``{"spark.sql.adaptive.enabled": "true"}``.
        estimated_impact: Quantified performance impact where known,
            e.g. ``"3–10× slowdown on datasets > 1 M rows"``.
        effort_level: Developer effort required for remediation.
        references: URLs to Spark docs, JIRA tickets, blog posts, or
            knowledge-base entries that support this finding.
    """

    rule_id: str
    severity: Severity
    dimension: Dimension
    file_path: str
    line_number: int
    message: str
    explanation: str
    recommendation: str
    column_offset: int | None = None
    before_code: str | None = None
    after_code: str | None = None
    config_suggestion: dict[str, Any] = field(default_factory=dict)
    estimated_impact: str = ""
    effort_level: EffortLevel = EffortLevel.MINOR_CODE_CHANGE
    references: list[str] = field(default_factory=list)
    llm_insight: str | None = None

    def __repr__(self) -> str:
        return (
            f"Finding(rule_id={self.rule_id!r}, "
            f"severity={self.severity!r}, "
            f"file={self.file_path!r}, "
            f"line={self.line_number})"
        )

    def __post_init__(self) -> None:
        """Validate field constraints after initialisation.

        Raises:
            ValueError: If ``rule_id`` does not match the expected pattern,
                or if ``line_number`` is less than 1.
        """
        if not self.rule_id.startswith("SPL-D"):
            raise ValueError(f"rule_id must start with 'SPL-D', got {self.rule_id!r}")
        if self.line_number < 1:
            raise ValueError(f"line_number must be >= 1, got {self.line_number}")

    def to_dict(self) -> dict[str, Any]:
        """Serialise this finding to a plain dictionary.

        All enum values are converted to their string names so the result
        is immediately JSON-serialisable.

        Returns:
            A dictionary representation suitable for JSON / Markdown reporters.
        """
        return {
            "rule_id": self.rule_id,
            "severity": self.severity.name,
            "dimension": self.dimension.name,
            "file_path": self.file_path,
            "line_number": self.line_number,
            "column_offset": self.column_offset,
            "message": self.message,
            "explanation": self.explanation,
            "recommendation": self.recommendation,
            "before_code": self.before_code,
            "after_code": self.after_code,
            "config_suggestion": self.config_suggestion,
            "estimated_impact": self.estimated_impact,
            "effort_level": self.effort_level.name,
            "references": self.references,
            "llm_insight": self.llm_insight,
        }


# =============================================================================
# AuditReport
# =============================================================================


@dataclass
class AuditReport:
    """Aggregated result of a complete spark-perf-lint scan run.

    Collects all ``Finding`` instances produced across every scanned file,
    records run metadata, and exposes convenience accessors used by
    reporters and the CLI exit-code logic.

    Attributes:
        findings: All findings emitted during the scan, in the order they
            were produced (file order, then line order within each file).
        files_scanned: Number of Python source files examined.
        scan_duration_seconds: Wall-clock time for the scan.
        config_used: The resolved configuration dictionary that was active
            during the scan (merged from defaults + project config).
        summary: Automatically computed counts; populated by
            ``__post_init__``. Keys: ``by_severity``, ``by_dimension``,
            ``by_effort``.
    """

    findings: list[Finding]
    files_scanned: int
    scan_duration_seconds: float
    config_used: dict[str, Any]
    summary: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Compute the summary counts after construction."""
        self.summary = self._build_summary()

    def __repr__(self) -> str:
        return (
            f"AuditReport(findings={len(self.findings)}, "
            f"files_scanned={self.files_scanned}, "
            f"critical={self.critical_count}, "
            f"passed={self.passed})"
        )

    # ------------------------------------------------------------------
    # Computed properties
    # ------------------------------------------------------------------

    @property
    def critical_count(self) -> int:
        """Number of CRITICAL severity findings.

        Returns:
            Count of findings with ``severity == Severity.CRITICAL``.
        """
        return sum(1 for f in self.findings if f.severity == Severity.CRITICAL)

    @property
    def warning_count(self) -> int:
        """Number of WARNING severity findings.

        Returns:
            Count of findings with ``severity == Severity.WARNING``.
        """
        return sum(1 for f in self.findings if f.severity == Severity.WARNING)

    @property
    def info_count(self) -> int:
        """Number of INFO severity findings.

        Returns:
            Count of findings with ``severity == Severity.INFO``.
        """
        return sum(1 for f in self.findings if f.severity == Severity.INFO)

    @property
    def passed(self) -> bool:
        """Whether the scan produced zero CRITICAL findings.

        The CLI uses this to determine the exit code when ``fail_on``
        includes ``"CRITICAL"``.

        Returns:
            ``True`` if ``critical_count == 0``.
        """
        return self.critical_count == 0

    # ------------------------------------------------------------------
    # Filtering helpers
    # ------------------------------------------------------------------

    def findings_by_severity(self, severity: Severity) -> list[Finding]:
        """Return all findings matching the given severity.

        Args:
            severity: The ``Severity`` level to filter on.

        Returns:
            A list of ``Finding`` objects with ``finding.severity == severity``.
        """
        return [f for f in self.findings if f.severity == severity]

    def findings_by_dimension(self, dimension: Dimension) -> list[Finding]:
        """Return all findings belonging to the given dimension.

        Args:
            dimension: The ``Dimension`` to filter on.

        Returns:
            A list of ``Finding`` objects with ``finding.dimension == dimension``.
        """
        return [f for f in self.findings if f.dimension == dimension]

    def findings_at_or_above(self, min_severity: Severity) -> list[Finding]:
        """Return findings whose severity is >= *min_severity*.

        Useful for reporters that apply a ``severity_threshold``.

        Args:
            min_severity: Lower bound (inclusive) for severity filtering.

        Returns:
            Filtered list of findings ordered as they appear in ``self.findings``.
        """
        return [f for f in self.findings if f.severity >= min_severity]

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def _build_summary(self) -> dict[str, Any]:
        """Compute structured counts from ``self.findings``.

        Returns:
            Dictionary with keys ``by_severity``, ``by_dimension``,
            ``by_effort``, and ``total``.
        """
        by_severity: dict[str, int] = {s.name: 0 for s in Severity}
        by_dimension: dict[str, int] = {d.name: 0 for d in Dimension}
        by_effort: dict[str, int] = {e.name: 0 for e in EffortLevel}

        for finding in self.findings:
            by_severity[finding.severity.name] += 1
            by_dimension[finding.dimension.name] += 1
            by_effort[finding.effort_level.name] += 1

        return {
            "total": len(self.findings),
            "by_severity": by_severity,
            "by_dimension": by_dimension,
            "by_effort": by_effort,
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialise the entire report to a plain dictionary.

        Returns:
            A JSON-serialisable dictionary containing all findings and
            run metadata.
        """
        return {
            "summary": self.summary,
            "files_scanned": self.files_scanned,
            "scan_duration_seconds": round(self.scan_duration_seconds, 3),
            "passed": self.passed,
            "findings": [f.to_dict() for f in self.findings],
            "config_used": self.config_used,
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialise the report to a JSON string.

        Args:
            indent: Number of spaces for JSON indentation.
                Pass ``0`` or ``None`` for compact output.

        Returns:
            A formatted JSON string.
        """
        return json.dumps(self.to_dict(), indent=indent or None)
