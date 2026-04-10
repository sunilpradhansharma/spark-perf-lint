"""Provider-agnostic tracing interface for spark-perf-lint.

``BaseTracer`` defines the contract that every observability backend must
satisfy.  The orchestrator calls the four lifecycle methods in order:

1. ``start_run``  — invoked once before scanning begins
2. ``record_file`` — invoked once per file after it is scanned
3. ``record_findings`` — invoked once with the final, sorted findings list
4. ``end_run``    — invoked once after the ``AuditReport`` is built

Implementors only need to override the methods they care about; the base
class provides no-op defaults so partial implementations are valid.

Built-in backends
-----------------
``FileTracer``
    Writes a single JSON trace document per run to a configurable directory.
    Zero external dependencies; the default backend.

``LangSmithTracer``
    Forwards traces to the LangSmith tracing platform via the
    ``langsmith`` SDK.  Stub implementation only — raises
    ``NotImplementedError`` until the integration is completed.

Factory
-------
``TracerFactory.from_config(config)`` selects and constructs the correct
backend from the resolved ``LintConfig``, returning a ``NullTracer`` when
observability is disabled.

Usage in tests::

    from spark_perf_lint.observability.tracer import NullTracer

    tracer = NullTracer()          # no-op; safe to pass anywhere
    tracer.start_run("run-001", config)
    tracer.end_run(report)
"""

from __future__ import annotations

import abc
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spark_perf_lint.config import LintConfig
    from spark_perf_lint.types import AuditReport, Finding


# =============================================================================
# TraceLevel
# =============================================================================


class TraceLevel:
    """Constants for the three trace verbosity levels.

    These mirror the ``observability.trace_level`` config values and control
    how much detail is written into each trace record.

    Attributes:
        MINIMAL: Run metadata and counts only — no per-file or per-finding
            data.  Smallest possible trace footprint.
        STANDARD: Run metadata, summary counts, and findings at WARNING or
            above.  Default level.
        VERBOSE: Full detail — all findings including INFO, per-file timing,
            and complete finding fields.
    """

    MINIMAL = "minimal"
    STANDARD = "standard"
    VERBOSE = "verbose"

    _ORDER = {MINIMAL: 0, STANDARD: 1, VERBOSE: 2}

    @classmethod
    def at_least(cls, level: str, minimum: str) -> bool:
        """Return ``True`` when *level* is at least as detailed as *minimum*.

        Args:
            level: The configured trace level string.
            minimum: The minimum required level string.

        Returns:
            ``True`` when ``level >= minimum`` in verbosity order.
        """
        return cls._ORDER.get(level, 1) >= cls._ORDER.get(minimum, 1)


# =============================================================================
# BaseTracer — abstract interface
# =============================================================================


class BaseTracer(abc.ABC):
    """Abstract base class for all observability backends.

    All four lifecycle methods have concrete no-op implementations so that
    subclasses only need to override the methods relevant to their backend.

    Args:
        config: Resolved ``LintConfig`` for the current scan run.
    """

    def __init__(self, config: LintConfig) -> None:
        self._config = config
        self._obs_cfg: dict = config.raw.get("observability", {})
        self._trace_level: str = self._obs_cfg.get("trace_level", TraceLevel.STANDARD)
        self._run_id: str = ""

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

    def start_run(self, run_id: str) -> None:
        """Record the beginning of a scan run.

        Called once by the orchestrator immediately before files are scanned.
        Implementations should store ``run_id`` for use in subsequent calls.

        Args:
            run_id: A unique identifier for this scan run (UUID string).
        """
        self._run_id = run_id

    def record_file(
        self,
        file_path: str,
        finding_count: int,
        duration_s: float,
    ) -> None:
        """Record the result of scanning a single file.

        Called once per scanned file, after all rules have been applied.
        The default implementation is a no-op.

        Args:
            file_path: Repo-relative path of the scanned file.
            finding_count: Number of findings produced for this file.
            duration_s: Wall-clock seconds spent scanning this file.
        """

    def record_findings(self, findings: list[Finding]) -> None:
        """Receive the complete, sorted list of findings for the run.

        Called once, after all files have been scanned and findings have been
        sorted and capped.  The default implementation is a no-op.

        Args:
            findings: All ``Finding`` objects produced in this run, in the
                canonical sorted order (CRITICAL first, then file, then line).
        """

    def end_run(self, report: AuditReport) -> None:
        """Finalise and flush the trace for this run.

        Called once after the ``AuditReport`` has been constructed.
        Implementations should write, upload, or close any open resources.

        Args:
            report: The fully assembled ``AuditReport`` for this run.
        """

    # ------------------------------------------------------------------
    # Helpers available to subclasses
    # ------------------------------------------------------------------

    def _should_include_findings(self) -> bool:
        """Return ``True`` when the trace level calls for finding detail."""
        return TraceLevel.at_least(self._trace_level, TraceLevel.STANDARD)

    def _should_include_verbose(self) -> bool:
        """Return ``True`` when the trace level is VERBOSE."""
        return TraceLevel.at_least(self._trace_level, TraceLevel.VERBOSE)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(run_id={self._run_id!r})"


# =============================================================================
# NullTracer — explicit no-op
# =============================================================================


class NullTracer(BaseTracer):
    """A no-op tracer used when observability is disabled.

    All lifecycle methods are inherited from ``BaseTracer`` and do nothing.
    The orchestrator passes a ``NullTracer`` when
    ``observability.enabled = false`` so the rest of the codebase never needs
    to branch on whether a tracer is present.
    """


# =============================================================================
# TracerFactory
# =============================================================================


class TracerFactory:
    """Constructs the appropriate ``BaseTracer`` from a ``LintConfig``.

    Usage::

        tracer = TracerFactory.from_config(config)
        tracer.start_run(run_id)
        # ... scan ...
        tracer.end_run(report)
    """

    @staticmethod
    def from_config(config: LintConfig) -> BaseTracer:
        """Return a tracer appropriate for the given configuration.

        When ``observability.enabled`` is ``False`` (the default), a
        ``NullTracer`` is returned immediately without importing any backend
        module.  This keeps the import path fast and dependency-free for the
        common case.

        Args:
            config: Resolved ``LintConfig`` for the current scan.

        Returns:
            A ``BaseTracer`` subclass instance.  Never ``None``.

        Raises:
            ValueError: If the configured backend name is unrecognised.
        """
        obs_cfg = config.raw.get("observability", {})
        if not obs_cfg.get("enabled", False):
            return NullTracer(config)

        backend = obs_cfg.get("backend", "file")

        if backend == "file":
            from spark_perf_lint.observability.file_tracer import FileTracer

            return FileTracer(config)

        if backend == "langsmith":
            from spark_perf_lint.observability.langsmith_tracer import LangSmithTracer

            return LangSmithTracer(config)

        raise ValueError(
            f"Unknown observability backend {backend!r}. "
            f"Must be one of: 'file', 'langsmith'."
        )

    @staticmethod
    def new_run_id() -> str:
        """Generate a fresh UUID4 run identifier.

        Returns:
            A hyphenated UUID4 string, e.g.
            ``"3f2504e0-4f89-11d3-9a0c-0305e82c3301"``.
        """
        return str(uuid.uuid4())
