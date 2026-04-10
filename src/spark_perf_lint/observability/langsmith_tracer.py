"""LangSmith tracing backend for spark-perf-lint.

.. note::

    **Stub implementation** — all methods raise ``NotImplementedError``.
    This file exists as a forward-compatible extension point so that the
    ``observability.backend = "langsmith"`` configuration option is
    recognised and routes correctly; the integration itself is a planned
    future milestone.

Planned integration
-------------------
When implemented this tracer will:

1. Create a ``langsmith.Client`` authenticated via the API key in
   ``observability.langsmith_api_key_env_var`` (default:
   ``LANGCHAIN_API_KEY``).
2. Map each scan run to a LangSmith *run* under the project named by
   ``observability.langsmith_project`` (default: ``"spark-perf-lint"``).
3. Post each ``Finding`` as a child span with severity, rule_id, file_path,
   and LLM insight (if populated) as metadata.
4. Attach the ``AuditReport`` summary as the top-level run output.

To implement, replace each ``raise NotImplementedError(...)`` with the
corresponding ``langsmith`` SDK calls and add ``langsmith`` to the
``[llm]`` optional dependency group in ``pyproject.toml``.

Configuration reference::

    observability:
      enabled: true
      backend: langsmith
      langsmith_project: spark-perf-lint
      langsmith_api_key_env_var: LANGCHAIN_API_KEY   # env var holding the key
      trace_level: standard
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from spark_perf_lint.observability.tracer import BaseTracer

if TYPE_CHECKING:
    from spark_perf_lint.config import LintConfig
    from spark_perf_lint.types import AuditReport, Finding


class LangSmithTracer(BaseTracer):
    """Forwards scan traces to the LangSmith platform.

    .. note::
        **Stub** — all methods raise ``NotImplementedError``.
        See module docstring for the implementation roadmap.

    Args:
        config: Resolved ``LintConfig`` for this scan run.

    Raises:
        NotImplementedError: On every method call until the integration is
            completed.
    """

    def __init__(self, config: LintConfig) -> None:
        super().__init__(config)
        obs_cfg = config.raw.get("observability", {})
        self._project: str = obs_cfg.get("langsmith_project", "spark-perf-lint")
        self._api_key_env_var: str = obs_cfg.get("langsmith_api_key_env_var", "LANGCHAIN_API_KEY")

    # ------------------------------------------------------------------
    # Lifecycle — all stubbed
    # ------------------------------------------------------------------

    def start_run(self, run_id: str) -> None:
        """Begin a LangSmith trace run.

        Args:
            run_id: UUID4 string identifying this scan run.

        Raises:
            NotImplementedError: Always — stub implementation.
        """
        raise NotImplementedError(
            "LangSmithTracer.start_run is not yet implemented. "
            "Set observability.backend = 'file' to use the available tracer, "
            "or implement this method using the langsmith SDK."
        )

    def record_file(
        self,
        file_path: str,
        finding_count: int,
        duration_s: float,
    ) -> None:
        """Record per-file scan results as a LangSmith child span.

        Args:
            file_path: Repo-relative path of the scanned file.
            finding_count: Findings produced for this file.
            duration_s: Wall-clock seconds spent on this file.

        Raises:
            NotImplementedError: Always — stub implementation.
        """
        raise NotImplementedError("LangSmithTracer.record_file is not yet implemented.")

    def record_findings(self, findings: list[Finding]) -> None:
        """Attach findings to the active LangSmith run as metadata.

        Args:
            findings: All findings from the completed scan.

        Raises:
            NotImplementedError: Always — stub implementation.
        """
        raise NotImplementedError("LangSmithTracer.record_findings is not yet implemented.")

    def end_run(self, report: AuditReport) -> None:
        """Finalise and upload the LangSmith trace.

        Args:
            report: The fully assembled ``AuditReport``.

        Raises:
            NotImplementedError: Always — stub implementation.
        """
        raise NotImplementedError(
            "LangSmithTracer.end_run is not yet implemented. "
            "Set observability.backend = 'file' to use the available tracer."
        )

    def __repr__(self) -> str:
        return f"LangSmithTracer(" f"project={self._project!r}, " f"run_id={self._run_id!r})"
