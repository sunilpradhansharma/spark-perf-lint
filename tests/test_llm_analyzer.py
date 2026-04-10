"""Tests for the Tier 2 LLM analysis layer.

Coverage targets:
  llm/provider.py   — ClaudeLLMProvider construction, complete(), retry, from_config
  llm/prompts.py    — PromptTemplates: all four template methods + SYSTEM prompt
  llm/analyzer.py   — LLMAnalyzer: enrichment, batching, degradation, budget limits
  types.py          — Finding.llm_insight field (new, backward-compat)

All Claude API calls are mocked; no anthropic package or API key required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from spark_perf_lint.config import LintConfig
from spark_perf_lint.llm.analyzer import LLMAnalysisResult, LLMAnalyzer
from spark_perf_lint.llm.prompts import PromptTemplates
from spark_perf_lint.llm.provider import ClaudeLLMProvider, LLMProvider
from spark_perf_lint.types import (
    AuditReport,
    Dimension,
    Finding,
    Severity,
    SparkConfigEntry,
)

# =============================================================================
# Shared helpers
# =============================================================================


def _finding(
    rule_id: str = "SPL-D03-001",
    severity: Severity = Severity.CRITICAL,
    file_path: str = "jobs/etl.py",
    line_number: int = 10,
    message: str = "Cartesian product detected",
    before_code: str | None = None,
    after_code: str | None = None,
    estimated_impact: str = "",
) -> Finding:
    """Return a minimal valid Finding for use in tests."""
    return Finding(
        rule_id=rule_id,
        severity=severity,
        dimension=Dimension.D03_JOINS,
        file_path=file_path,
        line_number=line_number,
        message=message,
        explanation="Cross joins produce N×M output rows.",
        recommendation="Replace with an equi-join on a shared key.",
        before_code=before_code,
        after_code=after_code,
        estimated_impact=estimated_impact,
    )


def _report(
    findings: list[Finding] | None = None,
    files_scanned: int = 3,
) -> AuditReport:
    """Return a minimal AuditReport for use in tests."""
    return AuditReport(
        findings=findings or [],
        files_scanned=files_scanned,
        scan_duration_seconds=0.5,
        config_used={},
    )


def _llm_config(
    enabled: bool = True,
    min_severity: str = "WARNING",
    max_calls: int = 20,
    batch_size: int = 5,
    model: str = "claude-sonnet-4-6",
) -> LintConfig:
    """Return a LintConfig with the given LLM settings."""
    return LintConfig.from_dict(
        {
            "llm": {
                "enabled": enabled,
                "provider": "anthropic",
                "model": model,
                "api_key_env_var": "ANTHROPIC_API_KEY",
                "max_tokens": 512,
                "batch_size": batch_size,
                "min_severity_for_llm": min_severity,
                "max_llm_calls": max_calls,
            }
        }
    )


def _mock_anthropic_response(text: str = "Mock LLM insight") -> Any:
    """Build a MagicMock that mimics an anthropic Messages response."""
    content_block = MagicMock()
    content_block.text = text
    response = MagicMock()
    response.content = [content_block]
    return response


def _mock_client(response_text: str = "Mock LLM insight") -> MagicMock:
    """Return a mock anthropic.Anthropic client."""
    client = MagicMock()
    client.messages.create.return_value = _mock_anthropic_response(response_text)
    return client


# =============================================================================
# 1. ClaudeLLMProvider
# =============================================================================


class TestClaudeLLMProviderConstruction:
    """Provider instantiation — key resolution, import guard."""

    def test_raises_valueerror_when_no_api_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        with pytest.raises(ValueError, match="API key not found"):
            ClaudeLLMProvider(api_key=None, api_key_env_var="ANTHROPIC_API_KEY")

    def test_explicit_api_key_bypasses_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        with patch.object(ClaudeLLMProvider, "_build_client", return_value=_mock_client()):
            provider = ClaudeLLMProvider(api_key="sk-explicit")
        assert provider.model == "claude-sonnet-4-6"

    def test_reads_api_key_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-from-env")
        with patch.object(ClaudeLLMProvider, "_build_client", return_value=_mock_client()):
            provider = ClaudeLLMProvider()
        assert provider.model == "claude-sonnet-4-6"

    def test_raises_importerror_when_anthropic_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
        with patch("builtins.__import__", side_effect=ImportError("No module named 'anthropic'")):
            with pytest.raises(ImportError, match="anthropic"):
                ClaudeLLMProvider._build_client("sk-test")

    def test_custom_model_and_max_tokens(self) -> None:
        with patch.object(ClaudeLLMProvider, "_build_client", return_value=_mock_client()):
            provider = ClaudeLLMProvider(
                api_key="sk-test",
                model="claude-opus-4-6",
                max_tokens=2048,
            )
        assert provider.model == "claude-opus-4-6"
        assert provider._default_max_tokens == 2048

    def test_repr(self) -> None:
        with patch.object(ClaudeLLMProvider, "_build_client", return_value=_mock_client()):
            provider = ClaudeLLMProvider(api_key="sk-test", model="claude-sonnet-4-6")
        assert "claude-sonnet-4-6" in repr(provider)

    def test_from_config_reads_model(self) -> None:
        config = _llm_config(model="claude-opus-4-6")
        with patch.object(ClaudeLLMProvider, "_build_client", return_value=_mock_client()):
            with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
                provider = ClaudeLLMProvider.from_config(config)
        assert provider.model == "claude-opus-4-6"

    def test_from_config_reads_max_tokens(self) -> None:
        config = LintConfig.from_dict({"llm": {"max_tokens": 4096, "api_key_env_var": "MY_KEY"}})
        with patch.object(ClaudeLLMProvider, "_build_client", return_value=_mock_client()):
            with patch.dict("os.environ", {"MY_KEY": "sk-test"}):
                provider = ClaudeLLMProvider.from_config(config)
        assert provider._default_max_tokens == 4096


class TestClaudeLLMProviderComplete:
    """complete() — happy path, system prompt, max_tokens, retry, exhaustion."""

    def _make_provider(self, client: MagicMock) -> ClaudeLLMProvider:
        with patch.object(ClaudeLLMProvider, "_build_client", return_value=client):
            return ClaudeLLMProvider(api_key="sk-test")

    def test_returns_response_text(self) -> None:
        client = _mock_client("Great insight here")
        provider = self._make_provider(client)
        result = provider.complete([{"role": "user", "content": "Explain this."}])
        assert result == "Great insight here"

    def test_passes_messages_to_api(self) -> None:
        client = _mock_client()
        provider = self._make_provider(client)
        messages = [{"role": "user", "content": "Analyse this code."}]
        provider.complete(messages)
        call_kwargs = client.messages.create.call_args.kwargs
        assert call_kwargs["messages"] == messages

    def test_passes_system_prompt(self) -> None:
        client = _mock_client()
        provider = self._make_provider(client)
        provider.complete([{"role": "user", "content": "Q"}], system="Be concise.")
        call_kwargs = client.messages.create.call_args.kwargs
        assert call_kwargs["system"] == "Be concise."

    def test_omits_system_when_none(self) -> None:
        client = _mock_client()
        provider = self._make_provider(client)
        provider.complete([{"role": "user", "content": "Q"}], system=None)
        call_kwargs = client.messages.create.call_args.kwargs
        assert "system" not in call_kwargs

    def test_per_call_max_tokens_overrides_default(self) -> None:
        client = _mock_client()
        with patch.object(ClaudeLLMProvider, "_build_client", return_value=client):
            provider = ClaudeLLMProvider(api_key="sk-test", max_tokens=512)
        provider.complete([{"role": "user", "content": "Q"}], max_tokens=2048)
        call_kwargs = client.messages.create.call_args.kwargs
        assert call_kwargs["max_tokens"] == 2048

    def test_uses_default_max_tokens_when_not_overridden(self) -> None:
        client = _mock_client()
        with patch.object(ClaudeLLMProvider, "_build_client", return_value=client):
            provider = ClaudeLLMProvider(api_key="sk-test", max_tokens=999)
        provider.complete([{"role": "user", "content": "Q"}])
        call_kwargs = client.messages.create.call_args.kwargs
        assert call_kwargs["max_tokens"] == 999

    def test_retries_on_transient_failure(self) -> None:
        client = MagicMock()
        good_response = _mock_anthropic_response("Success on retry")
        client.messages.create.side_effect = [Exception("Rate limited"), good_response]

        provider = self._make_provider(client)
        with patch("spark_perf_lint.llm.provider.time.sleep"):
            result = provider.complete([{"role": "user", "content": "Q"}])

        assert result == "Success on retry"
        assert client.messages.create.call_count == 2

    def test_raises_after_all_retries_exhausted(self) -> None:
        client = MagicMock()
        client.messages.create.side_effect = ConnectionError("Network down")

        provider = self._make_provider(client)
        with patch("spark_perf_lint.llm.provider.time.sleep"):
            with pytest.raises(RuntimeError, match="failed after"):
                provider.complete([{"role": "user", "content": "Q"}])

        assert client.messages.create.call_count == 3  # _RETRY_DELAYS has 3 entries

    def test_sleeps_between_retries(self) -> None:
        client = MagicMock()
        client.messages.create.side_effect = [
            Exception("Fail 1"),
            Exception("Fail 2"),
            Exception("Fail 3"),
        ]
        provider = self._make_provider(client)
        with patch("spark_perf_lint.llm.provider.time.sleep") as mock_sleep:
            with pytest.raises(RuntimeError):
                provider.complete([{"role": "user", "content": "Q"}])
        # Two sleeps between 3 attempts
        assert mock_sleep.call_count == 2


# =============================================================================
# 2. PromptTemplates
# =============================================================================


class TestPromptTemplatesSystem:
    def test_system_prompt_nonempty(self) -> None:
        assert PromptTemplates.SYSTEM
        assert len(PromptTemplates.SYSTEM) > 50

    def test_system_prompt_mentions_spark(self) -> None:
        assert "Spark" in PromptTemplates.SYSTEM

    def test_system_prompt_mentions_performance(self) -> None:
        assert "performance" in PromptTemplates.SYSTEM.lower()


class TestPromptTemplatesFindingAnalysis:
    """finding_analysis() — content coverage."""

    def _make_messages(self, **kwargs: Any) -> str:
        """Return the user content string for a finding_analysis call."""
        f = _finding(**kwargs)
        msgs = PromptTemplates.finding_analysis(f, "10 | df1.crossJoin(df2)")
        assert len(msgs) == 1
        assert msgs[0]["role"] == "user"
        return msgs[0]["content"]

    def test_returns_single_user_message(self) -> None:
        f = _finding()
        msgs = PromptTemplates.finding_analysis(f, "snippet")
        assert isinstance(msgs, list)
        assert len(msgs) == 1
        assert msgs[0]["role"] == "user"

    def test_contains_rule_id(self) -> None:
        content = self._make_messages(rule_id="SPL-D03-001")
        assert "SPL-D03-001" in content

    def test_contains_severity(self) -> None:
        content = self._make_messages(severity=Severity.CRITICAL)
        assert "CRITICAL" in content

    def test_contains_file_path(self) -> None:
        content = self._make_messages(file_path="src/jobs/pipeline.py")
        assert "src/jobs/pipeline.py" in content

    def test_contains_line_number(self) -> None:
        content = self._make_messages(line_number=42)
        assert "42" in content

    def test_contains_source_snippet(self) -> None:
        f = _finding()
        msgs = PromptTemplates.finding_analysis(f, "df1.crossJoin(df2)  # dangerous")
        assert "df1.crossJoin(df2)" in msgs[0]["content"]

    def test_includes_before_code_when_present(self) -> None:
        content = self._make_messages(before_code="df1.crossJoin(df2)")
        assert "df1.crossJoin(df2)" in content

    def test_includes_after_code_when_present(self) -> None:
        content = self._make_messages(after_code="df1.join(df2, 'id')")
        assert "df1.join(df2, 'id')" in content

    def test_omits_before_after_section_when_none(self) -> None:
        f = _finding(before_code=None, after_code=None)
        msgs = PromptTemplates.finding_analysis(f, "snippet")
        # Should not contain the section headers for code examples
        assert "Anti-pattern" not in msgs[0]["content"]
        assert "Corrected pattern" not in msgs[0]["content"]

    def test_includes_estimated_impact_when_set(self) -> None:
        content = self._make_messages(estimated_impact="10× slowdown")
        assert "10× slowdown" in content

    def test_asks_for_root_cause(self) -> None:
        content = self._make_messages()
        assert "Root cause" in content

    def test_asks_for_concrete_fix(self) -> None:
        content = self._make_messages()
        assert "fix" in content.lower()

    def test_asks_for_impact_estimate(self) -> None:
        content = self._make_messages()
        assert "Impact" in content or "impact" in content

    def test_asks_for_caveats(self) -> None:
        content = self._make_messages()
        assert "Caveats" in content or "caveat" in content.lower()


class TestPromptTemplatesCrossFile:
    """cross_file_analysis() — content coverage."""

    def _make(self, findings_by_file: dict[str, list[Finding]]) -> str:
        msgs = PromptTemplates.cross_file_analysis(findings_by_file)
        assert len(msgs) == 1 and msgs[0]["role"] == "user"
        return msgs[0]["content"]

    def test_returns_single_user_message(self) -> None:
        msgs = PromptTemplates.cross_file_analysis({})
        assert isinstance(msgs, list)
        assert msgs[0]["role"] == "user"

    def test_contains_file_paths(self) -> None:
        content = self._make(
            {
                "jobs/etl.py": [_finding(file_path="jobs/etl.py")],
                "jobs/report.py": [_finding(file_path="jobs/report.py")],
            }
        )
        assert "jobs/etl.py" in content
        assert "jobs/report.py" in content

    def test_contains_finding_count(self) -> None:
        content = self._make({"jobs/etl.py": [_finding(), _finding(line_number=20)]})
        assert "2" in content

    def test_contains_severity_labels(self) -> None:
        content = self._make({"jobs/etl.py": [_finding(severity=Severity.CRITICAL)]})
        assert "CRITICAL" in content

    def test_contains_rule_ids(self) -> None:
        content = self._make({"jobs/etl.py": [_finding(rule_id="SPL-D08-001")]})
        assert "SPL-D08-001" in content

    def test_asks_for_systemic_patterns(self) -> None:
        content = self._make({"f.py": [_finding()]})
        assert "Systemic" in content or "pattern" in content.lower()

    def test_asks_for_quick_wins(self) -> None:
        content = self._make({"f.py": [_finding()]})
        assert "quick win" in content.lower() or "Quick win" in content

    def test_empty_dict_doesnt_raise(self) -> None:
        content = self._make({})
        assert "0" in content or "no" in content.lower()


class TestPromptTemplatesConfigCoherence:
    """config_coherence() — content coverage."""

    def _entry(self, param: str = "spark.sql.adaptive.enabled") -> SparkConfigEntry:
        return SparkConfigEntry(
            parameter=param,
            current_value="false",
            recommended_value="true",
            reason="AQE improves performance.",
            impact="Static plan cannot adapt to skew.",
        )

    def _make(self, entries: list[SparkConfigEntry]) -> str:
        msgs = PromptTemplates.config_coherence(entries)
        assert msgs[0]["role"] == "user"
        return msgs[0]["content"]

    def test_returns_single_user_message(self) -> None:
        msgs = PromptTemplates.config_coherence([])
        assert len(msgs) == 1

    def test_contains_parameter_name(self) -> None:
        content = self._make([self._entry("spark.sql.adaptive.enabled")])
        assert "spark.sql.adaptive.enabled" in content

    def test_contains_current_value(self) -> None:
        content = self._make([self._entry()])
        assert "false" in content

    def test_contains_recommended_value(self) -> None:
        content = self._make([self._entry()])
        assert "true" in content

    def test_contains_all_entries(self) -> None:
        entries = [self._entry("spark.a"), self._entry("spark.b")]
        content = self._make(entries)
        assert "spark.a" in content
        assert "spark.b" in content

    def test_asks_for_coherence_assessment(self) -> None:
        content = self._make([self._entry()])
        assert "Coherence" in content or "coherence" in content.lower()

    def test_asks_for_dependency_order(self) -> None:
        content = self._make([self._entry()])
        assert "order" in content.lower() or "Dependency" in content


class TestPromptTemplatesExecutiveSummary:
    """executive_summary() — content coverage."""

    def _make(self, report: AuditReport) -> str:
        msgs = PromptTemplates.executive_summary(report)
        assert msgs[0]["role"] == "user"
        return msgs[0]["content"]

    def test_returns_single_user_message(self) -> None:
        msgs = PromptTemplates.executive_summary(_report())
        assert len(msgs) == 1

    def test_contains_files_scanned(self) -> None:
        report = _report(files_scanned=15)
        content = self._make(report)
        assert "15" in content

    def test_contains_total_findings(self) -> None:
        report = _report(findings=[_finding()] * 7)
        content = self._make(report)
        assert "7" in content

    def test_contains_critical_count(self) -> None:
        findings = [_finding(severity=Severity.CRITICAL)] * 3
        report = _report(findings=findings)
        content = self._make(report)
        assert "3" in content
        assert "CRITICAL" in content

    def test_contains_active_dimensions(self) -> None:
        # _finding() sets dimension=Dimension.D03_JOINS; the summary key is the
        # enum .name, i.e. "D03_JOINS", not the short "D03" code.
        report = _report(findings=[_finding()])
        content = self._make(report)
        assert "D03_JOINS" in content

    def test_asks_for_risk_score(self) -> None:
        content = self._make(_report())
        assert "Risk" in content or "risk" in content.lower()

    def test_asks_for_immediate_actions(self) -> None:
        content = self._make(_report())
        assert "action" in content.lower() or "Action" in content

    def test_asks_for_executive_summary_not_technical_jargon(self) -> None:
        content = self._make(_report())
        # Should mention writing for non-experts
        assert "non-" in content.lower() or "manager" in content.lower()


# =============================================================================
# 3. LLMAnalyzer — finding enrichment
# =============================================================================


class TestLLMAnalyzerEnrichFindings:
    """enrich_findings() — qualification, severity filter, batch cap, budget."""

    def _analyzer(
        self,
        response: str = "Detailed LLM insight",
        min_severity: str = "WARNING",
        batch_size: int = 5,
        max_calls: int = 20,
    ) -> tuple[LLMAnalyzer, MagicMock]:
        mock_provider = MagicMock(spec=LLMProvider)
        mock_provider.model = "claude-test"
        mock_provider.complete.return_value = response
        config = _llm_config(
            min_severity=min_severity,
            batch_size=batch_size,
            max_calls=max_calls,
        )
        return LLMAnalyzer(config=config, provider=mock_provider), mock_provider

    def test_sets_llm_insight_on_qualifying_finding(self) -> None:
        analyzer, _ = self._analyzer()
        findings = [_finding(severity=Severity.CRITICAL)]
        result = analyzer.enrich_findings(findings)
        assert result[0].llm_insight == "Detailed LLM insight"

    def test_strips_whitespace_from_response(self) -> None:
        analyzer, _ = self._analyzer(response="  Insight with spaces  \n")
        findings = [_finding(severity=Severity.CRITICAL)]
        result = analyzer.enrich_findings(findings)
        assert result[0].llm_insight == "Insight with spaces"

    def test_skips_finding_below_min_severity(self) -> None:
        analyzer, mock_provider = self._analyzer(min_severity="CRITICAL")
        findings = [
            _finding(severity=Severity.INFO),
            _finding(severity=Severity.WARNING, line_number=20),
        ]
        result = analyzer.enrich_findings(findings)
        assert all(f.llm_insight is None for f in result)
        mock_provider.complete.assert_not_called()

    def test_enriches_findings_at_or_above_min_severity(self) -> None:
        analyzer, mock_provider = self._analyzer(min_severity="WARNING")
        findings = [
            _finding(severity=Severity.INFO),
            _finding(severity=Severity.WARNING, line_number=20),
            _finding(severity=Severity.CRITICAL, line_number=30),
        ]
        result = analyzer.enrich_findings(findings)
        assert result[0].llm_insight is None  # INFO skipped
        assert result[1].llm_insight is not None  # WARNING enriched
        assert result[2].llm_insight is not None  # CRITICAL enriched
        assert mock_provider.complete.call_count == 2

    def test_respects_batch_size_per_file(self) -> None:
        analyzer, mock_provider = self._analyzer(batch_size=2)
        # 5 findings in the same file → only 2 should be enriched
        findings = [
            _finding(file_path="jobs/etl.py", line_number=i, severity=Severity.CRITICAL)
            for i in range(1, 6)
        ]
        result = analyzer.enrich_findings(findings)
        enriched = [f for f in result if f.llm_insight is not None]
        assert len(enriched) == 2

    def test_batch_size_applied_per_file_independently(self) -> None:
        analyzer, mock_provider = self._analyzer(batch_size=2)
        findings = [
            _finding(file_path="file_a.py", line_number=1, severity=Severity.CRITICAL),
            _finding(file_path="file_a.py", line_number=2, severity=Severity.CRITICAL),
            _finding(file_path="file_a.py", line_number=3, severity=Severity.CRITICAL),
            _finding(file_path="file_b.py", line_number=1, severity=Severity.CRITICAL),
            _finding(file_path="file_b.py", line_number=2, severity=Severity.CRITICAL),
            _finding(file_path="file_b.py", line_number=3, severity=Severity.CRITICAL),
        ]
        result = analyzer.enrich_findings(findings)
        enriched_a = [f for f in result if f.file_path == "file_a.py" and f.llm_insight]
        enriched_b = [f for f in result if f.file_path == "file_b.py" and f.llm_insight]
        assert len(enriched_a) == 2
        assert len(enriched_b) == 2

    def test_respects_max_calls_budget(self) -> None:
        analyzer, mock_provider = self._analyzer(max_calls=3)
        findings = [_finding(line_number=i, severity=Severity.CRITICAL) for i in range(1, 10)]
        result = analyzer.enrich_findings(findings, max_calls=2)
        enriched = [f for f in result if f.llm_insight is not None]
        assert len(enriched) == 2
        assert mock_provider.complete.call_count == 2

    def test_empty_findings_returns_empty(self) -> None:
        analyzer, mock_provider = self._analyzer()
        result = analyzer.enrich_findings([])
        assert result == []
        mock_provider.complete.assert_not_called()

    def test_mutates_findings_in_place(self) -> None:
        """enrich_findings must return the same list objects (mutation, not copy)."""
        analyzer, _ = self._analyzer()
        findings = [_finding(severity=Severity.CRITICAL)]
        original_id = id(findings[0])
        result = analyzer.enrich_findings(findings)
        assert id(result[0]) == original_id

    def test_provider_exception_skips_finding_gracefully(self) -> None:
        mock_provider = MagicMock(spec=LLMProvider)
        mock_provider.model = "claude-test"
        mock_provider.complete.side_effect = RuntimeError("API down")
        config = _llm_config()
        analyzer = LLMAnalyzer(config=config, provider=mock_provider)
        findings = [_finding(severity=Severity.CRITICAL)]
        # Should not raise
        result = analyzer.enrich_findings(findings)
        assert result[0].llm_insight is None

    def test_source_map_snippet_passed_to_provider(self) -> None:
        mock_provider = MagicMock(spec=LLMProvider)
        mock_provider.model = "claude-test"
        mock_provider.complete.return_value = "insight"
        config = _llm_config()
        analyzer = LLMAnalyzer(config=config, provider=mock_provider)

        source_map = {"jobs/etl.py": "\n".join(f"line {i}" for i in range(1, 30))}
        findings = [_finding(file_path="jobs/etl.py", line_number=15, severity=Severity.CRITICAL)]
        analyzer.enrich_findings(findings, source_map=source_map)

        # The messages passed should contain source lines
        call_args = mock_provider.complete.call_args
        messages = call_args.args[0]
        user_content = messages[0]["content"]
        assert "line 15" in user_content or "15 |" in user_content


# =============================================================================
# 4. LLMAnalyzer — analyze() orchestration
# =============================================================================


class TestLLMAnalyzerAnalyze:
    """analyze() — full orchestration, call counts, multi-phase flow."""

    def _analyzer_with_mock(
        self,
        response: str = "LLM text",
        max_calls: int = 20,
    ) -> tuple[LLMAnalyzer, MagicMock]:
        mock_provider = MagicMock(spec=LLMProvider)
        mock_provider.model = "claude-test"
        mock_provider.complete.return_value = response
        config = _llm_config(max_calls=max_calls)
        return LLMAnalyzer(config=config, provider=mock_provider), mock_provider

    def test_returns_llm_analysis_result(self) -> None:
        analyzer, _ = self._analyzer_with_mock()
        result = analyzer.analyze(_report())
        assert isinstance(result, LLMAnalysisResult)

    def test_enriched_findings_list_contains_all_original_findings(self) -> None:
        analyzer, _ = self._analyzer_with_mock()
        findings = [_finding(severity=Severity.CRITICAL, line_number=i) for i in range(1, 4)]
        result = analyzer.analyze(_report(findings=findings))
        assert len(result.enriched_findings) == 3

    def test_calls_made_reflects_actual_api_calls(self) -> None:
        analyzer, mock_provider = self._analyzer_with_mock()
        # 2 CRITICAL findings + 1 cross-file (single file, skip) + 1 exec summary
        findings = [
            _finding(severity=Severity.CRITICAL, line_number=1),
            _finding(severity=Severity.CRITICAL, line_number=2),
        ]
        result = analyzer.analyze(_report(findings=findings))
        # 2 per-finding + 1 exec summary (1 file → no cross-file) = 3
        assert result.calls_made == 3

    def test_cross_file_called_when_multiple_files(self) -> None:
        analyzer, mock_provider = self._analyzer_with_mock()
        findings = [
            _finding(file_path="file_a.py", severity=Severity.CRITICAL),
            _finding(file_path="file_b.py", severity=Severity.CRITICAL, line_number=5),
        ]
        result = analyzer.analyze(_report(findings=findings))
        # 2 per-finding + 1 cross-file + 1 exec summary = 4
        assert result.calls_made == 4

    def test_cross_file_skipped_when_single_file(self) -> None:
        analyzer, mock_provider = self._analyzer_with_mock()
        findings = [
            _finding(file_path="only.py", severity=Severity.CRITICAL, line_number=1),
            _finding(file_path="only.py", severity=Severity.CRITICAL, line_number=2),
        ]
        result = analyzer.analyze(_report(findings=findings))
        # No cross-file for single file
        assert result.cross_file_insights == ""

    def test_cross_file_skipped_when_no_findings(self) -> None:
        analyzer, _ = self._analyzer_with_mock()
        result = analyzer.analyze(_report(findings=[]))
        assert result.cross_file_insights == ""

    def test_executive_summary_produced(self) -> None:
        analyzer, _ = self._analyzer_with_mock(response="Summary text here")
        result = analyzer.analyze(_report())
        assert result.executive_summary == "Summary text here"

    def test_model_name_in_result(self) -> None:
        analyzer, mock_provider = self._analyzer_with_mock()
        result = analyzer.analyze(_report())
        assert result.model == "claude-test"

    def test_max_calls_zero_skips_all(self) -> None:
        analyzer, mock_provider = self._analyzer_with_mock(max_calls=0)
        findings = [_finding(severity=Severity.CRITICAL, line_number=i) for i in range(1, 5)]
        result = analyzer.analyze(_report(findings=findings))
        mock_provider.complete.assert_not_called()
        assert result.calls_made == 0
        assert result.executive_summary == ""
        assert result.cross_file_insights == ""

    def test_max_calls_one_goes_only_to_exec_summary(self) -> None:
        """With max_calls=1, finding budget is 0; the 1 call goes to exec summary."""
        analyzer, mock_provider = self._analyzer_with_mock(max_calls=1)
        findings = [_finding(severity=Severity.CRITICAL, line_number=1)]
        result = analyzer.analyze(_report(findings=findings))
        # Budget: 1 total - 2 reserved = 0 for findings → exec summary gets 1
        assert result.enriched_findings[0].llm_insight is None
        assert result.executive_summary != ""

    def test_cross_file_failure_recorded_in_errors(self) -> None:
        mock_provider = MagicMock(spec=LLMProvider)
        mock_provider.model = "claude-test"
        # 2 findings → 2 per-finding calls, then 1 cross-file call (fails), then exec summary
        mock_provider.complete.side_effect = [
            "finding insight a",  # per-finding enrichment: a.py
            "finding insight b",  # per-finding enrichment: b.py
            RuntimeError("cross-file API error"),  # cross-file analysis fails
            "exec summary",  # exec summary succeeds
        ]
        config = _llm_config()
        analyzer = LLMAnalyzer(config=config, provider=mock_provider)
        findings = [
            _finding(file_path="a.py", severity=Severity.CRITICAL, line_number=1),
            _finding(file_path="b.py", severity=Severity.CRITICAL, line_number=1),
        ]
        result = analyzer.analyze(_report(findings=findings))
        assert any("cross-file" in e.lower() for e in result.errors)
        assert result.cross_file_insights == ""

    def test_exec_summary_failure_recorded_in_errors(self) -> None:
        mock_provider = MagicMock(spec=LLMProvider)
        mock_provider.model = "claude-test"
        mock_provider.complete.side_effect = RuntimeError("exec summary error")
        config = _llm_config(min_severity="CRITICAL")
        analyzer = LLMAnalyzer(config=config, provider=mock_provider)
        # No findings → skip to exec summary, which fails
        result = analyzer.analyze(_report(findings=[]))
        assert any("executive" in e.lower() or "summary" in e.lower() for e in result.errors)
        assert result.executive_summary == ""


# =============================================================================
# 5. Graceful degradation
# =============================================================================


class TestLLMAnalyzerGracefulDegradation:
    """Analyzer must never crash; ImportError and ValueError degrade silently."""

    def test_analyze_survives_importerror_from_provider(self) -> None:
        """When anthropic is not installed, analyze() returns findings unmodified."""
        config = _llm_config()
        analyzer = LLMAnalyzer(config=config, provider=None)

        # Patch _get_provider so every call (model lookup + per-finding) raises.
        # analyze() wraps the model lookup; enrich_findings() per-finding loop
        # catches exceptions — so the full call must not propagate.
        with patch.object(
            LLMAnalyzer,
            "_get_provider",
            side_effect=ImportError("No module named 'anthropic'"),
        ):
            findings = [_finding(severity=Severity.CRITICAL)]
            result = analyzer.analyze(_report(findings=findings))

        assert len(result.enriched_findings) == 1
        assert result.enriched_findings[0].llm_insight is None

    def test_analyze_survives_valueerror_missing_api_key(self) -> None:
        config = _llm_config()
        analyzer = LLMAnalyzer(config=config, provider=None)

        with patch.object(
            LLMAnalyzer,
            "_get_provider",
            side_effect=ValueError("API key not found"),
        ):
            findings = [_finding(severity=Severity.CRITICAL)]
            result = analyzer.analyze(_report(findings=findings))

        assert result.enriched_findings[0].llm_insight is None

    def test_analyze_survives_runtimeerror_on_all_calls(self) -> None:
        mock_provider = MagicMock(spec=LLMProvider)
        mock_provider.model = "claude-test"
        mock_provider.complete.side_effect = RuntimeError("Service unavailable")
        config = _llm_config()
        analyzer = LLMAnalyzer(config=config, provider=mock_provider)

        findings = [
            _finding(severity=Severity.CRITICAL, file_path="a.py"),
            _finding(severity=Severity.CRITICAL, file_path="b.py", line_number=5),
        ]
        result = analyzer.analyze(_report(findings=findings))

        # All calls failed but result is still valid
        assert isinstance(result, LLMAnalysisResult)
        assert all(f.llm_insight is None for f in result.enriched_findings)

    def test_from_config_does_not_raise_without_anthropic(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Constructing LLMAnalyzer never imports anthropic."""
        config = _llm_config()
        # Should not raise even though no anthropic package and no API key
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        analyzer = LLMAnalyzer.from_config(config)
        assert isinstance(analyzer, LLMAnalyzer)

    def test_tier1_findings_preserved_when_llm_disabled(self) -> None:
        """When llm.enabled=False and no --llm flag, findings must be untouched."""
        config = _llm_config(enabled=False)
        mock_provider = MagicMock(spec=LLMProvider)
        LLMAnalyzer(config=config, provider=mock_provider)

        findings = [_finding(severity=Severity.CRITICAL, line_number=i) for i in range(1, 6)]
        # Simulate what the CLI does: only call analyze() if llm_enabled or --llm
        # Here we test that an analyst with max_calls=0 leaves findings unchanged
        config_no_calls = _llm_config(max_calls=0)
        analyzer_no_calls = LLMAnalyzer(config=config_no_calls, provider=mock_provider)
        result = analyzer_no_calls.analyze(_report(findings=findings))

        mock_provider.complete.assert_not_called()
        assert all(f.llm_insight is None for f in result.enriched_findings)
        assert [f.rule_id for f in result.enriched_findings] == [f.rule_id for f in findings]


# =============================================================================
# 6. Snippet extraction
# =============================================================================


class TestSnippetExtraction:
    """_extract_snippet() — source_map lookup, disk read, clamp, placeholder."""

    def _extract(
        self,
        finding: Finding,
        source_map: dict[str, str] | None,
    ) -> str:
        return LLMAnalyzer._extract_snippet(finding, source_map)

    def test_extracts_from_source_map(self) -> None:
        source = "\n".join(f"code line {i}" for i in range(1, 50))
        f = _finding(file_path="etl.py", line_number=25)
        snippet = self._extract(f, {"etl.py": source})
        assert "code line 25" in snippet
        # Should include context lines
        assert "code line 20" in snippet or "code line 15" in snippet

    def test_source_map_lookup_uses_file_path_key(self) -> None:
        source = "line A\nline B\nline C"
        f = _finding(file_path="jobs/etl.py", line_number=2)
        snippet = self._extract(f, {"jobs/etl.py": source})
        assert "line B" in snippet

    def test_returns_placeholder_when_file_not_in_map_and_not_on_disk(self) -> None:
        f = _finding(file_path="/nonexistent/path/ghost.py", line_number=1)
        snippet = self._extract(f, {})
        assert "not available" in snippet.lower() or "Source" in snippet

    def test_returns_placeholder_when_source_map_is_none_and_file_missing(self) -> None:
        f = _finding(file_path="/nonexistent/abc.py", line_number=1)
        snippet = self._extract(f, None)
        assert "not available" in snippet.lower() or "Source" in snippet

    def test_reads_from_disk_when_not_in_source_map(self, tmp_path: Any) -> None:
        src_file = tmp_path / "real.py"
        src_file.write_text("line one\nline two\nline three\n")
        f = _finding(file_path=str(src_file), line_number=2)
        snippet = self._extract(f, {})
        assert "line two" in snippet

    def test_line_numbers_appear_in_output(self) -> None:
        source = "\n".join(f"x = {i}" for i in range(1, 30))
        f = _finding(file_path="f.py", line_number=15)
        snippet = self._extract(f, {"f.py": source})
        # Numbers like "15 |" or similar should appear
        assert "15" in snippet

    def test_clamps_to_start_of_file(self) -> None:
        source = "line one\nline two\nline three\n"
        f = _finding(file_path="f.py", line_number=1)
        # Should not raise even though finding is at top of file
        snippet = self._extract(f, {"f.py": source})
        assert "line one" in snippet

    def test_clamps_to_end_of_file(self) -> None:
        source = "line one\nline two\nline three\n"
        f = _finding(file_path="f.py", line_number=3)
        snippet = self._extract(f, {"f.py": source})
        assert "line three" in snippet


# =============================================================================
# 7. Finding.llm_insight field (backward compat)
# =============================================================================


class TestFindingLlmInsightField:
    """The new llm_insight field must be backward-compatible and serialisable."""

    def test_defaults_to_none(self) -> None:
        f = _finding()
        assert f.llm_insight is None

    def test_can_be_set_at_construction(self) -> None:
        f = Finding(
            rule_id="SPL-D03-001",
            severity=Severity.CRITICAL,
            dimension=Dimension.D03_JOINS,
            file_path="test.py",
            line_number=1,
            message="msg",
            explanation="expl",
            recommendation="rec",
            llm_insight="LLM said this.",
        )
        assert f.llm_insight == "LLM said this."

    def test_can_be_set_after_construction(self) -> None:
        f = _finding()
        f.llm_insight = "Post-construction insight"
        assert f.llm_insight == "Post-construction insight"

    def test_included_in_to_dict_when_none(self) -> None:
        d = _finding().to_dict()
        assert "llm_insight" in d
        assert d["llm_insight"] is None

    def test_included_in_to_dict_when_set(self) -> None:
        f = _finding()
        f.llm_insight = "Specific Spark issue here"
        d = f.to_dict()
        assert d["llm_insight"] == "Specific Spark issue here"

    def test_other_fields_unaffected(self) -> None:
        f = _finding(rule_id="SPL-D08-001", severity=Severity.WARNING)
        f.llm_insight = "Some insight"
        assert f.rule_id == "SPL-D08-001"
        assert f.severity == Severity.WARNING
        assert f.llm_insight == "Some insight"


# =============================================================================
# 8. LLMAnalyzer construction and repr
# =============================================================================


class TestLLMAnalyzerConstruction:
    def test_from_config_returns_analyzer(self) -> None:
        config = _llm_config()
        analyzer = LLMAnalyzer.from_config(config)
        assert isinstance(analyzer, LLMAnalyzer)

    def test_repr_shows_lazy_before_first_call(self) -> None:
        config = _llm_config()
        analyzer = LLMAnalyzer.from_config(config)
        assert "lazy" in repr(analyzer)

    def test_repr_shows_provider_after_injection(self) -> None:
        mock_provider = MagicMock(spec=LLMProvider)
        mock_provider.__repr__ = lambda self: "MockProvider()"
        config = _llm_config()
        analyzer = LLMAnalyzer(config=config, provider=mock_provider)
        assert "MockProvider" in repr(analyzer)

    def test_provider_injected_directly(self) -> None:
        mock_provider = MagicMock(spec=LLMProvider)
        mock_provider.model = "injected-model"
        config = _llm_config()
        analyzer = LLMAnalyzer(config=config, provider=mock_provider)
        # _get_provider should return the injected provider without building a new one
        assert analyzer._get_provider() is mock_provider

    def test_get_provider_builds_lazily(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-lazy")
        config = _llm_config()
        analyzer = LLMAnalyzer.from_config(config)

        with patch.object(ClaudeLLMProvider, "_build_client", return_value=_mock_client()):
            provider = analyzer._get_provider()

        assert isinstance(provider, ClaudeLLMProvider)

    def test_get_provider_cached_on_second_call(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-lazy")
        config = _llm_config()
        analyzer = LLMAnalyzer.from_config(config)

        with patch.object(ClaudeLLMProvider, "_build_client", return_value=_mock_client()):
            p1 = analyzer._get_provider()
            p2 = analyzer._get_provider()

        assert p1 is p2


# =============================================================================
# 9. Prompt template — system prompt passed to provider
# =============================================================================


class TestSystemPromptPassedToProvider:
    """Verify the analyzer always forwards PromptTemplates.SYSTEM to the provider."""

    def test_finding_enrichment_uses_system_prompt(self) -> None:
        mock_provider = MagicMock(spec=LLMProvider)
        mock_provider.model = "claude-test"
        mock_provider.complete.return_value = "insight"
        config = _llm_config()
        analyzer = LLMAnalyzer(config=config, provider=mock_provider)

        findings = [_finding(severity=Severity.CRITICAL)]
        analyzer.enrich_findings(findings)

        call_kwargs = mock_provider.complete.call_args.kwargs
        assert "system" in call_kwargs
        assert call_kwargs["system"] == PromptTemplates.SYSTEM

    def test_cross_file_uses_system_prompt(self) -> None:
        mock_provider = MagicMock(spec=LLMProvider)
        mock_provider.model = "claude-test"
        mock_provider.complete.return_value = "cross-file"
        config = _llm_config()
        analyzer = LLMAnalyzer(config=config, provider=mock_provider)

        findings = [
            _finding(file_path="a.py", severity=Severity.CRITICAL),
            _finding(file_path="b.py", severity=Severity.CRITICAL, line_number=5),
        ]
        # Call _cross_file_analysis directly to isolate the check
        analyzer._cross_file_analysis(findings)

        call_kwargs = mock_provider.complete.call_args.kwargs
        assert call_kwargs.get("system") == PromptTemplates.SYSTEM

    def test_executive_summary_uses_system_prompt(self) -> None:
        mock_provider = MagicMock(spec=LLMProvider)
        mock_provider.model = "claude-test"
        mock_provider.complete.return_value = "summary"
        config = _llm_config()
        analyzer = LLMAnalyzer(config=config, provider=mock_provider)

        analyzer._executive_summary(_report())

        call_kwargs = mock_provider.complete.call_args.kwargs
        assert call_kwargs.get("system") == PromptTemplates.SYSTEM


# =============================================================================
# 10. LLMAnalysisResult dataclass
# =============================================================================


class TestLLMAnalysisResult:
    def test_default_fields(self) -> None:
        result = LLMAnalysisResult(enriched_findings=[])
        assert result.cross_file_insights == ""
        assert result.executive_summary == ""
        assert result.calls_made == 0
        assert result.model == ""
        assert result.errors == []

    def test_enriched_findings_stored(self) -> None:
        findings = [_finding()]
        result = LLMAnalysisResult(enriched_findings=findings)
        assert result.enriched_findings is findings

    def test_errors_are_independent_per_instance(self) -> None:
        r1 = LLMAnalysisResult(enriched_findings=[])
        r2 = LLMAnalysisResult(enriched_findings=[])
        r1.errors.append("error in r1")
        assert r2.errors == []
