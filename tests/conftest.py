"""Shared pytest fixtures and test utilities for spark-perf-lint.

Fixtures are organised into three groups:

  Code fixtures  — Python/PySpark source strings for rule testing
  Config fixtures — pre-built LintConfig instances at different strictness levels
  Path fixtures   — factory fixtures that materialise code/config to tmp files

Helper functions (not fixtures) are defined at module level so they can be
imported directly into test modules:

  assert_finding_exists   — assert a rule fired, optionally on a specific line
  assert_no_finding       — assert a rule did not fire
  assert_severity         — assert a finding has the expected severity
  count_findings_by_dimension — count findings for a dimension code

Custom markers registered here:
  slow   — performance/stress tests that may take seconds
  tier2  — tests that require a live Anthropic API key
  tier3  — tests that require a running Spark cluster or runtime
"""

from __future__ import annotations

from typing import Any

import pytest

from spark_perf_lint.config import LintConfig
from spark_perf_lint.types import AuditReport, Severity

# =============================================================================
# Marker registration
# =============================================================================


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers so pytest does not warn about unknown marks."""
    config.addinivalue_line("markers", "slow: slow performance tests (may take several seconds)")
    config.addinivalue_line("markers", "tier2: requires a live LLM API key (Anthropic)")
    config.addinivalue_line("markers", "tier3: requires a running Spark runtime")


# =============================================================================
# Code fixtures
# =============================================================================


@pytest.fixture
def sample_spark_code_with_join() -> str:
    """PySpark code containing a basic DataFrame join."""
    return """\
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
df1 = spark.read.parquet("/data/transactions")
df2 = spark.read.parquet("/data/customers")
result = df1.join(df2, "customer_id")
result.write.parquet("/output/joined")
"""


@pytest.fixture
def sample_spark_code_with_config() -> str:
    """PySpark code with a multi-config SparkSession builder chain."""
    return """\
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("etl-pipeline")
    .config("spark.executor.memory", "8g")
    .config("spark.executor.cores", "4")
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)

df = spark.read.parquet("/data/events")
df.write.mode("overwrite").parquet("/output/events")
"""


@pytest.fixture
def empty_python_file() -> str:
    """An empty Python source file (no content at all)."""
    return ""


@pytest.fixture
def non_spark_python_file() -> str:
    """A Python file with no Spark imports or API usage."""
    return """\
import os
import json
from pathlib import Path


def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def main() -> None:
    config = load_config(os.environ.get("CONFIG_PATH", "config.json"))
    output_dir = Path(config.get("output_dir", "/tmp/output"))
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    main()
"""


# =============================================================================
# Config fixtures
# =============================================================================


@pytest.fixture
def default_config() -> LintConfig:
    """A LintConfig built from built-in defaults only (no YAML file)."""
    return LintConfig.from_dict({})


@pytest.fixture
def strict_config() -> LintConfig:
    """A strict LintConfig: all rules enabled, fail on WARNING and above."""
    return LintConfig.from_dict(
        {
            "general": {
                "severity_threshold": "INFO",
                "fail_on": ["WARNING", "CRITICAL"],
            },
        }
    )


@pytest.fixture
def permissive_config() -> LintConfig:
    """A permissive LintConfig: only CRITICAL findings shown, fail on CRITICAL only."""
    return LintConfig.from_dict(
        {
            "general": {
                "severity_threshold": "CRITICAL",
                "fail_on": ["CRITICAL"],
            },
        }
    )


# =============================================================================
# Path / factory fixtures
# =============================================================================


@pytest.fixture
def temp_python_file(tmp_path):
    """Factory fixture: write code to a temporary .py file, return its path string.

    Usage::

        def test_something(temp_python_file):
            path = temp_python_file("df = spark.read.csv('/data')")
            # path is an absolute str, e.g. "/tmp/pytest-xxx/test_something0/test_spark.py"
    """

    def _create(code: str, filename: str = "test_spark.py") -> str:
        path = tmp_path / filename
        path.write_text(code, encoding="utf-8")
        return str(path)

    return _create


@pytest.fixture
def temp_config_file(tmp_path):
    """Factory fixture: write a config dict as YAML, return its path string.

    Usage::

        def test_something(temp_config_file):
            path = temp_config_file({"general": {"severity_threshold": "WARNING"}})
            config = LintConfig.load(start_dir=Path(path).parent)
    """

    def _create(config_dict: dict[str, Any]) -> str:
        import yaml

        path = tmp_path / ".spark-perf-lint.yaml"
        path.write_text(yaml.dump(config_dict), encoding="utf-8")
        return str(path)

    return _create


# =============================================================================
# Engine fixture
# =============================================================================


@pytest.fixture
def scan_code():
    """Convenience fixture: scan a code string and return an AuditReport.

    The orchestrator is not yet implemented; this fixture will raise
    ``NotImplementedError`` until ``ScanOrchestrator.scan_content`` is wired up
    in Phase 2.  Tests that depend on it should be marked accordingly::

        @pytest.mark.xfail(reason="engine not yet implemented", strict=False)
        def test_rule_fires(scan_code):
            report = scan_code("df.groupByKey().count()")
            assert_finding_exists(report, "SPL-D02-002")

    Usage::

        def test_rule_fires(scan_code, default_config):
            report = scan_code(
                "df1.crossJoin(df2).write.parquet('/out')",
                config=default_config,
            )
            assert_finding_exists(report, "SPL-D03-001")
    """

    def _scan(code: str, config: LintConfig | None = None) -> AuditReport:
        from spark_perf_lint.engine.orchestrator import ScanOrchestrator  # noqa: PLC0415

        resolved_config = config or LintConfig.from_dict({})
        orchestrator = ScanOrchestrator(resolved_config)
        return orchestrator.scan_content(code, "test_file.py")

    return _scan


# =============================================================================
# Helper functions (importable, not fixtures)
# =============================================================================


def assert_finding_exists(
    report: AuditReport,
    rule_id: str,
    line: int | None = None,
) -> None:
    """Assert that *rule_id* fired at least once in *report*.

    Args:
        report: The ``AuditReport`` returned by the scan.
        rule_id: Expected rule identifier, e.g. ``"SPL-D03-001"``.
        line: If given, assert the finding is on this 1-based line number.

    Raises:
        AssertionError: If no matching finding is present.
    """
    matches = [f for f in report.findings if f.rule_id == rule_id]
    assert matches, (
        f"Expected finding {rule_id!r} but it was not present.\n"
        f"Findings in report: {[f.rule_id for f in report.findings]}"
    )
    if line is not None:
        on_line = [f for f in matches if f.line_number == line]
        assert on_line, (
            f"Finding {rule_id!r} exists but not on line {line}. "
            f"Found on lines: {[f.line_number for f in matches]}"
        )


def assert_no_finding(report: AuditReport, rule_id: str) -> None:
    """Assert that *rule_id* did NOT fire in *report*.

    Args:
        report: The ``AuditReport`` returned by the scan.
        rule_id: Rule identifier that should be absent.

    Raises:
        AssertionError: If the rule fired one or more times.
    """
    matches = [f for f in report.findings if f.rule_id == rule_id]
    assert not matches, (
        f"Expected rule {rule_id!r} NOT to fire, but found {len(matches)} finding(s) "
        f"on line(s): {[f.line_number for f in matches]}"
    )


def assert_severity(
    report: AuditReport,
    rule_id: str,
    expected_severity: Severity,
) -> None:
    """Assert that all findings for *rule_id* carry *expected_severity*.

    Args:
        report: The ``AuditReport`` returned by the scan.
        rule_id: The rule whose severity to check.
        expected_severity: The ``Severity`` enum value expected.

    Raises:
        AssertionError: If the rule did not fire or its severity differs.
    """
    matches = [f for f in report.findings if f.rule_id == rule_id]
    assert matches, (
        f"Rule {rule_id!r} did not fire — cannot assert severity. "
        f"Findings: {[f.rule_id for f in report.findings]}"
    )
    for finding in matches:
        assert finding.severity == expected_severity, (
            f"Rule {rule_id!r} on line {finding.line_number}: "
            f"expected severity {expected_severity.name}, got {finding.severity.name}"
        )


def count_findings_by_dimension(report: AuditReport, dimension: str) -> int:
    """Count findings whose rule_id belongs to *dimension*.

    Args:
        report: The ``AuditReport`` returned by the scan.
        dimension: Dimension code string, e.g. ``"D03"`` or ``"D08"``.

    Returns:
        Number of findings whose ``rule_id`` contains the dimension code.
    """
    prefix = f"SPL-{dimension.upper()}-"
    return sum(1 for f in report.findings if f.rule_id.startswith(prefix))


# ---------------------------------------------------------------------------
# Convenience re-export so test modules can do:
#   from tests.conftest import assert_finding_exists
# without importing from a non-obvious location.
# ---------------------------------------------------------------------------
__all__ = [
    "assert_finding_exists",
    "assert_no_finding",
    "assert_severity",
    "count_findings_by_dimension",
]
