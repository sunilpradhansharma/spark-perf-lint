"""Configuration loader for spark-perf-lint.

Implements a four-layer configuration stack with the following priority
(lowest to highest):

1. Built-in defaults  — hardcoded in ``_DEFAULTS``
2. Project YAML file  — ``.spark-perf-lint.yaml`` discovered by walking up
                        the directory tree to the git root
3. Environment vars   — ``SPARK_PERF_LINT_*`` prefixed variables
4. CLI arguments      — passed explicitly at invocation time

Each layer is merged onto the previous so that individual keys can be
overridden without repeating unrelated settings.
"""

from __future__ import annotations

import fnmatch
import os
import subprocess
from pathlib import Path
from typing import Any

import yaml

from spark_perf_lint.types import EffortLevel, Severity  # noqa: F401 (re-exported)


# =============================================================================
# Built-in defaults
# =============================================================================

_DEFAULTS: dict[str, Any] = {
    "general": {
        "severity_threshold": "INFO",
        "fail_on": ["CRITICAL"],
        "report_format": ["terminal"],
        "max_findings": 0,
    },
    "rules": {
        "d01_cluster_config": {"enabled": True},
        "d02_shuffle": {"enabled": True},
        "d03_joins": {"enabled": True},
        "d04_partitioning": {"enabled": True},
        "d05_skew": {"enabled": True},
        "d06_caching": {"enabled": True},
        "d07_io_format": {"enabled": True},
        "d08_aqe": {"enabled": True},
        "d09_udf_code": {"enabled": True},
        "d10_catalyst": {"enabled": True},
        "d11_monitoring": {"enabled": True},
    },
    "thresholds": {
        "broadcast_threshold_mb": 10,
        "max_shuffle_partitions": 2000,
        "min_shuffle_partitions": 10,
        "target_partition_size_mb": 128,
        "max_cache_without_unpersist": 5,
        "skew_ratio_warning": 5.0,
        "skew_ratio_critical": 10.0,
        "max_partition_count": 10000,
        "min_partition_count": 2,
        "max_udf_complexity": 10,
        "min_executor_memory_gb": 4,
        "min_driver_memory_gb": 2,
        "max_executor_cores": 5,
        "min_executor_cores": 2,
        "max_with_column_chain": 10,
        "max_chained_filters": 3,
    },
    "severity_override": {},
    "ignore": {
        "files": [],
        "rules": [],
        "directories": [".venv", "venv", "build", "dist", ".eggs"],
    },
    "spark_config_audit": {
        "enabled": False,
        "configs": {},
    },
    "llm": {
        "enabled": False,
        "provider": "anthropic",
        "model": "claude-sonnet-4-6",
        "api_key_env_var": "ANTHROPIC_API_KEY",
        "max_tokens": 1024,
        "batch_size": 5,
        "min_severity_for_llm": "WARNING",
        "max_llm_calls": 20,
    },
    "observability": {
        "enabled": False,
        "backend": "file",
        "output_dir": ".spark-perf-lint-traces",
        "trace_level": "standard",
        "langsmith_project": "spark-perf-lint",
        "langsmith_api_key_env_var": "LANGCHAIN_API_KEY",
    },
}

# Environment variable → (config section, key) mapping.
# Variables not listed here are ignored.
_ENV_VAR_MAP: dict[str, tuple[str, str]] = {
    "SPARK_PERF_LINT_SEVERITY_THRESHOLD": ("general", "severity_threshold"),
    "SPARK_PERF_LINT_FAIL_ON": ("general", "fail_on"),           # comma-separated
    "SPARK_PERF_LINT_REPORT_FORMAT": ("general", "report_format"),  # comma-separated
    "SPARK_PERF_LINT_MAX_FINDINGS": ("general", "max_findings"),
    "SPARK_PERF_LINT_LLM_ENABLED": ("llm", "enabled"),
    "SPARK_PERF_LINT_LLM_MODEL": ("llm", "model"),
    "SPARK_PERF_LINT_LLM_MAX_CALLS": ("llm", "max_llm_calls"),
    "SPARK_PERF_LINT_OBSERVABILITY_ENABLED": ("observability", "enabled"),
    "SPARK_PERF_LINT_OBSERVABILITY_BACKEND": ("observability", "backend"),
    "SPARK_PERF_LINT_OBSERVABILITY_OUTPUT_DIR": ("observability", "output_dir"),
}

# Valid choices for string-enum fields
_VALID_SEVERITY_NAMES = {s.name for s in Severity}
_VALID_REPORT_FORMATS = {"terminal", "json", "markdown", "github_pr"}
_VALID_TRACE_LEVELS = {"minimal", "standard", "verbose"}
_VALID_OBS_BACKENDS = {"file", "langsmith"}
_VALID_LLM_PROVIDERS = {"anthropic"}

CONFIG_FILENAME = ".spark-perf-lint.yaml"


# =============================================================================
# Helpers
# =============================================================================


class ConfigError(ValueError):
    """Raised when the configuration file or values are invalid.

    The message is always user-facing: it names the offending key and
    describes what values are acceptable.
    """


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge *override* onto *base*.

    For nested dicts, keys are merged rather than replaced so that a YAML
    file that sets only ``thresholds.broadcast_threshold_mb`` does not wipe
    out every other threshold.

    Args:
        base: The lower-priority dictionary.
        override: The higher-priority dictionary whose values win on conflict.

    Returns:
        A new dictionary with override values applied on top of base.
    """
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _find_git_root(start: Path) -> Path | None:
    """Walk up from *start* until a ``.git`` directory is found.

    Args:
        start: Directory to begin searching from.

    Returns:
        The git root ``Path``, or ``None`` if not inside a git repository.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=start,
            capture_output=True,
            text=True,
            check=True,
        )
        return Path(result.stdout.strip())
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _find_config_file(start: Path) -> Path | None:
    """Search for ``.spark-perf-lint.yaml`` starting at *start*.

    Walks up the directory tree, stopping at the git root (inclusive) or
    the filesystem root if not inside a git repo.

    Args:
        start: Directory to begin searching from (typically ``Path.cwd()``).

    Returns:
        ``Path`` to the first config file found, or ``None``.
    """
    git_root = _find_git_root(start)
    current = start.resolve()

    while True:
        candidate = current / CONFIG_FILENAME
        if candidate.is_file():
            return candidate
        if git_root and current == git_root.resolve():
            break
        parent = current.parent
        if parent == current:
            # reached filesystem root
            break
        current = parent

    return None


def _coerce_bool(value: str) -> bool:
    """Parse a string as a boolean.

    Args:
        value: String to parse. Accepts ``"true"/"false"``, ``"1"/"0"``,
            ``"yes"/"no"`` (case-insensitive).

    Returns:
        Parsed boolean value.

    Raises:
        ConfigError: If the string is not a recognised boolean literal.
    """
    normalised = value.strip().lower()
    if normalised in {"true", "1", "yes"}:
        return True
    if normalised in {"false", "0", "no"}:
        return False
    raise ConfigError(
        f"Expected a boolean value (true/false/1/0/yes/no), got {value!r}"
    )


def _apply_env_vars(config: dict[str, Any]) -> dict[str, Any]:
    """Apply ``SPARK_PERF_LINT_*`` environment variables onto *config*.

    Scalar and list values are coerced to the correct Python type based on
    the target key's type in the existing config dict.

    Args:
        config: Merged config dictionary to update in-place (a copy is made
            internally; the original is not mutated).

    Returns:
        Updated configuration dictionary.
    """
    result = _deep_merge({}, config)  # shallow copy at top level

    for env_key, (section, key) in _ENV_VAR_MAP.items():
        raw = os.environ.get(env_key)
        if raw is None:
            continue

        existing = result.get(section, {}).get(key)

        if isinstance(existing, bool):
            value: Any = _coerce_bool(raw)
        elif isinstance(existing, int):
            try:
                value = int(raw)
            except ValueError:
                raise ConfigError(
                    f"Environment variable {env_key} must be an integer, got {raw!r}"
                )
        elif isinstance(existing, list):
            # Comma-separated string → list of stripped strings
            value = [item.strip() for item in raw.split(",") if item.strip()]
        else:
            value = raw

        if section not in result:
            result[section] = {}
        result[section][key] = value

    return result


# =============================================================================
# LintConfig
# =============================================================================


class LintConfig:
    """Resolved, validated configuration for a spark-perf-lint scan run.

    Merges four configuration layers (defaults → YAML file → environment
    variables → CLI overrides) and exposes a clean query interface used by
    the engine, rule modules, and reporters.

    Example::

        config = LintConfig.load()
        if config.is_rule_enabled("SPL-D03-001"):
            threshold = config.get_threshold("broadcast_threshold_mb")

    Attributes:
        config_file_path: Path of the YAML config file that was loaded, or
            ``None`` if only built-in defaults were used.
        raw: The fully merged configuration dictionary (read-only; do not
            mutate after construction).
    """

    def __init__(
        self,
        raw: dict[str, Any],
        config_file_path: Path | None = None,
    ) -> None:
        """Initialise from an already-merged and validated config dict.

        Prefer the class-method constructors ``LintConfig.load()`` or
        ``LintConfig.from_dict()`` over calling this directly.

        Args:
            raw: The merged configuration dictionary.
            config_file_path: Source YAML path (for diagnostics/repr).
        """
        self.raw = raw
        self.config_file_path = config_file_path

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def load(
        cls,
        start_dir: Path | None = None,
        cli_overrides: dict[str, Any] | None = None,
    ) -> "LintConfig":
        """Discover, load, merge, and validate configuration.

        Applies the four-layer priority stack:
        built-in defaults → YAML file → env vars → CLI overrides.

        Args:
            start_dir: Directory to start searching for
                ``.spark-perf-lint.yaml``. Defaults to ``Path.cwd()``.
            cli_overrides: Flat or nested dictionary of values supplied by
                the CLI (highest priority). Recognised keys mirror the YAML
                structure, e.g. ``{"general": {"severity_threshold": "WARNING"}}``.

        Returns:
            A fully resolved and validated ``LintConfig`` instance.

        Raises:
            ConfigError: If the YAML file is malformed or contains invalid
                values.
        """
        start_dir = start_dir or Path.cwd()

        # Layer 1: built-in defaults
        merged = _deep_merge({}, _DEFAULTS)

        # Layer 2: YAML file
        config_file_path: Path | None = None
        yaml_path = _find_config_file(start_dir)
        if yaml_path is not None:
            config_file_path = yaml_path
            yaml_data = cls._load_yaml(yaml_path)
            merged = _deep_merge(merged, yaml_data)

        # Layer 3: environment variables
        merged = _apply_env_vars(merged)

        # Layer 4: CLI overrides
        if cli_overrides:
            merged = _deep_merge(merged, cli_overrides)

        # Validate the fully-merged config
        cls._validate(merged)

        return cls(raw=merged, config_file_path=config_file_path)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LintConfig":
        """Construct from an explicit dictionary (useful in tests).

        Merges *data* on top of built-in defaults so tests only need to
        specify the keys they care about.

        Args:
            data: Partial or complete configuration dictionary.

        Returns:
            A validated ``LintConfig`` instance.

        Raises:
            ConfigError: If the merged configuration is invalid.
        """
        merged = _deep_merge(_DEFAULTS, data)
        cls._validate(merged)
        return cls(raw=merged)

    # ------------------------------------------------------------------
    # YAML loading
    # ------------------------------------------------------------------

    @staticmethod
    def _load_yaml(path: Path) -> dict[str, Any]:
        """Parse and basic-validate a YAML config file.

        Args:
            path: Absolute path to the YAML file.

        Returns:
            Parsed dictionary (may be empty if the file is blank).

        Raises:
            ConfigError: If the file cannot be read or is not valid YAML,
                or if the top-level structure is not a mapping.
        """
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConfigError(f"Cannot read config file {path}: {exc}") from exc

        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise ConfigError(
                f"Config file {path} is not valid YAML:\n  {exc}"
            ) from exc

        if data is None:
            return {}
        if not isinstance(data, dict):
            raise ConfigError(
                f"Config file {path} must be a YAML mapping at the top level, "
                f"got {type(data).__name__}"
            )
        return data

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate(cfg: dict[str, Any]) -> None:
        """Validate the fully merged configuration dictionary.

        Checks enum memberships, numeric sign constraints, and
        cross-field consistency.  Raises a single ``ConfigError`` on the
        first violation found.

        Args:
            cfg: Merged configuration dictionary.

        Raises:
            ConfigError: On the first invalid value encountered.
        """
        general = cfg.get("general", {})

        # severity_threshold
        st = general.get("severity_threshold", "INFO")
        if str(st).upper() not in _VALID_SEVERITY_NAMES:
            raise ConfigError(
                f"general.severity_threshold {st!r} is not valid. "
                f"Must be one of: {sorted(_VALID_SEVERITY_NAMES)}"
            )

        # fail_on
        for level in general.get("fail_on", []):
            if str(level).upper() not in _VALID_SEVERITY_NAMES:
                raise ConfigError(
                    f"general.fail_on contains invalid severity {level!r}. "
                    f"Must be one of: {sorted(_VALID_SEVERITY_NAMES)}"
                )

        # report_format
        for fmt in general.get("report_format", []):
            if fmt not in _VALID_REPORT_FORMATS:
                raise ConfigError(
                    f"general.report_format contains unknown format {fmt!r}. "
                    f"Must be one of: {sorted(_VALID_REPORT_FORMATS)}"
                )

        # max_findings
        mf = general.get("max_findings", 0)
        if not isinstance(mf, int) or mf < 0:
            raise ConfigError(
                f"general.max_findings must be a non-negative integer, got {mf!r}"
            )

        # thresholds — all must be positive numbers
        thresholds = cfg.get("thresholds", {})
        for key, val in thresholds.items():
            if not isinstance(val, (int, float)):
                raise ConfigError(
                    f"thresholds.{key} must be a number, got {val!r}"
                )
            if val <= 0:
                raise ConfigError(
                    f"thresholds.{key} must be > 0, got {val!r}"
                )

        # skew ratio ordering
        skew_warn = thresholds.get("skew_ratio_warning", 5.0)
        skew_crit = thresholds.get("skew_ratio_critical", 10.0)
        if skew_warn >= skew_crit:
            raise ConfigError(
                f"thresholds.skew_ratio_warning ({skew_warn}) must be less than "
                f"thresholds.skew_ratio_critical ({skew_crit})"
            )

        # min < max partition constraints
        min_part = thresholds.get("min_partition_count", 2)
        max_part = thresholds.get("max_partition_count", 10000)
        if min_part >= max_part:
            raise ConfigError(
                f"thresholds.min_partition_count ({min_part}) must be less than "
                f"thresholds.max_partition_count ({max_part})"
            )

        min_shuffle = thresholds.get("min_shuffle_partitions", 10)
        max_shuffle = thresholds.get("max_shuffle_partitions", 2000)
        if min_shuffle >= max_shuffle:
            raise ConfigError(
                f"thresholds.min_shuffle_partitions ({min_shuffle}) must be less than "
                f"thresholds.max_shuffle_partitions ({max_shuffle})"
            )

        # severity_override values
        for rule_id, sev in cfg.get("severity_override", {}).items():
            if str(sev).upper() not in _VALID_SEVERITY_NAMES:
                raise ConfigError(
                    f"severity_override[{rule_id!r}] = {sev!r} is not valid. "
                    f"Must be one of: {sorted(_VALID_SEVERITY_NAMES)}"
                )

        # observability
        obs = cfg.get("observability", {})
        backend = obs.get("backend", "file")
        if backend not in _VALID_OBS_BACKENDS:
            raise ConfigError(
                f"observability.backend {backend!r} is not valid. "
                f"Must be one of: {sorted(_VALID_OBS_BACKENDS)}"
            )
        trace_level = obs.get("trace_level", "standard")
        if trace_level not in _VALID_TRACE_LEVELS:
            raise ConfigError(
                f"observability.trace_level {trace_level!r} is not valid. "
                f"Must be one of: {sorted(_VALID_TRACE_LEVELS)}"
            )

        # llm
        llm = cfg.get("llm", {})
        provider = llm.get("provider", "anthropic")
        if provider not in _VALID_LLM_PROVIDERS:
            raise ConfigError(
                f"llm.provider {provider!r} is not valid. "
                f"Must be one of: {sorted(_VALID_LLM_PROVIDERS)}"
            )
        min_sev = llm.get("min_severity_for_llm", "WARNING")
        if str(min_sev).upper() not in _VALID_SEVERITY_NAMES:
            raise ConfigError(
                f"llm.min_severity_for_llm {min_sev!r} is not valid. "
                f"Must be one of: {sorted(_VALID_SEVERITY_NAMES)}"
            )

    # ------------------------------------------------------------------
    # Query interface
    # ------------------------------------------------------------------

    def is_rule_enabled(self, rule_id: str) -> bool:
        """Return whether a specific rule should run.

        A rule is disabled if:
        - Its dimension block has ``enabled: false``, or
        - The individual rule entry is explicitly ``false``, or
        - The rule ID appears in ``ignore.rules``.

        Args:
            rule_id: Rule identifier, e.g. ``"SPL-D03-001"``.

        Returns:
            ``True`` if the rule should be executed.
        """
        if self.should_ignore_rule(rule_id):
            return False

        # Derive dimension key from rule_id: "SPL-D03-001" → "d03"
        try:
            dim_code = rule_id.split("-")[1].lower()  # "d03"
        except IndexError:
            return True

        # Find matching dimension block (e.g. "d03_joins")
        rules_cfg = self.raw.get("rules", {})
        dim_block: dict[str, Any] | None = None
        for block_key, block_val in rules_cfg.items():
            if block_key.startswith(dim_code):
                dim_block = block_val
                break

        if dim_block is None:
            return True

        if not dim_block.get("enabled", True):
            return False

        # Check individual rule toggle (True by default)
        return bool(dim_block.get("rules", {}).get(rule_id, True))

    def get_threshold(self, name: str) -> float:
        """Return a numeric threshold value by name.

        Args:
            name: Threshold key as defined in the ``thresholds`` section,
                e.g. ``"broadcast_threshold_mb"``.

        Returns:
            The resolved numeric value (int or float).

        Raises:
            KeyError: If *name* is not a known threshold key.
        """
        thresholds = self.raw.get("thresholds", {})
        if name not in thresholds:
            raise KeyError(
                f"Unknown threshold {name!r}. "
                f"Known thresholds: {sorted(thresholds.keys())}"
            )
        return float(thresholds[name])

    def get_severity_for(self, rule_id: str) -> Severity:
        """Return the effective ``Severity`` for a rule.

        Checks ``severity_override`` first; falls back to the rule's
        built-in default (which callers must supply separately — this
        method only handles the override layer).

        Note:
            This method returns the *override* severity when one is
            configured.  Rule modules should call it and, if the result
            equals their own default, use that default instead so they
            remain self-documenting.

        Args:
            rule_id: Rule identifier, e.g. ``"SPL-D07-002"``.

        Returns:
            The overridden ``Severity``, or ``Severity.WARNING`` as a
            safe fallback when no override is configured.

        Raises:
            ConfigError: If the configured override value is not a valid
                severity name (should have been caught at load time).
        """
        overrides = self.raw.get("severity_override", {})
        if rule_id in overrides:
            name = str(overrides[rule_id]).upper()
            try:
                return Severity[name]
            except KeyError:
                raise ConfigError(
                    f"severity_override[{rule_id!r}] = {overrides[rule_id]!r} is not valid. "
                    f"Must be one of: {sorted(_VALID_SEVERITY_NAMES)}"
                )
        return Severity.WARNING  # callers should substitute their own default

    def has_severity_override(self, rule_id: str) -> bool:
        """Return whether a severity override is configured for *rule_id*.

        Args:
            rule_id: Rule identifier.

        Returns:
            ``True`` if an entry exists in ``severity_override``.
        """
        return rule_id in self.raw.get("severity_override", {})

    def should_ignore_file(self, path: str | Path) -> bool:
        """Return whether *path* should be excluded from scanning.

        Matches against ``ignore.files`` glob patterns and
        ``ignore.directories`` path components.

        Args:
            path: File path to test (absolute or repo-relative string).

        Returns:
            ``True`` if the file should be skipped.
        """
        path_str = str(path)
        ignore_cfg = self.raw.get("ignore", {})

        # Check ignored directories — any component of the path
        for ignored_dir in ignore_cfg.get("directories", []):
            parts = Path(path_str).parts
            if ignored_dir in parts:
                return True

        # Check file glob patterns
        for pattern in ignore_cfg.get("files", []):
            if fnmatch.fnmatch(path_str, pattern):
                return True
            # Also match against the basename alone
            if fnmatch.fnmatch(Path(path_str).name, pattern):
                return True

        return False

    def should_ignore_rule(self, rule_id: str) -> bool:
        """Return whether *rule_id* is globally suppressed.

        Args:
            rule_id: Rule identifier to check against ``ignore.rules``.

        Returns:
            ``True`` if the rule appears in the ignore list.
        """
        return rule_id in self.raw.get("ignore", {}).get("rules", [])

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    @property
    def severity_threshold(self) -> Severity:
        """The minimum ``Severity`` to include in reports.

        Returns:
            Resolved ``Severity`` enum value.
        """
        name = str(self.raw["general"]["severity_threshold"]).upper()
        return Severity[name]

    @property
    def fail_on(self) -> list[Severity]:
        """Severity levels that cause the linter to exit non-zero.

        Returns:
            List of ``Severity`` values parsed from ``general.fail_on``.
        """
        return [Severity[str(s).upper()] for s in self.raw["general"]["fail_on"]]

    @property
    def report_formats(self) -> list[str]:
        """Requested output formats.

        Returns:
            List of format strings, e.g. ``["terminal", "json"]``.
        """
        return list(self.raw["general"]["report_format"])

    @property
    def max_findings(self) -> int:
        """Maximum findings to emit. 0 = unlimited.

        Returns:
            Integer cap on findings output.
        """
        return int(self.raw["general"]["max_findings"])

    @property
    def llm_enabled(self) -> bool:
        """Whether Tier 2 LLM analysis is active.

        Returns:
            Boolean from ``llm.enabled``.
        """
        return bool(self.raw["llm"]["enabled"])

    @property
    def observability_enabled(self) -> bool:
        """Whether run tracing is active.

        Returns:
            Boolean from ``observability.enabled``.
        """
        return bool(self.raw["observability"]["enabled"])

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        source = str(self.config_file_path) if self.config_file_path else "defaults only"
        return (
            f"LintConfig(source={source!r}, "
            f"severity_threshold={self.severity_threshold.name}, "
            f"llm_enabled={self.llm_enabled})"
        )
