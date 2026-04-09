"""Rule registry with auto-discovery for spark-perf-lint.

The registry is the single source of truth for all active rules. It handles:

- **Auto-discovery**: on first access, all ``rules/d{nn}_*.py`` modules are
  imported via ``importlib``, triggering the ``@register_rule`` decorator on
  each rule class they define.
- **Singleton**: ``RuleRegistry.instance()`` returns the same object for the
  lifetime of the process; no global state is mutated outside the class.
- **Filtering**: callers can query by dimension, by severity, or by
  ``LintConfig`` (which applies ``ignore.rules`` and per-dimension toggles).

Typical usage in the engine::

    from spark_perf_lint.rules.registry import RuleRegistry

    registry = RuleRegistry.instance()
    for rule in registry.get_enabled_rules(config):
        findings += rule.check(analyzer, config)

Typical usage in a rule module::

    from spark_perf_lint.rules.registry import register_rule
    from spark_perf_lint.rules.base import CodeRule

    @register_rule
    class CrossJoinRule(CodeRule):
        rule_id = "SPL-D03-005"
        ...
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from spark_perf_lint.types import Dimension, Severity

if TYPE_CHECKING:
    from spark_perf_lint.config import LintConfig
    from spark_perf_lint.rules.base import BaseRule

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level rule class storage — populated by @register_rule before the
# singleton is constructed.
# ---------------------------------------------------------------------------
_PENDING: list[type[BaseRule]] = []


# =============================================================================
# @register_rule decorator
# =============================================================================


def register_rule(cls: type[BaseRule]) -> type[BaseRule]:
    """Decorator that registers a concrete rule class with the registry.

    Apply to every concrete ``BaseRule`` subclass in a rule module::

        @register_rule
        class BroadcastThresholdRule(ConfigRule):
            rule_id = "SPL-D01-001"
            ...

    The decorator is idempotent: registering the same class twice (e.g.
    via a re-import) silently skips the second registration.

    Args:
        cls: A concrete ``BaseRule`` subclass.

    Returns:
        The unchanged class (decorator is transparent).

    Raises:
        TypeError: If *cls* is not a ``BaseRule`` subclass.
    """
    from spark_perf_lint.rules.base import BaseRule as _BaseRule  # local import avoids cycle

    if not (isinstance(cls, type) and issubclass(cls, _BaseRule)):
        raise TypeError(f"@register_rule can only be applied to BaseRule subclasses, got {cls!r}")

    # Add to the pending list; duplicates are filtered when the singleton
    # is built (or immediately if it already exists).
    singleton = RuleRegistry._instance
    if singleton is not None:
        singleton._register(cls)
    else:
        if cls not in _PENDING:
            _PENDING.append(cls)

    return cls


# =============================================================================
# RuleRegistry
# =============================================================================


class RuleRegistry:
    """Singleton registry of all spark-perf-lint rule classes.

    Do not instantiate directly — always obtain the shared instance via
    ``RuleRegistry.instance()``, which triggers auto-discovery on first call.

    Internal data structures (populated at build time):

    - ``_by_id``: ``dict[str, BaseRule]``  — rule_id → instance
    - ``_by_dimension``: ``dict[Dimension, list[BaseRule]]``
    - ``_by_severity``: ``dict[Severity, list[BaseRule]]``
    """

    _instance: RuleRegistry | None = None

    # ------------------------------------------------------------------
    # Singleton constructor
    # ------------------------------------------------------------------

    def __init__(self) -> None:
        """Initialise empty index structures.  Called only once."""
        self._by_id: dict[str, BaseRule] = {}
        self._by_dimension: dict[Dimension, list[BaseRule]] = defaultdict(list)
        self._by_severity: dict[Severity, list[BaseRule]] = defaultdict(list)
        self._discovered: bool = False

    @classmethod
    def instance(cls) -> RuleRegistry:
        """Return the singleton registry, triggering auto-discovery if needed.

        Thread-safety note: discovery is idempotent and happens at import
        time in practice; no locking is required for the pre-commit use case.

        Returns:
            The shared ``RuleRegistry`` instance.
        """
        if cls._instance is None:
            cls._instance = cls()
            # Flush rules registered via @register_rule before the singleton
            # was created (i.e., from top-level decorated class definitions).
            for pending_cls in _PENDING:
                cls._instance._register(pending_cls)
            _PENDING.clear()
            cls._instance._discover()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Destroy the singleton so the next ``instance()`` call rebuilds it.

        Intended for testing only — allows tests to register mock rules in
        isolation without polluting the shared instance.
        """
        cls._instance = None
        _PENDING.clear()

    # ------------------------------------------------------------------
    # Auto-discovery
    # ------------------------------------------------------------------

    def _discover(self) -> None:
        """Import all ``rules/d{nn}_*.py`` modules under this package.

        Uses ``pkgutil.iter_modules`` to find sibling modules whose names
        start with ``d`` (dimension modules), then imports each one.
        The ``@register_rule`` decorators in those modules fire on import
        and call ``_register()`` via ``register_rule()``.

        Modules that fail to import are logged as warnings and skipped so
        a broken rule module never prevents the linter from running.
        """
        if self._discovered:
            return
        self._discovered = True

        import spark_perf_lint.rules as _rules_pkg  # noqa: PLC0415

        pkg_path = _rules_pkg.__path__
        pkg_name = _rules_pkg.__name__

        for module_info in pkgutil.iter_modules(pkg_path):
            name = module_info.name
            # Only import dimension rule modules (d01_*, d02_*, … d11_*)
            if not (name.startswith("d") and len(name) > 1 and name[1].isdigit()):
                continue
            full_name = f"{pkg_name}.{name}"
            try:
                importlib.import_module(full_name)
                logger.debug("Loaded rule module: %s", full_name)
            except ImportError as exc:
                logger.warning("Could not import rule module %s: %s", full_name, exc)

    # ------------------------------------------------------------------
    # Internal registration
    # ------------------------------------------------------------------

    def _register(self, cls: type[BaseRule]) -> None:
        """Register a single rule class into all indices.

        Args:
            cls: A concrete ``BaseRule`` subclass.  If a rule with the
                same ``rule_id`` is already registered, the duplicate is
                silently skipped (first-registered wins).
        """
        rule_id = cls.rule_id
        if rule_id in self._by_id:
            logger.debug("Rule %r already registered; skipping duplicate %s", rule_id, cls)
            return

        instance: BaseRule = cls()
        self._by_id[rule_id] = instance
        self._by_dimension[cls.dimension].append(instance)
        self._by_severity[cls.default_severity].append(instance)
        logger.debug("Registered rule %s (%s)", rule_id, cls.__name__)

    # ------------------------------------------------------------------
    # Public query interface
    # ------------------------------------------------------------------

    def get_all_rules(self) -> list[BaseRule]:
        """Return every registered rule, sorted by rule_id.

        Returns:
            List of ``BaseRule`` instances in ``rule_id`` lexicographic order
            (which is also dimension + numeric order, e.g.
            ``SPL-D01-001, SPL-D01-002, …, SPL-D11-005``).
        """
        return sorted(self._by_id.values(), key=lambda r: r.rule_id)

    def get_rule_by_id(self, rule_id: str) -> BaseRule | None:
        """Look up a rule by its unique identifier.

        Args:
            rule_id: Rule identifier, e.g. ``"SPL-D03-001"``.

        Returns:
            The matching ``BaseRule`` instance, or ``None`` if not found.
        """
        return self._by_id.get(rule_id)

    def get_rules_by_dimension(self, dimension: Dimension) -> list[BaseRule]:
        """Return all rules for a given performance dimension.

        Args:
            dimension: The ``Dimension`` to filter on.

        Returns:
            List of ``BaseRule`` instances, sorted by ``rule_id``.
        """
        return sorted(self._by_dimension.get(dimension, []), key=lambda r: r.rule_id)

    def get_rules_by_severity(self, severity: Severity) -> list[BaseRule]:
        """Return all rules whose *default* severity matches *severity*.

        Note: this ignores ``severity_override`` in config.  Use
        ``get_enabled_rules(config)`` for config-aware filtering.

        Args:
            severity: The ``Severity`` level to filter on.

        Returns:
            List of ``BaseRule`` instances, sorted by ``rule_id``.
        """
        return sorted(self._by_severity.get(severity, []), key=lambda r: r.rule_id)

    def get_enabled_rules(self, config: LintConfig) -> list[BaseRule]:
        """Return rules that are active under the given configuration.

        A rule is excluded when:

        - Its dimension block has ``enabled: false``, or
        - Its ``rule_id`` appears in ``ignore.rules``, or
        - It would be individually disabled via a per-rule toggle.

        The returned list is sorted by ``rule_id``.

        Args:
            config: Resolved ``LintConfig`` for the current scan.

        Returns:
            Subset of all registered rules that should run.
        """
        return [r for r in self.get_all_rules() if r.is_enabled(config)]

    def list_rules(self, config: LintConfig | None = None) -> list[dict[str, Any]]:
        """Return a list of rule metadata dicts suitable for CLI/JSON output.

        Each dict contains: ``rule_id``, ``name``, ``dimension``,
        ``severity``, ``description``, ``effort_level``, ``enabled``.

        Args:
            config: Optional config used to resolve ``enabled`` status and
                effective severity.  When ``None``, ``enabled`` is ``True``
                for all rules and ``severity`` reflects the default.

        Returns:
            List of dicts, one per rule, sorted by ``rule_id``.
        """
        rows: list[dict[str, Any]] = []
        for rule in self.get_all_rules():
            enabled = rule.is_enabled(config) if config is not None else True
            severity = rule.get_severity(config) if config is not None else rule.default_severity
            rows.append(
                {
                    "rule_id": rule.rule_id,
                    "name": rule.name,
                    "dimension": rule.dimension.value,
                    "dimension_name": rule.dimension.display_name,
                    "severity": severity.name,
                    "description": rule.description,
                    "effort_level": rule.effort_level.value,
                    "enabled": enabled,
                }
            )
        return rows

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._by_id)

    def __contains__(self, rule_id: str) -> bool:
        return rule_id in self._by_id

    def __repr__(self) -> str:
        return f"RuleRegistry(rules={len(self._by_id)}, discovered={self._discovered})"
