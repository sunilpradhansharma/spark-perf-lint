"""LLM provider interface and concrete Claude (Anthropic) implementation.

Two classes are defined here:

``LLMProvider`` — the abstract interface every provider must implement.
``ClaudeLLMProvider`` — backed by the ``anthropic`` SDK (install via
``pip install 'spark-perf-lint[llm]'``).

The ``anthropic`` package is imported lazily inside ``_build_client`` so that
the rest of spark-perf-lint remains importable even when the SDK is not
installed — Tier 2 features simply cannot be used without it.
"""

from __future__ import annotations

import abc
import logging
import os
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from spark_perf_lint.config import LintConfig

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "claude-sonnet-4-6"
# Three retry attempts with exponential back-off (seconds before each retry).
_RETRY_DELAYS: tuple[float, ...] = (1.0, 2.0, 4.0)


# =============================================================================
# Abstract interface
# =============================================================================


class LLMProvider(abc.ABC):
    """Abstract base class for LLM providers used in Tier 2 analysis.

    Concrete providers must implement ``complete()`` and expose the active
    ``model`` identifier.  The interface is intentionally minimal: a single
    chat-completion call is all the analyzer needs.

    Implementations must be safe to call from a single thread; thread-safety
    across concurrent callers is *not* required.
    """

    @abc.abstractmethod
    def complete(
        self,
        messages: list[dict[str, str]],
        *,
        system: str | None = None,
        max_tokens: int = 1024,
    ) -> str:
        """Send a list of chat messages and return the model response text.

        Args:
            messages: OpenAI-style message list, e.g.
                ``[{"role": "user", "content": "…"}]``.
            system: Optional system prompt.  When ``None`` the provider's
                default (if any) is used.
            max_tokens: Upper bound on the response length in tokens.

        Returns:
            The model's reply as a plain string (leading/trailing whitespace
            stripped by the caller as needed).

        Raises:
            RuntimeError: If all retry attempts are exhausted.
        """

    @property
    @abc.abstractmethod
    def model(self) -> str:
        """Return the model identifier, e.g. ``'claude-sonnet-4-6'``."""


# =============================================================================
# Claude (Anthropic) implementation
# =============================================================================


class ClaudeLLMProvider(LLMProvider):
    """Anthropic Claude provider backed by the ``anthropic`` Python SDK.

    The SDK is imported lazily in ``_build_client`` so that importing this
    module does not raise ``ImportError`` when ``anthropic`` is absent.

    Retry logic uses a fixed schedule of up to three attempts with increasing
    delays, tolerating transient API errors (rate limits, network blips).

    Args:
        model: Claude model identifier to use.
            Defaults to ``"claude-sonnet-4-6"``.
        api_key: Explicit API key.  When ``None``, the value of the
            environment variable named by *api_key_env_var* is used.
        api_key_env_var: Name of the environment variable that holds the
            Anthropic API key.  Defaults to ``"ANTHROPIC_API_KEY"``.
        max_tokens: Default upper bound on response length.  Callers can
            override this per-call via the ``max_tokens`` argument to
            ``complete()``.
    """

    def __init__(
        self,
        *,
        model: str = _DEFAULT_MODEL,
        api_key: str | None = None,
        api_key_env_var: str = "ANTHROPIC_API_KEY",
        max_tokens: int = 1024,
    ) -> None:
        self._model = model
        self._default_max_tokens = max_tokens

        resolved_key = api_key or os.environ.get(api_key_env_var)
        if not resolved_key:
            raise ValueError(
                f"Anthropic API key not found. "
                f"Set the {api_key_env_var!r} environment variable "
                "or pass api_key= explicitly to ClaudeLLMProvider()."
            )
        self._client = self._build_client(resolved_key)

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_client(api_key: str) -> Any:
        """Instantiate and return an ``anthropic.Anthropic`` client.

        Args:
            api_key: Anthropic API key.

        Returns:
            Configured ``anthropic.Anthropic`` instance.

        Raises:
            ImportError: If the ``anthropic`` package is not installed.
        """
        try:
            import anthropic

            return anthropic.Anthropic(api_key=api_key)
        except ImportError as exc:
            raise ImportError(
                "The 'anthropic' package is required for Tier 2 LLM analysis. "
                "Install it with: pip install 'spark-perf-lint[llm]'"
            ) from exc

    @classmethod
    def from_config(cls, config: LintConfig) -> ClaudeLLMProvider:
        """Construct a provider from a resolved ``LintConfig``.

        Reads ``llm.model``, ``llm.api_key_env_var``, and ``llm.max_tokens``
        from the config dict.

        Args:
            config: Fully resolved ``LintConfig`` for the current scan.

        Returns:
            A ready-to-use ``ClaudeLLMProvider`` instance.
        """
        llm_cfg = config.raw.get("llm", {})
        return cls(
            model=llm_cfg.get("model", _DEFAULT_MODEL),
            api_key_env_var=llm_cfg.get("api_key_env_var", "ANTHROPIC_API_KEY"),
            max_tokens=llm_cfg.get("max_tokens", 1024),
        )

    # ------------------------------------------------------------------
    # LLMProvider interface
    # ------------------------------------------------------------------

    @property
    def model(self) -> str:
        """Return the Claude model identifier."""
        return self._model

    def complete(
        self,
        messages: list[dict[str, str]],
        *,
        system: str | None = None,
        max_tokens: int | None = None,
    ) -> str:
        """Call the Claude messages API with retry on transient errors.

        Args:
            messages: Chat messages in ``[{"role": …, "content": …}]`` format.
            system: Optional system prompt injected before the user messages.
            max_tokens: Per-call override; falls back to ``self._default_max_tokens``.

        Returns:
            The model's first content block as a plain string.

        Raises:
            RuntimeError: After all ``_RETRY_DELAYS`` attempts are exhausted.
        """
        effective_max = max_tokens if max_tokens is not None else self._default_max_tokens
        kwargs: dict[str, Any] = {
            "model": self._model,
            "max_tokens": effective_max,
            "messages": messages,
        }
        if system:
            kwargs["system"] = system

        last_exc: Exception | None = None
        for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
            try:
                response = self._client.messages.create(**kwargs)
                return str(response.content[0].text)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.warning(
                    "LLM call failed (attempt %d/%d): %s",
                    attempt,
                    len(_RETRY_DELAYS),
                    exc,
                )
                if attempt < len(_RETRY_DELAYS):
                    time.sleep(delay)

        raise RuntimeError(f"LLM call failed after {len(_RETRY_DELAYS)} attempts") from last_exc

    def __repr__(self) -> str:
        return f"ClaudeLLMProvider(model={self._model!r})"
