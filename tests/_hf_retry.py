"""Shared helpers for tolerating transient HuggingFace Hub failures in tests.

CI runners share egress IPs with many other users, so calls to
``huggingface.co`` occasionally come back as ``HTTP 429`` (rate-limited).
That is unrelated to the code under test, so we retry a few times with
backoff and skip the test if the limit persists.

This module is the single source of truth for that behaviour and is consumed
by both ``tests/ai/transformers/*`` (Python ``huggingface_hub`` client) and
``tests/integration/io/huggingface/*`` (Daft Rust IO layer using ``reqwest``).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

import pytest
from tenacity import (
    RetryError,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_fixed,
)

T = TypeVar("T")


def _is_rate_limit_error(exc: BaseException) -> bool:
    """Return True if ``exc`` looks like a HuggingFace Hub rate-limit (HTTP 429).

    Recognises the shapes we have observed in CI:
      * ``requests`` / ``urllib3``: response with ``status_code == 429``
      * ``huggingface_hub.HfHubHTTPError``: "HTTP Error 429 thrown while ..."
      * Generic strings: "429 Client Error: Too Many Requests", "rate limit"
      * Daft Rust IO via ``reqwest``: ``DaftError::External ... Status(429, ...)``
    """
    response = getattr(exc, "response", None)
    if getattr(response, "status_code", None) == 429:
        return True

    message = str(exc)
    lowered = message.lower()
    if "429" not in message and "http error 429" not in lowered and "status(429" not in lowered:
        return False
    return (
        "http error 429" in lowered
        or "rate limit" in lowered
        or "too many requests" in lowered
        or "status(429" in lowered
    )


def call_with_hf_retry(
    fn: Callable[..., T],
    *args: Any,
    retries: int = 3,
    backoff_seconds: float = 5.0,
    **kwargs: Any,
) -> T:
    """Invoke ``fn`` with retries on HuggingFace Hub rate-limit errors.

    On HTTP 429 we retry up to ``retries`` times with a fixed backoff. If every
    attempt is rate-limited we ``pytest.skip`` the current test, since the
    failure is caused by the external service rather than the code under test.
    Any other exception is re-raised immediately.
    """
    runner = retry(
        reraise=True,
        stop=stop_after_attempt(retries),
        wait=wait_fixed(backoff_seconds),
        retry=retry_if_exception(_is_rate_limit_error),
    )(fn)

    try:
        return runner(*args, **kwargs)
    except RetryError as retry_err:  # pragma: no cover - defensive
        last = retry_err.last_attempt.exception() if retry_err.last_attempt else retry_err
        pytest.skip(f"HuggingFace Hub rate-limited (HTTP 429) after {retries} attempts: {last}")
    except Exception as exc:
        if _is_rate_limit_error(exc):
            pytest.skip(f"HuggingFace Hub rate-limited (HTTP 429) after {retries} attempts: {exc}")
        raise
