"""Shared helpers for tolerating transient HuggingFace Hub failures in tests.

CI runners share egress IPs with many other users, so calls to
``huggingface.co`` occasionally come back as ``HTTP 429`` (rate-limited),
or fail with transient network errors such as TLS handshake timeouts,
connect/read timeouts, or connection resets. None of these are related to
the code under test, so we retry a few times with backoff and skip the
test if the failure persists.

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


# Substrings (case-insensitive) that indicate a transient network failure
# unrelated to the code under test. Observed in CI logs across httpx,
# httpcore, urllib3, requests, and Daft's Rust IO layer (reqwest).
_TRANSIENT_NETWORK_MARKERS = (
    "connecttimeout",
    "connect timeout",
    "connection timed out",
    "handshake operation timed out",
    "handshake timed out",
    "ssl handshake",
    "read timed out",
    "readtimeout",
    "remote disconnected",
    "connection reset",
    "connection aborted",
    "connection refused",
    "temporary failure in name resolution",
    "name or service not known",
    "max retries exceeded",
    "bad gateway",
    "gateway timeout",
    "service unavailable",
    "status(502",
    "status(503",
    "status(504",
)


def _is_rate_limit_error(exc: BaseException) -> bool:
    """Return True if ``exc`` looks like a transient HuggingFace Hub failure.

    Despite the legacy name, this also covers transient network errors
    (TLS handshake timeouts, connect/read timeouts, connection resets,
    5xx gateway errors) in addition to HTTP 429 rate-limit responses.

    Recognises the shapes we have observed in CI:
      * ``requests`` / ``urllib3``: response with ``status_code == 429``
      * ``huggingface_hub.HfHubHTTPError``: "HTTP Error 429 thrown while ..."
      * Generic strings: "429 Client Error: Too Many Requests", "rate limit"
      * Daft Rust IO via ``reqwest``: ``DaftError::External ... Status(429, ...)``
      * ``httpx`` / ``httpcore``: ``ConnectTimeout``, ``ReadTimeout``,
        "handshake operation timed out", etc.
    """
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code == 429 or status_code in (502, 503, 504):
        return True

    message = str(exc)
    lowered = message.lower()

    # HTTP 429 rate-limit signatures.
    is_rate_limited = (
        "http error 429" in lowered
        or "rate limit" in lowered
        or "too many requests" in lowered
        or "status(429" in lowered
        or "429 client error" in lowered
    )
    if is_rate_limited:
        return True

    # Transient network errors (TLS handshake timeout, connect timeout, ...).
    # Also match against the exception class name so that bare exceptions
    # without an informative message (e.g. ``ConnectTimeout()``) are caught.
    type_name = type(exc).__name__.lower()
    if any(marker in type_name for marker in ("timeout", "connectionerror", "connecterror")):
        return True
    return any(marker in lowered for marker in _TRANSIENT_NETWORK_MARKERS)


def call_with_hf_retry(
    fn: Callable[..., T],
    *args: Any,
    retries: int = 3,
    backoff_seconds: float = 5.0,
    **kwargs: Any,
) -> T:
    """Invoke ``fn`` with retries on transient HuggingFace Hub failures.

    On HTTP 429 or transient network errors (connect/read/handshake timeouts,
    connection resets, 5xx gateway responses) we retry up to ``retries`` times
    with a fixed backoff. If every attempt fails for a transient reason we
    ``pytest.skip`` the current test, since the failure is caused by the
    external service or network rather than the code under test. Any other
    exception is re-raised immediately.
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
        pytest.skip(f"HuggingFace Hub transient failure after {retries} attempts: {last}")
    except Exception as exc:
        if _is_rate_limit_error(exc):
            pytest.skip(f"HuggingFace Hub transient failure after {retries} attempts: {exc}")
        raise
