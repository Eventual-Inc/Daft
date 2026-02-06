from __future__ import annotations

import time

import pytest

from daft.retries import retry_with_backoff


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda _: None)


def test_retry_success_immediate() -> None:
    calls = {"count": 0}

    @retry_with_backoff(max_retries=3, jitter_ms=0, max_backoff_ms=0)
    def _fn() -> str:
        calls["count"] += 1
        return "success"

    assert _fn() == "success"
    assert calls["count"] == 1


def test_retry_eventual_success() -> None:
    calls = {"count": 0}

    @retry_with_backoff(max_retries=3, jitter_ms=0, max_backoff_ms=0)
    def _fn() -> str:
        calls["count"] += 1
        if calls["count"] < 3:
            raise ValueError("fail")
        return "success"

    assert _fn() == "success"
    assert calls["count"] == 3


def test_retry_exhaustion() -> None:
    calls = {"count": 0}

    @retry_with_backoff(max_retries=3, jitter_ms=0, max_backoff_ms=0)
    def _fn() -> None:
        calls["count"] += 1
        raise ValueError("persistent failure")

    with pytest.raises(ValueError, match="persistent failure"):
        _fn()
    assert calls["count"] == 3


def test_retry_non_retryable_error() -> None:
    calls = {"count": 0}

    def _should_retry(exc: Exception) -> bool:
        return isinstance(exc, ValueError)

    @retry_with_backoff(max_retries=3, jitter_ms=0, max_backoff_ms=0, should_retry=_should_retry)
    def _fn() -> None:
        calls["count"] += 1
        raise RuntimeError("critical error")

    with pytest.raises(RuntimeError, match="critical error"):
        _fn()
    assert calls["count"] == 1


def test_retry_retryable_error() -> None:
    calls = {"count": 0}

    def _should_retry(exc: Exception) -> bool:
        return isinstance(exc, ValueError)

    @retry_with_backoff(max_retries=3, jitter_ms=0, max_backoff_ms=0, should_retry=_should_retry)
    def _fn() -> str:
        calls["count"] += 1
        if calls["count"] < 3:
            raise ValueError("retryable")
        return "success"

    assert _fn() == "success"
    assert calls["count"] == 3
