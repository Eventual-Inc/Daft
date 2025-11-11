from __future__ import annotations

import contextvars
from contextlib import contextmanager
from typing import TYPE_CHECKING

from daft.daft import PyMetricsCollector as MetricsCollector

if TYPE_CHECKING:
    from collections.abc import Generator

_CURRENT_COUNTERS: contextvars.ContextVar[dict[str, int] | None] = contextvars.ContextVar(
    "daft_udf_metrics_counters", default=None
)


class _MetricBase:
    __slots__ = ("_name",)

    def __init__(self, name: str) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("Metric name must be a non-empty string")
        self._name = name

    @property
    def name(self) -> str:
        return self._name


class Counter(_MetricBase):
    def increment(self, amount: int = 1) -> None:
        counters = _require_counters()
        counters[self._name] = counters.get(self._name, 0) + amount


def counter(name: str) -> Counter:
    return Counter(name)


def increment_counter(name: str, amount: int = 1) -> None:
    counter(name).increment(amount)


class _MetricsHandle:
    __slots__ = "_counters"

    def __init__(self, counters: dict[str, int]) -> None:
        self._counters = counters

    def payload(self) -> dict[str, dict[str, int]]:
        if not self._counters:
            return {}
        return {"counters": self._counters.copy()}


def _require_counters() -> dict[str, int]:
    counters = _CURRENT_COUNTERS.get()
    if counters is None:
        raise RuntimeError(
            "Custom UDF metrics can only be used inside an active metrics context; the engine should establish this automatically."
        )
    return counters


@contextmanager
def _metrics_context() -> Generator[_MetricsHandle, None, None]:
    counters: dict[str, int] = {}
    counters_token = _CURRENT_COUNTERS.set(counters)
    try:
        yield _MetricsHandle(counters)
    finally:
        _CURRENT_COUNTERS.reset(counters_token)


__all__ = [
    "Counter",
    "MetricsCollector",
    "counter",
    "increment_counter",
]
