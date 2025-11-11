from __future__ import annotations

import contextvars
from contextlib import contextmanager
from typing import TYPE_CHECKING

from daft.daft import OperatorMetrics

if TYPE_CHECKING:
    from collections.abc import Generator

_CURRENT_METRICS: contextvars.ContextVar[OperatorMetrics | None] = contextvars.ContextVar(
    "daft_udf_metrics_metrics", default=None
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
        metrics = _require_metrics()
        metrics.inc_counter(self._name, amount)


def counter(name: str) -> Counter:
    return Counter(name)


def increment_counter(name: str, amount: int = 1) -> None:
    counter(name).increment(amount)


def _require_metrics() -> OperatorMetrics:
    metrics = _CURRENT_METRICS.get()
    if metrics is None:
        raise RuntimeError(
            "Custom UDF metrics can only be used inside an active metrics context; the engine should establish this automatically."
        )
    return metrics


@contextmanager
def _metrics_context() -> Generator[OperatorMetrics, None, None]:
    metrics = OperatorMetrics()
    token = _CURRENT_METRICS.set(metrics)
    try:
        yield metrics
    finally:
        _CURRENT_METRICS.reset(token)


__all__ = [
    "Counter",
    "counter",
    "increment_counter",
]
