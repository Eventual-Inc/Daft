from __future__ import annotations

import contextvars
from contextlib import contextmanager
from typing import Any

from daft.daft import _udf_metrics as _rust_metrics  # type: ignore[attr-defined]

_CURRENT_UDF_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar("daft_udf_metrics_udf_id", default=None)


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
    __slots__ = ()

    def increment(self, amount: int = 1) -> None:
        if not isinstance(amount, int):
            raise TypeError("Counter increment amount must be an int")
        udf_id = _require_udf_id()
        _rust_metrics.increment_counter(udf_id, self._name, amount)


class Gauge(_MetricBase):
    __slots__ = ()

    def set(self, value: float) -> None:
        udf_id = _require_udf_id()
        _rust_metrics.set_gauge(udf_id, self._name, float(value))


def counter(name: str) -> Counter:
    return Counter(name)


def gauge(name: str) -> Gauge:
    return Gauge(name)


def _require_udf_id() -> str:
    udf_id = _CURRENT_UDF_ID.get()
    if udf_id is None:
        raise RuntimeError(
            "Custom UDF metrics can only be used inside an active metrics context; the engine should establish this automatically."
        )
    return udf_id


def increment_counter(name: str, amount: int = 1) -> None:
    counter(name).increment(amount)


def set_gauge(name: str, value: float) -> None:
    gauge(name).set(value)


@contextmanager
def _metrics_context(udf_id: str) -> Any:
    if not isinstance(udf_id, str) or not udf_id:
        raise ValueError("_metrics_context requires a non-empty string udf_id")
    token = _CURRENT_UDF_ID.set(udf_id)
    try:
        yield
    finally:
        _CURRENT_UDF_ID.reset(token)


def _drain_metrics(udf_id: str) -> dict[str, dict[str, Any]]:
    snapshot = _rust_metrics._drain_metrics(udf_id)
    assert isinstance(snapshot, dict)
    return snapshot


__all__ = [
    "Counter",
    "Gauge",
    "counter",
    "gauge",
    "increment_counter",
    "set_gauge",
]
