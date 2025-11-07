from __future__ import annotations

import contextvars
from contextlib import contextmanager
from typing import Any

from daft.daft import _udf_metrics as _rust_metrics  # type: ignore[attr-defined]

_CURRENT_UDF_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar("daft_udf_metrics_udf_id", default=None)


def _require_udf_id() -> str:
    udf_id = _CURRENT_UDF_ID.get()
    if udf_id is None:
        raise RuntimeError(
            "Custom UDF metrics can only be used inside an active metrics_context; the engine should establish this automatically."
        )
    return udf_id


def increment_counter(name: str, amount: int = 1) -> None:
    udf_id = _require_udf_id()
    _rust_metrics.increment_counter(udf_id, name, amount)


def set_gauge(name: str, value: float) -> None:
    udf_id = _require_udf_id()
    _rust_metrics.set_gauge(udf_id, name, float(value))


@contextmanager
def metrics_context(udf_id: str) -> Any:
    if not isinstance(udf_id, str) or not udf_id:
        raise ValueError("metrics_context requires a non-empty string udf_id")
    token = _CURRENT_UDF_ID.set(udf_id)
    try:
        yield
    finally:
        _CURRENT_UDF_ID.reset(token)


def _snapshot(udf_id: str) -> dict[str, dict[str, Any]]:
    snapshot = _rust_metrics._snapshot(udf_id)
    assert isinstance(snapshot, dict)
    return snapshot


__all__ = [
    "increment_counter",
    "metrics_context",
    "set_gauge",
]
