from __future__ import annotations

import contextvars
import warnings
from contextlib import contextmanager
from typing import TYPE_CHECKING

from daft.daft import OperatorMetrics

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping

_CURRENT_METRICS: contextvars.ContextVar[OperatorMetrics | None] = contextvars.ContextVar(
    "daft_udf_metrics_metrics", default=None
)


def increment_counter(
    name: str,
    amount: int = 1,
    *,
    description: str | None = None,
    attributes: Mapping[str, str] | None = None,
) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError("Metric name must be a non-empty string")

    normalized_attributes: dict[str, str] | None = None
    if attributes is not None:
        normalized_attributes = dict(attributes)

    metrics = _CURRENT_METRICS.get()
    if metrics is None:
        warnings.warn(
            "Custom UDF metrics will only be recorded during execution within a UDF function or class method.",
            RuntimeWarning,
        )
        return

    metrics.inc_counter(name, amount, description=description, attributes=normalized_attributes)


@contextmanager
def _metrics_context() -> Generator[OperatorMetrics, None, None]:
    metrics = OperatorMetrics()
    token = _CURRENT_METRICS.set(metrics)
    try:
        yield metrics
    finally:
        _CURRENT_METRICS.reset(token)


__all__ = [
    "increment_counter",
]
