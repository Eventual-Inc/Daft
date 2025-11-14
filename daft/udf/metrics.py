from __future__ import annotations

import contextvars
import warnings
from collections.abc import Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING

from daft.daft import OperatorMetrics

if TYPE_CHECKING:
    from collections.abc import Generator

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

    if description is not None and not isinstance(description, str):
        raise ValueError("Metric description must be a string if provided")

    normalized_attributes: dict[str, str] | None = None
    if attributes is not None:
        if not isinstance(attributes, Mapping):
            raise ValueError("Metric attributes must be a mapping of string keys to string values")
        normalized_attributes = {}
        for key, value in attributes.items():
            if not isinstance(key, str) or not key:
                raise ValueError("Metric attribute keys must be non-empty strings")
            if not isinstance(value, str):
                raise ValueError("Metric attribute values must be strings")
            normalized_attributes[key] = value

        if not normalized_attributes:
            normalized_attributes = None

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
