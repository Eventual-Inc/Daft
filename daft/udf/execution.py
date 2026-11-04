from __future__ import annotations

import asyncio
import logging
import random
from typing import TYPE_CHECKING, Any, Concatenate, TypeVar

from daft import DataType
from daft.dependencies import np, pa
from daft.expressions.expressions import Expression
from daft.series import Series
from daft.udf.metrics import _metrics_context

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from daft.daft import OperatorMetrics, PyDataType, PySeries

    from .udf_v2 import ClsBase

C = TypeVar("C")

logger = logging.getLogger(__name__)

# Mirrors the Rust retry backoff in src/daft-dsl/src/python_udf/retry.rs.
_INITIAL_RETRY_DELAY_MS = 100.0
_MAX_RETRY_DELAY_MS = 60_000.0


def replace_expressions_with_evaluated_args(
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[Any],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    args, kwargs = original_args
    new_args = tuple(evaluated_args.pop(0) if isinstance(arg, Expression) else arg for arg in args)
    new_kwargs = {key: (evaluated_args.pop(0) if isinstance(arg, Expression) else arg) for key, arg in kwargs.items()}
    return new_args, new_kwargs


def _next_retry_delay_ms(error: BaseException, delay_ms: float) -> float:
    retry_after = getattr(error, "retry_after", None)
    if isinstance(retry_after, (int, float)) and retry_after >= 0:
        delay_ms = min(max(delay_ms, retry_after * 1000.0), _MAX_RETRY_DELAY_MS)
    return delay_ms


async def _retry_failed_rows(
    outputs: list[Any],
    call_for_row: Callable[[int], Coroutine[Any, Any, Any]],
    on_error: str,
    max_retries: int,
) -> None:
    """Retry rows that failed while the batch was gathered, then null the ones still failing.

    Retries happen per row so one transient failure does not discard the other
    rows in the batch. Mutates ``outputs`` in place.
    """
    delay_ms = _INITIAL_RETRY_DELAY_MS
    for index, output in enumerate(outputs):
        if isinstance(output, BaseException) and not isinstance(output, Exception):
            # Cancellation and similar control-flow exceptions still abort the batch.
            raise output
        if not isinstance(output, Exception):
            continue
        error: Exception = output
        for _ in range(max_retries):
            delay_ms = _next_retry_delay_ms(error, delay_ms)
            jitter = random.uniform(0.0, delay_ms / 2.0)
            await asyncio.sleep((delay_ms * 3.0 / 4.0 + jitter) / 1000.0)
            delay_ms = min(delay_ms * 2.0, _MAX_RETRY_DELAY_MS)
            try:
                outputs[index] = await call_for_row(index)
            except Exception as retry_error:
                error = retry_error
            else:
                break
        else:
            if on_error == "log":
                logger.warning("Python UDF error: %s", error, exc_info=error)
            outputs[index] = None


async def call_async_func_batched(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Any],
    return_dtype: PyDataType,
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args_list: list[list[Any]],
    on_error: str = "raise",
    max_retries: int = 0,
) -> tuple[PySeries, OperatorMetrics]:
    bound_method = cls._daft_bind_method(method)

    def call_for_row(evaluated_args: list[Any]) -> Coroutine[Any, Any, Any]:
        # `replace_expressions_with_evaluated_args` consumes the list, and rows may
        # be retried, so hand it a copy.
        args, kwargs = replace_expressions_with_evaluated_args(original_args, list(evaluated_args))
        return bound_method(*args, **kwargs)

    dtype = DataType._from_pydatatype(return_dtype)
    with _metrics_context() as metrics:
        # Gather per-row results so a single failure does not abort the batch.
        outputs = await asyncio.gather(
            *(call_for_row(args) for args in evaluated_args_list),
            return_exceptions=True,
        )

        if on_error == "raise":
            for output in outputs:
                if isinstance(output, BaseException):
                    raise output
        else:
            await _retry_failed_rows(
                outputs,
                lambda index: call_for_row(evaluated_args_list[index]),
                on_error,
                max_retries,
            )

    series = Series.from_pylist(outputs, dtype=dtype)._series
    return series, metrics


def call_func(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Any],
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[Any],
) -> tuple[Any, OperatorMetrics]:
    """Called from Rust to evaluate a Python scalar UDF. Returns a list of Python objects."""
    args, kwargs = replace_expressions_with_evaluated_args(original_args, evaluated_args)

    bound_method = cls._daft_bind_method(method)

    with _metrics_context() as metrics:
        output = bound_method(*args, **kwargs)

    return output, metrics


def call_batch_func(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Series],
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[PySeries],
) -> tuple[PySeries, OperatorMetrics]:
    """Called from Rust to evaluate a Python batch UDF. Returns a PySeries."""
    evaluated_args_series = [Series._from_pyseries(arg) for arg in evaluated_args]
    args, kwargs = replace_expressions_with_evaluated_args(original_args, evaluated_args_series)

    bound_method = cls._daft_bind_method(method)

    with _metrics_context() as metrics:
        output = bound_method(*args, **kwargs)

    if isinstance(output, Series):
        output_series = output
    elif isinstance(output, list):
        output_series = Series.from_pylist(output)
    elif np.module_available() and isinstance(output, np.ndarray):
        output_series = Series.from_numpy(output)
    elif pa.module_available() and isinstance(output, (pa.Array, pa.ChunkedArray)):
        output_series = Series.from_arrow(output)
    else:
        raise ValueError(f"Expected output to be a Series, list, numpy array, or pyarrow array, got {type(output)}")

    return output_series._series, metrics


async def call_batch_async(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Coroutine[Any, Any, Series]],
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[PySeries],
) -> tuple[PySeries, OperatorMetrics]:
    """Called from Rust to evaluate a Python batch UDF. Returns a PySeries."""
    evaluated_args_series = [Series._from_pyseries(arg) for arg in evaluated_args]
    args, kwargs = replace_expressions_with_evaluated_args(original_args, evaluated_args_series)

    bound_coroutine: Callable[..., Coroutine[Any, Any, Series]] = cls._daft_bind_coroutine_method(method)

    with _metrics_context() as metrics:
        output = await bound_coroutine(*args, **kwargs)

    if isinstance(output, Series):
        output_series = output
    elif isinstance(output, list):
        output_series = Series.from_pylist(output)
    elif np.module_available() and isinstance(output, np.ndarray):
        output_series = Series.from_numpy(output)
    elif pa.module_available() and isinstance(output, (pa.Array, pa.ChunkedArray)):
        output_series = Series.from_arrow(output)
    else:
        raise ValueError(f"Expected output to be a Series, list, numpy array, or pyarrow array, got {type(output)}")

    return output_series._series, metrics
