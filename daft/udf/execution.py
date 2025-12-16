from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Concatenate, TypeVar

from daft import DataType
from daft.dependencies import np, pa
from daft.errors import UDFException
from daft.expressions.expressions import Expression
from daft.series import Series
from daft.udf.metrics import _metrics_context

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from daft.daft import OperatorMetrics, PyDataType, PySeries

    from .udf_v2 import ClsBase

C = TypeVar("C")


def _handle_udf_exception(
    exception: Exception,
    method_name: str,
    evaluated_args_series: list[Series] | None = None,
) -> None:
    """Helper to wrap user function exceptions with UDFException.

    Args:
        exception: The exception raised by the user function
        method_name: Name of the method that failed
        evaluated_args_series: Optional list of Series arguments for detailed error message
    """
    # Remove the call-site from the traceback
    tb = exception.__traceback__
    exception = exception.with_traceback(tb.tb_next if tb else None)

    if evaluated_args_series:
        series_info = [
            f"{series.name()} ({series.datatype()}, length={len(series)})" for series in evaluated_args_series
        ]
        error_note = f"User-defined function `{method_name}` failed when executing on inputs:\n" + "\n".join(
            f"  - {info}" for info in series_info
        )
    else:
        error_note = f"User-defined function `{method_name}` failed during execution"

    raise UDFException(error_note) from exception


def replace_expressions_with_evaluated_args(
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[Any],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    args, kwargs = original_args
    new_args = tuple(evaluated_args.pop(0) if isinstance(arg, Expression) else arg for arg in args)
    new_kwargs = {key: (evaluated_args.pop(0) if isinstance(arg, Expression) else arg) for key, arg in kwargs.items()}
    return new_args, new_kwargs


async def call_async_func_batched(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Any],
    return_dtype: PyDataType,
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args_list: list[list[Any]],
) -> tuple[PySeries, OperatorMetrics]:
    import asyncio

    bound_method = cls._daft_bind_method(method)

    tasks = []
    for evaluated_args in evaluated_args_list:
        args, kwargs = replace_expressions_with_evaluated_args(original_args, evaluated_args)
        coroutine = bound_method(*args, **kwargs)
        tasks.append(coroutine)

    dtype = DataType._from_pydatatype(return_dtype)
    with _metrics_context() as metrics:
        try:
            outputs = await asyncio.gather(*tasks)
        except Exception as user_function_exception:
            _handle_udf_exception(user_function_exception, method.__name__)

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
        try:
            output = bound_method(*args, **kwargs)
        except Exception as user_function_exception:
            _handle_udf_exception(user_function_exception, method.__name__)

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
        try:
            output = bound_method(*args, **kwargs)
        except Exception as user_function_exception:
            _handle_udf_exception(user_function_exception, method.__name__, evaluated_args_series)

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
        try:
            output = await bound_coroutine(*args, **kwargs)
        except Exception as user_function_exception:
            _handle_udf_exception(user_function_exception, method.__name__, evaluated_args_series)

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
