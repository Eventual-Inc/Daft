from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from daft.datatype import DataType
from daft.dependencies import np, pa
from daft.expressions.expressions import Expression
from daft.series import Series

if sys.version_info < (3, 10):
    from typing_extensions import Concatenate
else:
    from typing import Concatenate

if TYPE_CHECKING:
    from daft.daft import PyDataType, PySeries

    from .udf_v2 import ClsBase

C = TypeVar("C")


def replace_expressions_with_evaluated_args(
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[Any],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    args, kwargs = original_args
    new_args = tuple(evaluated_args.pop(0) if isinstance(arg, Expression) else arg for arg in args)
    new_kwargs = {key: (evaluated_args.pop(0) if isinstance(arg, Expression) else arg) for key, arg in kwargs.items()}
    return new_args, new_kwargs


def call_async_batch(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Any],
    return_dtype: PyDataType,
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args_list: list[list[Any]],
) -> PySeries:
    import asyncio

    bound_method = cls._daft_bind_method(method)

    tasks = []
    for evaluated_args in evaluated_args_list:
        args, kwargs = replace_expressions_with_evaluated_args(original_args, evaluated_args)
        coroutine = bound_method(*args, **kwargs)
        tasks.append(coroutine)

    async def run_tasks() -> list[Any]:
        return await asyncio.gather(*tasks)

    dtype = DataType._from_pydatatype(return_dtype)

    try:
        # try to use existing event loop
        event_loop = asyncio.get_running_loop()
        outputs = asyncio.run_coroutine_threadsafe(run_tasks(), event_loop).result()
    except RuntimeError:
        outputs = asyncio.run(run_tasks())

    return Series.from_pylist(outputs, dtype=dtype)._series


def call_func(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Any],
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[Any],
) -> list[Any]:
    """Called from Rust to evaluate a Python scalar UDF. Returns a list of Python objects."""
    args, kwargs = replace_expressions_with_evaluated_args(original_args, evaluated_args)

    bound_method = cls._daft_bind_method(method)

    output = bound_method(*args, **kwargs)
    return output


def call_batch(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Series],
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[PySeries],
) -> PySeries:
    """Called from Rust to evaluate a Python batch UDF. Returns a PySeries."""
    evaluated_args_series = [Series._from_pyseries(arg) for arg in evaluated_args]
    args, kwargs = replace_expressions_with_evaluated_args(original_args, evaluated_args_series)

    bound_method = cls._daft_bind_method(method)

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

    return output_series._series
