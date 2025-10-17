from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from daft.dependencies import np, pa
from daft.expressions.expressions import Expression
from daft.series import Series

if sys.version_info < (3, 10):
    from typing_extensions import Concatenate
else:
    from typing import Concatenate

if TYPE_CHECKING:
    from daft.daft import PySeries

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
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args_dict: dict[int, list[Any]],
) -> tuple[dict[int, Any], dict[int, str]]:
    import asyncio

    bound_method = cls._daft_bind_method(method)
    errors = {}

    # Preallocate outputs with None
    outputs = {}

    async def run_task(idx: int, args: tuple, kwargs: dict) -> tuple[int, Any, Exception | None]:  # type: ignore
        try:
            result = await bound_method(*args, **kwargs)
            return idx, result, None
        except Exception as e:
            return idx, None, e

    async def run_tasks() -> list[tuple[int, Any, Exception | None]]:
        tasks = []
        for idx, evaluated_args in evaluated_args_dict.items():
            args, kwargs = replace_expressions_with_evaluated_args(original_args, evaluated_args)
            tasks.append(run_task(idx, args, kwargs))
        return await asyncio.gather(*tasks)

    try:
        event_loop = asyncio.get_running_loop()
        results = asyncio.run_coroutine_threadsafe(run_tasks(), event_loop).result()
    except RuntimeError:
        results = asyncio.run(run_tasks())

    for idx, result, error in results:
        if error is None:
            outputs[idx] = result
        else:
            errors[idx] = str(error)

    return outputs, errors


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
