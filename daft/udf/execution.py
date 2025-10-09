from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from daft.datatype import DataType
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


def call_async_batch(
    cls: ClsBase[C],
    method: Callable[Concatenate[C, ...], Any],
    return_dtype: PyDataType,
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args_list: list[list[Any]],
) -> PySeries:
    import asyncio

    args, kwargs = original_args

    bound_method = cls._daft_bind_method(method)

    tasks = []
    for evaluated_args in evaluated_args_list:
        new_args = [evaluated_args.pop(0) if isinstance(arg, Expression) else arg for arg in args]
        new_kwargs = {
            key: (evaluated_args.pop(0) if isinstance(arg, Expression) else arg) for key, arg in kwargs.items()
        }
        coroutine = bound_method(*new_args, **new_kwargs)
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
    args, kwargs = original_args

    new_args = [evaluated_args.pop(0) if isinstance(arg, Expression) else arg for arg in args]
    new_kwargs = {key: (evaluated_args.pop(0) if isinstance(arg, Expression) else arg) for key, arg in kwargs.items()}

    bound_method = cls._daft_bind_method(method)

    output = bound_method(*new_args, **new_kwargs)
    return output
