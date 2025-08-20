from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, get_type_hints

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from daft import Series
from daft.datatype import DataType
from daft.expressions import Expression

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from daft.daft import PyDataType, PySeries
    from daft.datatype import DataTypeLike


P = ParamSpec("P")
T = TypeVar("T")


class RowWiseUdf(Generic[P, T]):
    def __init__(self, fn: Callable[P, T], return_dtype: DataTypeLike | None):
        self._inner = fn

        module_name = getattr(fn, "__module__")
        qual_name: str = getattr(fn, "__qualname__")
        if module_name:
            self.name = f"{module_name}.{qual_name}"
        else:
            self.name = qual_name

        type_hints = get_type_hints(fn)
        if return_dtype is None:
            if "return" in type_hints:
                self.return_dtype = DataType._infer_type(type_hints["return"])
            else:
                raise ValueError("return_dtype is required when function has no return annotation")
        elif isinstance(return_dtype, DataType):
            self.return_dtype = return_dtype
        else:
            self.return_dtype = DataType._infer_type(return_dtype)

    def eval(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Run the decorated function eagerly and return the result immediately."""
        return self._inner(*args, **kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> Expression:
        expr_args = []
        for arg in args:
            if isinstance(arg, Expression):
                expr_args.append(arg)
        for arg in kwargs.values():
            if isinstance(arg, Expression):
                expr_args.append(arg)

        return Expression._row_wise_udf(self.name, self._inner, self.return_dtype, (args, kwargs), expr_args)


def __call_async_batch(
    fn: Callable[..., Awaitable[Any]],
    return_dtype: PyDataType,
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args_list: list[list[Any]],
) -> PySeries:
    import asyncio

    args, kwargs = original_args

    tasks = []
    for evaluated_args in evaluated_args_list:
        new_args = [evaluated_args.pop(0) if isinstance(arg, Expression) else arg for arg in args]
        new_kwargs = {
            key: (evaluated_args.pop(0) if isinstance(arg, Expression) else arg) for key, arg in kwargs.items()
        }
        coroutine = fn(*new_args, **new_kwargs)
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


def __call_func(
    fn: Callable[..., Any],
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[Any],
) -> list[Any]:
    """Called from Rust to evaluate a Python scalar UDF. Returns a list of Python objects."""
    args, kwargs = original_args

    new_args = [evaluated_args.pop(0) if isinstance(arg, Expression) else arg for arg in args]
    new_kwargs = {key: (evaluated_args.pop(0) if isinstance(arg, Expression) else arg) for key, arg in kwargs.items()}

    output = fn(*new_args, **new_kwargs)
    return output
