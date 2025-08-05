from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, get_type_hints, overload

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from daft.datatype import DataType
from daft.expressions import Expression
from daft.series import Series

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from daft.daft import PyDataType, PySeries


P = ParamSpec("P")
T = TypeVar("T")


class RowWiseUdf(Generic[P, T]):
    def __init__(self, fn: Callable[P, T], return_dtype: DataType | None):
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
        else:
            self.return_dtype = return_dtype

    @overload
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...
    @overload
    def __call__(self, *args: Any, **kwargs: Any) -> Expression: ...

    def __call__(self, *args: Any, **kwargs: Any) -> Expression | T:
        expr_args = []
        for arg in args:
            if isinstance(arg, Expression):
                expr_args.append(arg)
        for arg in kwargs.values():
            if isinstance(arg, Expression):
                expr_args.append(arg)

        if len(expr_args) == 0:
            # all inputs are Python literals, evaluate immediately
            return self._inner(*args, **kwargs)
        else:
            return Expression._row_wise_udf(self.name, self._inner, self.return_dtype, (args, kwargs), expr_args)


def call_async_batch_with_evaluated_exprs(
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
    outputs = asyncio.run(run_tasks())

    return Series.from_pylist(outputs, dtype=dtype)._series


def call_func_with_evaluated_exprs(
    fn: Callable[..., Any],
    return_dtype: PyDataType,
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluated_args: list[Any],
) -> PySeries:
    args, kwargs = original_args

    new_args = [evaluated_args.pop(0) if isinstance(arg, Expression) else arg for arg in args]
    new_kwargs = {key: (evaluated_args.pop(0) if isinstance(arg, Expression) else arg) for key, arg in kwargs.items()}

    output = fn(*new_args, **new_kwargs)
    dtype = DataType._from_pydatatype(return_dtype)
    return Series.from_pylist([output], dtype=dtype)._series
