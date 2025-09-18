from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, get_type_hints, overload

from daft.daft import row_wise_udf

from ._internal import check_fn_serializable, get_expr_args, get_unique_function_name

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
    """A user-defined Daft row-wise function, created by calling `daft.func`.

    Row-wise functions are called with data from one row at a time, and map that to a single output value for that row.
    """

    def __init__(self, fn: Callable[P, T], return_dtype: DataTypeLike | None, unnest: bool):
        self._inner = fn
        self.name = get_unique_function_name(fn)
        self.unnest = unnest

        if return_dtype is None:
            type_hints = get_type_hints(fn)
            if "return" not in type_hints:
                raise ValueError(
                    "`@daft.func` requires either a return type hint or the `return_dtype` argument to be specified."
                )

            return_dtype = type_hints["return"]
        self.return_dtype = DataType._infer_type(return_dtype)

        if self.unnest and not self.return_dtype.is_struct():
            raise ValueError(
                f"Expected Daft function `return_dtype` to be `DataType.struct` when `unnest=True`, instead found: {self.return_dtype}"
            )

    @overload
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...
    @overload
    def __call__(self, *args: Expression, **kwargs: Expression) -> Expression: ...
    @overload
    def __call__(self, *args: Any, **kwargs: Any) -> Expression | T: ...

    def __call__(self, *args: Any, **kwargs: Any) -> Expression | T:
        expr_args = get_expr_args(args, kwargs)
        check_fn_serializable(self._inner, "@daft.func")

        # evaluate the function eagerly if there are no expression arguments
        if len(expr_args) == 0:
            return self._inner(*args, **kwargs)

        expr = Expression._from_pyexpr(
            row_wise_udf(self.name, self._inner, self.return_dtype._dtype, (args, kwargs), expr_args)
        )

        if self.unnest:
            expr = expr.unnest()

        return expr


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
