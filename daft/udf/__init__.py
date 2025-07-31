from __future__ import annotations

from daft.udf.row_wise import RowWiseUdf
from typing import overload, Callable, TypeVar, TYPE_CHECKING
import sys

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

import functools
from daft.udf.legacy import udf, UDF

P = ParamSpec("P")
T = TypeVar("T")

if TYPE_CHECKING:
    from daft.datatype import DataType


class _DaftFuncDecorator:
    """`@daft.func` Decorator to convert a Python function into a `UDF`.

    Unlike `@daft.udf(...)`, `@daft.func` operates on single values instead of batches.

    Examples:
    >>> import daft
    >>> from daft import col
    >>> @daft.func  # or @daft.func()
    ... # or you can specify the return type like @daft.udf
    ... # @daft.func(return_dtype=daft.DataType.int64())
    ... def my_sum(a: int, b: int) -> int:
    ...     return a + b
    >>>
    >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    >>> df.select(my_sum(col("x"), col("y"))).collect()
    ╭───────╮
    │ x     │
    │ ---   │
    │ Int64 │
    ╞═══════╡
    │ 5     │
    ├╌╌╌╌╌╌╌┤
    │ 7     │
    ├╌╌╌╌╌╌╌┤
    │ 9     │
    ╰───────╯
    <BLANKLINE>
    (Showing first 3 of 3 rows)
    """

    @overload
    def __new__(cls, *, return_dtype: DataType | None = None) -> Callable[[Callable[P, T]], RowWiseUdf[P, T]]: ...  # type: ignore
    @overload
    def __new__(cls, fn: Callable[P, T], *, return_dtype: DataType | None = None) -> RowWiseUdf[P, T]: ...  # type: ignore

    def __new__(  # type: ignore
        cls, fn: Callable[P, T] | None = None, *, return_dtype: DataType | None = None
    ) -> RowWiseUdf[P, T] | Callable[[Callable[P, T]], RowWiseUdf[P, T]]:
        def partial_udf(fn: Callable[P, T]) -> RowWiseUdf[P, T]:
            return RowWiseUdf(fn, return_dtype=return_dtype)

        return partial_udf if fn is None else partial_udf(fn)


__all__ = ["UDF", "udf"]
