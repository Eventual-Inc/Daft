from daft.udf.row_wise import RowWiseUdf
from daft.datatype import DataType
from typing import overload, Any, Callable, Optional, Union
import functools
from daft.udf.legacy import udf, UDF


class _DaftFuncDecorator:
    """`@daft.func` Decorator to convert a Python function into a `UDF`.

    Unlike `@daft.udf(...)`, `@daft.func` operates on single values instead of batches.

    Examples:
    >>> import daft
    >>> from daft import col
    >>> @daft.func  # or @daft.func()
    ... # or you can specify the return type like our existing @daft.udf
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
    def __new__(cls, *, return_dtype: Optional[DataType] = None) -> Callable[[Any], RowWiseUdf]: ...  # type: ignore
    @overload
    def __new__(cls, fn: Callable[..., Any], *, return_dtype: Optional[DataType] = None) -> RowWiseUdf: ...  # type: ignore

    def __new__(  # type: ignore
        cls, fn: Optional[Callable[..., Any]] = None, *, return_dtype: Optional[DataType] = None
    ) -> Union[RowWiseUdf, Callable[[Callable[..., Any]], RowWiseUdf]]:
        partial_udf = functools.partial(RowWiseUdf, return_dtype=return_dtype)
        return partial_udf if fn is None else partial_udf(fn)


__all__ = ["UDF", "udf"]
