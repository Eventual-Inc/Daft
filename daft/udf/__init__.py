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
    from daft.datatype import DataTypeLike


class _DaftFuncDecorator:
    """`@daft.func` Decorator to convert a Python function into a `UDF`.

    This decorator can be used to define a row-wise UDF that can be applied to a Daft DataFrame.

    The `@daft.func` decorator transforms regular Python functions into Daft User-Defined Functions (UDFs) that operate on individual values.

    Decorated functions accept their original argument types or Expressions. Additionally, they return lazily-evaluated Expressions. If you want to run the function directly, call `<your_function>.eval(<args>)`.

    It accepts both synchronous and asynchronous functions.

    It can be used with existing functions or as a decorator to define new functions.

    The function either needs to be annotated with types, or you must provide a return type for the function using the `return_dtype` parameter. Daft requires type information to plan and optimize query execution.

    Examples:
        Basic Example

        >>> import daft
        >>> from daft import col
        >>> @daft.func
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

        Basic Example with existing function

        >>> import daft
        >>> from daft import col
        >>> def tokenize(text: str) -> list[int]:
        ...     vocab = {char: i for i, char in enumerate(text)}
        ...     return [vocab[char] for char in text]
        >>>
        >>> daft_tokenize = daft.func(tokenize)
        >>> df = daft.from_pydict({"text": ["hello", "world", "daft"]})
        >>> df.select(daft_tokenize(df["text"])).collect()
        ╭─────────────────╮
        │ text            │
        │ ---             │
        │ List[Int64]     │
        ╞═════════════════╡
        │ [0, 1, 3, 3, 4] │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [0, 1, 2, 3, 4] │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [0, 1, 2, 3]    │
        ╰─────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        Basic Example with existing function and no type hints

        >>> import daft
        >>> from daft import col
        >>> def tokenize(text: str):
        ...     vocab = {char: i for i, char in enumerate(text)}
        ...     return [vocab[char] for char in text]
        >>> daft_tokenize = daft.func(tokenize, return_dtype=list[int])
        >>> df = daft.from_pydict({"text": ["hello", "world", "daft"]})
        >>> df.select(daft_tokenize(df["text"])).collect()
        ╭─────────────────╮
        │ text            │
        │ ---             │
        │ List[Int64]     │
        ╞═════════════════╡
        │ [0, 1, 3, 3, 4] │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [0, 1, 2, 3, 4] │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [0, 1, 2, 3]    │
        ╰─────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        Async Example

        >>> import daft
        >>> import asyncio
        >>> from daft import col
        >>> @daft.func
        ... async def my_sum(a: int, b: int) -> int:
        ...     await asyncio.sleep(1)
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
    def __new__(cls, *, return_dtype: DataTypeLike | None = None) -> Callable[[Callable[P, T]], RowWiseUdf[P, T]]: ...  # type: ignore
    @overload
    def __new__(cls, fn: Callable[P, T], *, return_dtype: DataTypeLike | None = None) -> RowWiseUdf[P, T]: ...  # type: ignore

    def __new__(  # type: ignore
        cls, fn: Callable[P, T] | None = None, *, return_dtype: DataTypeLike | None = None
    ) -> RowWiseUdf[P, T] | Callable[[Callable[P, T]], RowWiseUdf[P, T]]:
        def partial_udf(fn: Callable[P, T]) -> RowWiseUdf[P, T]:
            return RowWiseUdf(fn, return_dtype=return_dtype)

        return partial_udf if fn is None else partial_udf(fn)


__all__ = ["UDF", "udf"]
