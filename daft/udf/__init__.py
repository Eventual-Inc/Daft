from __future__ import annotations

from typing import overload, Callable, TypeVar, TYPE_CHECKING
import sys

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from .gen import GeneratorUdf
from .row_wise import RowWiseUdf
from .legacy import udf, UDF

P = ParamSpec("P")
T = TypeVar("T")

if TYPE_CHECKING:
    from daft.datatype import DataTypeLike
    from collections.abc import Iterator


class _DaftFuncDecorator:
    """Decorator to convert a Python function into a Daft user-defined function.

    Daft function variants:
    - `@daft.func` - Row-wise scalar function (1 in, 1 out)
    - `@daft.func.gen` - Generator function (1 in, N out)

    Decorated functions accept both their original argument types and Daft Expressions, and return an Expression which can then be lazily evaluated.
    To run the original function, call `<your_function>.eval(<args>)`.

    Args:
        return_dtype: The data type that this function should return. If not specified, uses the function's return type hint.

    Examples:
        Basic Example

        >>> import daft
        >>> @daft.func
        ... def my_sum(a: int, b: int) -> int:
        ...     return a + b
        >>>
        >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> df.select(my_sum(df["x"], df["y"])).collect()
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

        Calling the decorator directly on an existing function

        >>> import daft
        >>> def tokenize(text: str) -> list[int]:
        ...     vocab = {char: i for i, char in enumerate(text)}
        ...     return [vocab[char] for char in text]
        >>>
        >>> daft_tokenize = daft.func(tokenize)  # creates a new function rather than modifying `tokenize`
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

        Manually specifying the return type

        >>> import daft
        >>> @daft.func(return_dtype=daft.DataType.int32())
        ... def my_sum(a: int, b: int):
        ...     return a + b
        >>>
        >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> df.select(my_sum(df["x"], df["y"])).collect()
        ╭───────╮
        │ x     │
        │ ---   │
        │ Int32 │
        ╞═══════╡
        │ 5     │
        ├╌╌╌╌╌╌╌┤
        │ 7     │
        ├╌╌╌╌╌╌╌┤
        │ 9     │
        ╰───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        Decorating an async function

        >>> import daft
        >>> import asyncio
        >>> @daft.func
        ... async def my_sum(a: int, b: int) -> int:
        ...     await asyncio.sleep(1)
        ...     return a + b
        >>>
        >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> df.select(my_sum(df["x"], df["y"])).collect()
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

    @overload
    @staticmethod
    def gen(
        *, return_dtype: DataTypeLike | None = None
    ) -> Callable[[Callable[P, Iterator[T]]], GeneratorUdf[P, T]]: ...

    @overload
    @staticmethod
    def gen(fn: Callable[P, Iterator[T]], *, return_dtype: DataTypeLike | None = None) -> GeneratorUdf[P, T]: ...

    @staticmethod
    def gen(
        fn: Callable[P, Iterator[T]] | None = None, *, return_dtype: DataTypeLike | None = None
    ) -> GeneratorUdf[P, T] | Callable[[Callable[P, Iterator[T]]], GeneratorUdf[P, T]]:
        """Decorator to convert a Python function into a Daft user-defined generator function.

        This decorator can be used on Python generator functions and any Python function that returns an iterator.
        Unlike row-wise functions created via `@daft.func` which return one value per input row, generator functions may yield multiple values per row.
        Each value is placed in its own row, with the other output columns are broadcasted to match the number of generated values.

        Args:
            return_dtype: The data type that the iterator returned by this function should yield. If not specified, uses the function's yield type hint.

        Example:
            >>> import daft
            >>> from typing import Iterator
            >>> @daft.func.gen
            ... def my_gen_func(to_repeat: str, n: int) -> Iterator[str]:
            ...     for _ in range(n):
            ...         yield to_repeat
            >>>
            >>> df = daft.from_pydict({"id": [0, 1, 2], "value": ["pip", "install", "daft"], "occurrences": [0, 2, 4]})
            >>> df = df.select("id", my_gen_func(df["value"], df["occurrences"]))
            >>> df.collect()
            ╭───────┬─────────╮
            │ id    ┆ value   │
            │ ---   ┆ ---     │
            │ Int64 ┆ Utf8    │
            ╞═══════╪═════════╡
            │ 0     ┆ None    │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ 1     ┆ install │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ 1     ┆ install │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ 2     ┆ daft    │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ 2     ┆ daft    │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ 2     ┆ daft    │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ 2     ┆ daft    │
            ╰───────┴─────────╯
            <BLANKLINE>
            (Showing first 7 of 7 rows)
        """

        def partial_udf(fn: Callable[P, Iterator[T]]) -> GeneratorUdf[P, T]:
            return GeneratorUdf(fn, return_dtype=return_dtype)

        return partial_udf if fn is None else partial_udf(fn)


__all__ = ["UDF", "udf"]
