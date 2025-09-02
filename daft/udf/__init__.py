from __future__ import annotations

from typing import Any, overload, Callable, TypeVar, TYPE_CHECKING
import sys
from dataclasses import dataclass
from inspect import isgeneratorfunction

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from .generator import GeneratorUdf
from .row_wise import RowWiseUdf
from .legacy import udf, UDF

P = ParamSpec("P")
T = TypeVar("T")

if TYPE_CHECKING:
    from daft.datatype import DataTypeLike
    from collections.abc import Iterator


@dataclass
class _PartialUdf:
    """Helper class to provide typing overloads for using `daft.func` as a decorator."""

    return_dtype: DataTypeLike | None

    @overload
    def __call__(self, fn: Callable[P, Iterator[T]]) -> GeneratorUdf[P, T]: ...  # type: ignore[overload-overlap]
    @overload
    def __call__(self, fn: Callable[P, T]) -> RowWiseUdf[P, T]: ...

    def __call__(self, fn: Callable[P, Any]) -> GeneratorUdf[P, Any] | RowWiseUdf[P, Any]:
        if isgeneratorfunction(fn):
            return GeneratorUdf(fn, return_dtype=self.return_dtype)
        else:
            return RowWiseUdf(fn, return_dtype=self.return_dtype)


class _DaftFuncDecorator:
    """Decorator to convert a Python function into a Daft user-defined function.

    Daft function variants:
    - **Row-wise** (1 row in, 1 row out) - the default variant
    - **Async row-wise** (1 row in, 1 row out) - created by decorating a Python async function
    - **Generator** (1 row in, N rows out) - created by decorating a Python generator function

    Decorated functions accept both their original argument types and Daft Expressions.
    When any arguments are Expressions, they return a Daft Expression that can be used in DataFrame operations.
    When called with their original arguments, they execute immediately and the behavior is the same as if the function was not decorated.

    Args:
        return_dtype: The data type that this function should return or yield. If not specified, it is derived from the function's return type hint.

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

        Decorating a generator function

        >>> import daft
        >>> from typing import Iterator
        >>> @daft.func
        ... def my_gen_func(to_repeat: str, n: int) -> Iterator[str]:
        ...     for _ in range(n):
        ...         yield to_repeat
        >>>
        >>> df = daft.from_pydict({"id": [0, 1, 2], "value": ["pip", "install", "daft"], "occurrences": [0, 2, 4]})
        >>> df = df.select("id", my_gen_func(df["value"], df["occurrences"]))
        >>> df.collect()  # other output columns are repeated to match generator output length
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

    @overload
    def __new__(cls, *, return_dtype: DataTypeLike | None = None) -> _PartialUdf: ...  # type: ignore[misc]
    @overload
    def __new__(  # type: ignore[misc]
        cls, fn: Callable[P, Iterator[T]], *, return_dtype: DataTypeLike | None = None
    ) -> GeneratorUdf[P, T]: ...
    @overload
    def __new__(cls, fn: Callable[P, T], *, return_dtype: DataTypeLike | None = None) -> RowWiseUdf[P, T]: ...  # type: ignore[misc]

    def __new__(  # type: ignore[misc]
        cls, fn: Callable[P, Any] | None = None, *, return_dtype: DataTypeLike | None = None
    ) -> _PartialUdf | GeneratorUdf[P, Any] | RowWiseUdf[P, Any]:
        partial_udf = _PartialUdf(return_dtype=return_dtype)
        return partial_udf if fn is None else partial_udf(fn)


__all__ = ["UDF", "udf"]
