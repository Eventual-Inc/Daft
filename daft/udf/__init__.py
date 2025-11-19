from __future__ import annotations

from typing import Any, overload, Callable, TypeVar, TYPE_CHECKING
import sys

from typing import ParamSpec

from .legacy import udf, UDF
from . import metrics
from .udf_v2 import Func, mark_cls_method, wrap_cls

P = ParamSpec("P")
T = TypeVar("T")

if TYPE_CHECKING:
    from typing import Literal
    from daft.datatype import DataTypeLike


class _FuncDecorator:
    @overload
    def __call__(
        self,
        *,
        return_dtype: DataTypeLike | None = None,
        unnest: bool = False,
        use_process: bool | None = None,
        max_retries: int | None = None,
        on_error: Literal["raise", "log", "ignore"] | None = None,
    ) -> Callable[[Callable[P, T]], Func[P, T, None]]: ...
    @overload
    def __call__(
        self,
        fn: Callable[P, T],
        *,
        return_dtype: DataTypeLike | None = None,
        unnest: bool = False,
        use_process: bool | None = None,
        max_retries: int | None = None,
        on_error: Literal["raise", "log", "ignore"] | None = None,
    ) -> Func[P, T, None]: ...

    def __call__(
        self,
        fn: Callable[P, T] | None = None,
        *,
        return_dtype: DataTypeLike | None = None,
        unnest: bool = False,
        use_process: bool | None = None,
        max_retries: int | None = None,
        on_error: Literal["raise", "log", "ignore"] | None = None,
    ) -> Callable[[Callable[P, T]], Func[P, T, None]] | Func[P, T, None]:
        """Decorator to convert a Python function into a Daft user-defined function.

        Args:
            return_dtype: The data type that this function should return or yield. If not specified, it is derived from the function's return type hint.
            unnest: Whether to unnest/flatten out return type fields into columns. Return dtype must be `DataType.struct(..)` when this is set to true.
            use_process: Whether to run each instance of the function in a separate process. If unset, Daft will automatically choose based on runtime performance.

        Daft function variants:
        - **Row-wise** (1 row in, 1 row out) - the default variant
        - **Async row-wise** (1 row in, 1 row out) - created by decorating a Python async function
        - **Generator** (1 row in, N rows out) - created by decorating a Python generator function

        Decorated functions accept both their original argument types and Daft Expressions.
        When any arguments are Expressions, they return a Daft Expression that can be used in DataFrame operations.
        When called without Expression arguments, they execute immediately and the behavior is the same as if the function was not decorated.

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
            >>> df = daft.from_pydict({"x": [1], "y": [2]})
            >>> df.select(my_sum(df["x"], df["y"])).collect()
            ╭───────╮
            │ x     │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 3     │
            ╰───────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)

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
            │ Int64 ┆ String  │
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

            Unnesting multiple return fields

            >>> import daft
            >>> from daft import DataType
            >>> @daft.func(return_dtype=DataType.struct({"int": DataType.int64(), "str": DataType.string()}), unnest=True)
            ... def my_multi_return(val: int):
            ...     return {"int": val * 2, "str": str(val) * 2}
            >>> df = daft.from_pydict({"x": [1, 2, 3]})
            >>> df.select(my_multi_return(df["x"])).collect()
            ╭───────┬────────╮
            │ int   ┆ str    │
            │ ---   ┆ ---    │
            │ Int64 ┆ String │
            ╞═══════╪════════╡
            │ 2     ┆ 11     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 4     ┆ 22     │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 6     ┆ 33     │
            ╰───────┴────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """

        def partial_func(fn: Callable[P, T]) -> Func[P, T, None]:
            return Func._from_func(
                fn, return_dtype, unnest, use_process, False, None, max_retries=max_retries, on_error=on_error
            )

        return partial_func if fn is None else partial_func(fn)

    def batch(
        self,
        *,
        return_dtype: DataTypeLike,
        unnest: bool = False,
        use_process: bool | None = None,
        batch_size: int | None = None,
        max_retries: int | None = None,
        on_error: Literal["raise", "log", "ignore"] | None = None,
    ) -> Callable[[Callable[P, T]], Func[P, T, None]]:
        """Decorator to convert a Python function into a Daft user-defined batch function.

        Args:
            return_dtype: The data type that this function should return.
            unnest: Whether to unnest/flatten out return type fields into columns. Return dtype must be `DataType.struct(..)` when this is set to true.
            use_process: Whether to run each instance of the function in a separate process. If unset, Daft will automatically choose based on runtime performance.
            batch_size: The max number of rows in each input batch.

        Batch functions receive `daft.Series` arguments, and return a `daft.Series`, `list`, `numpy.ndarray`, or `pyarrow.Array`.
        You can also call them with scalar arguments, which will be passed in without modification.
        When called without Expression arguments, they execute immediately and the behavior is the same as if the function was not decorated.

        Examples:
            Basic Usage

            >>> import daft
            >>> from daft import DataType, Series
            >>>
            >>> @daft.func.batch(return_dtype=DataType.int64())
            ... def my_sum(a: Series, b: Series) -> Series:
            ...     import pyarrow.compute as pc
            ...
            ...     a = a.to_arrow()
            ...     b = b.to_arrow()
            ...     result = pc.add(a, b)
            ...     return result
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

            Mixing Series and Scalar Arguments

            >>> @daft.func.batch(return_dtype=daft.DataType.int64())
            ... def my_sum_with_scalar(a: daft.Series, b: int) -> daft.Series:
            ...     import pyarrow.compute as pc
            ...
            ...     a = a.to_arrow()
            ...     result = pc.add(a, b)
            ...     return result
            >>>
            >>> df = daft.from_pydict({"x": [1, 2, 3]})
            >>> df.select(my_sum_with_scalar(df["x"], 4)).collect()
            ╭───────╮
            │ x     │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 5     │
            ├╌╌╌╌╌╌╌┤
            │ 6     │
            ├╌╌╌╌╌╌╌┤
            │ 7     │
            ╰───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """

        def partial_func(fn: Callable[P, T]) -> Func[P, T, None]:
            return Func._from_func(fn, return_dtype, unnest, use_process, True, batch_size, max_retries, on_error)

        return partial_func


func = _FuncDecorator()


@overload
def cls(
    *,
    gpus: int = 0,
    use_process: bool | None = None,
    max_concurrency: int | None = None,
    max_retries: int | None = None,
    on_error: Literal["raise", "log", "ignore"] | None = None,
) -> Callable[[type], type]: ...
@overload
def cls(
    class_: type,
    *,
    gpus: int = 0,
    use_process: bool | None = None,
    max_concurrency: int | None = None,
    max_retries: int | None = None,
    on_error: Literal["raise", "log", "ignore"] | None = None,
) -> type: ...
def cls(
    class_: type | None = None,
    *,
    gpus: int = 0,
    use_process: bool | None = None,
    max_concurrency: int | None = None,
    max_retries: int | None = None,
    on_error: Literal["raise", "log", "ignore"] | None = None,
) -> type | Callable[[type], type]:
    """Decorator to convert a Python class into a Daft user-defined class.

    Args:
        gpus: The number of GPUs each instance of the class requires. Defaults to 0.
        use_process: Whether to run each instance of the class in a separate process. If unset, Daft will automatically choose based on runtime performance.
        max_concurrency: The maximum number of concurrent instances of the class.

    Daft classes allow you to initialize a class instance once, and then reuse it for multiple rows of data.
    This is useful for expensive initializations that need to be amortized across multiple rows of data, such as loading a model or establishing a network connection.

    Daft classes are initialized lazily. This means that when you create a Daft class, the arguments are saved and only passed into the `__init__` method of each instance once a query is executed.
    Methods can also be called with scalar arguments to run locally, in which case `__init__` will be called locally first.

    Methods in a Daft class can be used as Daft functions. Use the `@daft.method` decorator to override default arguments.

    Examples:
        Basic Usage

        >>> import daft
        >>> from daft import DataType
        >>> @daft.cls
        ... class MyModel:
        ...     def __init__(self, model_path: str):
        ...         self.model = some_slow_initialization_step(model_path)
        ...
        ...     def __call__(self, prompt: str) -> str:
        ...         return self.model(prompt)
        >>>
        >>> my_model = MyModel("path/to/model")
        >>>
        >>> df = daft.from_pydict({"prompt": ["hello", "world", "daft"]})
        >>> df = df.with_columns(
        ...     {
        ...         "generated": my_model(df["prompt"]),
        ...     }
        ... )

        Multiple Methods

        >>> import daft
        >>> from daft import DataType
        >>> @daft.cls # doctest: +SKIP
        >>> class MyModel:  # doctest: +SKIP
        ...     def __init__(self, model_path: str):  # doctest: +SKIP
        ...         self.model = some_slow_initialization_step(model_path)  # doctest: +SKIP
        ...
        ...     # no decoration is equivalent to `@daft.method` (default arguments)
        ...     def generate(self, prompt: str) -> str:  # doctest: +SKIP
        ...         return self.model(prompt)  # doctest: +SKIP
        ...
        ...     # decorate with `@daft.method` to override default arguments
        ...     @daft.method(return_dtype=DataType.list(DataType.string()))  # doctest: +SKIP
        ...     def classify(self, value: str):  # doctest: +SKIP
        ...         return self.model.classify(value)  # doctest: +SKIP
        ...
        ...     # batch method
        ...     @daft.method.batch(return_dtype=DataType.list(DataType.string()))  # doctest: +SKIP
        ...     def batch_classify(self, value: daft.Series):  # doctest: +SKIP
        ...         return self.model.batch_classify(value)  # doctest: +SKIP
        >>> # Specify the initialization arguments for the class. `__init__` will not be called yet.
        >>> my_model = MyModel("path/to/model")  # doctest: +SKIP
        >>> df = daft.from_pydict({"prompt": ["hello", "world", "daft"]})  # doctest: +SKIP
        >>> # Use class methods as Daft functions.
        >>> df = df.with_columns(  # doctest: +SKIP
        ...     {
        ...         "generated": my_model.generate(df["prompt"]),
        ...         "classified": my_model.classify(df["prompt"]),
        ...         "batch_classified": my_model.batch_classify(df["prompt"]),
        ...     }
        ... )
    """

    def partial_cls(c: type) -> type:
        return wrap_cls(c, gpus, use_process, max_concurrency, max_retries, on_error)

    return partial_cls if class_ is None else partial_cls(class_)


class _MethodDecorator:
    @overload
    def __call__(
        self, *, return_dtype: DataTypeLike | None = None, unnest: bool = False
    ) -> Callable[[Callable[P, T]], Callable[P, T]]: ...
    @overload
    def __call__(self, method: Callable[P, T], *, return_dtype: DataTypeLike | None = None) -> Callable[P, T]: ...
    @overload
    def __call__(self, method: Callable[P, T]) -> Callable[P, T]: ...
    def __call__(
        self,
        method: Callable[P, T] | None = None,
        *,
        return_dtype: DataTypeLike | None = None,
        unnest: bool = False,
        max_retries: int | None = None,
        on_error: Literal["raise", "log", "ignore"] | None = None,
    ) -> Callable[P, T] | Callable[[Callable[P, T]], Callable[P, T]]:
        """Decorator to convert a Python method into a Daft user-defined function. This should be used in a class that is decorated with `@daft.cls`.

        Args:
            return_dtype: The data type that this function should return or yield. If not specified, it is derived from the method's return type hint.
            unnest: Whether to unnest/flatten out return type fields into columns. Return dtype must be `DataType.struct(..)` when this is set to true. Defaults to false.

        Similar to `@daft.func`, `@daft.method` supports three variants: row-wise, async row-wise, and generator. See `@daft.func` for more details.

        Decorated methods accept both their original argument types and Daft Expressions.
        When any arguments are Expressions, they return a Daft Expression that can be used in DataFrame operations.
        When called without Expression arguments, methods execute immediately, first initializing a local instance of the class if it does not already exist.

        See `@daft.func` and `@daft.cls` for more details.
        """

        def partial_method(m: Callable[P, T]) -> Callable[P, T]:
            return mark_cls_method(m, return_dtype, unnest, False, None, max_retries, on_error)

        return partial_method if method is None else partial_method(method)

    @overload
    def batch(
        self,
        *,
        return_dtype: DataTypeLike | None = None,
        unnest: bool = False,
        batch_size: int | None = None,
        max_retries: int | None = None,
        on_error: Literal["raise", "log", "ignore"] | None = None,
    ) -> Callable[[Callable[P, T]], Callable[P, T]]: ...
    @overload
    def batch(
        self,
        method: Callable[P, T],
        *,
        return_dtype: DataTypeLike | None = None,
        unnest: bool = False,
        batch_size: int | None = None,
        max_retries: int | None = None,
        on_error: Literal["raise", "log", "ignore"] | None = None,
    ) -> Callable[P, T]: ...
    def batch(
        self,
        method: Callable[P, T] | None = None,
        *,
        return_dtype: DataTypeLike | None = None,
        unnest: bool = False,
        batch_size: int | None = None,
        max_retries: int | None = None,
        on_error: Literal["raise", "log", "ignore"] | None = None,
    ) -> Callable[P, T] | Callable[[Callable[P, T]], Callable[P, T]]:
        """Decorator to convert a Python method into a Daft user-defined batch function. This should be used in a class that is decorated with `@daft.cls`.

        Args:
            return_dtype: The data type that this function should return.
            unnest: Whether to unnest/flatten out return type fields into columns. Return dtype must be `DataType.struct(..)` when this is set to true.
            batch_size: The max number of rows in each input batch.

        Batch methods receive `daft.Series` arguments, and return a `daft.Series`, `list`, `numpy.ndarray`, or `pyarrow.Array`.
        You can also call them with scalar arguments, which will be passed in without modification.
        When called without Expression arguments, they execute immediately, first initializing a local instance of the class if it does not already exist.

        See `@daft.func.batch` and `@daft.cls` for more details.
        """

        def partial_method(m: Callable[P, T]) -> Callable[P, T]:
            return mark_cls_method(m, return_dtype, unnest, True, batch_size, max_retries, on_error)

        return partial_method if method is None else partial_method(method)


method = _MethodDecorator()


__all__ = ["UDF", "metrics", "udf"]
