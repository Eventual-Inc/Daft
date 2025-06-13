from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable
from typing import TYPE_CHECKING, Any, Callable, Union

from daft.dependencies import pa

if TYPE_CHECKING:
    from daft.expressions import Expression

# Column input type definitions
ColumnInputType = Union["Expression", str]
ManyColumnsInputType = Union[ColumnInputType, Iterable[ColumnInputType]]


def get_arrow_version() -> tuple[int, ...]:
    return tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())


def in_notebook() -> bool:
    """Check if we are in a Jupyter notebook."""
    try:
        from IPython import get_ipython

        if "IPKernelApp" not in get_ipython().config:  # pragma: no cover
            return False
    except ImportError:
        return False
    except AttributeError:
        return False
    return True


def pydict_to_rows(pydict: dict[str, list[Any]]) -> list[frozenset[tuple[str, Any]]]:
    """Converts a dataframe pydict to a list of rows representation.

    e.g.
    {
        "fruit": ["apple", "banana", "carrot"],
        "number": [1, 2, 3],
    }

    becomes
    [
        {("fruit", "apple"), ("number", 1)},
        {("fruit", "banana"), ("number", 2)},
        {("fruit", "carrot"), ("number", 3)},
    ]
    """
    return [
        frozenset((key, freeze(value)) for key, value in zip(pydict.keys(), values)) for values in zip(*pydict.values())
    ]


def freeze(input: dict[Any, Any] | list[Any] | Any) -> frozenset[Any] | tuple[Any, ...] | Any:
    """Freezes mutable containers for equality comparison."""
    if isinstance(input, dict):
        return frozenset((key, freeze(value)) for key, value in input.items())
    elif isinstance(input, list):
        return tuple(freeze(item) for item in input)
    else:
        return input


def map_operator_arrow_semantics_bool(
    operator: Callable[[Any, Any], Any],
    left_pylist: list[Any],
    right_pylist: list[Any],
) -> list[bool | None]:
    return [
        (bool(operator(left, right)) if (left is not None and right is not None) else None)
        for (left, right) in zip(left_pylist, right_pylist)
    ]


def python_list_membership_check(
    left_pylist: list[Any],
    right_pylist: list[Any],
) -> list[Any]:
    try:
        right_pyset = set(right_pylist)
        return [elem in right_pyset for elem in left_pylist]
    except TypeError:
        return [elem in right_pylist for elem in left_pylist]


def python_list_between_check(value_pylist: list[Any], lower_pylist: list[Any], upper_pylist: list[Any]) -> list[Any]:
    return [value <= upper and value >= lower for value, lower, upper in zip(value_pylist, lower_pylist, upper_pylist)]


def map_operator_arrow_semantics(
    operator: Callable[[Any, Any], Any],
    left_pylist: list[Any],
    right_pylist: list[Any],
) -> list[Any]:
    return [
        operator(left, right) if (left is not None and right is not None) else None
        for (left, right) in zip(left_pylist, right_pylist)
    ]


def pyarrow_supports_fixed_shape_tensor() -> bool:
    """Whether pyarrow supports the fixed_shape_tensor canonical extension type."""
    from daft.context import get_context

    return hasattr(pa, "fixed_shape_tensor") and (
        (get_context().get_or_create_runner().name != "ray") or get_arrow_version() >= (13, 0, 0)
    )


# Column utility functions
def is_column_input(x: Any) -> bool:
    from daft.expressions import Expression

    return isinstance(x, str) or isinstance(x, Expression)


def column_inputs_to_expressions(columns: ManyColumnsInputType) -> list[Expression]:
    """Inputs to dataframe operations can be passed in as individual arguments or an iterable.

    In addition, they may be strings or Expressions.
    This method normalizes the inputs to a list of Expressions.
    """
    from daft.expressions import col

    column_iter: Iterable[ColumnInputType] = [columns] if is_column_input(columns) else columns  # type: ignore
    return [col(c) if isinstance(c, str) else c for c in column_iter]


class SyncFromAsyncIterator:
    """Convert an async iterator to a sync iterator.

    Note that the async iterator is created lazily upon first iteration.
    """

    def __init__(self, async_iter_producer: Callable[[], AsyncIterator[Any]]):
        self.async_iter_producer = async_iter_producer
        self.async_iter: Any | None = None
        self.loop = asyncio.new_event_loop()
        self.stopped = False

    def __iter__(self) -> SyncFromAsyncIterator:
        if self.stopped:
            raise StopIteration
        return self

    def __next__(self) -> Any:
        if self.stopped:
            raise StopIteration

        try:
            return self.loop.run_until_complete(self._get_next())
        except StopAsyncIteration:
            self.stopped = True
            self.loop.close()
            raise StopIteration

    async def _get_next(self) -> Any:
        if self.async_iter is None:
            self.async_iter = self.async_iter_producer()
        res = await self.async_iter.__anext__()
        if res is None:
            raise StopAsyncIteration
        return res
