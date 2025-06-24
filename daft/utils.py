from __future__ import annotations

import os
from collections.abc import Iterable
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


def freeze(
    input: dict[Any, Any] | list[Any] | Any,
) -> frozenset[Any] | tuple[Any, ...] | Any:
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
    return hasattr(pa, "fixed_shape_tensor")


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


def detect_ray_state() -> bool:
    ray_is_initialized = False
    ray_is_in_job = False
    in_ray_worker = False
    try:
        import ray

        if ray.is_initialized():
            ray_is_initialized = True
            # Check if running inside a Ray worker
            if ray._private.worker.global_worker.mode == ray.WORKER_MODE:
                in_ray_worker = True
        # In a Ray job, Ray might not be initialized yet but we can pick up an environment variable as a heuristic here
        elif os.getenv("RAY_JOB_ID") is not None:
            ray_is_in_job = True

    except ImportError:
        pass

    return not in_ray_worker and (ray_is_initialized or ray_is_in_job)
