from __future__ import annotations

import pickle
import random
import statistics
from typing import Any, Callable

import pyarrow as pa

ARROW_VERSION = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())


def pydict_to_rows(pydict: dict[str, list]) -> list[frozenset[tuple[str, Any]]]:
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


def freeze(input: dict | list | Any) -> frozenset | tuple | Any:
    """Freezes mutable containers for equality comparison."""
    if isinstance(input, dict):
        return frozenset((key, freeze(value)) for key, value in input.items())
    elif isinstance(input, list):
        return tuple(freeze(item) for item in input)
    else:
        return input


def estimate_size_bytes_pylist(pylist: list) -> int:
    """Estimate the size of this list by sampling and pickling its objects."""
    if len(pylist) == 0:
        return 0

    # The pylist is non-empty.
    # Sample up to 1MB or 10000 items to determine total size.
    MAX_SAMPLE_QUANTITY = 10000
    MAX_SAMPLE_SIZE = 1024 * 1024

    sample_candidates = random.sample(pylist, min(len(pylist), MAX_SAMPLE_QUANTITY))

    sampled_sizes = []
    sample_size_allowed = MAX_SAMPLE_SIZE
    for sample in sample_candidates:
        size = len(pickle.dumps(sample))
        sampled_sizes.append(size)
        sample_size_allowed -= size
        if sample_size_allowed <= 0:
            break

    # Sampling complete.
    # If we ended up measuring the entire list, just return the exact value.
    if len(sampled_sizes) == len(pylist):
        return sum(sampled_sizes)

    # Otherwise, reduce to a one-item estimate and extrapolate.
    if len(sampled_sizes) == 1:
        [one_item_size_estimate] = sampled_sizes
    else:
        mean, stdev = statistics.mean(sampled_sizes), statistics.stdev(sampled_sizes)
        one_item_size_estimate = int(mean + stdev)

    return one_item_size_estimate * len(pylist)


def map_operator_arrow_semantics_bool(
    operator: Callable[[Any, Any], Any],
    left_pylist: list,
    right_pylist: list,
) -> list[bool | None]:
    return [
        bool(operator(left, right)) if (left is not None and right is not None) else None
        for (left, right) in zip(left_pylist, right_pylist)
    ]


def python_list_membership_check(
    left_pylist: list,
    right_pylist: list,
) -> list:
    try:
        right_pyset = set(right_pylist)
        return [elem in right_pyset for elem in left_pylist]
    except TypeError:
        return [elem in right_pylist for elem in left_pylist]


def python_list_between_check(value_pylist: list, lower_pylist: list, upper_pylist: list) -> list:
    return [value <= upper and value >= lower for value, lower, upper in zip(value_pylist, lower_pylist, upper_pylist)]


def map_operator_arrow_semantics(
    operator: Callable[[Any, Any], Any],
    left_pylist: list,
    right_pylist: list,
) -> list:
    return [
        operator(left, right) if (left is not None and right is not None) else None
        for (left, right) in zip(left_pylist, right_pylist)
    ]


def pyarrow_supports_fixed_shape_tensor() -> bool:
    """Whether pyarrow supports the fixed_shape_tensor canonical extension type."""
    from daft.context import get_context

    return hasattr(pa, "fixed_shape_tensor") and (not get_context().is_ray_runner or ARROW_VERSION >= (13, 0, 0))
