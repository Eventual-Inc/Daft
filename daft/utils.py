from __future__ import annotations

import pickle
import random
import statistics
from typing import Any


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

    return [frozenset((key, value) for key, value in zip(pydict.keys(), values)) for values in zip(*pydict.values())]


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
    # Sample up to 100 of the items (to keep the operation fast) to determine total size.
    sample_quantity = min(len(pylist), 100)

    samples = random.sample(pylist, sample_quantity)
    sample_sizes = [len(pickle.dumps(sample)) for sample in samples]

    if sample_quantity == len(pylist):
        return sum(sample_sizes)

    mean, stdev = statistics.mean(sample_sizes), statistics.stdev(sample_sizes)
    one_item_size_estimate = int(mean + stdev)

    return one_item_size_estimate * len(pylist)
