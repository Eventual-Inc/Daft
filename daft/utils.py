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
    mean, stdev = statistics.mean(sampled_sizes), statistics.stdev(sampled_sizes)
    one_item_size_estimate = int(mean + stdev)

    return one_item_size_estimate * len(pylist)
