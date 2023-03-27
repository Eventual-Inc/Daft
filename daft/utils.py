from __future__ import annotations

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
