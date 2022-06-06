import dataclasses

from typing import Any, Callable, Literal, cast, Union

# Node IDs in the NetworkX graph are uuid strings
NodeId = str

# Columns are just aliases for strings
QueryColumn = str

Comparator = Union[
    Literal[">"],
    Literal[">="],
    Literal["<"],
    Literal["<="],
    Literal["="],
]
COMPARATOR_MAP = {
    ">": "__gt__",
    ">=": "__ge__",
    "<": "__lt__",
    "<=": "__le__",
    "=": "__eq__",
}
