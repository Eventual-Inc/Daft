import dataclasses

from typing import Any, Callable, cast

# Node IDs in the NetworkX graph are uuid strings
NodeId = str


@dataclasses.dataclass(frozen=True)
class QueryColumn:
    name: str


_COMPARATOR_MAP = {
    ">": "__gt__",
    ">=": "__ge__",
    "<": "__lt__",
    "<=": "__le__",
    "=": "__eq__",
}


@dataclasses.dataclass(frozen=True)
class FilterPredicate:
    """Predicate describing a condition for operations such as a filter or join"""

    left: str
    comparator: str
    right: str

    def __post_init__(self):
        if self.comparator not in _COMPARATOR_MAP:
            raise ValueError(f"Comparator {self.comparator} not found in accepted comparators {_COMPARATOR_MAP.keys()}")

    def get_callable(self) -> Callable[[Any], bool]:
        def f(x: Any) -> bool:
            comparator_magic_method = _COMPARATOR_MAP[self.comparator]
            if dataclasses.is_dataclass(x):
                return cast(bool, getattr(getattr(x, self.left), comparator_magic_method)(self.right))
            return cast(bool, getattr(x[self.left], comparator_magic_method)(self.right))

        return f
