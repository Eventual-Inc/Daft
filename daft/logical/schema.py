from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from daft.expressions import Expression


@dataclass(frozen=True)
class PlanSchema:
    fields: List[str]

    def __post_init__(self) -> None:
        if len(self.fields) != len(set(self.fields)):
            raise ValueError(f"found duplicate entries in schema {self.fields}")

    def add_columns(self, names: List[str]) -> PlanSchema:
        names_copy = self.fields.copy()
        for name in names:
            if name in names_copy:
                raise ValueError(f"column {name} already exists")
        names_copy.extend(names)
        return PlanSchema(fields=names_copy)

    def exclude_columns(self, names: List[str]) -> PlanSchema:
        names_copy = self.fields.copy()
        for each in names:
            if each not in names_copy:
                raise ValueError(f"{each} not in Schema: {names_copy}")
        names_filtered = list(filter(lambda x: x in names, names_copy))
        return PlanSchema(fields=names_filtered)

    def contains(self, name: str) -> bool:
        return name in self.fields

    @classmethod
    def from_expressions(cls, exprs: List[Expression]) -> PlanSchema:
        names_with_nones: List[Optional[str]] = [expr.name() for expr in exprs]
        names: List[str] = []
        for i, name in enumerate(names_with_nones):
            if name is None:
                name = f"col_{i}"
            names.append(name)
        return cls(names)
