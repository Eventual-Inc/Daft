from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class PlanSchema:
    fields: List[str]

    def add_columns(self, names: List[str]) -> PlanSchema:
        names_copy = self.fields.copy()
        names_copy.extend(names)
        return PlanSchema(fields=names_copy)

    def exclude_columns(self, names: List[str]) -> PlanSchema:
        names_copy = self.fields.copy()
        for each in names:
            if each not in names_copy:
                raise ValueError(f"{each} not in Schema: {names_copy}")
        names_filtered = list(filter(lambda x: x in names, names_copy))
        return PlanSchema(fields=names_filtered)
