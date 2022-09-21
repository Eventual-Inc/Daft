from __future__ import annotations

from dataclasses import dataclass
from typing import List, cast

import pandas as pd

from daft.execution.operators import ExpressionType
from daft.logical.schema import ExpressionList


@dataclass(frozen=True)
class DataFrameSchemaField:
    name: str
    daft_type: ExpressionType


class DataFrameSchema:
    def __init__(self, fields: List[DataFrameSchemaField]):
        self._fields = {f.name: f for f in fields}

    def __getitem__(self, key: str) -> DataFrameSchemaField:
        return self._fields[key]

    def __len__(self) -> int:
        return len(self._fields)

    def column_names(self) -> List[str]:
        return list(self._fields.keys())

    @classmethod
    def from_expression_list(cls, exprs: ExpressionList) -> DataFrameSchema:
        fields = []
        for e in exprs:
            if e.resolved_type() is None:
                raise ValueError(f"Unable to parse schema from expression without type: {e}")
            if e.name() is None:
                raise ValueError(f"Unable to parse schema from expression without name: {e}")
            fields.append(DataFrameSchemaField(e.name(), e.resolved_type()))
        return cls(fields)

    def _to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame({field.name: [str(field.daft_type)] for field in self._fields.values()})

    def __repr__(self) -> str:
        return cast(str, self._to_pandas().__repr__())

    def _repr_html_(self) -> str:
        return cast(str, self._to_pandas()._repr_html_())
