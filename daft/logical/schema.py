from __future__ import annotations

from typing import Iterator

from daft.expressions import ExpressionList, col
from daft.logical.field import Field
from daft.types import ExpressionType


class Schema:
    fields: dict[str, Field]

    def __init__(self) -> None:
        raise NotImplementedError(f"Initializing a schema with __init__ is not supported")

    @classmethod
    def _from_name_and_types(self, fields: list[tuple[str, ExpressionType]]) -> Schema:
        s = Schema.__new__(Schema)
        s.fields = {name: Field(name=name, dtype=dtype) for name, dtype in fields}
        return s

    def __getitem__(self, key: str) -> Field:
        if key not in self.fields:
            raise ValueError(f"{key} was not found in Schema of fields {self.fields.keys()}")
        return self.fields[key]

    def __len__(self) -> int:
        return len(self.fields)

    def __iter__(self) -> Iterator[Field]:
        return iter(self.fields.values())

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Schema) and self.fields == other.fields

    def column_names(self) -> list[str]:
        return list(self.fields.keys())

    def to_name_set(self) -> set[str]:
        return set(self.column_names())

    def __repr__(self) -> str:
        return repr([(field.name, field.dtype) for field in self.fields.values()])

    def _repr_html_(self) -> str:
        return repr([(field.name, field.dtype) for field in self.fields.values()])

    def to_column_expressions(self) -> ExpressionList:
        return ExpressionList([col(f.name) for f in self.fields.values()])

    def union(self, other: Schema) -> Schema:
        assert isinstance(other, Schema), f"expected Schema, got {type(other)}"
        seen = {}
        for f in self.fields.values():
            assert f.name not in seen
            seen[f.name] = f

        for f in other.fields.values():
            assert f.name not in seen
            seen[f.name] = f

        return Schema._from_name_and_types([(f.name, f.dtype) for f in seen.values()])
