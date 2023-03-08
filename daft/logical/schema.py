from __future__ import annotations

from typing import Iterator

from daft.expressions import ExpressionList
from daft.logical.field import Field
from daft.types import ExpressionType


class Schema:
    _fields: dict[str, Field]

    def __init__(self) -> None:
        raise NotImplementedError(f"Initializing a schema with __init__ is not supported")

    @classmethod
    def _from_field_name_and_types(self, fields: list[tuple[str, ExpressionType]]) -> Schema:
        s = Schema.__new__(Schema)
        s._fields = {name: Field(name=name, dtype=dtype) for name, dtype in fields}
        return s

    def __getitem__(self, key: str) -> Field:
        if key not in self._fields:
            raise ValueError(f"{key} was not found in Schema of fields {self._fields.keys()}")
        return self._fields[key]

    def __len__(self) -> int:
        return len(self._fields)

    def __iter__(self) -> Iterator[Field]:
        return iter(self._fields.values())

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Schema) and self._fields == other._fields

    def column_names(self) -> list[str]:
        return list(self._fields.keys())

    def to_name_set(self) -> set[str]:
        return set(self.column_names())

    def __repr__(self) -> str:
        return repr([(field.name, field.dtype) for field in self._fields.values()])

    def _repr_html_(self) -> str:
        return repr([(field.name, field.dtype) for field in self._fields.values()])

    def union(self, other: Schema) -> Schema:
        assert isinstance(other, Schema), f"expected Schema, got {type(other)}"
        seen = {}
        for f in self._fields.values():
            assert f.name not in seen
            seen[f.name] = f

        for f in other._fields.values():
            assert f.name not in seen
            seen[f.name] = f

        return Schema._from_field_name_and_types([(f.name, f.dtype) for f in seen.values()])

    def resolve_expressions(self, exprs: ExpressionList) -> Schema:
        fields = [e.to_field(self) for e in exprs]
        return Schema._from_field_name_and_types([(f.name, f.dtype) for f in fields])
