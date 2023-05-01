from __future__ import annotations

from typing import Iterator

from daft.daft import PyField as _PyField
from daft.daft import PySchema as _PySchema
from daft.datatype import DataType


class Field:
    _field: _PyField

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Field via __init__ ")

    @staticmethod
    def _from_pyfield(field: _PyField) -> Field:
        f = Field.__new__(Field)
        f._field = field
        return f

    @property
    def name(self):
        return self._field.name()

    @property
    def dtype(self) -> DataType:
        return DataType._from_pydatatype(self._field.dtype())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Field):
            return False
        return self._field.eq(other._field)

    def __repr__(self) -> str:
        return f"Field(name={self.name}, dtype={self.dtype})"


class Schema:
    _schema: _PySchema

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Schema via __init__ ")

    @staticmethod
    def _from_pyschema(schema: _PySchema) -> Schema:
        s = Schema.__new__(Schema)
        s._schema = schema
        return s

    @classmethod
    def _from_field_name_and_types(self, fields: list[tuple[str, DataType]]) -> Schema:
        assert isinstance(fields, list), f"Expected a list of field tuples, received: {fields}"
        for field in fields:
            assert isinstance(field, tuple), f"Expected a tuple, received: {field}"
            assert len(field) == 2, f"Expected a tuple of length 2, received: {field}"
            name, dtype = field
            assert isinstance(name, str), f"Expected a string name, received: {name}"
            assert isinstance(dtype, DataType), f"Expected a DataType dtype, received: {dtype}"

        s = Schema.__new__(Schema)
        s._schema = _PySchema.from_field_name_and_types([(name, dtype._dtype) for name, dtype in fields])
        return s

    def __getitem__(self, key: str) -> Field:
        assert isinstance(key, str), f"Expected str for key, but received: {type(key)}"
        if key not in self._schema.names():
            raise ValueError(f"{key} was not found in Schema of fields {[f.name for f in self]}")
        pyfield = self._schema[key]
        return Field._from_pyfield(pyfield)

    def __len__(self) -> int:
        return len(self._schema.names())

    def column_names(self) -> list[str]:
        return list(self._schema.names())

    def __iter__(self) -> Iterator[Field]:
        col_names = self.column_names()
        yield from (self[name] for name in col_names)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Schema) and self._schema.eq(other._schema)

    def to_name_set(self) -> set[str]:
        return set(self.column_names())

    def __repr__(self) -> str:
        return repr([(field.name, field.dtype) for field in self])

    def union(self, other: Schema) -> Schema:
        if not isinstance(other, Schema):
            raise ValueError(f"Expected Schema, got other: {type(other)}")

        intersecting_names = self.to_name_set().intersection(other.to_name_set())
        if intersecting_names:
            raise ValueError(f"Cannot union schemas with overlapping names: {intersecting_names}")

        return Schema._from_pyschema(self._schema.union(other._schema))

    def __getstate__(self) -> bytes:
        return self._schema.__getstate__()

    def __setstate__(self, state: bytes) -> None:
        self._schema = _PySchema.__new__(_PySchema)
        self._schema.__setstate__(state)
