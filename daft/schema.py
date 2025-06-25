from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Callable

from daft.daft import CsvParseOptions, JsonParseOptions, PySchema
from daft.daft import PyField as _PyField
from daft.daft import PySchema as _PySchema
from daft.daft import read_csv_schema as _read_csv_schema
from daft.daft import read_json_schema as _read_json_schema
from daft.daft import read_parquet_schema as _read_parquet_schema
from daft.datatype import DataType, TimeUnit, _ensure_registered_super_ext_type

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pyarrow as pa

    from daft.io import IOConfig


__all__ = [
    "DataType",
    "Field",
    "Schema",
]


class Field:
    _field: _PyField

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Field via __init__ ")

    @staticmethod
    def _from_pyfield(field: _PyField) -> Field:
        f = Field.__new__(Field)
        f._field = field
        return f

    @staticmethod
    def create(name: str, dtype: DataType, metadata: dict[str, str] | None = None) -> Field:
        pyfield = _PyField.create(name, dtype._dtype, metadata)
        return Field._from_pyfield(pyfield)

    @property
    def name(self) -> str:
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
    def from_pyarrow_schema(cls, pa_schema: pa.Schema) -> Schema:
        """Creates a Daft Schema from a PyArrow Schema.

        Args:
            pa_schema (pa.Schema): PyArrow schema to convert

        Returns:
            Schema: Converted Daft schema
        """
        # NOTE: This does not retain schema-level metadata, as Daft Schemas do not have a metadata field.
        fields = []
        for pa_field in pa_schema:
            metadata = None
            if pa_field.metadata:
                metadata = {k.decode(): v.decode() for k, v in pa_field.metadata.items()}
            fields.append(Field.create(pa_field.name, DataType.from_arrow_type(pa_field.type), metadata))
        return cls._from_fields(fields)

    @classmethod
    def from_field_name_and_types(cls, fields: list[tuple[str, DataType]]) -> Schema:
        """Creates a Daft Schema from a list of field name and types.

        Args:
            fields (list[tuple[str, DataType]]): List of field name and types

        Returns:
            Schema: Daft schema with the provided field names and types
        """
        return cls._from_field_name_and_types(fields)

    def to_pyarrow_schema(self) -> pa.Schema:
        """Converts a Daft Schema to a PyArrow Schema.

        Returns:
            pa.Schema: PyArrow schema that corresponds to the provided Daft schema
        """
        _ensure_registered_super_ext_type()
        return self._schema.to_pyarrow_schema()

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

    @classmethod
    def _from_fields(self, fields: list[Field]) -> Schema:
        s = Schema.__new__(Schema)
        s._schema = _PySchema.from_fields([f._field for f in fields])
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

    def estimate_row_size_bytes(self) -> float:
        return self._schema.estimate_row_size_bytes()

    def __iter__(self) -> Iterator[Field]:
        col_names = self.column_names()
        yield from (self[name] for name in col_names)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Schema) and self._schema == other._schema

    def to_name_set(self) -> set[str]:
        return set(self.column_names())

    def __repr__(self) -> str:
        return repr(self._schema)

    def display_with_metadata(self, include_metadata: bool = False) -> str:
        """Returns a string representation of the schema, optionally including metadata."""
        return self._schema.display_with_metadata(include_metadata)

    def _repr_html_(self) -> str:
        return self._schema._repr_html_()

    def _truncated_table_html(self) -> str:
        return self._schema._truncated_table_html()

    def _truncated_table_string(self) -> str:
        return self._schema._truncated_table_string()

    def apply_hints(self, hints: Schema) -> Schema:
        return Schema._from_pyschema(self._schema.apply_hints(hints._schema))

    # Takes the unions between two schemas. Throws an error if the schemas contain overlapping keys.
    def union(self, other: Schema) -> Schema:
        if not isinstance(other, Schema):
            raise ValueError(f"Expected Schema, got other: {type(other)}")

        return Schema._from_pyschema(self._schema.union(other._schema))

    def __reduce__(self) -> tuple[Callable[[PySchema], Schema], tuple[PySchema]]:
        return Schema._from_pyschema, (self._schema,)

    @classmethod
    def from_pydict(cls, fields: dict[str, DataType]) -> Schema:
        return cls._from_fields([Field.create(k, v) for k, v in fields.items()])

    @classmethod
    def from_parquet(
        cls,
        path: str,
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
        coerce_int96_timestamp_unit: TimeUnit = TimeUnit.ns(),
    ) -> Schema:
        return Schema._from_pyschema(
            _read_parquet_schema(
                uri=path,
                io_config=io_config,
                multithreaded_io=multithreaded_io,
                coerce_int96_timestamp_unit=coerce_int96_timestamp_unit._timeunit,
            )
        )

    @classmethod
    def from_csv(
        cls,
        path: str,
        parse_options: CsvParseOptions | None = None,
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
    ) -> Schema:
        return Schema._from_pyschema(
            _read_csv_schema(
                uri=path,
                parse_options=parse_options,
                io_config=io_config,
                multithreaded_io=multithreaded_io,
            )
        )

    @classmethod
    def from_json(
        cls,
        path: str,
        parse_options: JsonParseOptions | None = None,
        io_config: IOConfig | None = None,
        multithreaded_io: bool | None = None,
    ) -> Schema:
        return Schema._from_pyschema(
            _read_json_schema(
                uri=path,
                parse_options=parse_options,
                io_config=io_config,
                multithreaded_io=multithreaded_io,
            )
        )
