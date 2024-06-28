from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from daft.daft import CsvParseOptions, JsonParseOptions
from daft.daft import PyField as _PyField
from daft.daft import PySchema as _PySchema
from daft.daft import read_csv_schema as _read_csv_schema
from daft.daft import read_json_schema as _read_json_schema
from daft.daft import read_parquet_schema as _read_parquet_schema
from daft.datatype import DataType, TimeUnit

if TYPE_CHECKING:
    import pyarrow as pa

    from daft.io import IOConfig


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
    def create(name: str, dtype: DataType) -> Field:
        pyfield = _PyField.create(name, dtype._dtype)
        return Field._from_pyfield(pyfield)

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
    def from_pyarrow_schema(cls, pa_schema: pa.Schema) -> Schema:
        """Creates a Daft Schema from a PyArrow Schema

        Args:
            pa_schema (pa.Schema): PyArrow schema to convert

        Returns:
            Schema: Converted Daft schema
        """
        return cls._from_field_name_and_types(
            [(pa_field.name, DataType.from_arrow_type(pa_field.type)) for pa_field in pa_schema]
        )

    def to_pyarrow_schema(self) -> pa.Schema:
        """Converts a Daft Schema to a PyArrow Schema

        Returns:
            pa.Schema: PyArrow schema that corresponds to the provided Daft schema
        """
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
        return isinstance(other, Schema) and self._schema.eq(other._schema)

    def to_name_set(self) -> set[str]:
        return set(self.column_names())

    def __repr__(self) -> str:
        return repr(self._schema)

    def _repr_html_(self) -> str:
        return self._schema._repr_html_()

    def _truncated_table_html(self) -> str:
        return self._schema._truncated_table_html()

    def _truncated_table_string(self) -> str:
        return self._schema._truncated_table_string()

    def union(self, other: Schema) -> Schema:
        if not isinstance(other, Schema):
            raise ValueError(f"Expected Schema, got other: {type(other)}")

        intersecting_names = self.to_name_set().intersection(other.to_name_set())
        if intersecting_names:
            raise ValueError(f"Cannot union schemas with overlapping names: {intersecting_names}")

        return Schema._from_pyschema(self._schema.union(other._schema))

    def __reduce__(self) -> tuple:
        return Schema._from_pyschema, (self._schema,)

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
