from __future__ import annotations

from typing import Dict

from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema, SchemaVisitor
from pyiceberg.types import ListType, MapType, NestedField, PrimitiveType, StructType

from daft import DataType
from daft.daft import PyField

FieldIdMapping = Dict[int, PyField]


def _nested_field_to_daft_pyfield(field: NestedField) -> PyField:
    return PyField.create(field.name, DataType.from_arrow_type(schema_to_pyarrow(field.field_type))._dtype)


class SchemaFieldIdMappingVisitor(SchemaVisitor[FieldIdMapping]):
    """Extracts a mapping of {field_id: PyField} from an Iceberg schema"""

    def schema(self, schema: Schema, struct_result: FieldIdMapping) -> FieldIdMapping:
        """Visit a Schema."""
        return struct_result

    def struct(self, struct: StructType, field_results: list[FieldIdMapping]) -> FieldIdMapping:
        """Visit a StructType."""
        result = {field.field_id: _nested_field_to_daft_pyfield(field) for field in struct.fields}
        for r in field_results:
            result.update(r)
        return result

    def field(self, field: NestedField, field_result: FieldIdMapping) -> FieldIdMapping:
        """Visit a NestedField."""
        field_result[field.field_id] = _nested_field_to_daft_pyfield(field)
        return field_result

    def list(self, list_type: ListType, element_result: FieldIdMapping) -> FieldIdMapping:
        """Visit a ListType."""
        element_result[list_type.element_id] = _nested_field_to_daft_pyfield(list_type.element_field)
        return element_result

    def map(self, map_type: MapType, key_result: FieldIdMapping, value_result: FieldIdMapping) -> FieldIdMapping:
        result = {**key_result, **value_result}
        result[map_type.key_id] = _nested_field_to_daft_pyfield(map_type.key_field)
        result[map_type.value_id] = _nested_field_to_daft_pyfield(map_type.value_field)
        return result

    def primitive(self, primitive: PrimitiveType) -> FieldIdMapping:
        """Visit a PrimitiveType."""
        return {}
