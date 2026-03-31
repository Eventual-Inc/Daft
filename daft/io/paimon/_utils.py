"""Shared utilities for Paimon integration.

This module provides shared utilities for working with Paimon,
including schema conversion and type handling.
"""
from __future__ import annotations

import pyarrow as pa


def convert_arrow_schema_for_paimon(arrow_schema: pa.Schema) -> pa.Schema:
    """Convert PyArrow schema to be compatible with pypaimon.

    pypaimon doesn't support large_string/large_binary types, so we need to
    convert them to regular string/binary types.

    Args:
        arrow_schema: PyArrow schema to convert

    Returns:
        pa.Schema: Converted schema compatible with pypaimon
    """
    new_fields = []
    need_conversion = False

    for field in arrow_schema:
        field_type = field.type
        # Convert large_string to string
        if pa.types.is_large_string(field_type):
            field_type = pa.string()
            need_conversion = True
        # Convert large_binary to binary
        elif pa.types.is_large_binary(field_type):
            field_type = pa.binary()
            need_conversion = True
        new_fields.append(
            pa.field(field.name, field_type, nullable=field.nullable, metadata=field.metadata)
        )

    if need_conversion:
        return pa.schema(new_fields)
    return arrow_schema


def convert_arrow_table_for_paimon(arrow_table: pa.Table) -> pa.Table:
    """Convert PyArrow table to be compatible with pypaimon.

    This converts the schema and casts the table to the new schema.

    Args:
        arrow_table: PyArrow table to convert

    Returns:
        pa.Table: Converted table compatible with pypaimon
    """
    original_schema = arrow_table.schema
    new_schema = convert_arrow_schema_for_paimon(original_schema)

    if new_schema.equals(original_schema):
        return arrow_table

    return arrow_table.cast(new_schema)
