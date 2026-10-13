"""DDL statement builders for Hologres tables.

Hologres DDL notes:
- Tables are created with ``CREATE TABLE``; table properties (distribution
  key, orientation, indexes, ...) are set with ``CALL SET_TABLE_PROPERTY``
  statements, each executed independently (Hologres does not support mixing
  DDL and DML in one transaction, and Daft avoids transactions entirely).
- Logical partition tables use ``PARTITION BY LIST (col)``.
- Vector columns are ``float4[]``/``real[]`` with a dimension CHECK
  constraint, matching the Hologres vector index conventions.

All builders return SQL strings or psycopg Composed objects; identifiers are
always safely quoted.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from psycopg import sql

if TYPE_CHECKING:
    from collections.abc import Sequence

    from daft.logical.schema import Schema


@dataclass(frozen=True)
class ColumnDef:
    """A single column definition for CREATE TABLE."""

    name: str
    pg_type: str
    dimension: int | None = None  # for vector columns: embedding dimension


def build_column_defs(schema: Schema) -> list[ColumnDef]:
    """Build column definitions from a Daft Schema.

    Embedding columns get a dimension so the DDL builder can emit the vector
    dimension CHECK constraint expected by Hologres vector indexes.
    """
    from daft.io._hologres._schema_loader import daft_type_to_pg

    defs: list[ColumnDef] = []
    for field in schema:
        dimension: int | None = None
        if field.dtype.is_embedding():
            try:
                dimension = field.dtype.size
            except Exception:
                dimension = None
        elif field.dtype.is_fixed_size_list():
            try:
                if field.dtype.dtype.is_float32():
                    dimension = field.dtype.size
            except Exception:
                dimension = None
        defs.append(ColumnDef(name=field.name, pg_type=daft_type_to_pg(field.dtype), dimension=dimension))
    return defs


def build_create_table_sql(
    schema_name: str | None,
    table_name: str,
    columns: Sequence[ColumnDef],
    primary_keys: Sequence[str] = (),
    partition_by: str | None = None,
    if_not_exists: bool = False,
) -> sql.Composed:
    """Build a ``CREATE TABLE`` statement.

    Args:
        schema_name: optional schema; None uses the connection's search path.
        columns: column definitions in order.
        primary_keys: primary key column names (emitted as a table constraint).
        partition_by: column name for a logical partition table.
        if_not_exists: emit ``IF NOT EXISTS``.
    """
    col_parts: list[sql.Composable] = []
    for col in columns:
        col_sql = sql.SQL("{} {}").format(sql.Identifier(col.name), sql.SQL(col.pg_type))
        if col.dimension is not None and col.pg_type.endswith("[]"):
            # Vector dimension CHECK (Hologres vector index convention).
            col_sql = sql.SQL("{} CHECK (array_dims({}) = {})").format(
                col_sql,
                sql.Identifier(col.name),
                sql.Literal(f"[0:{col.dimension - 1}]"),
            )
        col_parts.append(col_sql)

    if primary_keys:
        col_parts.append(
            sql.SQL("PRIMARY KEY ({})").format(sql.SQL(", ").join(sql.Identifier(pk) for pk in primary_keys))
        )

    quoted_table = sql.Identifier(table_name)
    if schema_name:
        quoted_table = sql.SQL(".").join([sql.Identifier(schema_name), quoted_table])

    stmt = sql.SQL("CREATE TABLE {}{} ({})").format(
        sql.SQL("IF NOT EXISTS ") if if_not_exists else sql.SQL(""),
        quoted_table,
        sql.SQL(", ").join(col_parts),
    )

    if partition_by:
        stmt = sql.SQL("{} PARTITION BY LIST ({})").format(stmt, sql.Identifier(partition_by))

    return stmt


def build_set_table_property_sql(schema_name: str, table_name: str, key: str, value: str) -> str:
    """Build a ``CALL SET_TABLE_PROPERTY(...)`` statement.

    Args:
        key: property name, e.g. "distribution_key", "orientation", "index".
        value: property value; emitted as a quoted string literal.

    Note:
        Hologres takes the target table as a single string literal
        (``'schema.table'``), not as a quoted identifier.
    """
    if not key or not isinstance(key, str):
        raise ValueError(f"Invalid table property key: {key!r}")
    return (
        sql.SQL("CALL SET_TABLE_PROPERTY({}, {}, {})")
        .format(
            sql.Literal(f"{schema_name}.{table_name}"),
            sql.Literal(key),
            sql.Literal(str(value)),
        )
        .as_string()
    )


def build_table_property_statements(
    schema_name: str,
    table_name: str,
    options: dict,
) -> list[str]:
    """Translate ``create_table`` options into SET_TABLE_PROPERTY statements.

    Supported options (all optional):
        distribution_key, clustering_key, orientation, time_to_live_in_days,
        bitmap_columns, dictionary_encoding, index, proxima_vectors,
        table_properties (free-form dict).

    Note: shard_count is a table-group-level setting on Hologres and cannot
    be set per table, so it is intentionally not an option here.
    """
    statements: list[str] = []

    simple_keys = (
        "distribution_key",
        "clustering_key",
        "orientation",
        "bitmap_columns",
        "dictionary_encoding",
        "index",
    )
    for key in simple_keys:
        if key in options and options[key] is not None:
            statements.append(build_set_table_property_sql(schema_name, table_name, key, options[key]))

    if options.get("time_to_live_in_days") is not None:
        # Hologres 5.x exposes TTL as time_to_live_in_seconds (min 86400).
        statements.append(
            build_set_table_property_sql(
                schema_name,
                table_name,
                "time_to_live_in_seconds",
                int(options["time_to_live_in_days"]) * 86400,
            )
        )
    if options.get("time_to_live_in_seconds") is not None:
        statements.append(
            build_set_table_property_sql(
                schema_name, table_name, "time_to_live_in_seconds", int(options["time_to_live_in_seconds"])
            )
        )
    if options.get("proxima_vectors") is not None:
        proxima = options["proxima_vectors"]
        statements.append(
            build_set_table_property_sql(
                schema_name, table_name, "proxima_vectors", proxima if isinstance(proxima, str) else json.dumps(proxima)
            )
        )
    table_properties = options.get("table_properties")
    if table_properties:
        if not isinstance(table_properties, dict):
            raise ValueError("table_properties must be a dict of property key -> value")
        for key, value in table_properties.items():
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            statements.append(build_set_table_property_sql(schema_name, table_name, str(key), value))

    return statements


def build_drop_table_sql(schema_name: str | None, table_name: str, if_exists: bool = False) -> sql.Composed:
    """Build a ``DROP TABLE`` statement."""
    quoted_table = sql.Identifier(table_name)
    if schema_name:
        quoted_table = sql.SQL(".").join([sql.Identifier(schema_name), quoted_table])
    return sql.SQL("DROP TABLE {}{}").format(sql.SQL("IF EXISTS ") if if_exists else sql.SQL(""), quoted_table)


def build_truncate_table_sql(schema_name: str, table_name: str) -> str:
    """Build a ``TRUNCATE TABLE`` statement (fast overwrite of a whole table)."""
    return sql.SQL("TRUNCATE TABLE {}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name)).as_string()


def build_create_schema_sql(schema_name: str, if_not_exists: bool = False) -> sql.Composed:
    """Build a ``CREATE SCHEMA`` statement."""
    return sql.SQL("CREATE SCHEMA {}{}").format(
        sql.SQL("IF NOT EXISTS ") if if_not_exists else sql.SQL(""), sql.Identifier(schema_name)
    )


def build_drop_schema_sql(schema_name: str, if_exists: bool = False, cascade: bool = False) -> sql.Composed:
    """Build a ``DROP SCHEMA`` statement."""
    stmt = sql.SQL("DROP SCHEMA {}{}").format(
        sql.SQL("IF EXISTS ") if if_exists else sql.SQL(""), sql.Identifier(schema_name)
    )
    if cascade:
        stmt = sql.SQL("{} CASCADE").format(stmt)
    return stmt
