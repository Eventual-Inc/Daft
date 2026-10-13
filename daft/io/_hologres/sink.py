"""Hologres DataSink writing DataFrames via stream/bulk-load COPY.

The write is distributed: the driver runs ``start()`` once (existence checks,
``TRUNCATE`` for overwrite), and every worker streams its micropartition to
Hologres with ``COPY ... FROM STDIN`` in its own autocommit connection.

Default mode is the stream (fixed plan) COPY with row-level locks, which is
safe for concurrent writers and performs primary-key upserts. ``bulk_load``
locks the whole table for maximum throughput.

Adapts value coercion from holo-client-py (hologres/collector.py).
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Literal

from daft.datatype import DataType
from daft.io._hologres._copy import build_copy_sql, encode_binary_header, encode_binary_row
from daft.io._hologres._schema_loader import (
    HologresTableSchema,
    load_table_schema,
    table_exists,
)
from daft.io._hologres.types import CopyFormat, CopyMode, OnConflictAction
from daft.io.sink import DataSink, WriteResult
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pyarrow as pa

    from daft.recordbatch import MicroPartition

logger = logging.getLogger(__name__)

_FLOAT4_OID = 700
_BYTEA_OID = 17

_METRICS_SCHEMA = Schema._from_field_name_and_types(
    [
        ("total_written_rows", DataType.int64()),
        ("total_written_bytes", DataType.int64()),
    ]
)


class HologresDataSink(DataSink[dict[str, Any] | None]):
    """Writes micropartitions to a Hologres table via COPY.

    Args:
        connection_uri: PostgreSQL connection string for the Hologres instance.
        schema_name: schema of the target table.
        table_name: target table name.
        write_mode: ``append`` inserts/upserts rows, ``overwrite`` truncates
            the table first (driver-side), ``fail`` errors when the table
            already exists.
        copy_mode: ``stream`` (fixed plan, row locks, supports upsert) or
            ``bulk_load`` (table lock, highest throughput).
        copy_format: ``binary`` (default, faster) or ``text`` (compatibility
            fallback).
        on_conflict: primary-key conflict action for ``append``:
            ``update`` (default when the table has a primary key), ``ignore``,
            ``replace`` or ``none``.
    """

    def __init__(
        self,
        connection_uri: str,
        schema_name: str,
        table_name: str,
        write_mode: Literal["append", "overwrite", "fail"] = "append",
        copy_mode: Literal["stream", "bulk_load"] = "stream",
        copy_format: Literal["binary", "text"] = "binary",
        on_conflict: Literal["update", "ignore", "replace", "none"] | None = None,
    ) -> None:
        import psycopg

        self._connection_uri = connection_uri
        self._schema_name = schema_name
        self._table_name = table_name
        self._write_mode = write_mode
        self._copy_mode = CopyMode(copy_mode)
        self._copy_format = CopyFormat(copy_format)

        conn = psycopg.connect(connection_uri, autocommit=True)
        try:
            if not table_exists(conn, schema_name, table_name):
                raise ValueError(f"Hologres table {schema_name}.{table_name} does not exist")
            self._holo_schema: HologresTableSchema = load_table_schema(conn, schema_name, table_name)
        finally:
            conn.close()

        if on_conflict is None:
            on_conflict = "update" if self._holo_schema.has_primary_key else "none"
        self._on_conflict = OnConflictAction(on_conflict)

    def name(self) -> str:
        return "HologresDataSink"

    def schema(self) -> Schema:
        return _METRICS_SCHEMA

    def start(self) -> None:
        """Driver-side one-shot setup: existence checks and truncation."""
        from daft.catalog.__hologres import hologres_connection
        from daft.io._hologres._ddl import build_truncate_table_sql

        with hologres_connection(self._connection_uri) as conn:
            if self._write_mode == "fail" and table_exists(conn, self._schema_name, self._table_name):
                raise ValueError(
                    f"Table {self._schema_name}.{self._table_name} already exists, cannot write with write_mode='fail'."
                )
            if self._write_mode == "overwrite":
                with conn.cursor() as cur:
                    cur.execute(build_truncate_table_sql(self._schema_name, self._table_name))
                logger.info("Truncated table %s.%s for overwrite", self._schema_name, self._table_name)

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[dict[str, Any] | None]]:
        import psycopg

        for micropartition in micropartitions:
            table = micropartition.to_arrow()
            if table.num_rows == 0:
                continue

            columns = list(table.column_names)
            plan, has_float_array = _CopyPlan.build(self._holo_schema, columns)
            rows_written = table.num_rows
            bytes_written = table.nbytes

            # Hologres' binary COPY rejects PostgreSQL array wire data for
            # float4[] columns, so writes touching them fall back to TEXT.
            copy_format = CopyFormat.TEXT if has_float_array else self._copy_format

            conn = psycopg.connect(self._connection_uri, autocommit=True)
            try:
                with conn.cursor() as cur:
                    copy_sql = build_copy_sql(
                        schema_name=self._schema_name,
                        table_name=self._table_name,
                        columns=columns,
                        fmt=copy_format,
                        mode=self._copy_mode,
                        on_conflict=self._on_conflict,
                        has_pk=self._holo_schema.has_primary_key,
                    )
                    with cur.copy(copy_sql) as copy:
                        if copy_format is CopyFormat.BINARY:
                            copy.write(encode_binary_header())
                            encoder = _BinaryRowEncoder(plan, cur)
                            for row in plan.iter_rows(table):
                                copy.write(encoder.encode(row))
                        else:
                            for row in plan.iter_rows(table):
                                copy.write_row(row)
            finally:
                conn.close()

            yield WriteResult(
                result=None,
                rows_written=rows_written,
                bytes_written=bytes_written,
            )

    def finalize(self, write_results: list[WriteResult[dict[str, Any] | None]]) -> MicroPartition:
        from daft.recordbatch import MicroPartition

        total_rows = sum(r.rows_written for r in write_results)
        total_bytes = sum(r.bytes_written for r in write_results)
        return MicroPartition.from_pydict(
            {
                "total_written_rows": [total_rows],
                "total_written_bytes": [total_bytes],
            }
        )


class _CopyPlan:
    """Per-write plan mapping DataFrame columns to COPY-compatible values."""

    def __init__(self, columns: list[tuple[str, int, Any]]) -> None:
        # (column name, copy OID or None for default, value converter)
        self._columns = columns

    @classmethod
    def build(cls, holo_schema: HologresTableSchema, column_names: list[str]) -> tuple[_CopyPlan, bool]:
        plan: list[tuple[str, int, Any]] = []
        has_float_array = False
        for name in column_names:
            idx = holo_schema.get_column_index(name)
            if idx is None:
                raise ValueError(
                    f"Column {name!r} not found in Hologres table {holo_schema.schema_name}.{holo_schema.table_name}"
                )
            col = holo_schema.columns[idx]
            pg_type = col.type_name
            oid = col.type_oid

            if pg_type in ("real[]", "float4[]", "_float4"):
                # float4[] values are written as PostgreSQL array literals in
                # TEXT format; the binary COPY path rejects array wire data.
                has_float_array = True
                plan.append((name, oid, _to_float4_array_literal))
            elif pg_type in ("jsonb", "json", "text", "varchar", "character varying") or pg_type.startswith(
                "character"
            ):
                plan.append((name, oid, _to_json_text))
            else:
                plan.append((name, oid, _identity))
        return cls(plan), has_float_array

    def iter_rows(self, table: pa.Table) -> Iterator[tuple]:
        columns = [table.column(name).to_pylist() for name, _, _ in self._columns]
        converted = [[conv(v) for v in values] for values, (_, _, conv) in zip(columns, self._columns)]
        return zip(*converted)


def _identity(value: Any) -> Any:
    return value


def _to_float4_array_literal(value: Any) -> Any:
    if value is None:
        return None
    inner = ",".join("NULL" if v is None else repr(float(v)) for v in value)
    return "{" + inner + "}"


def _to_json_text(value: Any) -> Any:
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, default=str)


class _BinaryRowEncoder:
    """Encodes rows with per-column psycopg dumpers (vendored wire format)."""

    def __init__(self, plan: _CopyPlan, cursor: Any) -> None:
        self._column_names = [name for name, _, _ in plan._columns]
        self._oids = {name: oid for name, oid, _ in plan._columns}
        self._names = {name: "custom" for name in self._column_names}
        self._cursor = cursor

    def encode(self, values: tuple) -> bytes:

        return encode_binary_row(values, self._column_names, self._oids, self._names, self._cursor)
