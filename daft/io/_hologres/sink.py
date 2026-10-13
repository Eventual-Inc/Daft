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
import uuid
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


_STAGE_FILE_SIZE_LIMIT = 64 * 1024 * 1024  # 64 MB


def _prepare_stage_table(table, holo_schema: HologresTableSchema):
    """Project a decoded batch onto the table columns and JSON-encode jsonb.

    The stage wire format stores json/jsonb as UTF-8 text and timestamps as
    date64; :meth:`ArrowStageBuffer.put_table` casts the rest.
    """
    import pyarrow as pa

    columns = []
    fields = []
    for col_meta in holo_schema.columns:
        col = table.column(col_meta.name)
        if col_meta.type_name in ("json", "jsonb"):
            col = pa.array([None if v is None else _to_json_text(v) for v in col.to_pylist()], pa.utf8())
        columns.append(col)
        fields.append(pa.field(col_meta.name, col.type, nullable=True))
    return pa.Table.from_arrays(columns, schema=pa.schema(fields))


def _commit_stage_sql(self) -> str:
    from daft.io._hologres._stage_sql import build_insert_select_from_stage_sql_pk

    columns = [(c.name, c.type_name, c.precision, c.scale) for c in self._holo_schema.columns]
    conflict = None
    if self._holo_schema.has_primary_key and self._on_conflict is not OnConflictAction.NONE:
        conflict = "ignore" if self._on_conflict is OnConflictAction.INSERT_OR_IGNORE else "update"
    return build_insert_select_from_stage_sql_pk(
        schema_name=self._schema_name,
        table_name=self._table_name,
        columns=columns,
        primary_keys=self._holo_schema.primary_keys,
        stages=[self._stage_name],
        conflict_action=conflict,
        is_overwrite=self._write_mode == "overwrite",
    )


def _child_partition_name(table_name: str, value: Any) -> str:
    """Deterministic child partition name for a partition value."""
    import hashlib
    import re

    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", str(value).lower())[:40]
    digest = hashlib.md5(str(value).encode()).hexdigest()[:8]
    return f"{table_name}_p_{sanitized}_{digest}"[:63]


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
        copy_mode: Literal["stream", "bulk_load", "stage"] = "stream",
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

        if copy_mode == "stage":
            from daft.io._hologres._schema_loader import get_holo_version

            conn = psycopg.connect(connection_uri, autocommit=True)
            try:
                version = get_holo_version(conn)
            finally:
                conn.close()
            if version < (4, 1, 0):
                raise ValueError(
                    f"copy_mode='stage' requires Hologres >= 4.1.0, but the instance reports "
                    f"{version[0]}.{version[1]}.{version[2]}. Use copy_mode='stream' or 'bulk_load' instead."
                )
        self._stage_name: str | None = (
            f"daft_{schema_name}_{table_name}_{uuid.uuid4().hex[:8]}"[:63] if copy_mode == "stage" else None
        )

    def name(self) -> str:
        return "HologresDataSink"

    def schema(self) -> Schema:
        return _METRICS_SCHEMA

    def start(self) -> None:
        """Driver-side one-shot setup: existence checks, truncation, staging."""
        from daft.catalog.__hologres import hologres_connection
        from daft.io._hologres._ddl import build_truncate_table_sql
        from daft.io._hologres._stage_sql import build_create_stage_sql

        with hologres_connection(self._connection_uri) as conn:
            if self._write_mode == "fail" and table_exists(conn, self._schema_name, self._table_name):
                raise ValueError(
                    f"Table {self._schema_name}.{self._table_name} already exists, cannot write with write_mode='fail'."
                )
            if self._write_mode == "overwrite" and self._copy_mode is not CopyMode.STAGE:
                # Stage mode overwrites with INSERT OVERWRITE at commit time.
                with conn.cursor() as cur:
                    cur.execute(build_truncate_table_sql(self._schema_name, self._table_name))
                logger.info("Truncated table %s.%s for overwrite", self._schema_name, self._table_name)
            if self._stage_name:
                with conn.cursor() as cur:
                    cur.execute(build_create_stage_sql(self._stage_name))
                logger.info("Created internal stage %s", self._stage_name)

    def _ensure_partitions(self, table) -> None:
        """Create child partitions for the partition values in ``table``.

        Logical partition tables reject rows whose partition value has no
        child partition, so values are created up front. DDL is idempotent
        (``IF NOT EXISTS``) and duplicate-creation races are ignored.
        """
        import psycopg

        partition_col = self._holo_schema.partition_column
        if partition_col not in table.column_names:
            raise ValueError(
                f"Partition column {partition_col!r} missing from the DataFrame written to "
                f"{self._schema_name}.{self._table_name}"
            )

        values = table.column(partition_col).to_pylist()
        distinct = sorted({v for v in values if v is not None})
        if not distinct:
            return

        from psycopg import sql

        conn = psycopg.connect(self._connection_uri, autocommit=True)
        try:
            with conn.cursor() as cur:
                for value in distinct:
                    child = _child_partition_name(self._table_name, value)
                    # DDL does not accept bind parameters; the value goes in as
                    # a safely-quoted literal.
                    cur.execute(
                        sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} PARTITION OF {}.{} FOR VALUES IN ({})").format(
                            sql.Identifier(self._schema_name),
                            sql.Identifier(child),
                            sql.Identifier(self._schema_name),
                            sql.Identifier(self._table_name),
                            sql.Literal(value),
                        )
                    )
        except psycopg.errors.DuplicateTable:
            logger.debug("Partition already created by a concurrent writer")
        finally:
            conn.close()

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[dict[str, Any] | None]]:
        import psycopg

        if self._copy_mode is CopyMode.STAGE:
            yield from self._write_stage(micropartitions)
            return

        for micropartition in micropartitions:
            table = micropartition.to_arrow()
            if table.num_rows == 0:
                continue

            if self._holo_schema.is_partition_parent:
                self._ensure_partitions(table)

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

    def _fixed_fe_connect(self):
        """Connect routed to the Hologres FixedFE (required for stage uploads)."""
        import psycopg

        return psycopg.connect(self._connection_uri, autocommit=True, options="type=fixed")

    def _write_stage(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[dict[str, Any] | None]]:
        """Upload micropartitions to the shared internal stage as Arrow files.

        Files are named uniquely per task; the coordinator commits once in
        ``finalize()`` so every staged file is loaded exactly once.
        """
        from daft.io._hologres._arrow_codec import ArrowStageBuffer, columns_for

        columns = columns_for(self._holo_schema, [c.name for c in self._holo_schema.columns])
        buffer = ArrowStageBuffer(columns)
        file_prefix = f"{self._schema_name}_{self._table_name}_{uuid.uuid4().hex[:8]}"
        file_index = 0
        rows_written = 0
        bytes_written = 0
        conn = self._fixed_fe_connect()
        try:
            with conn.cursor() as cur:
                for micropartition in micropartitions:
                    table = micropartition.to_arrow()
                    if table.num_rows == 0:
                        continue
                    table = _prepare_stage_table(table, self._holo_schema)
                    buffer.put_table(table)
                    rows_written += table.num_rows
                    bytes_written += table.nbytes
                    if buffer.data_size >= _STAGE_FILE_SIZE_LIMIT:
                        data = buffer.end_and_get_bytes()
                        self._upload_stage_file(cur, file_prefix, file_index, data)
                        file_index += 1
                data = buffer.end_and_get_bytes()
                if data:
                    self._upload_stage_file(cur, file_prefix, file_index, data)
                    file_index += 1
        finally:
            conn.close()

        yield WriteResult(
            result={"stage": self._stage_name},
            rows_written=rows_written,
            bytes_written=bytes_written,
        )

    def _upload_stage_file(self, cur, file_prefix: str, file_index: int, data: bytes) -> None:
        from daft.io._hologres._stage_sql import build_copy_in_stage_sql

        file_name = f"{file_prefix}_{file_index}.arrow"
        with cur.copy(build_copy_in_stage_sql(self._stage_name, file_name)) as copy:
            copy.write(data)
        logger.info("Uploaded %d bytes to stage %s as %s", len(data), self._stage_name, file_name)

    def _commit_stage(self) -> None:
        """Coordinator: load every staged file into the table, then drop it."""
        import psycopg

        from daft.io._hologres._stage_sql import build_drop_stage_sql

        insert_sql = _commit_stage_sql(self)
        logger.info("Stage commit SQL: %s", insert_sql)
        conn = psycopg.connect(self._connection_uri, autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute(insert_sql)
        finally:
            conn.close()
        try:
            with psycopg.connect(self._connection_uri, autocommit=True) as conn, conn.cursor() as cur:
                cur.execute(build_drop_stage_sql(self._stage_name))
        except Exception as e:
            logger.warning("Failed to drop stage %s: %s", self._stage_name, e)

    def finalize(self, write_results: list[WriteResult[dict[str, Any] | None]]) -> MicroPartition:
        from daft.recordbatch import MicroPartition

        if self._copy_mode is CopyMode.STAGE:
            self._commit_stage()
            return MicroPartition.from_pydict(
                {
                    "total_written_rows": [sum(r.rows_written for r in write_results)],
                    "total_written_bytes": [sum(r.bytes_written for r in write_results)],
                }
            )

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
