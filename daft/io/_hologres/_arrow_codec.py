"""Arrow type mapping and IPC serialization for Hologres stage writes.

Maps Hologres column types to Apache Arrow types and buffers Arrow tables as
Arrow IPC streams for upload to an internal stage.

Adapted from holo-client-py (hologres/_arrow.py); operates on
:class:`HologresColumn` metadata instead of holo-client Column objects.
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING

from daft.dependencies import pa
from daft.io._hologres._schema_loader import HologresColumn, HologresTableSchema

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def column_to_arrow_type(col: HologresColumn) -> pa.DataType:
    """Map a Hologres column to an Apache Arrow type (stage wire format)."""
    col_type = col.type_name.strip().lower()

    if col_type in ("smallint", "int2"):
        return pa.int16()
    elif col_type in ("integer", "int4", "int", "serial", "smallserial"):
        return pa.int32()
    elif col_type in ("bigint", "int8", "bigserial"):
        return pa.int64()
    elif col_type in ("real", "float4"):
        return pa.float32()
    elif col_type in ("double precision", "float8"):
        return pa.float64()
    elif col_type in ("numeric", "decimal"):
        precision = col.precision if col.precision > 0 else 38
        scale = max(col.scale, 0)
        return pa.decimal128(precision, scale)
    elif col_type in ("boolean", "bool"):
        return pa.bool_()
    elif col_type in ("char", "bpchar", "character", "varchar", "character varying", "text"):
        return pa.utf8()
    elif col_type == "bytea":
        return pa.binary()
    elif col_type == "date":
        return pa.date32()
    elif col_type in ("time", "time without time zone"):
        return pa.time64("us")
    elif col_type == "timetz":
        return pa.binary(16)
    elif col_type in ("timestamp", "timestamp without time zone"):
        return pa.timestamp("us")
    elif col_type in ("timestamptz", "timestamp with time zone"):
        # Hologres stages timestamptz as date64 (ms), matching the Java client.
        return pa.date64()
    elif col_type == "json" or col_type == "jsonb":
        # Staged as text; cast back to jsonb during INSERT ... SELECT.
        return pa.utf8()
    elif col_type in ("uuid",):
        return pa.utf8()
    elif col_type == "roaringbitmap":
        return pa.binary()
    elif col_type.endswith("[]"):
        return _array_arrow_type(col_type)
    else:
        return pa.utf8()


def _array_arrow_type(type_name: str) -> pa.DataType:
    """Map an array type name to an Arrow list type.

    Handles both PG internal names (``_int4``) and user-facing names
    (``integer[]``).
    """
    element_map = {
        "_int4": pa.int32(),
        "_int8": pa.int64(),
        "_int2": pa.int16(),
        "_float4": pa.float32(),
        "_float8": pa.float64(),
        "_bool": pa.bool_(),
        "_text": pa.utf8(),
        "_varchar": pa.utf8(),
        # User-facing type names (from format_type)
        "integer[]": pa.int32(),
        "int[]": pa.int32(),
        "int4[]": pa.int32(),
        "bigint[]": pa.int64(),
        "int8[]": pa.int64(),
        "smallint[]": pa.int16(),
        "int2[]": pa.int16(),
        "real[]": pa.float32(),
        "float4[]": pa.float32(),
        "double precision[]": pa.float64(),
        "float8[]": pa.float64(),
        "boolean[]": pa.bool_(),
        "bool[]": pa.bool_(),
        "text[]": pa.utf8(),
        "varchar[]": pa.utf8(),
        "character varying[]": pa.utf8(),
    }
    element_type = element_map.get(type_name)
    if element_type is None:
        raise ValueError(f"Unsupported array element type: {type_name}")
    return pa.list_(pa.field("item", element_type, nullable=True))


def build_arrow_schema(
    columns: Sequence[HologresColumn],
) -> pa.Schema:
    """Build the Arrow schema matching the stage wire format for ``columns``."""
    return pa.schema([pa.field(c.name, column_to_arrow_type(c), nullable=True) for c in columns])


def columns_for(holo_schema: HologresTableSchema, column_names: Sequence[str]) -> list[HologresColumn]:
    """Resolve the writer's column list against a table schema."""
    resolved: list[HologresColumn] = []
    for name in column_names:
        idx = holo_schema.get_column_index(name)
        if idx is None:
            raise ValueError(
                f"Column {name!r} not found in schema of {holo_schema.schema_name}.{holo_schema.table_name}"
            )
        resolved.append(holo_schema.columns[idx])
    return resolved


class ArrowStageBuffer:
    """Buffers Arrow tables and serializes them as an Arrow IPC stream.

    Tables are projected onto the writer's column list and cast to the stage
    wire schema, so callers may pass extra columns or a different column order.
    """

    def __init__(self, columns: Sequence[HologresColumn]) -> None:
        self._columns = list(columns)
        self._arrow_schema = build_arrow_schema(self._columns)
        self._buffer: io.BytesIO | None = None
        self._writer = None
        self._data_size = 0

    @property
    def data_size(self) -> int:
        """Current size of accumulated Arrow data in bytes."""
        return self._data_size

    def put_table(self, table: pa.Table) -> None:
        """Add an Arrow table to the stream (projected and cast)."""
        table = table.select(self._arrow_schema.names).cast(self._arrow_schema)
        if table.num_rows == 0:
            return
        self._ensure_writer()
        self._writer.write_table(table)
        self._data_size = self._buffer.tell()

    def _ensure_writer(self) -> None:
        if self._buffer is None:
            self._buffer = io.BytesIO()
            self._writer = pa.ipc.new_stream(self._buffer, self._arrow_schema)

    def end_and_get_bytes(self) -> bytes:
        """Close the IPC stream and return all bytes; resets internal state."""
        if self._writer is None:
            return b""
        self._writer.close()
        data = self._buffer.getvalue()
        self._buffer = None
        self._writer = None
        self._data_size = 0
        return data

    def close(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception as e:  # noqa: BLE001 -- best-effort close must not mask the original error
                logger.debug("Failed to close Arrow IPC writer: %s", e)
        self._buffer = None
        self._writer = None
        self._data_size = 0
