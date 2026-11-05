"""COPY-based bulk write support for Hologres.

Supports two formats:
- text: uses psycopg's ``write_row()`` for automatic text encoding
- binary: uses psycopg dumpers for binary encoding per the PG binary COPY protocol

Supports two copy modes:
- STREAM: Hologres streaming mode with row-level locks, supports on_conflict
- BULK_LOAD: standard PostgreSQL batch COPY, table-level lock, highest throughput

Adapted from holo-client-py (hologres/copy.py).
"""

from __future__ import annotations

import logging
import struct
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

import psycopg
import psycopg.pq
from psycopg import sql

from daft.io._hologres._schema_loader import qualified_name_for_sql
from daft.io._hologres.types import CopyFormat, CopyMode, OnConflictAction

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

# PostgreSQL binary COPY header constants
_PG_BINARY_SIGNATURE = b"PGCOPY\n\xff\r\n\x00"
_PG_BINARY_FLAGS = 0
_PG_BINARY_EXTENSION_LENGTH = 0
_NUMERIC_OID = 1700
_BPCHAR_OID = 1042
_TEXT_OID = 25
_BYTEA_OID = 17

_COPY_TEMPLATE = "COPY {0}.{1}({2}) FROM STDIN WITH (FORMAT {3}, STREAM_MODE {4})"
_COPY_TEMPLATE_PK = "COPY {0}.{1}({2}) FROM STDIN WITH (FORMAT {3}, STREAM_MODE {4}, ON_CONFLICT {5})"

_TIMESTAMPTZ_OID = 1184
_TIMETZ_OID = 1266


def build_copy_sql(
    schema_name: str,
    table_name: str,
    columns: Sequence[str],
    fmt: CopyFormat,
    mode: CopyMode,
    on_conflict: OnConflictAction = OnConflictAction.INSERT_OR_REPLACE,
    has_pk: bool = False,
) -> sql.Composed:
    """Build a ``COPY ... FROM STDIN`` SQL statement."""
    col_ids = sql.SQL(", ").join(map(sql.Identifier, columns))
    stream_mode = "true" if mode == CopyMode.STREAM else "false"

    on_conflict_str = ""
    if has_pk and on_conflict is not OnConflictAction.NONE and mode in (CopyMode.STREAM, CopyMode.BULK_LOAD):
        if on_conflict == OnConflictAction.INSERT_OR_IGNORE:
            on_conflict_str = "ignore"
        else:
            on_conflict_str = "update"

    if on_conflict_str:
        return sql.SQL(_COPY_TEMPLATE_PK).format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            col_ids,
            fmt.value,
            stream_mode,
            on_conflict_str,
        )
    return sql.SQL(_COPY_TEMPLATE).format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        col_ids,
        fmt.value,
        stream_mode,
    )


def encode_binary_header() -> bytes:
    """Encode the PostgreSQL binary COPY header."""
    header = bytearray()
    header.extend(_PG_BINARY_SIGNATURE)
    header.extend(struct.pack("!I", _PG_BINARY_FLAGS))
    header.extend(struct.pack("!I", _PG_BINARY_EXTENSION_LENGTH))
    return bytes(header)


def coerce_binary_value(value: object, type_oid: int, type_name: str) -> object:
    """Coerce a Python value for binary COPY encoding of the given column type.

    - timestamptz/timetz: Hologres session timezones can be unparseable for
      psycopg, so naive values are assumed to be UTC.
    - numeric: floats are converted to Decimal for exact encoding.
    """
    if value is None:
        return None
    if type_oid == _NUMERIC_OID and isinstance(value, float):
        return Decimal(str(value))
    if type_oid in (_TIMESTAMPTZ_OID, _TIMETZ_OID) and isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def encode_binary_row(
    values: Sequence,
    column_names: Sequence[str],
    column_type_oids: dict[str, int],
    column_type_names: dict[str, str],
    cur: psycopg.Cursor,
) -> bytes:
    """Encode a single row in PostgreSQL binary COPY format.

    Uses psycopg's dumper system for type-correct binary encoding.
    """
    row_data = bytearray()
    num_cols = len(values)
    row_data.extend(struct.pack("!h", num_cols))

    for idx, value in enumerate(values):
        if value is None:
            row_data.extend(struct.pack("!i", -1))
            continue
        col_name = column_names[idx]
        type_oid = column_type_oids[col_name]
        type_name = column_type_names.get(col_name, "")

        # Extension types with raw-bytes binary format (e.g. roaringbitmap)
        # have dynamic OIDs unknown to psycopg; encode via the bytea dumper.
        if type_name in ("roaringbitmap",):
            lookup_oid = _BYTEA_OID
        # bpchar (char(n)) has no binary dumper in psycopg; use the text OID
        # dumper instead — the binary wire format is identical.
        elif type_oid == _BPCHAR_OID:
            lookup_oid = _TEXT_OID
        else:
            lookup_oid = type_oid

        dumper_class = cur.adapters.get_dumper_by_oid(lookup_oid, psycopg.pq.Format.BINARY)
        dumper = dumper_class(value.__class__, cur)

        binary_value = dumper.dump(coerce_binary_value(value, type_oid, type_name))
        row_data.extend(struct.pack("!i", len(binary_value)))
        row_data.extend(binary_value)

    return bytes(row_data)


_TYPE_OID_SQL = sql.SQL("""\
SELECT a.attname, a.atttypid, t.typname
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
WHERE a.attrelid = {0}::regclass
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
""")


def load_column_type_info(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str,
) -> tuple[dict[str, int], dict[str, str]]:
    """Load column name -> (type OID, type name) mappings from pg_attribute."""
    qualified = sql.Literal(qualified_name_for_sql(schema_name, table_name))
    with conn.cursor() as cur:
        cur.execute(_TYPE_OID_SQL.format(qualified))
        oids: dict[str, int] = {}
        names: dict[str, str] = {}
        for row in cur.fetchall():
            oids[row[0]] = int(row[1])
            names[row[0]] = row[2]
        return oids, names


class BinaryRowEncoder:
    """Encodes rows to the PostgreSQL binary COPY wire format.

    Loads the target table's column type OIDs once and reuses the connection's
    psycopg dumper registry for every row.
    """

    def __init__(
        self,
        column_names: Sequence[str],
        column_type_oids: dict[str, int],
        column_type_names: dict[str, str],
        cursor: psycopg.Cursor,
    ) -> None:
        self._column_names = list(column_names)
        self._column_type_oids = column_type_oids
        self._column_type_names = column_type_names
        self._cursor = cursor

    @classmethod
    def for_table(
        cls, conn: psycopg.Connection, schema_name: str, table_name: str, columns: Sequence[str]
    ) -> tuple[BinaryRowEncoder, psycopg.Cursor]:
        """Create an encoder bound to ``conn`` for the given table columns."""
        oids, names = load_column_type_info(conn, schema_name, table_name)
        cursor = conn.cursor()
        return cls(columns, oids, names, cursor), cursor

    def encode(self, values: Sequence) -> bytes:
        """Encode one row (sequence of Python values, column order)."""
        return encode_binary_row(
            values,
            self._column_names,
            self._column_type_oids,
            self._column_type_names,
            self._cursor,
        )
