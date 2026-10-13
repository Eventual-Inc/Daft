"""Load table schema/metadata from a Hologres database and map types.

Produces a Daft-native :class:`HologresTableSchema` (columns, primary keys,
distribution keys, logical partition column, shard count) and converts between
PostgreSQL/Hologres type names and Daft DataTypes.

Adapted from holo-client-py (hologres/_schema_loader.py, hologres/column.py).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from psycopg import sql

from daft.datatype import DataType

if TYPE_CHECKING:
    from collections.abc import Sequence

    import psycopg

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HologresColumn:
    """One column of a Hologres table."""

    name: str
    type_name: str  # normalized PG type name, e.g. "int4", "text", "jsonb", "float4[]"
    type_oid: int = 0
    precision: int = 0
    scale: int = 0
    nullable: bool = True
    is_primary_key: bool = False
    default_value: str | None = None


@dataclass(frozen=True)
class HologresTableSchema:
    """Describes the full schema/metadata of a Hologres table."""

    schema_name: str
    table_name: str
    columns: list[HologresColumn] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    distribution_keys: list[str] = field(default_factory=list)
    clustering_keys: list[str] = field(default_factory=list)
    partition_column: str | None = None
    shard_count: int = 0
    table_id: str = ""
    schema_version: str = ""
    table_group: str = ""

    def get_column_index(self, name: str) -> int | None:
        for i, col in enumerate(self.columns):
            if col.name == name:
                return i
        return None

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def has_primary_key(self) -> bool:
        return len(self.primary_keys) > 0

    @property
    def is_partition_parent(self) -> bool:
        return self.partition_column is not None


_QUALIFIED_NAME = sql.SQL("""\
SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type_name,
       a.atttypid AS type_oid, a.attnotnull, a.attnum,
       pg_get_expr(d.adbin, d.adrelid) AS default_value
FROM pg_attribute a
LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attrelid = {0}::regclass AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
""")

_PK_SQL = sql.SQL("""\
SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = {0}::regclass AND i.indisprimary
ORDER BY array_position(i.indkey, a.attnum)
""")

_TABLE_PROPERTIES_SQL = sql.SQL("""\
SELECT property_key, property_value
FROM hologres.hg_table_properties
WHERE table_namespace = {0} AND table_name = {1}
  AND property_key IN ('distribution_key', 'clustering_key', 'table_id', 'schema_version', 'table_group')
""")

_SHARD_COUNT_SQL = sql.SQL("""\
SELECT g.property_value
FROM hologres.hg_table_properties t
JOIN hologres.hg_table_group_properties g ON t.property_value = g.tablegroup_name
WHERE t.property_key = 'table_group' AND g.property_key = 'shard_count'
  AND t.table_namespace = {0} AND t.table_name = {1}
""")

_PARTITION_SQL = sql.SQL("""\
SELECT a.attname
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_partitioned_table part ON c.oid = part.partrelid
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = part.partattrs[0]
WHERE n.nspname = {0} AND c.relname = {1} AND part.partstrat = 'l'
LIMIT 1
""")


def _extract_precision_scale(raw: str) -> tuple[int, int]:
    """Extract (precision, scale) from a type like ``numeric(38,18)``, else (0, 0)."""
    lower = raw.lower().strip()
    paren = lower.find("(")
    if paren < 0:
        return (0, 0)
    close = lower.find(")", paren)
    if close < 0:
        return (0, 0)
    parts = lower[paren + 1 : close].split(",")
    try:
        precision = int(parts[0].strip())
        scale = int(parts[1].strip()) if len(parts) > 1 else 0
        return (precision, scale)
    except (ValueError, IndexError):
        return (0, 0)


def _strip_type_args(raw: str) -> str:
    """Strip length/precision modifiers: ``character varying(255)`` -> ``character varying``."""
    lower = raw.lower().strip()
    if lower.endswith("[]"):
        base = lower[:-2]
        return _strip_type_args(base) + "[]"
    paren = lower.find("(")
    if paren > 0:
        return lower[:paren].rstrip()
    return lower


def load_table_schema(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str,
) -> HologresTableSchema:
    """Load a :class:`HologresTableSchema` from the database."""
    qualified = sql.Literal(f"{schema_name}.{table_name}")
    schema_lit = sql.Literal(schema_name)
    table_lit = sql.Literal(table_name)

    columns: list[HologresColumn] = []
    with conn.cursor() as cur:
        cur.execute(_QUALIFIED_NAME.format(qualified))
        for col_name, raw_type, type_oid, not_null, _attnum, default_value in cur.fetchall():
            precision, scale = _extract_precision_scale(raw_type)
            columns.append(
                HologresColumn(
                    name=col_name,
                    type_name=_strip_type_args(raw_type),
                    type_oid=int(type_oid),
                    precision=precision,
                    scale=scale,
                    nullable=not not_null,
                    default_value=default_value,
                )
            )

    primary_keys: list[str] = []
    with conn.cursor() as cur:
        cur.execute(_PK_SQL.format(qualified))
        primary_keys = [row[0] for row in cur.fetchall()]
    columns = [replace(col, is_primary_key=col.name in set(primary_keys)) for col in columns]

    props: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(_TABLE_PROPERTIES_SQL.format(schema_lit, table_lit))
        for key, value in cur.fetchall():
            props[key] = value or ""

    shard_count = 0
    with conn.cursor() as cur:
        cur.execute(_SHARD_COUNT_SQL.format(schema_lit, table_lit))
        row = cur.fetchone()
        if row and row[0]:
            try:
                shard_count = int(row[0])
            except (ValueError, TypeError):
                pass

    partition_column: str | None = None
    with conn.cursor() as cur:
        cur.execute(_PARTITION_SQL.format(schema_lit, table_lit))
        row = cur.fetchone()
        if row:
            partition_column = row[0]

    def _split_keys(raw: str) -> list[str]:
        return [k.strip() for k in raw.split(",") if k.strip()] if raw else []

    return HologresTableSchema(
        schema_name=schema_name,
        table_name=table_name,
        columns=columns,
        primary_keys=primary_keys,
        distribution_keys=_split_keys(props.get("distribution_key", "")),
        clustering_keys=_split_keys(props.get("clustering_key", "")),
        partition_column=partition_column,
        shard_count=shard_count,
        table_id=props.get("table_id", ""),
        schema_version=props.get("schema_version", ""),
        table_group=props.get("table_group", ""),
    )


def table_exists(conn: psycopg.Connection, schema_name: str, table_name: str) -> bool:
    """Check whether a table (or partitioned parent) exists."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (schema_name, table_name),
        )
        return cur.fetchone() is not None


def get_holo_version(conn: psycopg.Connection) -> tuple[int, int, int]:
    """Detect the Hologres version as a (major, minor, fix) tuple."""
    import re

    pattern = re.compile(r"Hologres ([^ -]*)")
    with conn.cursor() as cur:
        cur.execute("SELECT hg_version()")
        row = cur.fetchone()
        if row is None:
            return (0, 0, 0)
        match = pattern.search(str(row[0]))
        if not match:
            return (0, 0, 0)
        parts = match.group(1).split(".", 2)
        try:
            return (int(parts[0]), int(parts[1]) if len(parts) > 1 else 0, int(parts[2]) if len(parts) > 2 else 0)
        except ValueError:
            return (0, 0, 0)


# ---------------------------------------------------------------------------
# Type mappings: Hologres/PG type names <-> Daft DataTypes
# ---------------------------------------------------------------------------

_PG_TO_DAFT_SCALARS: dict[str, DataType] = {
    "boolean": DataType.bool(),
    "bool": DataType.bool(),
    "smallint": DataType.int16(),
    "int2": DataType.int16(),
    "integer": DataType.int32(),
    "int4": DataType.int32(),
    "int": DataType.int32(),
    "bigint": DataType.int64(),
    "int8": DataType.int64(),
    "real": DataType.float32(),
    "float4": DataType.float32(),
    "double precision": DataType.float64(),
    "float8": DataType.float64(),
    "text": DataType.string(),
    "varchar": DataType.string(),
    "character varying": DataType.string(),
    "char": DataType.string(),
    "character": DataType.string(),
    "bpchar": DataType.string(),
    "date": DataType.date(),
    "time": DataType.time("us"),
    "time without time zone": DataType.time("us"),
    "timetz": DataType.string(),  # Hologres timetz has no exact Daft analog
    "time with time zone": DataType.string(),
    "timestamp": DataType.timestamp("us"),
    "timestamp without time zone": DataType.timestamp("us"),
    "timestamptz": DataType.timestamp("us", "UTC"),
    "timestamp with time zone": DataType.timestamp("us", "UTC"),
    "bytea": DataType.binary(),
    "uuid": DataType.string(),
    "roaringbitmap": DataType.binary(),
    "json": DataType.string(),
    "jsonb": DataType.string(),
}


def pg_type_to_daft(type_name: str, precision: int = 0, scale: int = 0) -> DataType | None:
    """Map a normalized PostgreSQL/Hologres type name to a Daft DataType.

    Returns None for unmappable types; callers may fall back to inference.
    """
    name = type_name.lower().strip()
    if name.endswith("[]"):
        inner = pg_type_to_daft(name[:-2])
        if inner is None:
            return None
        return DataType.list(inner)
    if name in ("numeric", "decimal"):
        if precision > 0:
            return DataType.decimal128(precision, max(scale, 0))
        return DataType.decimal128(38, 9)
    return _PG_TO_DAFT_SCALARS.get(name)


def daft_type_to_pg(dtype: DataType, set_dimensions: bool = True) -> str:
    """Convert a Daft DataType to a Hologres (PostgreSQL-compatible) DDL type string.

    Mirrors the PostgreSQL integration mapping; Hologres-specific choices:
    embeddings map to ``float4[]`` (no pgvector; vector indexes are configured
    via table properties) and complex nested types map to ``jsonb``.
    """
    if dtype.is_int8() or dtype.is_int16():
        return "smallint"
    elif dtype.is_int32():
        return "integer"
    elif dtype.is_int64():
        return "bigint"
    elif dtype.is_uint8():
        return "smallint"  # Hologres has no unsigned types
    elif dtype.is_uint16():
        return "integer"
    elif dtype.is_uint32():
        return "bigint"
    elif dtype.is_uint64():
        return "bigint"  # may overflow
    elif dtype.is_float32():
        return "real"
    elif dtype.is_float64():
        return "double precision"
    elif dtype.is_boolean():
        return "boolean"
    elif dtype.is_string():
        return "text"
    elif dtype.is_binary() or dtype.is_fixed_size_binary():
        return "bytea"
    elif dtype.is_date():
        return "date"
    elif dtype.is_timestamp():
        try:
            if dtype.timezone:
                return "timestamptz"
        except AttributeError:
            pass
        return "timestamp"
    elif dtype.is_time():
        return "time"
    elif dtype.is_decimal128():
        # Hologres requires explicit precision and scale.
        precision = dtype.precision if dtype.precision and dtype.precision > 0 else 38
        scale = dtype.scale if dtype.scale and dtype.scale >= 0 else 0
        return f"numeric({precision},{scale})"
    elif dtype.is_list() or dtype.is_fixed_size_list():
        try:
            inner = dtype.dtype
        except AttributeError:
            return "jsonb"
        if dtype.is_fixed_size_list() and set_dimensions and inner.is_float32():
            # Hologres vector convention: float4[]; the DDL builder adds the
            # dimension CHECK constraint.
            return "float4[]"
        inner_pg = daft_type_to_pg(inner, set_dimensions=False)
        if "[]" in inner_pg or inner_pg in ("jsonb",):
            return "jsonb"
        return f"{inner_pg}[]"
    elif dtype.is_embedding():
        # Hologres has no pgvector type; embeddings are float arrays with a
        # dimension CHECK constraint added by the DDL builder.
        return f"{daft_type_to_pg(DataType.float32(), set_dimensions=False)}[]"
    elif (
        dtype.is_struct()
        or dtype.is_map()
        or dtype.is_extension()
        or (
            dtype.is_tensor()
            or dtype.is_fixed_shape_tensor()
            or dtype.is_sparse_tensor()
            or dtype.is_fixed_shape_sparse_tensor()
        )
        or dtype.is_image()
        or dtype.is_fixed_shape_image()
    ):
        return "jsonb"
    elif dtype.is_python() or dtype.is_null():
        return "text"
    else:
        return "jsonb"


def daft_type_to_arrow_value(value: object, pg_type: str) -> object:
    """Convert a Daft/Arrow-decoded Python value for text-mode COPY writes.

    Complex types that map to jsonb/text columns must be JSON-serialized
    before being handed to psycopg.
    """
    import json

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value
    if pg_type in ("jsonb", "json", "text", "varchar"):
        try:
            return json.dumps(value)
        except TypeError:
            return str(value)
    return value


def schema_to_daft(holo_schema: HologresTableSchema) -> list[tuple[str, DataType]]:
    """Convert a HologresTableSchema to (name, DataType) pairs.

    Unmappable columns fall back to string.
    """
    pairs: list[tuple[str, DataType]] = []
    for col in holo_schema.columns:
        dtype = pg_type_to_daft(col.type_name, col.precision, col.scale)
        pairs.append((col.name, dtype if dtype is not None else DataType.string()))
    return pairs


def columns_from_names_and_types(names: Sequence[str], types: Sequence[DataType]) -> list[HologresColumn]:
    """Build HologresColumns from parallel name/type lists (DDL planning helper)."""
    return [HologresColumn(name=n, type_name=daft_type_to_pg(t)) for n, t in zip(names, types)]
