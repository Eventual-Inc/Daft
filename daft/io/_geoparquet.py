"""Geo metadata helpers for the GeoParquet/Delta path (Python mirror of daft-parquet's geo_metadata)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from daft.datatype import DataType
from daft.schema import Schema

if TYPE_CHECKING:
    import pyarrow as pa

_GEOPARQUET_VERSION = "1.1.0"
GEO_METADATA_KEY = "geo"  # parquet footer key
GEO_FIELD_METADATA_KEY = "daft.geo"  # Arrow field metadata key (persisted via delta-rs field metadata)


def build_geo_metadata(
    schema: Schema,
    crs: str | None = None,
    only_columns: list[str] | None = None,
) -> str | None:
    """Build a GeoParquet 1.1.0 metadata JSON string for Geometry columns in *schema*.

    Returns ``None`` when no Geometry columns are found (or all are excluded by
    *only_columns*).  The returned JSON shape exactly mirrors the Rust
    ``build_geo_metadata`` in ``daft-parquet`` so that a Task-5 consistency test
    can assert Python JSON == Rust footer JSON::

        {
            "version": "1.1.0",
            "primary_column": "<first geometry column>",
            "columns": {"<col>": {"encoding": "WKB", "geometry_types": []}},
        }

    Args:
        schema: Daft schema to inspect.
        crs: Optional CRS string to embed in each column entry.
        only_columns: If given, restrict to this subset of column names.
    """
    geom_dtype = DataType.geometry()
    cols = [f.name for f in schema if f.dtype == geom_dtype and (only_columns is None or f.name in only_columns)]
    if not cols:
        return None
    col_meta: dict[str, object] = {"encoding": "WKB", "geometry_types": []}
    if crs is not None:
        col_meta["crs"] = crs
    return json.dumps(
        {
            "version": _GEOPARQUET_VERSION,
            "primary_column": cols[0],
            "columns": {name: dict(col_meta) for name in cols},
        }
    )


def detect_geo_columns(geo_json: str, schema: Schema) -> list[str]:
    """Return column names that should be re-typed to ``DataType.geometry()``.

    Parses *geo_json* (the value of the ``daft.geo`` Delta field metadata, or
    the Parquet footer ``"geo"`` metadata) and returns column names whose:
    - encoding is ``"WKB"`` (case-insensitive), and
    - current dtype in *schema* is ``Binary`` or ``Geometry`` (both are WKB-
      compatible).

    Returns an empty list on any parse error (lenient).
    """
    try:
        meta = json.loads(geo_json)
        columns = meta["columns"]
    except (ValueError, KeyError, TypeError):
        return []
    binary_like = {DataType.binary(), DataType.geometry()}
    names = {f.name: f.dtype for f in schema}
    return [
        name
        for name, c in columns.items()
        if isinstance(c, dict) and str(c.get("encoding", "")).upper() == "WKB" and names.get(name) in binary_like
    ]


def attach_geo_field_metadata(schema: pa.Schema, geo_json: str) -> pa.Schema:
    """Return a copy of *schema* with geometry columns annotated with *geo_json*.

    For each column named in the ``"columns"`` dict of *geo_json*, the
    corresponding PyArrow field gets ``{b"daft.geo": geo_json.encode()}``
    added to its field metadata.  This survives the delta-rs 1.6 write→read
    round-trip through the normal ``write_deltalake`` API and is remote-safe.

    Non-geometry fields are left untouched.  Returns *schema* unchanged if
    *geo_json* cannot be parsed or no matching fields exist.
    """
    import pyarrow as pa

    try:
        meta = json.loads(geo_json)
        geo_col_names: set[str] = set(meta.get("columns", {}).keys())
    except (ValueError, KeyError, TypeError):
        return schema

    if not geo_col_names:
        return schema

    geo_json_bytes = geo_json.encode()
    new_fields = []
    for i in range(len(schema)):
        field = schema.field(i)
        if field.name in geo_col_names:
            existing_meta = dict(field.metadata or {})
            existing_meta[GEO_FIELD_METADATA_KEY.encode()] = geo_json_bytes
            field = field.with_metadata(existing_meta)
        new_fields.append(field)

    return pa.schema(new_fields, metadata=schema.metadata)
