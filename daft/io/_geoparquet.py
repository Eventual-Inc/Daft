"""Geo metadata helpers for the GeoParquet/Delta path (Python mirror of daft-parquet's geo_metadata)."""
from __future__ import annotations

import json
import time

from daft.datatype import DataType
from daft.schema import Schema

_GEOPARQUET_VERSION = "1.1.0"
GEO_METADATA_KEY = "geo"  # parquet footer key
GEO_DELTA_PROPERTY = "daft.geo"  # delta table-configuration key


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
            "columns": {
                "<col>": {"encoding": "WKB", "geometry_types": []}
            }
        }

    Args:
        schema: Daft schema to inspect.
        crs: Optional CRS string to embed in each column entry.
        only_columns: If given, restrict to this subset of column names.
    """
    geom_dtype = DataType.geometry()
    cols = [
        f.name
        for f in schema
        if f.dtype == geom_dtype and (only_columns is None or f.name in only_columns)
    ]
    if not cols:
        return None
    col_meta: dict = {"encoding": "WKB", "geometry_types": []}
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

    Parses *geo_json* (value of the ``daft.geo`` table property) and returns
    column names whose:
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
        if isinstance(c, dict)
        and str(c.get("encoding", "")).upper() == "WKB"
        and names.get(name) in binary_like
    ]


def _write_geo_metadata_to_delta_log(table_uri: str, geo_json: str) -> None:
    """Append a metadata-only Delta commit that stores *geo_json* in the table configuration.

    The delta-kernel (deltalake >= 1.0.0) validates known table properties at
    write time and rejects unknown keys like ``daft.geo``.  The workaround is to
    write the commit entry directly into the ``_delta_log`` directory, bypassing
    kernel validation.  The delta log *reader* happily returns arbitrary string
    values from ``configuration``.

    This function:
    1. Opens the existing DeltaTable to read current metadata (id, schema, etc.).
    2. Writes a new ``<version+1>`` JSON log file containing a ``metaData`` block
       with ``configuration = {"daft.geo": geo_json}``.

    Thread/process safety: this is a best-effort single-writer operation; it is
    the caller's responsibility to ensure no concurrent writers are racing on the
    same table version.  For the round-trip use-case (fresh write then annotate)
    this is safe.
    """
    import os

    from deltalake import DeltaTable

    try:
        t = DeltaTable(table_uri)
    except Exception:
        # Table can't be opened — skip silently so the primary write is not
        # rolled back.
        return

    meta = t.metadata()
    schema_str = t.schema().to_json()

    log_path = os.path.join(table_uri, "_delta_log")
    next_version = t.version() + 1
    log_file = os.path.join(log_path, f"{next_version:020d}.json")

    commit_info = json.dumps(
        {
            "commitInfo": {
                "timestamp": int(time.time() * 1000),
                "operation": "SET TBLPROPERTIES",
                "operationParameters": {"properties": json.dumps({GEO_DELTA_PROPERTY: geo_json})},
                "engineInfo": "daft",
            }
        }
    )
    metadata_entry = json.dumps(
        {
            "metaData": {
                "id": meta.id,
                "name": meta.name,
                "description": meta.description,
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_str,
                "partitionColumns": meta.partition_columns,
                "createdTime": int(time.time() * 1000),
                "configuration": {GEO_DELTA_PROPERTY: geo_json},
            }
        }
    )

    with open(log_file, "w") as fh:
        fh.write(commit_info + "\n")
        fh.write(metadata_entry + "\n")
