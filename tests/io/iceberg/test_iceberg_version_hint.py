from __future__ import annotations

import json
import os

import pytest

pyiceberg = pytest.importorskip("pyiceberg")

import daft
from daft.io.iceberg._iceberg import _resolve_metadata_location


def _create_iceberg_table(tmpdir, version=1, schema_fields=None):
    """Helper to create a minimal Iceberg table directory structure."""
    metadata_dir = os.path.join(str(tmpdir), "metadata")
    os.makedirs(metadata_dir, exist_ok=True)

    if schema_fields is None:
        schema_fields = [{"id": 1, "name": "x", "required": False, "type": "long"}]

    metadata = {
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000000",
        "location": str(tmpdir),
        "last-sequence-number": 0,
        "last-updated-ms": 0,
        "last-column-id": max(f["id"] for f in schema_fields),
        "current-schema-id": 0,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": schema_fields}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 0,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
    }

    metadata_file = os.path.join(metadata_dir, f"v{version}.metadata.json")
    with open(metadata_file, "w") as f:
        json.dump(metadata, f)

    return metadata_dir, metadata_file


class TestResolveMetadataLocation:
    def test_direct_metadata_path_returned_as_is(self):
        path = "/path/to/v1.metadata.json"
        assert _resolve_metadata_location(path) == path

    def test_direct_metadata_path_with_scheme(self):
        path = "s3://bucket/path/to/v1.metadata.json"
        assert _resolve_metadata_location(path) == path

    def test_resolves_numeric_version_hint(self, tmp_path):
        metadata_dir, _ = _create_iceberg_table(tmp_path, version=3)
        with open(os.path.join(metadata_dir, "version-hint.text"), "w") as f:
            f.write("3")

        result = _resolve_metadata_location(str(tmp_path))
        assert result == os.path.join(metadata_dir, "v3.metadata.json")

    def test_resolves_full_filename_version_hint(self, tmp_path):
        metadata_dir, _ = _create_iceberg_table(tmp_path, version=2)
        with open(os.path.join(metadata_dir, "version-hint.text"), "w") as f:
            f.write("v2.metadata.json")

        result = _resolve_metadata_location(str(tmp_path))
        assert result == os.path.join(metadata_dir, "v2.metadata.json")

    def test_resolves_version_hint_with_whitespace(self, tmp_path):
        metadata_dir, _ = _create_iceberg_table(tmp_path, version=1)
        with open(os.path.join(metadata_dir, "version-hint.text"), "w") as f:
            f.write("1\n")

        result = _resolve_metadata_location(str(tmp_path))
        assert result == os.path.join(metadata_dir, "v1.metadata.json")

    def test_missing_version_hint_returns_original(self, tmp_path):
        # No version-hint.text file — should return original location
        result = _resolve_metadata_location(str(tmp_path))
        assert result == str(tmp_path)


class TestReadIcebergVersionHint:
    def test_read_iceberg_with_table_location(self, tmp_path):
        metadata_dir, _ = _create_iceberg_table(tmp_path)
        with open(os.path.join(metadata_dir, "version-hint.text"), "w") as f:
            f.write("1")

        df = daft.read_iceberg(str(tmp_path))
        assert "x" in df.schema().column_names()
        result = df.collect()
        assert len(result) == 0  # empty table, no snapshots

    def test_read_iceberg_with_direct_metadata_path(self, tmp_path):
        _, metadata_file = _create_iceberg_table(tmp_path)

        df = daft.read_iceberg(metadata_file)
        assert "x" in df.schema().column_names()

    def test_read_iceberg_sql_with_table_location(self, tmp_path):
        metadata_dir, _ = _create_iceberg_table(tmp_path)
        with open(os.path.join(metadata_dir, "version-hint.text"), "w") as f:
            f.write("1")

        df = daft.sql(f"SELECT * FROM read_iceberg('{tmp_path}')")
        assert "x" in df.schema().column_names()
