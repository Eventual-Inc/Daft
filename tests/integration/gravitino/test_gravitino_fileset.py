"""Integration tests for Gravitino fileset catalog operations."""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path
from urllib.parse import urlparse

import pytest

import daft
from daft.daft import FileFormat
from daft.filesystem import glob_path_with_stats

from .test_utils import (
    create_catalog,
    create_fileset,
    create_schema,
    delete_catalog,
    delete_fileset,
    delete_schema,
    ensure_metalake,
)


def _resolve_storage_uri(client, gvfs_path: str) -> str:
    parsed = urlparse(gvfs_path)
    if parsed.scheme != "gvfs" or parsed.netloc != "fileset":
        raise ValueError(f"Unsupported gvfs path: {gvfs_path}")

    segments = [segment for segment in parsed.path.split("/") if segment]
    if len(segments) < 3:
        raise ValueError(f"GVFS path must include catalog/schema/fileset: {gvfs_path}")

    catalog, schema, fileset, *rest = segments
    fileset_fqn = f"{catalog}.{schema}.{fileset}"
    fileset_obj = client.load_fileset(fileset_fqn)
    storage_uri = fileset_obj.fileset_info.storage_location.rstrip("/")

    if rest:
        storage_uri = f"{storage_uri}/{'/'.join(rest)}".rstrip("/")

    return storage_uri


def _storage_uri_to_local_path(storage_uri: str) -> Path:
    parsed = urlparse(storage_uri)
    if parsed.scheme != "file":
        raise ValueError(f"Deletion test expects file:// storage, found: {storage_uri}")
    return Path(parsed.path)


@pytest.fixture
def prepared_fileset(local_gravitino_client, gravitino_metalake, tmp_path_factory):
    base_dir = tmp_path_factory.mktemp("gravitino-fileset")
    file_path = base_dir / "sample.parquet"
    sample_data = {"id": [1, 2, 3], "value": ["alpha", "beta", "gamma"]}
    daft.from_pydict(sample_data).write_parquet(file_path)

    catalog_name = f"daft_catalog_{uuid.uuid4().hex[:8]}"
    schema_name = f"daft_schema_{uuid.uuid4().hex[:8]}"
    fileset_name = f"daft_fileset_{uuid.uuid4().hex[:8]}"
    storage_uri = base_dir.as_uri()

    ensure_metalake(local_gravitino_client, gravitino_metalake)
    create_catalog(local_gravitino_client, gravitino_metalake, catalog_name)
    create_schema(local_gravitino_client, gravitino_metalake, catalog_name, schema_name)
    create_fileset(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        schema_name,
        fileset_name,
        storage_uri,
    )

    gvfs_root = f"gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}"

    try:
        yield {
            "gvfs_root": gvfs_root,
            "file_name": file_path.name,
            "data": sample_data,
            "catalog": catalog_name,
            "schema": schema_name,
            "fileset": fileset_name,
            "local_path": base_dir,
            "fileset_fqn": f"{catalog_name}.{schema_name}.{fileset_name}",
        }
    finally:
        delete_fileset(local_gravitino_client, gravitino_metalake, catalog_name, schema_name, fileset_name)
        delete_schema(local_gravitino_client, gravitino_metalake, catalog_name, schema_name)
        delete_catalog(local_gravitino_client, gravitino_metalake, catalog_name)
        shutil.rmtree(base_dir, ignore_errors=True)


@pytest.mark.integration()
def test_read_fileset_over_gvfs(prepared_fileset, gravitino_io_config):
    gvfs_file = f"{prepared_fileset['gvfs_root']}/{prepared_fileset['file_name']}"
    df = daft.read_parquet(gvfs_file, io_config=gravitino_io_config)
    result = df.sort("id").to_pydict()
    assert result == prepared_fileset["data"]


@pytest.mark.integration()
def test_list_files_via_glob(prepared_fileset, gravitino_io_config):
    glob_pattern = f"{prepared_fileset['gvfs_root']}/**/*.parquet"
    file_infos = glob_path_with_stats(glob_pattern, FileFormat.Parquet, gravitino_io_config)

    # write_parquet creates a directory, so we expect files inside sample.parquet/
    expected_prefix = f"{prepared_fileset['gvfs_root']}/{prepared_fileset['file_name']}/"
    assert len(file_infos.file_paths) > 0
    assert all(path.startswith(expected_prefix) for path in file_infos.file_paths)
    assert all(path.endswith(".parquet") for path in file_infos.file_paths)


@pytest.mark.integration()
def test_from_glob_path_reads_files(prepared_fileset, gravitino_io_config):
    glob_pattern = f"{prepared_fileset['gvfs_root']}/**/*.parquet"
    files_df = daft.from_glob_path(glob_pattern, io_config=gravitino_io_config)
    collected = files_df.collect().to_pydict()

    # write_parquet creates a directory, so we expect files inside sample.parquet/
    expected_prefix = f"{prepared_fileset['gvfs_root']}/{prepared_fileset['file_name']}/"
    assert len(collected["path"]) > 0
    assert all(path.startswith(expected_prefix) for path in collected["path"])
    assert all(path.endswith(".parquet") for path in collected["path"])


@pytest.mark.integration()
def test_delete_file_via_gvfs_path(prepared_fileset, local_gravitino_client, gravitino_io_config):
    gvfs_file = f"{prepared_fileset['gvfs_root']}/{prepared_fileset['file_name']}"
    storage_uri = _resolve_storage_uri(local_gravitino_client, gvfs_file)
    local_path = _storage_uri_to_local_path(storage_uri)

    # write_parquet creates a directory, so we need to remove it recursively
    assert local_path.exists()
    shutil.rmtree(local_path)
    assert not local_path.exists()

    # After deletion, globbing should return empty dataframe when collecting
    glob_pattern = f"{prepared_fileset['gvfs_root']}/**/*.parquet"
    files_df = daft.from_glob_path(glob_pattern, io_config=gravitino_io_config)
    assert 0 == len(files_df.collect())

    # Reading the deleted path should also raise an error
    with pytest.raises(Exception):
        daft.read_parquet(gvfs_file, io_config=gravitino_io_config).collect()
