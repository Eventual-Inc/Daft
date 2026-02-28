"""Integration tests for Gravitino fileset with S3 storage.

Note: Most tests require a Gravitino instance configured with S3 filesystem support.
The default Gravitino docker image only supports file:// and hdfs:// schemes.
"""

from __future__ import annotations

import os
import uuid

import pytest
import s3fs

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
    update_catalog,
)

# Note: S3 support is configured via docker-compose entrypoint
# which downloads the gravitino-aws-bundle and enables S3 filesystem provider


@pytest.fixture
def s3_bucket(gravitino_minio_io_config):
    """Creates a MinIO bucket for testing."""
    bucket_name = f"gravitino-test-{uuid.uuid4().hex[:8]}"

    fs = s3fs.S3FileSystem(
        key=gravitino_minio_io_config.s3.key_id,
        password=gravitino_minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": gravitino_minio_io_config.s3.endpoint_url},
    )

    # Create bucket if it doesn't exist
    # Note: s3fs requires explicit bucket creation
    try:
        if not fs.exists(bucket_name):
            # Use makedir with create_parents=False for bucket creation
            fs.makedir(bucket_name, exist_ok=True)

        # Verify bucket was created
        assert fs.exists(bucket_name), f"Failed to create bucket: {bucket_name}"
    except Exception as e:
        raise RuntimeError(f"Failed to create S3 bucket {bucket_name}: {e}") from e

    try:
        yield bucket_name
    finally:
        # Clean up bucket and all contents
        try:
            if fs.exists(bucket_name):
                fs.rm(bucket_name, recursive=True)
        except Exception:
            pass  # Best effort cleanup


@pytest.fixture
def prepared_s3_fileset(
    local_gravitino_client,
    gravitino_metalake,
    gravitino_minio_io_config,
    s3_bucket,
):
    """Creates a Gravitino fileset backed by MinIO S3 storage with test data."""
    # Write test data to S3 in a subfolder
    sample_data = {"id": [1, 2, 3], "value": ["alpha", "beta", "gamma"]}
    df = daft.from_pydict(sample_data)

    # Use a subfolder to organize test data
    test_subfolder = f"test_data_{uuid.uuid4().hex[:8]}"
    s3_path = f"s3://{s3_bucket}/{test_subfolder}/test_data.parquet"
    df.write_parquet(s3_path, io_config=gravitino_minio_io_config)

    # Verify the file was written by listing the bucket
    fs = s3fs.S3FileSystem(
        key=gravitino_minio_io_config.s3.key_id,
        password=gravitino_minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": gravitino_minio_io_config.s3.endpoint_url},
    )

    # List files in the subfolder to get actual filenames (use find for recursive listing)
    written_files = fs.find(f"{s3_bucket}/{test_subfolder}")
    assert len(written_files) > 0, f"No files written to s3://{s3_bucket}/{test_subfolder}"

    # Get the actual parquet files (write_parquet creates a directory structure)
    parquet_files = [f for f in written_files if f.endswith(".parquet")]
    assert len(parquet_files) > 0, f"No parquet files found in s3://{s3_bucket}/{test_subfolder}"

    # Create Gravitino catalog, schema, and fileset
    catalog_name = f"s3_catalog_{uuid.uuid4().hex[:8]}"
    schema_name = f"s3_schema_{uuid.uuid4().hex[:8]}"
    fileset_name = f"s3_fileset_{uuid.uuid4().hex[:8]}"
    storage_uri = f"s3a://{s3_bucket}/{test_subfolder}/"

    ensure_metalake(local_gravitino_client, gravitino_metalake)
    create_catalog(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        properties={
            "filesystem-providers": "s3",
            "s3-endpoint": "http://daft-gravitino-minio:9000",
            "s3-access-key-id": "minioadmin",
            "s3-secret-access-key": "minioadmin",
        },
    )
    create_schema(local_gravitino_client, gravitino_metalake, catalog_name, schema_name)
    create_fileset(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        schema_name,
        fileset_name,
        storage_uri,
    )

    # Update catalog s3-endpoint to point to localhost for tests running outside Docker
    update_catalog(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        updates=[{"@type": "setProperty", "property": "s3-endpoint", "value": "http://127.0.0.1:9001"}],
    )

    gvfs_root = f"gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}"

    try:
        yield {
            "gvfs_root": gvfs_root,
            "file_name": "test_data.parquet",
            "data": sample_data,
            "catalog": catalog_name,
            "schema": schema_name,
            "fileset": fileset_name,
            "bucket": s3_bucket,
            "s3_path": s3_path,
            "fileset_fqn": f"{catalog_name}.{schema_name}.{fileset_name}",
            "test_subfolder": test_subfolder,
            "written_files": parquet_files,
        }
    finally:
        delete_fileset(local_gravitino_client, gravitino_metalake, catalog_name, schema_name, fileset_name)
        delete_schema(local_gravitino_client, gravitino_metalake, catalog_name, schema_name)
        delete_catalog(local_gravitino_client, gravitino_metalake, catalog_name)


@pytest.mark.integration()
def test_read_s3_fileset_over_gvfs(prepared_s3_fileset, gravitino_io_config):
    """Test reading parquet files from S3-backed fileset via gvfs:// protocol."""
    gvfs_file = f"{prepared_s3_fileset['gvfs_root']}/{prepared_s3_fileset['file_name']}"
    df = daft.read_parquet(gvfs_file, io_config=gravitino_io_config)
    result = df.sort("id").to_pydict()
    assert result == prepared_s3_fileset["data"]


@pytest.mark.integration()
def test_list_s3_files_via_glob(prepared_s3_fileset, gravitino_io_config):
    """Test listing files in S3-backed fileset using glob patterns."""
    glob_pattern = f"{prepared_s3_fileset['gvfs_root']}/**/*.parquet"
    file_infos = glob_path_with_stats(glob_pattern, FileFormat.Parquet, gravitino_io_config)

    # write_parquet creates a directory, so we expect files inside test_data.parquet/
    expected_prefix = f"{prepared_s3_fileset['gvfs_root']}/{prepared_s3_fileset['file_name']}/"

    assert len(file_infos.file_paths) > 0
    assert all(path.startswith(expected_prefix) for path in file_infos.file_paths)
    assert all(path.endswith(".parquet") for path in file_infos.file_paths)


@pytest.mark.integration()
def test_from_glob_path_s3_reads_files(prepared_s3_fileset, gravitino_io_config):
    """Test using from_glob_path to read files from S3-backed fileset."""
    glob_pattern = f"{prepared_s3_fileset['gvfs_root']}/**/*.parquet"
    files_df = daft.from_glob_path(glob_pattern, io_config=gravitino_io_config)
    collected = files_df.collect().to_pydict()

    # write_parquet creates a directory, so we expect files inside test_data.parquet/
    expected_prefix = f"{prepared_s3_fileset['gvfs_root']}/{prepared_s3_fileset['file_name']}/"

    assert len(collected["path"]) > 0
    assert all(path.startswith(expected_prefix) for path in collected["path"])
    assert all(path.endswith(".parquet") for path in collected["path"])


@pytest.mark.integration()
def test_s3_fileset_partitioned_data(
    local_gravitino_client,
    gravitino_metalake,
    gravitino_minio_io_config,
    gravitino_io_config,
    s3_bucket,
):
    """Test S3 fileset with partitioned parquet data."""
    # Create partitioned data in a subfolder
    sample_data = {
        "id": [1, 2, 3, 4, 5, 6],
        "category": ["A", "A", "B", "B", "C", "C"],
        "value": [10, 20, 30, 40, 50, 60],
    }
    df = daft.from_pydict(sample_data)

    test_subfolder = f"partitioned_data_{uuid.uuid4().hex[:8]}"
    s3_path = f"s3://{s3_bucket}/{test_subfolder}"
    df.write_parquet(s3_path, partition_cols=["category"], io_config=gravitino_minio_io_config)

    # Verify the file was written by listing the bucket
    fs = s3fs.S3FileSystem(
        key=gravitino_minio_io_config.s3.key_id,
        password=gravitino_minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": gravitino_minio_io_config.s3.endpoint_url},
    )

    written_files = fs.find(f"{s3_bucket}/{test_subfolder}")
    assert len(written_files) > 0, f"No files written to s3://{s3_bucket}/{test_subfolder}"

    parquet_files = [f for f in written_files if f.endswith(".parquet")]
    assert len(parquet_files) > 0, f"No parquet files found in s3://{s3_bucket}/{test_subfolder}"

    # Create fileset
    catalog_name = f"part_cat_{uuid.uuid4().hex[:8]}"
    schema_name = f"part_schema_{uuid.uuid4().hex[:8]}"
    fileset_name = f"part_fileset_{uuid.uuid4().hex[:8]}"
    storage_uri = f"s3a://{s3_bucket}/{test_subfolder}/"

    ensure_metalake(local_gravitino_client, gravitino_metalake)
    create_catalog(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        properties={
            "filesystem-providers": "s3",
            "s3-endpoint": "http://daft-gravitino-minio:9000",
            "s3-access-key-id": "minioadmin",
            "s3-secret-access-key": "minioadmin",
        },
    )
    create_schema(local_gravitino_client, gravitino_metalake, catalog_name, schema_name)
    create_fileset(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        schema_name,
        fileset_name,
        storage_uri,
    )

    # Update catalog s3-endpoint to point to localhost for tests running outside Docker
    update_catalog(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        updates=[{"@type": "setProperty", "property": "s3-endpoint", "value": "http://127.0.0.1:9001"}],
    )

    try:
        gvfs_root = f"gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}"
        glob_pattern = f"{gvfs_root}/**/*.parquet"

        # Read all partitions
        read_df = daft.read_parquet(glob_pattern, io_config=gravitino_io_config)
        result = read_df.sort("id").to_pydict()

        assert len(result["id"]) == 6
        assert result["id"] == [1, 2, 3, 4, 5, 6]
        assert result["category"] == ["A", "A", "B", "B", "C", "C"]

    finally:
        delete_fileset(local_gravitino_client, gravitino_metalake, catalog_name, schema_name, fileset_name)
        delete_schema(local_gravitino_client, gravitino_metalake, catalog_name, schema_name)
        delete_catalog(local_gravitino_client, gravitino_metalake, catalog_name)


@pytest.mark.integration()
def test_s3_fileset_missing_file(prepared_s3_fileset, gravitino_io_config):
    """Test error handling when trying to read non-existent file from S3 fileset."""
    gvfs_file = f"{prepared_s3_fileset['gvfs_root']}/nonexistent.parquet"

    with pytest.raises(Exception):
        daft.read_parquet(gvfs_file, io_config=gravitino_io_config).collect()


@pytest.mark.integration()
def test_s3_fileset_empty_glob(
    local_gravitino_client,
    gravitino_metalake,
    gravitino_io_config,
    s3_bucket,
):
    """Test globbing an empty S3 fileset directory."""
    # Create fileset pointing to empty directory
    catalog_name = f"empty_cat_{uuid.uuid4().hex[:8]}"
    schema_name = f"empty_schema_{uuid.uuid4().hex[:8]}"
    fileset_name = f"empty_fileset_{uuid.uuid4().hex[:8]}"
    storage_uri = f"s3a://{s3_bucket}/empty_dir/"

    ensure_metalake(local_gravitino_client, gravitino_metalake)
    create_catalog(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        properties={
            "filesystem-providers": "s3",
            "s3-endpoint": "http://daft-gravitino-minio:9000",
            "s3-access-key-id": "minioadmin",
            "s3-secret-access-key": "minioadmin",
        },
    )
    create_schema(local_gravitino_client, gravitino_metalake, catalog_name, schema_name)
    create_fileset(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        schema_name,
        fileset_name,
        storage_uri,
    )

    # Update catalog s3-endpoint to point to localhost for tests running outside Docker
    update_catalog(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        updates=[{"@type": "setProperty", "property": "s3-endpoint", "value": "http://127.0.0.1:9001"}],
    )

    try:
        gvfs_root = f"gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}"
        glob_pattern = f"{gvfs_root}/**/*.parquet"

        # Globbing empty directory should return empty dataframe when collecting
        files_df = daft.from_glob_path(glob_pattern, io_config=gravitino_io_config)
        assert 0 == len(files_df.collect())

    finally:
        delete_fileset(local_gravitino_client, gravitino_metalake, catalog_name, schema_name, fileset_name)
        delete_schema(local_gravitino_client, gravitino_metalake, catalog_name, schema_name)
        delete_catalog(local_gravitino_client, gravitino_metalake, catalog_name)


# Tests that work with externally configured S3 filesets (via environment variables)
# These don't require Gravitino to be configured with S3 support during catalog creation


@pytest.mark.integration()
@pytest.mark.skipif(
    "GRAVITINO_TEST_DIR" not in os.environ or not os.environ["GRAVITINO_TEST_DIR"].startswith("gvfs://"),
    reason="Requires GRAVITINO_TEST_DIR environment variable pointing to an existing S3-backed fileset",
)
def test_read_existing_s3_fileset(gravitino_sample_dir, gravitino_io_config):
    """Test reading from an existing S3-backed fileset via environment variable.

    This test works with any Gravitino deployment that has S3 filesets already configured.
    Set GRAVITINO_TEST_DIR to point to an S3-backed fileset, e.g.:
    export GRAVITINO_TEST_DIR="gvfs://fileset/my_catalog/my_schema/my_s3_fileset/"
    """
    # Try to list files in the S3-backed fileset
    glob_pattern = f"{gravitino_sample_dir}**/*.parquet"

    try:
        files_df = daft.from_glob_path(glob_pattern, io_config=gravitino_io_config)
        collected = files_df.collect().to_pydict()

        # Should have at least some files
        assert len(collected["path"]) > 0
        # All paths should start with the gvfs prefix
        assert all(path.startswith(gravitino_sample_dir) for path in collected["path"])

    except FileNotFoundError:
        pytest.skip("No files found in GRAVITINO_TEST_DIR - fileset may be empty")


@pytest.fixture
def empty_s3_fileset(
    local_gravitino_client,
    gravitino_metalake,
    s3_bucket,
):
    """Creates an empty Gravitino fileset for write tests."""
    test_subfolder = f"write_test_{uuid.uuid4().hex[:8]}"
    catalog_name = f"write_cat_{uuid.uuid4().hex[:8]}"
    schema_name = f"write_schema_{uuid.uuid4().hex[:8]}"
    fileset_name = f"write_fileset_{uuid.uuid4().hex[:8]}"
    storage_uri = f"s3a://{s3_bucket}/{test_subfolder}/"

    ensure_metalake(local_gravitino_client, gravitino_metalake)
    create_catalog(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        properties={
            "filesystem-providers": "s3",
            "s3-endpoint": "http://daft-gravitino-minio:9000",
            "s3-access-key-id": "minioadmin",
            "s3-secret-access-key": "minioadmin",
        },
    )
    create_schema(local_gravitino_client, gravitino_metalake, catalog_name, schema_name)
    create_fileset(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        schema_name,
        fileset_name,
        storage_uri,
    )

    # Update catalog s3-endpoint to point to localhost for tests running outside Docker
    update_catalog(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        updates=[{"@type": "setProperty", "property": "s3-endpoint", "value": "http://127.0.0.1:9001"}],
    )

    gvfs_root = f"gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}"

    try:
        yield {
            "gvfs_root": gvfs_root,
            "catalog": catalog_name,
            "schema": schema_name,
            "fileset": fileset_name,
            "bucket": s3_bucket,
            "test_subfolder": test_subfolder,
        }
    finally:
        delete_fileset(local_gravitino_client, gravitino_metalake, catalog_name, schema_name, fileset_name)
        delete_schema(local_gravitino_client, gravitino_metalake, catalog_name, schema_name)
        delete_catalog(local_gravitino_client, gravitino_metalake, catalog_name)


@pytest.mark.integration()
def test_write_parquet_to_gvfs(empty_s3_fileset, gravitino_io_config, gravitino_minio_io_config):
    """Test writing parquet files to gvfs:// path."""
    sample_data = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    df = daft.from_pydict(sample_data)

    gvfs_path = f"{empty_s3_fileset['gvfs_root']}/test_data.parquet"
    df.write_parquet(gvfs_path, io_config=gravitino_io_config)

    # Verify files were written to S3 by checking with s3fs
    fs = s3fs.S3FileSystem(
        key=gravitino_minio_io_config.s3.key_id,
        password=gravitino_minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": gravitino_minio_io_config.s3.endpoint_url},
    )

    # Check that files exist in the expected location
    written_files = fs.find(f"{empty_s3_fileset['bucket']}/{empty_s3_fileset['test_subfolder']}/test_data.parquet")
    parquet_files = [f for f in written_files if f.endswith(".parquet")]

    assert len(parquet_files) > 0, (
        f"No parquet files found in s3://{empty_s3_fileset['bucket']}/{empty_s3_fileset['test_subfolder']}/test_data.parquet"
    )

    # Verify by reading from underlying S3 path
    s3_path = f"s3://{empty_s3_fileset['bucket']}/{empty_s3_fileset['test_subfolder']}/test_data.parquet/*.parquet"
    read_df = daft.read_parquet(s3_path, io_config=gravitino_minio_io_config)
    result = read_df.sort("id").to_pydict()

    assert result["id"] == sample_data["id"]
    assert result["name"] == sample_data["name"]
    assert result["age"] == sample_data["age"]


@pytest.mark.integration()
def test_write_csv_to_gvfs(empty_s3_fileset, gravitino_io_config, gravitino_minio_io_config):
    """Test writing CSV files to gvfs:// path."""
    sample_data = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    df = daft.from_pydict(sample_data)

    gvfs_path = f"{empty_s3_fileset['gvfs_root']}/test_data.csv"
    df.write_csv(gvfs_path, io_config=gravitino_io_config)

    # Verify files were written to S3 by checking with s3fs
    fs = s3fs.S3FileSystem(
        key=gravitino_minio_io_config.s3.key_id,
        password=gravitino_minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": gravitino_minio_io_config.s3.endpoint_url},
    )

    # Check that files exist in the expected location
    written_files = fs.find(f"{empty_s3_fileset['bucket']}/{empty_s3_fileset['test_subfolder']}/test_data.csv")
    csv_files = [f for f in written_files if f.endswith(".csv")]

    assert len(csv_files) > 0, (
        f"No CSV files found in s3://{empty_s3_fileset['bucket']}/{empty_s3_fileset['test_subfolder']}/test_data.csv"
    )

    # Verify by reading from underlying S3 path
    s3_path = f"s3://{empty_s3_fileset['bucket']}/{empty_s3_fileset['test_subfolder']}/test_data.csv/*.csv"
    read_df = daft.read_csv(s3_path, io_config=gravitino_minio_io_config)
    result = read_df.sort("id").to_pydict()

    assert result["id"] == sample_data["id"]
    assert result["name"] == sample_data["name"]
    assert result["age"] == sample_data["age"]


@pytest.mark.integration()
def test_write_json_to_gvfs(empty_s3_fileset, gravitino_io_config, gravitino_minio_io_config):
    """Test writing JSON files to gvfs:// path."""
    sample_data = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    df = daft.from_pydict(sample_data)

    gvfs_path = f"{empty_s3_fileset['gvfs_root']}/test_data.json"
    df.write_json(gvfs_path, io_config=gravitino_io_config)

    # Verify files were written to S3 by checking with s3fs
    fs = s3fs.S3FileSystem(
        key=gravitino_minio_io_config.s3.key_id,
        password=gravitino_minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": gravitino_minio_io_config.s3.endpoint_url},
    )

    # Check that files exist in the expected location
    written_files = fs.find(f"{empty_s3_fileset['bucket']}/{empty_s3_fileset['test_subfolder']}/test_data.json")
    json_files = [f for f in written_files if f.endswith(".json")]

    assert len(json_files) > 0, (
        f"No JSON files found in s3://{empty_s3_fileset['bucket']}/{empty_s3_fileset['test_subfolder']}/test_data.json"
    )

    # Verify by reading from underlying S3 path
    s3_path = f"s3://{empty_s3_fileset['bucket']}/{empty_s3_fileset['test_subfolder']}/test_data.json/*.json"
    read_df = daft.read_json(s3_path, io_config=gravitino_minio_io_config)
    result = read_df.sort("id").to_pydict()

    assert result["id"] == sample_data["id"]
    assert result["name"] == sample_data["name"]
    assert result["age"] == sample_data["age"]


@pytest.mark.integration()
def test_write_and_read_roundtrip_gvfs(empty_s3_fileset, gravitino_io_config):
    """Test writing to gvfs:// and reading back via gvfs:// for a complete roundtrip."""
    sample_data = {"id": [1, 2, 3], "value": ["alpha", "beta", "gamma"], "score": [1.1, 2.2, 3.3]}
    df = daft.from_pydict(sample_data)

    gvfs_path = f"{empty_s3_fileset['gvfs_root']}/roundtrip_test.parquet"

    # Write via gvfs://
    df.write_parquet(gvfs_path, io_config=gravitino_io_config)

    # Read back via gvfs://
    read_df = daft.read_parquet(gvfs_path, io_config=gravitino_io_config)
    result = read_df.sort("id").to_pydict()

    assert result["id"] == sample_data["id"]
    assert result["value"] == sample_data["value"]
    assert result["score"] == sample_data["score"]


@pytest.mark.integration()
@pytest.mark.skipif(
    "GRAVITINO_TEST_FILE" not in os.environ or not os.environ["GRAVITINO_TEST_FILE"].startswith("gvfs://"),
    reason="Requires GRAVITINO_TEST_FILE environment variable pointing to an existing S3-backed file",
)
def test_read_specific_s3_file(gravitino_sample_file, gravitino_io_config):
    """Test reading a specific file from an S3-backed fileset.

    This test works with any Gravitino deployment that has S3 filesets already configured.
    Set GRAVITINO_TEST_FILE to point to a specific file in an S3-backed fileset, e.g.:
    export GRAVITINO_TEST_FILE="gvfs://fileset/my_catalog/my_schema/my_s3_fileset/data.parquet"
    """
    # Try to read the specific file
    df = daft.read_parquet(gravitino_sample_file, io_config=gravitino_io_config)
    result = df.collect()

    # Should successfully read and have some data
    assert len(result) > 0
