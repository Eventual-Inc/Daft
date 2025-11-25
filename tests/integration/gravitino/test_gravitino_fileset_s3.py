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
    print("[DEBUG] Updating catalog s3-endpoint from Docker network to localhost")
    update_catalog(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        updates=[{"@type": "setProperty", "property": "s3-endpoint", "value": "http://127.0.0.1:9000"}],
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
    print(f"\n[DEBUG] Step 1: Fixture prepared with data: {prepared_s3_fileset}")
    print(f"[DEBUG] Step 2: GVFS root: {prepared_s3_fileset['gvfs_root']}")
    print(f"[DEBUG] Step 3: File name: {prepared_s3_fileset['file_name']}")
    print(f"[DEBUG] Step 4: Written files in S3: {prepared_s3_fileset['written_files']}")

    gvfs_file = f"{prepared_s3_fileset['gvfs_root']}/{prepared_s3_fileset['file_name']}"
    print(f"[DEBUG] Step 5: Reading from GVFS path: {gvfs_file}")

    df = daft.read_parquet(gvfs_file, io_config=gravitino_io_config)
    print("[DEBUG] Step 6: Successfully created dataframe, now sorting by 'id'")

    result = df.sort("id").to_pydict()
    print(f"[DEBUG] Step 7: Sorted result: {result}")
    print(f"[DEBUG] Step 8: Expected data: {prepared_s3_fileset['data']}")

    assert result == prepared_s3_fileset["data"]
    print("[DEBUG] Step 9: Assertion passed - test completed successfully")


@pytest.mark.integration()
def test_list_s3_files_via_glob(prepared_s3_fileset, gravitino_io_config):
    """Test listing files in S3-backed fileset using glob patterns."""
    print(f"\n[DEBUG] Step 1: Starting glob test with GVFS root: {prepared_s3_fileset['gvfs_root']}")

    glob_pattern = f"{prepared_s3_fileset['gvfs_root']}/**/*.parquet"
    print(f"[DEBUG] Step 2: Glob pattern: {glob_pattern}")

    file_infos = glob_path_with_stats(glob_pattern, FileFormat.Parquet, gravitino_io_config)
    print(f"[DEBUG] Step 3: Found {len(file_infos.file_paths)} files")
    print(f"[DEBUG] Step 4: File paths: {file_infos.file_paths}")

    # write_parquet creates a directory, so we expect files inside test_data.parquet/
    expected_prefix = f"{prepared_s3_fileset['gvfs_root']}/{prepared_s3_fileset['file_name']}/"
    print(f"[DEBUG] Step 5: Expected prefix: {expected_prefix}")

    assert len(file_infos.file_paths) > 0
    print("[DEBUG] Step 6: Verified file count > 0")

    assert all(path.startswith(expected_prefix) for path in file_infos.file_paths)
    print("[DEBUG] Step 7: Verified all paths start with expected prefix")

    assert all(path.endswith(".parquet") for path in file_infos.file_paths)
    print("[DEBUG] Step 8: Verified all paths end with .parquet - test completed successfully")


@pytest.mark.integration()
def test_from_glob_path_s3_reads_files(prepared_s3_fileset, gravitino_io_config):
    """Test using from_glob_path to read files from S3-backed fileset."""
    print(f"\n[DEBUG] Step 1: Starting from_glob_path test with GVFS root: {prepared_s3_fileset['gvfs_root']}")

    glob_pattern = f"{prepared_s3_fileset['gvfs_root']}/**/*.parquet"
    print(f"[DEBUG] Step 2: Glob pattern: {glob_pattern}")

    files_df = daft.from_glob_path(glob_pattern, io_config=gravitino_io_config)
    print("[DEBUG] Step 3: Created dataframe from glob path")

    collected = files_df.collect().to_pydict()
    print(f"[DEBUG] Step 4: Collected {len(collected['path'])} paths")
    print(f"[DEBUG] Step 5: Paths: {collected['path']}")

    # write_parquet creates a directory, so we expect files inside test_data.parquet/
    expected_prefix = f"{prepared_s3_fileset['gvfs_root']}/{prepared_s3_fileset['file_name']}/"
    print(f"[DEBUG] Step 6: Expected prefix: {expected_prefix}")

    assert len(collected["path"]) > 0
    print("[DEBUG] Step 7: Verified path count > 0")

    assert all(path.startswith(expected_prefix) for path in collected["path"])
    print("[DEBUG] Step 8: Verified all paths start with expected prefix")

    assert all(path.endswith(".parquet") for path in collected["path"])
    print("[DEBUG] Step 9: Verified all paths end with .parquet - test completed successfully")


@pytest.mark.integration()
def test_s3_fileset_partitioned_data(
    local_gravitino_client,
    gravitino_metalake,
    gravitino_minio_io_config,
    gravitino_io_config,
    s3_bucket,
):
    """Test S3 fileset with partitioned parquet data."""
    print(f"\n[DEBUG] Step 1: Starting partitioned data test with bucket: {s3_bucket}")

    # Create partitioned data in a subfolder
    sample_data = {
        "id": [1, 2, 3, 4, 5, 6],
        "category": ["A", "A", "B", "B", "C", "C"],
        "value": [10, 20, 30, 40, 50, 60],
    }
    df = daft.from_pydict(sample_data)
    print("[DEBUG] Step 2: Created dataframe with partitioned data")

    test_subfolder = f"partitioned_data_{uuid.uuid4().hex[:8]}"
    s3_path = f"s3://{s3_bucket}/{test_subfolder}"
    print(f"[DEBUG] Step 3: Writing partitioned data to: {s3_path}")

    df.write_parquet(s3_path, partition_cols=["category"], io_config=gravitino_minio_io_config)
    print("[DEBUG] Step 4: Finished writing partitioned parquet data")

    # Verify the file was written by listing the bucket
    fs = s3fs.S3FileSystem(
        key=gravitino_minio_io_config.s3.key_id,
        password=gravitino_minio_io_config.s3.access_key,
        client_kwargs={"endpoint_url": gravitino_minio_io_config.s3.endpoint_url},
    )

    written_files = fs.find(f"{s3_bucket}/{test_subfolder}")
    print(f"[DEBUG] Step 5: Found {len(written_files)} files in S3")
    assert len(written_files) > 0, f"No files written to s3://{s3_bucket}/{test_subfolder}"

    parquet_files = [f for f in written_files if f.endswith(".parquet")]
    print(f"[DEBUG] Step 6: Found {len(parquet_files)} parquet files: {parquet_files}")
    assert len(parquet_files) > 0, f"No parquet files found in s3://{s3_bucket}/{test_subfolder}"

    # Create fileset
    catalog_name = f"part_cat_{uuid.uuid4().hex[:8]}"
    schema_name = f"part_schema_{uuid.uuid4().hex[:8]}"
    fileset_name = f"part_fileset_{uuid.uuid4().hex[:8]}"
    storage_uri = f"s3a://{s3_bucket}/{test_subfolder}/"
    print(f"[DEBUG] Step 7: Creating Gravitino fileset: {catalog_name}.{schema_name}.{fileset_name}")

    ensure_metalake(local_gravitino_client, gravitino_metalake)
    print("[DEBUG] Step 8: Ensured metalake exists")

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
    print("[DEBUG] Step 9: Created catalog")

    create_schema(local_gravitino_client, gravitino_metalake, catalog_name, schema_name)
    print("[DEBUG] Step 10: Created schema")

    create_fileset(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        schema_name,
        fileset_name,
        storage_uri,
    )
    print(f"[DEBUG] Step 11: Created fileset with storage URI: {storage_uri}")

    # Update catalog s3-endpoint to point to localhost for tests running outside Docker
    print("[DEBUG] Updating catalog s3-endpoint from Docker network to localhost")
    update_catalog(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        updates=[{"@type": "setProperty", "property": "s3-endpoint", "value": "http://127.0.0.1:9000"}],
    )
    try:
        gvfs_root = f"gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}"
        glob_pattern = f"{gvfs_root}/**/*.parquet"
        print(f"[DEBUG] Step 12: Reading from GVFS path: {glob_pattern}")

        # Read all partitions
        read_df = daft.read_parquet(glob_pattern, io_config=gravitino_io_config)
        print("[DEBUG] Step 13: Created dataframe, now sorting")

        result = read_df.sort("id").to_pydict()
        print(f"[DEBUG] Step 14: Sorted result: {result}")

        assert len(result["id"]) == 6
        print("[DEBUG] Step 15: Verified result has 6 rows")

        assert result["id"] == [1, 2, 3, 4, 5, 6]
        print("[DEBUG] Step 16: Verified id values")

        assert result["category"] == ["A", "A", "B", "B", "C", "C"]
        print("[DEBUG] Step 17: Verified category values - test completed successfully")

    finally:
        print("[DEBUG] Step 18: Cleaning up Gravitino resources")
        delete_fileset(local_gravitino_client, gravitino_metalake, catalog_name, schema_name, fileset_name)
        delete_schema(local_gravitino_client, gravitino_metalake, catalog_name, schema_name)
        delete_catalog(local_gravitino_client, gravitino_metalake, catalog_name)
        print("[DEBUG] Step 19: Cleanup completed")


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
    print("[DEBUG] Updating catalog s3-endpoint from Docker network to localhost")
    update_catalog(
        local_gravitino_client,
        gravitino_metalake,
        catalog_name,
        updates=[{"@type": "setProperty", "property": "s3-endpoint", "value": "http://127.0.0.1:9000"}],
    )

    try:
        gvfs_root = f"gvfs://fileset/{catalog_name}/{schema_name}/{fileset_name}"
        glob_pattern = f"{gvfs_root}/**/*.parquet"

        # Globbing empty directory should raise FileNotFoundError when collecting
        files_df = daft.from_glob_path(glob_pattern, io_config=gravitino_io_config)
        with pytest.raises(FileNotFoundError):
            files_df.collect()

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
