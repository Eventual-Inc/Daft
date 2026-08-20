from __future__ import annotations

import os
import uuid

import pytest

import daft


@pytest.fixture(scope="module", autouse=True)
def skip_no_gcs_write_credentials(pytestconfig):
    if not pytestconfig.getoption("--credentials"):
        pytest.skip(reason="GCS overwrite integration tests require the `--credentials` flag.")
    if not os.environ.get("GCS_WRITE_TEST_BUCKET"):
        pytest.skip(reason="GCS overwrite integration tests require the GCS_WRITE_TEST_BUCKET environment variable.")


@pytest.fixture(scope="module")
def gcs_write_test_config() -> daft.io.IOConfig:
    return daft.io.IOConfig(
        gcs=daft.io.GCSConfig(
            project_id=os.environ.get("GCS_TEST_PROJECT_ID"),
            anonymous=False,
        )
    )


@pytest.fixture(scope="function")
def gcs_write_test_path():
    gcsfs = pytest.importorskip("gcsfs")

    bucket = os.environ["GCS_WRITE_TEST_BUCKET"]
    prefix = f"daft-tests/parquet-overwrite-{uuid.uuid4()}"
    path = f"gs://{bucket}/{prefix}"
    fs = gcsfs.GCSFileSystem(project=os.environ.get("GCS_TEST_PROJECT_ID"), token="google_default")

    try:
        yield path
    finally:
        try:
            fs.rm(path, recursive=True)
        except FileNotFoundError:
            pass


@pytest.mark.integration()
def test_gcs_parquet_overwrite_roundtrip(gcs_write_test_config, gcs_write_test_path):
    first = daft.from_pydict({"x": [1, 2], "tag": ["old", "old"]}).repartition(2)
    first.write_parquet(gcs_write_test_path, io_config=gcs_write_test_config, write_mode="overwrite")

    second = daft.from_pydict({"x": [3, 4, 5], "tag": ["new", "new", "new"]}).repartition(2)
    second.write_parquet(gcs_write_test_path, io_config=gcs_write_test_config, write_mode="overwrite")

    result = daft.read_parquet(gcs_write_test_path, io_config=gcs_write_test_config).sort("x").to_pydict()
    assert result == {"x": [3, 4, 5], "tag": ["new", "new", "new"]}
