from __future__ import annotations

from datetime import datetime, timedelta, timezone

import adlfs
import pyarrow as pa
import pytest
from azure.storage.blob import ContainerSasPermissions, generate_container_sas

import daft
from daft.daft import AzureConfig, IOConfig, io_glob

from .conftest import (
    AZURITE_ACCOUNT_KEY,
    AZURITE_ACCOUNT_NAME,
    AZURITE_BLOB_ENDPOINT,
    azurite_connection_string,
    azurite_create_container,
    azurite_upload_bytes,
    azurite_upload_parquet,
)

NESTED_PARQUET_BLOB = "nested/dir/file.parquet"
PARQUET_TABLE = pa.table({"x": [1, 2, 3, 4]})


def _adlfs_for_azurite() -> adlfs.AzureBlobFileSystem:
    return adlfs.AzureBlobFileSystem(
        connection_string=azurite_connection_string(),
    )


def _adlfs_recursive_list(fs: adlfs.AzureBlobFileSystem, path: str) -> list:
    all_results = []
    curr_level_result = fs.ls(path.replace("az://", ""), detail=True)
    for item in curr_level_result:
        if item["type"] == "directory":
            new_path = f"az://{item['name']}"
            all_results.extend(_adlfs_recursive_list(fs, new_path))
            item["name"] += "/"
            all_results.append(item)
        else:
            all_results.append(item)
    return all_results


def _compare_az_glob(daft_ls_result: list, fsspec_result: list) -> None:
    daft_files = [(f["path"], f["type"].lower()) for f in daft_ls_result]
    azfs_files = [(f"az://{f['name']}", f["type"]) for f in fsspec_result if f["type"] == "file"]
    assert sorted(daft_files) == sorted(azfs_files)


@pytest.mark.integration()
def test_azurite_read_parquet_nested_path() -> None:
    with azurite_create_container() as (blob_service, container, io_config):
        url = azurite_upload_parquet(blob_service, container, NESTED_PARQUET_BLOB, PARQUET_TABLE)

        df = daft.read_parquet(url, io_config=io_config)
        assert df.to_pydict() == PARQUET_TABLE.to_pydict()


@pytest.mark.integration()
def test_azurite_url_download() -> None:
    with azurite_create_container() as (blob_service, container, io_config):
        data = b"hello azurite"
        url = azurite_upload_bytes(blob_service, container, "download-me.bin", data)

        df = daft.from_pydict({"urls": [url]})
        df = df.with_column("data", df["urls"].download(io_config=io_config))
        result = df.to_pydict()
        assert result["data"] == [data]


@pytest.mark.integration()
def test_azurite_read_blob() -> None:
    with azurite_create_container() as (blob_service, container, io_config):
        data = b"\x00\x01\x02hello blob"
        url = azurite_upload_bytes(blob_service, container, "file.bin", data)

        df = daft.read_blob(url, io_config=io_config)
        result = df.to_pydict()
        assert len(result["path"]) == 1
        assert result["path"][0].endswith("file.bin")
        assert result["size"] == [len(data)]
        assert result["content"] == [data]


@pytest.mark.integration()
def test_azurite_not_found() -> None:
    with azurite_create_container() as (_, container, io_config):
        path = f"az://{container}/missing.parquet"
        with pytest.raises(FileNotFoundError, match=path):
            daft.read_parquet(path, io_config=io_config)


@pytest.mark.integration()
def test_azurite_read_parquet_with_container_sas() -> None:
    with azurite_create_container() as (blob_service, container, _io_config):
        url = azurite_upload_parquet(blob_service, container, NESTED_PARQUET_BLOB, PARQUET_TABLE)

        sas_token = generate_container_sas(
            account_name=AZURITE_ACCOUNT_NAME,
            container_name=container,
            account_key=AZURITE_ACCOUNT_KEY,
            permission=ContainerSasPermissions(read=True, list=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        sas_io_config = IOConfig(
            azure=AzureConfig(
                storage_account=AZURITE_ACCOUNT_NAME,
                sas_token=sas_token,
                endpoint_url=AZURITE_BLOB_ENDPOINT,
                use_ssl=False,
            )
        )

        df = daft.read_parquet(url, io_config=sas_io_config)
        assert df.to_pydict() == PARQUET_TABLE.to_pydict()


@pytest.mark.integration()
def test_azurite_glob_exact_file() -> None:
    with azurite_create_container() as (blob_service, container, io_config):
        azurite_upload_bytes(blob_service, container, "a.txt", b"a")
        path = f"az://{container}/a.txt"

        daft_ls_result = io_glob(path, io_config=io_config)
        fs = _adlfs_for_azurite()
        fsspec_result = fs.ls(path.replace("az://", ""), detail=True)
        _compare_az_glob(daft_ls_result, fsspec_result)


@pytest.mark.integration()
def test_azurite_glob_recursive() -> None:
    with azurite_create_container() as (blob_service, container, io_config):
        files = ["a.txt", "nested/b.txt", "nested/c.txt"]
        for name in files:
            azurite_upload_bytes(blob_service, container, name, name.encode())

        # Match test_list_files_azure.py: bare `**` at container root lists with prefix
        # "/" which Azurite blob keys do not use, so include a file wildcard.
        path = f"az://{container}/"
        glob_path = path.rstrip("/") + "/**/*.*"
        daft_ls_result = io_glob(glob_path, io_config=io_config)

        fs = _adlfs_for_azurite()
        fsspec_result = _adlfs_recursive_list(fs, path)
        _compare_az_glob(daft_ls_result, fsspec_result)
