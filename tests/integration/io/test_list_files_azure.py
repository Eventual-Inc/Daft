from __future__ import annotations

import adlfs
import pytest

from daft.daft import AzureConfig, IOConfig, io_glob

STORAGE_ACCOUNT = "dafttestdata"
CONTAINER = "public-anonymous"
DEFAULT_AZURE_CONFIG = AzureConfig(storage_account=STORAGE_ACCOUNT, anonymous=True)


def adlfs_recursive_list(fs, path) -> list:
    all_results = []
    curr_level_result = fs.ls(path.replace("az://", ""), detail=True)
    for item in curr_level_result:
        if item["type"] == "directory":
            new_path = f'az://{item["name"]}'
            all_results.extend(adlfs_recursive_list(fs, new_path))
            item["name"] += "/"
            all_results.append(item)
        else:
            all_results.append(item)
    return all_results


def compare_az_result(daft_ls_result: list, fsspec_result: list):
    daft_files = [(f["path"], f["type"].lower()) for f in daft_ls_result]
    azfs_files = [(f"az://{f['name']}", f["type"]) for f in fsspec_result]

    # Remove all directories: our glob utilities don't return dirs
    azfs_files = [(path, type_) for path, type_ in azfs_files if type_ == "file"]

    assert len(daft_files) == len(azfs_files)
    assert sorted(daft_files) == sorted(azfs_files)


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path",
    [
        f"az://{CONTAINER}",
        f"az://{CONTAINER}/",
        f"az://{CONTAINER}/test_ls/",
        f"az://{CONTAINER}/test_ls//",
    ],
)
@pytest.mark.parametrize("recursive", [False, True])
@pytest.mark.parametrize("fanout_limit", [None, 1])
def test_az_flat_directory_listing(path, recursive, fanout_limit):
    fs = adlfs.AzureBlobFileSystem(account_name=STORAGE_ACCOUNT)
    glob_path = path.rstrip("/") + "/**/*.*" if recursive else path
    daft_ls_result = io_glob(glob_path, io_config=IOConfig(azure=DEFAULT_AZURE_CONFIG), fanout_limit=fanout_limit)
    fsspec_result = adlfs_recursive_list(fs, path) if recursive else fs.ls(path.replace("az://", ""), detail=True)
    compare_az_result(daft_ls_result, fsspec_result)


@pytest.mark.integration()
def test_az_single_file_listing():
    path = f"az://{CONTAINER}/mvp.parquet"
    fs = adlfs.AzureBlobFileSystem(account_name=STORAGE_ACCOUNT)
    daft_ls_result = io_glob(path, io_config=IOConfig(azure=DEFAULT_AZURE_CONFIG))
    fsspec_result = fs.ls(path.replace("az://", ""), detail=True)
    compare_az_result(daft_ls_result, fsspec_result)


@pytest.mark.integration()
def test_az_notfound():
    path = f"az://{CONTAINER}/test_"
    with pytest.raises(FileNotFoundError, match=path):
        io_glob(path, io_config=IOConfig(azure=DEFAULT_AZURE_CONFIG))
