from __future__ import annotations

import gcsfs
import pytest

from daft.daft import io_glob

BUCKET = "daft-public-data-gs"


def gcsfs_recursive_list(fs, path) -> list:
    all_results = []
    curr_level_result = fs.ls(path, detail=True)
    for item in curr_level_result:
        if item["type"] == "directory":
            new_path = f'gs://{item["name"]}'
            all_results.extend(gcsfs_recursive_list(fs, new_path))
            item["name"] += "/"
            all_results.append(item)
        else:
            all_results.append(item)
    return all_results


def compare_gcs_result(daft_ls_result: list, fsspec_result: list):
    daft_files = [(f["path"], f["type"].lower()) for f in daft_ls_result]
    gcsfs_files = [(f"gs://{f['name']}", f["type"]) for f in fsspec_result]

    # Remove all directories: our glob utilities don't return dirs
    gcsfs_files = [(path, type_) for path, type_ in gcsfs_files if type_ == "file"]

    # Remove size-0 directory marker files
    gcsfs_files = [(path, type_) for path, type_ in gcsfs_files if not path.endswith("/")]

    assert len(daft_files) == len(gcsfs_files)
    assert sorted(daft_files) == sorted(gcsfs_files)


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path",
    [
        f"gs://{BUCKET}",
        f"gs://{BUCKET}/",
        f"gs://{BUCKET}/test_ls",
        f"gs://{BUCKET}/test_ls/",
        f"gs://{BUCKET}/test_ls//",
        f"gs://{BUCKET}/test_ls/paginated-1100-files/",
    ],
)
@pytest.mark.parametrize("recursive", [False, True])
@pytest.mark.parametrize("fanout_limit", [None, 1])
def test_gs_flat_directory_listing(gcs_public_config, path, recursive, fanout_limit):
    fs = gcsfs.GCSFileSystem()
    glob_path = path.rstrip("/") + "/**" if recursive else path
    daft_ls_result = io_glob(glob_path, io_config=gcs_public_config, fanout_limit=fanout_limit)
    fsspec_result = gcsfs_recursive_list(fs, path) if recursive else fs.ls(path, detail=True)
    compare_gcs_result(daft_ls_result, fsspec_result)


@pytest.mark.integration()
@pytest.mark.parametrize("recursive", [False, True])
def test_gs_single_file_listing(gcs_public_config, recursive):
    path = f"gs://{BUCKET}/test_ls/file.txt"
    fs = gcsfs.GCSFileSystem()
    daft_ls_result = io_glob(path, io_config=gcs_public_config)
    fsspec_result = gcsfs_recursive_list(fs, path) if recursive else fs.ls(path, detail=True)
    compare_gcs_result(daft_ls_result, fsspec_result)


@pytest.mark.integration()
def test_gs_notfound(gcs_public_config):
    path = f"gs://{BUCKET}/test_"
    with pytest.raises(FileNotFoundError, match=path):
        io_glob(path, io_config=gcs_public_config)
