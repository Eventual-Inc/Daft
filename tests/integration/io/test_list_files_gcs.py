from __future__ import annotations

import gcsfs
import pytest

from daft.daft import io_list

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

    # Perform necessary post-processing of fsspec results to match expected behavior from Daft:
    # NOTE: gcsfs sometimes does not return the trailing / for directories, so we have to ensure it
    gcsfs_files = [
        (f"{path.rstrip('/')}/", type_) if type_ == "directory" else (path, type_) for path, type_ in gcsfs_files
    ]

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
def test_gs_flat_directory_listing(path, recursive):
    fs = gcsfs.GCSFileSystem()
    daft_ls_result = io_list(path, recursive=recursive)
    fsspec_result = gcsfs_recursive_list(fs, path) if recursive else fs.ls(path, detail=True)
    compare_gcs_result(daft_ls_result, fsspec_result)


@pytest.mark.integration()
@pytest.mark.parametrize("recursive", [False, True])
def test_gs_single_file_listing(recursive):
    path = f"gs://{BUCKET}/test_ls/file.txt"
    fs = gcsfs.GCSFileSystem()
    daft_ls_result = io_list(path, recursive=recursive)
    fsspec_result = gcsfs_recursive_list(fs, path) if recursive else fs.ls(path, detail=True)
    compare_gcs_result(daft_ls_result, fsspec_result)


@pytest.mark.integration()
def test_gs_notfound():
    path = f"gs://{BUCKET}/test_ls/MISSING"
    fs = gcsfs.GCSFileSystem()
    with pytest.raises(FileNotFoundError):
        fs.ls(path, detail=True)
    with pytest.raises(FileNotFoundError, match=path):
        io_list(path)
