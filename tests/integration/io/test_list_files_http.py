from __future__ import annotations

from pathlib import Path

import pytest
from fsspec.implementations.http import HTTPFileSystem

from daft.daft import io_list
from tests.integration.io.conftest import mount_data_nginx


def compare_http_result(daft_ls_result: list, fsspec_result: list):
    daft_files = [(f["path"], f["type"].lower()) for f in daft_ls_result]
    gcsfs_files = [(f"http://{f['name']}", f["type"]) for f in fsspec_result]

    # Perform necessary post-processing of fsspec results to match expected behavior from Daft:

    # # NOTE: gcsfs sometimes does not return the trailing / for directories, so we have to ensure it
    # gcsfs_files = [
    #     (f"{path.rstrip('/')}/", type_) if type_ == "directory" else (path, type_) for path, type_ in gcsfs_files
    # ]

    # # NOTE: gcsfs will sometimes return 0-sized marker files for manually-created folders, which we ignore here
    # # Be careful here because this will end up pruning any truly size-0 files that are actually files and not folders!
    # size_0_files = {f"gs://{f['name']}" for f in fsspec_result if f["size"] == 0 and f["type"] == "file"}
    # gcsfs_files = [(path, type_) for path, type_ in gcsfs_files if path not in size_0_files]

    assert len(daft_files) == len(gcsfs_files)
    assert sorted(daft_files) == sorted(gcsfs_files)


@pytest.fixture(scope="module")
def nginx_http_url(nginx_config, tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp("test-list-http")
    data_path = Path(tmpdir)
    (Path(data_path) / "file.txt").touch()
    (Path(data_path) / "test_ls").mkdir()
    (Path(data_path) / "test_ls" / "file.txt").touch()
    (Path(data_path) / "test_ls" / "paginated-10-files").mkdir()
    for i in range(10):
        (Path(data_path) / "test_ls" / "paginated-10-files" / f"file.{i}.txt").touch()

    with mount_data_nginx(nginx_config, data_path):
        yield nginx_config[0]


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path",
    [
        f"",
        f"/",
        f"test_ls",
        f"test_ls/",
        f"test_ls/paginated-10-files/",
    ],
)
def test_http_flat_directory_listing(path, nginx_http_url):
    http_path = f"{nginx_http_url}/{path}"
    fs = HTTPFileSystem()
    fsspec_result = fs.ls(http_path, detail=True)
    daft_ls_result = io_list(http_path)
    compare_http_result(daft_ls_result, fsspec_result)


# @pytest.mark.integration()
# def test_gs_single_file_listing():
#     path = f"gs://{BUCKET}/test_ls/file.txt"
#     fs = gcsfs.GCSFileSystem()
#     daft_ls_result = io_list(path)
#     fsspec_result = fs.ls(path, detail=True)
#     compare_http_result(daft_ls_result, fsspec_result)


# @pytest.mark.integration()
# def test_gs_notfound():
#     path = f"gs://{BUCKET}/test_ls/MISSING"
#     fs = gcsfs.GCSFileSystem()
#     with pytest.raises(FileNotFoundError):
#         fs.ls(path, detail=True)

#     # NOTE: Google Cloud does not return a 404 to indicate anything missing, but just returns empty results
#     # Thus Daft is unable to differentiate between "missing" folders and "empty" folders
#     daft_ls_result = io_list(path)
#     assert daft_ls_result == []


# @pytest.mark.integration()
# @pytest.mark.parametrize(
#     "path",
#     [
#         f"gs://{BUCKET}/test_ls",
#         f"gs://{BUCKET}/test_ls/",
#     ],
# )
# def test_gs_flat_directory_listing_recursive(path):
#     fs = gcsfs.GCSFileSystem()
#     daft_ls_result = io_list(path, recursive=True)
#     fsspec_result = list(fs.glob(path.rstrip("/") + "/**", detail=True).values())
#     compare_gcs_result(daft_ls_result, fsspec_result)
