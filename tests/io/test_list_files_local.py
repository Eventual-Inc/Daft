from __future__ import annotations

import os

import pytest
from fsspec.implementations.local import LocalFileSystem

from daft.daft import io_glob


def local_recursive_list(fs, path) -> list:
    all_results = []
    curr_level_result = fs.ls(path, detail=True)
    for item in curr_level_result:
        if item["type"] == "directory":
            new_path = item["name"]
            all_results.extend(local_recursive_list(fs, new_path))
            item["name"] += "/"
            all_results.append(item)
        else:
            all_results.append(item)
    return all_results


def compare_local_result(daft_ls_result: list, fs_result: list):
    daft_files = [(f["path"], f["type"].lower()) for f in daft_ls_result]
    fs_files = [(f'file://{f["name"]}', f["type"]) for f in fs_result]

    # io_glob does not return directories
    fs_files = [(p, t) for p, t in fs_files if t == "file"]

    # io_glob returns posix-style paths
    fs_files = [(p.replace("\\", "/"), t) for p, t in fs_files]

    assert sorted(daft_files) == sorted(fs_files)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_flat_directory_listing(tmp_path, include_protocol):
    d = tmp_path / "dir"
    d.mkdir()
    files = ["a", "b", "c"]
    for name in files:
        p = d / name
        p.touch()
    d = str(d) + "/"
    if include_protocol:
        d = "file://" + d
    daft_ls_result = io_glob(d)
    fs = LocalFileSystem()
    fs_result = fs.ls(d, detail=True)
    compare_local_result(daft_ls_result, fs_result)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_recursive_curr_dir_listing(tmp_path, include_protocol):
    d = tmp_path / "dir"
    d.mkdir()
    files = ["a", "b", "c"]
    for name in files:
        p = d / name
        p.touch()
    d = str(d) + "/"

    pwd = os.getcwd()
    os.chdir(str(d))

    try:
        path = "file://**" if include_protocol else "**"

        daft_ls_result = io_glob(path)
        fs = LocalFileSystem()
        fs_result = fs.ls(d, detail=True)
        compare_local_result(daft_ls_result, fs_result)
    finally:
        os.chdir(pwd)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_recursive_directory_listing(tmp_path, include_protocol):
    d = tmp_path / "dir"
    d.mkdir()
    files = ["a", "b/bb", "c/cc/ccc"]
    for name in files:
        p = d
        segments = name.split("/")
        for intermediate_dir in segments[:-1]:
            p /= intermediate_dir
            p.mkdir()
        p /= segments[-1]
        p.touch()
    if include_protocol:
        d = "file://" + str(d)
    daft_ls_result = io_glob(str(d) + "/**")
    fs = LocalFileSystem()
    fs_result = local_recursive_list(fs, d)
    compare_local_result(daft_ls_result, fs_result)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_single_file_directory_listing(tmp_path, include_protocol):
    d = tmp_path / "dir"
    d.mkdir()
    files = ["a", "b/bb", "c/cc/ccc"]
    for name in files:
        p = d
        segments = name.split("/")
        for intermediate_dir in segments[:-1]:
            p /= intermediate_dir
            p.mkdir()
        p /= segments[-1]
        p.touch()
    p = f"{d}/c/cc/ccc"
    if include_protocol:
        p = "file://" + p

    daft_ls_result = io_glob(p)
    fs_result = [{"name": f"{d}/c/cc/ccc", "type": "file"}]
    assert len(daft_ls_result) == 1
    compare_local_result(daft_ls_result, fs_result)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_wildcard_listing(tmp_path, include_protocol):
    d = tmp_path / "dir"
    d.mkdir()
    files = ["a/x.txt", "b/y.txt", "c/z.txt"]
    for name in files:
        p = d
        segments = name.split("/")
        for intermediate_dir in segments[:-1]:
            p /= intermediate_dir
            p.mkdir()
        p /= segments[-1]
        p.touch()
    p = f"{d}/*/*.txt"
    if include_protocol:
        p = "file://" + p

    daft_ls_result = io_glob(p)
    fs_result = [
        {"name": f"{d}/a/x.txt", "type": "file"},
        {"name": f"{d}/b/y.txt", "type": "file"},
        {"name": f"{d}/c/z.txt", "type": "file"},
    ]
    assert len(daft_ls_result) == 3
    compare_local_result(daft_ls_result, fs_result)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_missing_file_path(tmp_path, include_protocol):
    d = tmp_path / "dir"
    d.mkdir()
    files = ["a", "b/bb", "c/cc/ccc"]
    for name in files:
        p = d
        segments = name.split("/")
        for intermediate_dir in segments[:-1]:
            p /= intermediate_dir
            p.mkdir()
        p /= segments[-1]
        p.touch()
    p = f"{d}/c/cc/ddd"
    if include_protocol:
        p = "file://" + p
    with pytest.raises(FileNotFoundError, match="/c/cc/ddd not found"):
        io_glob(p)
