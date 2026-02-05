from __future__ import annotations

import os
import re

import pytest
from fsspec.implementations.local import LocalFileSystem

from daft.daft import io_glob
from daft.exceptions import DaftCoreException


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
    fs_files = [(f"file://{f['name']}", f["type"]) for f in fs_result]

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


@pytest.mark.parametrize("include_protocol", [False, True])
def test_invalid_double_asterisk_usage_local(tmp_path, include_protocol):
    d = tmp_path / "dir"
    d.mkdir()

    path = str(d) + "/**.pq"
    if include_protocol:
        path = "file://" + path

    expected_correct_path = str(d) + "/**/*.pq"
    if include_protocol:
        expected_correct_path = "file://" + expected_correct_path

    # Need to escape these or the regex matcher will complain
    expected_correct_path = re.escape(expected_correct_path)

    with pytest.raises(DaftCoreException, match=expected_correct_path):
        io_glob(path)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_literal_double_asterisk_file(tmp_path, include_protocol):
    d = tmp_path / "dir"
    d.mkdir()
    file_with_literal_name = d / "*.pq"
    file_with_literal_name.touch()

    path = str(d) + "/\\**.pq"
    if include_protocol:
        path = "file://" + path

    fs = LocalFileSystem()
    fs_result = fs.ls(str(d), detail=True)
    fs_result = [f for f in fs_result if f["name"] == str(file_with_literal_name)]

    daft_ls_result = io_glob(path)

    assert len(daft_ls_result) == 1
    compare_local_result(daft_ls_result, fs_result)


# ============================================================================
# Numeric Range Expansion Tests (Issue #2708)
# ============================================================================


@pytest.mark.parametrize("include_protocol", [False, True])
def test_numeric_range_simple(tmp_path, include_protocol):
    """Test basic numeric range expansion {0..3}."""
    d = tmp_path / "dir"
    d.mkdir()

    # Create files 0.txt, 1.txt, 2.txt, 3.txt
    for i in range(4):
        (d / f"{i}.txt").touch()

    # Also create 4.txt which should NOT be matched
    (d / "4.txt").touch()

    path = f"{d}/{{0..3}}.txt"
    if include_protocol:
        path = "file://" + path

    daft_ls_result = io_glob(path)

    assert len(daft_ls_result) == 4
    expected_files = [{"name": f"{d}/{i}.txt", "type": "file"} for i in range(4)]
    compare_local_result(daft_ls_result, expected_files)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_numeric_range_with_leading_zeros(tmp_path, include_protocol):
    """Test numeric range with leading zeros {00..05}."""
    d = tmp_path / "dir"
    d.mkdir()

    # Create files 00.txt through 05.txt
    for i in range(6):
        (d / f"{i:02d}.txt").touch()

    path = f"{d}/{{00..05}}.txt"
    if include_protocol:
        path = "file://" + path

    daft_ls_result = io_glob(path)

    assert len(daft_ls_result) == 6
    expected_files = [{"name": f"{d}/{i:02d}.txt", "type": "file"} for i in range(6)]
    compare_local_result(daft_ls_result, expected_files)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_numeric_range_reverse(tmp_path, include_protocol):
    """Test reverse numeric range {3..0}."""
    d = tmp_path / "dir"
    d.mkdir()

    # Create files 0.txt through 3.txt
    for i in range(4):
        (d / f"{i}.txt").touch()

    path = f"{d}/{{3..0}}.txt"
    if include_protocol:
        path = "file://" + path

    daft_ls_result = io_glob(path)

    assert len(daft_ls_result) == 4
    expected_files = [{"name": f"{d}/{i}.txt", "type": "file"} for i in range(4)]
    compare_local_result(daft_ls_result, expected_files)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_numeric_range_in_middle_of_path(tmp_path, include_protocol):
    """Test numeric range in the middle of filename."""
    d = tmp_path / "dir"
    d.mkdir()

    # Create files data_0_suffix.txt through data_2_suffix.txt
    for i in range(3):
        (d / f"data_{i}_suffix.txt").touch()

    path = f"{d}/data_{{0..2}}_suffix.txt"
    if include_protocol:
        path = "file://" + path

    daft_ls_result = io_glob(path)

    assert len(daft_ls_result) == 3
    expected_files = [{"name": f"{d}/data_{i}_suffix.txt", "type": "file"} for i in range(3)]
    compare_local_result(daft_ls_result, expected_files)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_numeric_range_in_directory(tmp_path, include_protocol):
    """Test numeric range in directory path."""
    d = tmp_path / "dir"
    d.mkdir()

    # Create directories 0/, 1/, 2/ with file.txt in each
    for i in range(3):
        subdir = d / str(i)
        subdir.mkdir()
        (subdir / "file.txt").touch()

    path = f"{d}/{{0..2}}/file.txt"
    if include_protocol:
        path = "file://" + path

    daft_ls_result = io_glob(path)

    assert len(daft_ls_result) == 3
    expected_files = [{"name": f"{d}/{i}/file.txt", "type": "file"} for i in range(3)]
    compare_local_result(daft_ls_result, expected_files)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_numeric_range_multiple(tmp_path, include_protocol):
    """Test multiple numeric ranges in one path."""
    d = tmp_path / "dir"
    d.mkdir()

    # Create directories 0/, 1/ with files 0.txt, 1.txt in each
    for i in range(2):
        subdir = d / str(i)
        subdir.mkdir()
        for j in range(2):
            (subdir / f"{j}.txt").touch()

    path = f"{d}/{{0..1}}/{{0..1}}.txt"
    if include_protocol:
        path = "file://" + path

    daft_ls_result = io_glob(path)

    assert len(daft_ls_result) == 4
    expected_files = [{"name": f"{d}/{i}/{j}.txt", "type": "file"} for i in range(2) for j in range(2)]
    compare_local_result(daft_ls_result, expected_files)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_numeric_range_mixed_with_wildcard(tmp_path, include_protocol):
    """Test numeric range combined with wildcard patterns."""
    d = tmp_path / "dir"
    d.mkdir()

    # Create files 0_a.txt, 0_b.txt, 1_a.txt, 1_b.txt
    for i in range(2):
        for suffix in ["a", "b"]:
            (d / f"{i}_{suffix}.txt").touch()

    path = f"{d}/{{0..1}}_*.txt"
    if include_protocol:
        path = "file://" + path

    daft_ls_result = io_glob(path)

    assert len(daft_ls_result) == 4
    expected_files = [{"name": f"{d}/{i}_{s}.txt", "type": "file"} for i in range(2) for s in ["a", "b"]]
    compare_local_result(daft_ls_result, expected_files)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_numeric_range_single_value(tmp_path, include_protocol):
    """Test numeric range with same start and end {5..5}."""
    d = tmp_path / "dir"
    d.mkdir()

    # Create file 5.txt
    (d / "5.txt").touch()

    path = f"{d}/{{5..5}}.txt"
    if include_protocol:
        path = "file://" + path

    daft_ls_result = io_glob(path)

    assert len(daft_ls_result) == 1
    expected_files = [{"name": f"{d}/5.txt", "type": "file"}]
    compare_local_result(daft_ls_result, expected_files)


def test_numeric_range_too_large(tmp_path):
    """Test that overly large numeric ranges raise an error."""
    d = tmp_path / "dir"
    d.mkdir()

    # Try to use a range that's too large (exceeds MAX_RANGE_SIZE of 10,000)
    path = f"{d}/{{0..100000}}.txt"

    with pytest.raises(DaftCoreException, match="exceeds the maximum"):
        io_glob(path)


@pytest.mark.parametrize("include_protocol", [False, True])
def test_numeric_range_negative(tmp_path, include_protocol):
    """Test numeric range with negative numbers {-2..2}."""
    d = tmp_path / "dir"
    d.mkdir()

    # Create files -2.txt, -1.txt, 0.txt, 1.txt, 2.txt
    for i in range(-2, 3):
        (d / f"{i}.txt").touch()

    path = f"{d}/{{-2..2}}.txt"
    if include_protocol:
        path = "file://" + path

    daft_ls_result = io_glob(path)

    assert len(daft_ls_result) == 5
    expected_files = [{"name": f"{d}/{i}.txt", "type": "file"} for i in range(-2, 3)]
    compare_local_result(daft_ls_result, expected_files)
