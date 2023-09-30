from __future__ import annotations

import pytest

from daft.daft import io_glob, io_list

from .conftest import minio_create_bucket


def compare_s3_result(daft_ls_result: list, s3fs_result: list):
    daft_files = [(f["path"], f["type"].lower()) for f in daft_ls_result]
    s3fs_files = [(f"s3://{f['name']}", f["type"]) for f in s3fs_result]
    assert sorted(daft_files) == sorted(s3fs_files)


def s3fs_recursive_list(fs, path) -> list:
    all_results = []
    curr_level_result = fs.ls(path, detail=True)
    for item in curr_level_result:
        if item["type"] == "directory":
            new_path = f's3://{item["name"]}'
            all_results.extend(s3fs_recursive_list(fs, new_path))
            item["name"] += "/"
            all_results.append(item)
        else:
            all_results.append(item)
    return all_results


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path_expect_pair",
    [
        # Exact filepath:
        (f"s3://bucket/a.match", [{"type": "File", "path": "s3://bucket/a.match", "size": 0}]),
        ###
        # `**`: recursive wildcard
        ###
        # All files with **
        (
            f"s3://bucket/**",
            [
                {"type": "File", "path": "s3://bucket/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/b.nomatch", "size": 0},
                {"type": "File", "path": "s3://bucket/c.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/b.nomatch", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/c.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested2/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested2/b.nomatch", "size": 0},
                {"type": "File", "path": "s3://bucket/nested2/c.match", "size": 0},
            ],
        ),
        # Exact filepath after **
        (
            f"s3://bucket/**/a.match",
            [
                {"type": "File", "path": "s3://bucket/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested2/a.match", "size": 0},
            ],
        ),
        # Wildcard filepath after **
        (
            f"s3://bucket/**/nested1/*.match",
            [
                {"type": "File", "path": "s3://bucket/nested1/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/c.match", "size": 0},
            ],
        ),
        # Wildcard folder before **
        (
            f"s3://bucket/*/**/*.match",
            [
                {"type": "File", "path": "s3://bucket/nested1/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/c.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested2/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested2/c.match", "size": 0},
            ],
        ),
        ###
        # `*`: wildcard
        ###
        # Wildcard file
        (
            f"s3://bucket/*.match",
            [
                {"type": "File", "path": "s3://bucket/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/c.match", "size": 0},
            ],
        ),
        # Wildcard file
        (
            f"s3://bucket/*",
            [
                {"type": "File", "path": "s3://bucket/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/b.nomatch", "size": 0},
                {"type": "File", "path": "s3://bucket/c.match", "size": 0},
            ],
        ),
        # Nested wildcard file
        (
            f"s3://bucket/nested1/*.match",
            [
                {"type": "File", "path": "s3://bucket/nested1/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/c.match", "size": 0},
            ],
        ),
        # Wildcard folder + wildcard file
        (
            f"s3://bucket/*1/*.match",
            [
                {"type": "File", "path": "s3://bucket/nested1/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/c.match", "size": 0},
            ],
        ),
        # Wildcard folder + exact file
        (
            f"s3://bucket/*/a.match",
            [
                {"type": "File", "path": "s3://bucket/nested1/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested2/a.match", "size": 0},
            ],
        ),
        ###
        # Missing paths
        ###
        # Exact filepath missing:
        (f"s3://bucket/MISSING", FileNotFoundError),
        # Wildcard file no match:
        (f"s3://bucket/*.MISSING", []),
        # Exact folder missing:
        (f"s3://bucket/MISSING/*.match", []),
        # Wildcard folder no match:
        (f"s3://bucket/*NOMATCH/*.match", []),
        ###
        # Directories: glob ignores directories and never returns them
        ###
        # Exact directory: fall back to ls behavior
        (
            f"s3://bucket/nested1",
            [
                {"type": "File", "path": "s3://bucket/nested1/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/b.nomatch", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/c.match", "size": 0},
            ],
        ),
        # Wildcard folder: we don't select directories with wildcards because we think it's a File
        (f"s3://bucket/nested*", []),
        # Wildcard folder: Directories can be selected with a trailing /
        (
            f"s3://bucket/nested*/",
            [
                {"type": "File", "path": "s3://bucket/nested1/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/b.nomatch", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/c.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested2/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested2/b.nomatch", "size": 0},
                {"type": "File", "path": "s3://bucket/nested2/c.match", "size": 0},
            ],
        ),
        # Exact folder after **: we don't return directories
        (f"s3://bucket/**/nested1", []),
        # Exact folder after **: return results if user specifies a trailing /
        (
            f"s3://bucket/**/nested1/",
            [
                {"type": "File", "path": "s3://bucket/nested1/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/b.nomatch", "size": 0},
                {"type": "File", "path": "s3://bucket/nested1/c.match", "size": 0},
            ],
        ),
    ],
)
def test_directory_globbing_fragment_wildcard(minio_io_config, path_expect_pair):
    globpath, expect = path_expect_pair
    with minio_create_bucket(minio_io_config, bucket_name="bucket") as fs:
        files = [
            "a.match",
            "b.nomatch",
            "c.match",
            "nested1/a.match",
            "nested1/b.nomatch",
            "nested1/c.match",
            "nested2/a.match",
            "nested2/b.nomatch",
            "nested2/c.match",
        ]
        for name in files:
            fs.touch(f"bucket/{name}")

        if type(expect) == type and issubclass(expect, BaseException):
            with pytest.raises(expect):
                io_glob(globpath, io_config=minio_io_config)
        else:
            daft_ls_result = io_glob(globpath, io_config=minio_io_config)
            assert sorted(daft_ls_result, key=lambda d: d["path"]) == sorted(expect, key=lambda d: d["path"])


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path_expect_pair",
    [
        # A "\*" is escaped to a literal *
        (r"s3://bucket/\*.match", [{"type": "File", "path": "s3://bucket/*.match", "size": 0}]),
        # A "\\" is escaped to just a \
        (r"s3://bucket/\\.match", [{"type": "File", "path": r"s3://bucket/\.match", "size": 0}]),
        # Ignore \ followed by non-special character
        (r"s3://bucket/\a.match", [{"type": "File", "path": "s3://bucket/a.match", "size": 0}]),
    ],
)
def test_directory_globbing_escape_characters(minio_io_config, path_expect_pair):
    globpath, expect = path_expect_pair
    with minio_create_bucket(minio_io_config, bucket_name="bucket") as fs:
        files = ["a.match", "*.match", r"\.match"]
        for name in files:
            fs.touch(f"bucket/{name}")
        daft_ls_result = io_glob(globpath, io_config=minio_io_config)
        assert sorted(daft_ls_result, key=lambda d: d["path"]) == sorted(expect, key=lambda d: d["path"])


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path_expect_pair",
    [
        # Test [] square brackets for matching specified single characters
        (
            "s3://bucket/[ab].match",
            [
                {"type": "File", "path": "s3://bucket/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/b.match", "size": 0},
            ],
        ),
        # Test ? for matching any single characters
        (
            "s3://bucket/?.match",
            [
                {"type": "File", "path": "s3://bucket/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/b.match", "size": 0},
                {"type": "File", "path": "s3://bucket/c.match", "size": 0},
                {"type": "File", "path": "s3://bucket/d.match", "size": 0},
            ],
        ),
        # Test {} for matching arbitrary globs
        (
            "s3://bucket/{a,[bc]}.match",
            [
                {"type": "File", "path": "s3://bucket/a.match", "size": 0},
                {"type": "File", "path": "s3://bucket/b.match", "size": 0},
                {"type": "File", "path": "s3://bucket/c.match", "size": 0},
            ],
        ),
    ],
)
def test_directory_globbing_special_characters(minio_io_config, path_expect_pair):
    globpath, expect = path_expect_pair
    with minio_create_bucket(minio_io_config, bucket_name="bucket") as fs:
        files = ["a.match", "b.match", "c.match", "d.match"]
        for name in files:
            fs.touch(f"bucket/{name}")
        daft_ls_result = io_glob(globpath, io_config=minio_io_config)
        assert sorted(daft_ls_result, key=lambda d: d["path"]) == sorted(expect, key=lambda d: d["path"])


@pytest.mark.integration()
def test_flat_directory_listing(minio_io_config):
    bucket_name = "bucket"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        files = ["a", "b", "c"]
        for name in files:
            fs.touch(f"{bucket_name}/{name}")
        daft_ls_result = io_list(f"s3://{bucket_name}", io_config=minio_io_config)
        s3fs_result = fs.ls(f"s3://{bucket_name}", detail=True)
        compare_s3_result(daft_ls_result, s3fs_result)


@pytest.mark.integration()
def test_recursive_directory_listing(minio_io_config):
    bucket_name = "bucket"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        files = ["a", "b/bb", "c/cc/ccc"]
        for name in files:
            fs.write_bytes(f"s3://{bucket_name}/{name}", b"")
        daft_ls_result = io_list(f"s3://{bucket_name}/", io_config=minio_io_config, recursive=True)
        fs.invalidate_cache()
        s3fs_result = s3fs_recursive_list(fs, path=f"s3://{bucket_name}")
        compare_s3_result(daft_ls_result, s3fs_result)


@pytest.mark.integration()
@pytest.mark.parametrize(
    "recursive",
    [False, True],
)
def test_single_file_directory_listing(minio_io_config, recursive):
    bucket_name = "bucket"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        files = ["a", "b/bb", "c/cc/ccc"]
        for name in files:
            fs.write_bytes(f"s3://{bucket_name}/{name}", b"")
        daft_ls_result = io_list(f"s3://{bucket_name}/c/cc/ccc", io_config=minio_io_config, recursive=recursive)
        fs.invalidate_cache()
        s3fs_result = s3fs_recursive_list(fs, path=f"s3://{bucket_name}/c/cc/ccc")
        assert len(daft_ls_result) == 1
        compare_s3_result(daft_ls_result, s3fs_result)


@pytest.mark.integration()
@pytest.mark.parametrize(
    "recursive",
    [False, True],
)
def test_single_file_directory_listing_trailing(minio_io_config, recursive):
    bucket_name = "bucket"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        files = ["a", "b/bb", "c/cc/ccc"]
        for name in files:
            fs.write_bytes(f"s3://{bucket_name}/{name}", b"")
        daft_ls_result = io_list(f"s3://{bucket_name}/c/cc///", io_config=minio_io_config, recursive=recursive)
        fs.invalidate_cache()
        s3fs_result = s3fs_recursive_list(fs, path=f"s3://{bucket_name}/c/cc///")
        assert len(daft_ls_result) == 1
        compare_s3_result(daft_ls_result, s3fs_result)


@pytest.mark.integration()
@pytest.mark.parametrize(
    "recursive",
    [False, True],
)
def test_missing_file_path(minio_io_config, recursive):
    bucket_name = "bucket"
    with minio_create_bucket(minio_io_config, bucket_name=bucket_name) as fs:
        files = ["a", "b/bb", "c/cc/ccc"]
        for name in files:
            fs.write_bytes(f"s3://{bucket_name}/{name}", b"")
        with pytest.raises(FileNotFoundError, match=f"s3://{bucket_name}/c/cc/ddd"):
            daft_ls_result = io_list(f"s3://{bucket_name}/c/cc/ddd", io_config=minio_io_config, recursive=recursive)
