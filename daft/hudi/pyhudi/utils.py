from __future__ import annotations

import os
from dataclasses import dataclass

import pyarrow.parquet as pq
from fsspec import AbstractFileSystem


def fs_basename(path: str, fs: AbstractFileSystem) -> str:
    return path.rsplit(fs.sep, 1)[-1]


@dataclass(init=False)
class FsFileMetadata:
    def __init__(self, path: str):
        self.path = path
        metadata = pq.read_metadata(path)
        self.size = metadata.serialized_size
        self.num_records = metadata.num_rows


def get_full_file_paths(path: str, fs: AbstractFileSystem, includes: list[str] | None) -> list[FsFileMetadata]:
    sub_paths = fs.ls(path, detail=True)
    file_paths = []
    for sub_path in sub_paths:
        if sub_path["type"] == "file":
            if includes and os.path.splitext(sub_path["name"])[-1] in includes:
                file_paths.append(FsFileMetadata(sub_path["name"]))

    return file_paths


def get_full_sub_dirs(path: str, fs: AbstractFileSystem, excludes: list[str] | None) -> list[str]:
    sub_paths = fs.ls(path, detail=True)
    sub_dirs = []
    for sub_path in sub_paths:
        if sub_path["type"] == "directory":
            if not excludes or (excludes and fs_basename(sub_path["name"], fs) not in excludes):
                sub_dirs.append(sub_path["name"])

    return sub_dirs


def get_leaf_dirs(path: str, fs: AbstractFileSystem, common_prefix_len=0) -> list[str]:
    sub_paths = fs.ls(path, detail=True)
    leaf_dirs = []

    for sub_path in sub_paths:
        if sub_path["type"] == "directory":
            leaf_dirs.extend(get_leaf_dirs(sub_path["name"], fs, common_prefix_len))

    # leaf directory
    if len(leaf_dirs) == 0:
        leaf_path = path[common_prefix_len:]
        leaf_dirs.append(leaf_path)

    return leaf_dirs
