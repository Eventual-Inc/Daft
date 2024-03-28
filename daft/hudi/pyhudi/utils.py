from __future__ import annotations

import os
from dataclasses import dataclass

import pyarrow as pa
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
        self.schema, self.min_values, self.max_values = FsFileMetadata._extract_min_max(metadata)

    @staticmethod
    def _extract_min_max(metadata: pq.FileMetaData):
        arrow_schema = pa.schema(metadata.schema.to_arrow_schema())
        n_columns = len(arrow_schema)
        min_vals = [None] * n_columns
        max_vals = [None] * n_columns
        num_rg = metadata.num_row_groups
        for rg in range(num_rg):
            row_group = metadata.row_group(rg)
            for col in range(n_columns):
                column = row_group.column(col)
                if column.is_stats_set and column.statistics.has_min_max:
                    if min_vals[col] is None or column.statistics.min < min_vals[col]:
                        min_vals[col] = column.statistics.min
                    if max_vals[col] is None or column.statistics.max > max_vals[col]:
                        max_vals[col] = column.statistics.max
        return arrow_schema, min_vals, max_vals


def list_full_file_paths(path: str, fs: AbstractFileSystem, includes: list[str] | None) -> list[FsFileMetadata]:
    sub_paths = fs.ls(path, detail=True)
    file_paths = []
    for sub_path in sub_paths:
        if sub_path["type"] == "file":
            if includes and os.path.splitext(sub_path["name"])[-1] in includes:
                file_paths.append(FsFileMetadata(sub_path["name"]))

    return file_paths


def list_full_sub_dirs(path: str, fs: AbstractFileSystem, excludes: list[str] | None) -> list[str]:
    sub_paths = fs.ls(path, detail=True)
    sub_dirs = []
    for sub_path in sub_paths:
        if sub_path["type"] == "directory":
            if not excludes or (excludes and fs_basename(sub_path["name"], fs) not in excludes):
                sub_dirs.append(sub_path["name"])

    return sub_dirs


def list_leaf_dirs(path: str, fs: AbstractFileSystem, common_prefix_len=0) -> list[str]:
    sub_paths = fs.ls(path, detail=True)
    leaf_dirs = []

    for sub_path in sub_paths:
        if sub_path["type"] == "directory":
            leaf_dirs.extend(list_leaf_dirs(sub_path["name"], fs, common_prefix_len))

    # leaf directory
    if len(leaf_dirs) == 0:
        leaf_path = path[common_prefix_len:]
        leaf_dirs.append(leaf_path)

    return leaf_dirs
