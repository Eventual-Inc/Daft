from __future__ import annotations

import os
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq


@dataclass(init=False)
class FsFileMetadata:
    def __init__(self, fs: pafs.FileSystem, base_path: str, path: str, base_name: str):
        self.base_path = base_path
        self.path = path
        self.base_name = base_name
        metadata = pq.read_metadata(os.path.join(base_path, path), filesystem=fs)
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


def list_relative_file_paths(
    base_path: str, sub_path: str, fs: pafs.FileSystem, includes: list[str] | None
) -> list[FsFileMetadata]:
    listed_paths: list[pafs.FileInfo] = fs.get_file_info(pafs.FileSelector(os.path.join(base_path, sub_path)))
    file_paths = []
    common_prefix_len = len(base_path) + 1
    for listed_path in listed_paths:
        if listed_path.type == pafs.FileType.File:
            if includes and os.path.splitext(listed_path.base_name)[-1] in includes:
                file_paths.append(
                    FsFileMetadata(fs, base_path, listed_path.path[common_prefix_len:], listed_path.base_name)
                )

    return file_paths


def list_full_sub_dirs(path: str, fs: pafs.FileSystem, excludes: list[str] | None) -> list[str]:
    sub_paths: list[pafs.FileInfo] = fs.get_file_info(pafs.FileSelector(path))
    sub_dirs = []
    for sub_path in sub_paths:
        if sub_path.type == pafs.FileType.Directory:
            if not excludes or (excludes and sub_path.base_name not in excludes):
                sub_dirs.append(sub_path.path)

    return sub_dirs


def list_leaf_dirs(path: str, fs: pafs.FileSystem) -> list[str]:
    sub_paths: list[pafs.FileInfo] = fs.get_file_info(pafs.FileSelector(path))
    leaf_dirs = []

    for sub_path in sub_paths:
        if sub_path.type == pafs.FileType.Directory:
            leaf_dirs.extend(list_leaf_dirs(sub_path.path, fs))

    # leaf directory
    if len(leaf_dirs) == 0:
        leaf_dirs.append(path)

    return leaf_dirs
