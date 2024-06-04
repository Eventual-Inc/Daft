from __future__ import annotations

import os
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from daft.filesystem import join_path


@dataclass(init=False)
class FsFileMetadata:
    def __init__(self, fs: pafs.FileSystem, base_path: str, path: str, base_name: str):
        self.base_path = base_path
        self.path = path
        self.base_name = base_name
        with fs.open_input_file(join_path(fs, base_path, path)) as f:
            metadata = pq.read_metadata(f)
        self.size = metadata.serialized_size
        self.num_records = metadata.num_rows
        self.schema = FsFileMetadata._extract_arrow_schema(metadata)
        self.colstats_schema, self.min_values, self.max_values = FsFileMetadata._extract_min_max(metadata)

    @staticmethod
    def _get_colstats_schema(original_schema: pa.Schema, extracted_field_names: set[str]) -> pa.Schema:
        extracted_fields = []
        for i in range(len(original_schema)):
            f = original_schema.field(i)
            if f.name in extracted_field_names:
                extracted_fields.append(f)
        return pa.schema(extracted_fields)

    @staticmethod
    def _extract_arrow_schema(metadata: pa.FileMetadata) -> pa.Schema:
        return metadata.schema.to_arrow_schema().remove_metadata()

    @staticmethod
    def _is_nested(path_in_schema: str) -> bool:
        return path_in_schema.find(".") != -1

    @staticmethod
    def _extract_min_max(metadata: pq.FileMetaData):
        num_columns = metadata.num_columns
        num_row_groups = metadata.num_row_groups
        min_vals = [None] * num_columns
        max_vals = [None] * num_columns
        colstats_field_names = set()
        for rg in range(num_row_groups):
            row_group = metadata.row_group(rg)
            for col in range(num_columns):
                column = row_group.column(col)
                if (
                    column.is_stats_set
                    and column.statistics.has_min_max
                    and not FsFileMetadata._is_nested(column.path_in_schema)
                ):
                    colstats_field_names.add(column.path_in_schema)
                    if min_vals[col] is None or column.statistics.min < min_vals[col]:
                        min_vals[col] = column.statistics.min
                    if max_vals[col] is None or column.statistics.max > max_vals[col]:
                        max_vals[col] = column.statistics.max
        filtered_min_vals = list(filter(lambda v: v is not None, min_vals))
        filtered_max_vals = list(filter(lambda v: v is not None, max_vals))

        colstats_schema = FsFileMetadata._get_colstats_schema(metadata.schema.to_arrow_schema(), colstats_field_names)
        assert len(colstats_schema) == len(filtered_min_vals) == len(filtered_max_vals)
        return colstats_schema, filtered_min_vals, filtered_max_vals


def list_relative_file_paths(
    base_path: str, sub_path: str, fs: pafs.FileSystem, includes: list[str] | None
) -> list[FsFileMetadata]:
    listed_paths: list[pafs.FileInfo] = fs.get_file_info(pafs.FileSelector(join_path(fs, base_path, sub_path)))
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
