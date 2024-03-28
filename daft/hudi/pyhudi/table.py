from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.fs as pafs

from daft.hudi.pyhudi.filegroup import BaseFile, FileGroup, FileSlice
from daft.hudi.pyhudi.timeline import Timeline
from daft.hudi.pyhudi.utils import (
    list_full_file_paths,
    list_full_sub_dirs,
    list_leaf_dirs,
)

# TODO(Shiyan): support base file in .orc
BASE_FILE_EXTENSIONS = [".parquet"]


@dataclass
class MetaClient:
    fs: pafs.FileSystem
    base_path: str
    timeline: Timeline | None

    def get_active_timeline(self) -> Timeline:
        if not self.timeline:
            self.timeline = Timeline(self.base_path, self.fs)
        return self.timeline

    def get_partition_paths(self, relative=True) -> list[str]:
        first_level_full_partition_paths = list_full_sub_dirs(self.base_path, self.fs, excludes=[".hoodie"])
        partition_paths = []
        common_prefix_len = len(self.base_path) + 1 if relative else 0
        for p in first_level_full_partition_paths:
            partition_paths.extend(list_leaf_dirs(p, self.fs, common_prefix_len))
        return partition_paths

    def get_full_partition_path(self, partition_path: str) -> str:
        return os.path.join(self.base_path, partition_path)

    def get_file_groups(self, partition_path: str) -> list[FileGroup]:
        full_partition_path = self.get_full_partition_path(partition_path)
        base_file_metadata = list_full_file_paths(full_partition_path, self.fs, includes=BASE_FILE_EXTENSIONS)
        fg_id_to_base_files = defaultdict(list)
        for metadata in base_file_metadata:
            base_file = BaseFile(metadata)
            fg_id_to_base_files[base_file.file_group_id].append(base_file)
        file_groups = []
        for fg_id, base_files in fg_id_to_base_files.items():
            file_group = FileGroup(fg_id, partition_path)
            for base_file in base_files:
                file_group.add_base_file(base_file)
            file_groups.append(file_group)
        return file_groups


@dataclass(init=False)
class FileSystemView:
    def __init__(self, meta_client: MetaClient):
        self.meta_client = meta_client
        self.partition_to_file_groups: dict[str, list[FileGroup]] = {}
        self._load_partitions()

    def _load_partitions(self):
        partition_paths = self.meta_client.get_partition_paths()
        for partition_path in partition_paths:
            self._load_partition(partition_path)

    def _load_partition(self, partition_path: str):
        file_groups = self.meta_client.get_file_groups(partition_path)
        self.partition_to_file_groups[partition_path] = file_groups

    def get_latest_file_slices(self) -> list[FileSlice]:
        file_slices = []
        for file_groups in self.partition_to_file_groups.values():
            for file_group in file_groups:
                file_slice = file_group.get_latest_file_slice()
                if file_slice is not None:
                    file_slices.append(file_slice)

        return file_slices


@dataclass(init=False)
class HudiTableProps:
    def __init__(self, fs: pafs.FileSystem, table_uri: str):
        self._props = {}
        hoodie_properties_file = os.path.join(table_uri, ".hoodie", "hoodie.properties")
        with fs.open_input_file(hoodie_properties_file) as f:
            lines = f.readall().decode("utf-8").splitlines()
            for line in lines:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key, value = line.split("=")
                self._props[key] = value

    @property
    def name(self) -> str:
        return self._props["hoodie.table.name"]

    @property
    def partition_fields(self) -> list[str]:
        return self._props["hoodie.table.partition.fields"]


@dataclass
class HudiTableMetadata:

    files_metadata: pa.RecordBatch
    colstats_min_values: pa.RecordBatch
    colstats_max_values: pa.RecordBatch


@dataclass(init=False)
class HudiTable:
    def __init__(self, fs: pafs.FileSystem, table_uri: str):
        self._meta_client = MetaClient(fs, table_uri, timeline=None)
        self._props = HudiTableProps(fs, table_uri)

    def latest_table_metadata(self) -> HudiTableMetadata:
        file_slices = FileSystemView(self._meta_client).get_latest_file_slices()
        files_metadata = []
        min_vals_arr = []
        max_vals_arr = []
        for file_slice in file_slices:
            files_metadata.append(file_slice.files_metadata)
            min_vals, max_vals = file_slice.colstats_min_max
            min_vals_arr.append(min_vals)
            max_vals_arr.append(max_vals)
        metadata_arrays = [pa.array(column) for column in list(zip(*files_metadata))]
        min_value_arrays = [pa.array(column) for column in list(zip(*min_vals_arr))]
        max_value_arrays = [pa.array(column) for column in list(zip(*max_vals_arr))]
        return HudiTableMetadata(
            pa.RecordBatch.from_arrays(metadata_arrays, schema=FileSlice.FILES_METADATA_SCHEMA),
            pa.RecordBatch.from_arrays(min_value_arrays, schema=self.schema),
            pa.RecordBatch.from_arrays(max_value_arrays, schema=self.schema),
        )

    @property
    def table_uri(self) -> str:
        return self._meta_client.base_path

    @property
    def schema(self) -> pa.Schema:
        return self._meta_client.get_active_timeline().get_latest_commit_schema()

    @property
    def is_partitioned(self) -> bool:
        return self._props.partition_fields == ""

    @property
    def props(self) -> HudiTableProps:
        return self._props
