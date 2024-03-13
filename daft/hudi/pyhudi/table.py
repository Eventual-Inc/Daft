from collections import defaultdict
from dataclasses import dataclass
from typing import List, Optional, Dict

import pyarrow as pa
from fsspec import AbstractFileSystem

from daft.hudi.pyhudi.filegroup import FileGroup, BaseFile, FileSlice
from daft.hudi.pyhudi.timeline import Timeline
from daft.hudi.pyhudi.utils import get_full_sub_dirs, get_leaf_dirs, get_full_file_paths

BASE_FILE_EXTENSIONS = [".parquet"]


@dataclass
class MetaClient:
    fs: AbstractFileSystem
    base_path: str
    timeline: Timeline = None

    def get_active_timeline(self) -> Timeline:
        if not self.timeline:
            self.timeline = Timeline([])
        return self.timeline

    def reload_active_timeline(self) -> Timeline:
        self.timeline = Timeline([])
        return self.timeline

    def get_partition_paths(self, relative=True) -> List[str]:
        first_level_full_partition_paths = get_full_sub_dirs(self.base_path, self.fs, excludes=['.hoodie'])
        partition_paths = []
        common_prefix_len = len(self.base_path) + 1 if relative else 0
        for p in first_level_full_partition_paths:
            partition_paths.extend(get_leaf_dirs(p, self.fs, common_prefix_len))
        return partition_paths

    def get_full_partition_path(self, partition_path: str) -> str:
        return self.fs.sep.join(self.base_path, partition_path)

    def get_file_groups(self, partition_path: str) -> List[FileGroup]:
        full_partition_path = self.get_full_partition_path(partition_path)
        base_file_paths = get_full_file_paths(full_partition_path, self.fs, includes=BASE_FILE_EXTENSIONS)
        fg_id_to_base_files = defaultdict(list)
        for p in base_file_paths:
            base_file = BaseFile(p, self.fs)
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
        self.partition_to_file_groups = {}
        self._load_partitions()

    def _load_partitions(self):
        partition_paths = self.meta_client.get_partition_paths()
        for partition_path in partition_paths:
            self._load_partition(partition_path)

    def _load_partition(self, partition_path: str):
        file_groups = self.meta_client.get_file_groups(partition_path)
        self.partition_to_file_groups[partition_path] = file_groups

    def get_latest_file_slices(self) -> List[FileSlice]:
        file_slices = []
        for file_groups in self.partition_to_file_groups.values():
            for file_group in file_groups:
                file_slices.append(file_group.get_latest_file_slice())

        return file_slices


@dataclass
class HudiTableProps:
    name: str
    schema: str
    record_keys: List[str]
    partition_fields: List[str]

    def py_arrow_schema(self) -> pa.Schema:
        return None


@dataclass(init=False)
class HudiTable:
    def __init__(self, table_uri: str, storage_options: Optional[Dict[str, str]] = None):
        self.table_uri = table_uri
        self._storage_options = storage_options

    def get_latest_file_slices(self) -> pa.RecordBatch:
        return None

    def props(self) -> HudiTableProps:
        return HudiTableProps()
