from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa
from sortedcontainers import SortedDict

from daft.hudi.pyhudi.utils import FsFileMetadata


@dataclass(init=False)
class BaseFile:
    def __init__(self, fs_metadata: FsFileMetadata):
        self.metadata = fs_metadata
        file_name = fs_metadata.base_name
        self.file_name = file_name
        file_group_id, _, commit_time_ext = file_name.split("_")
        self.file_group_id = file_group_id
        self.commit_time = commit_time_ext.split(".")[0]

    @property
    def path(self) -> str:
        return self.metadata.path

    @property
    def size(self) -> int:
        return self.metadata.size

    @property
    def num_records(self) -> int:
        return self.metadata.num_records

    @property
    def schema(self) -> pa.Schema:
        return self.metadata.schema

    @property
    def colstats_schema(self) -> pa.Schema:
        return self.metadata.colstats_schema

    @property
    def min_values(self):
        return self.metadata.min_values

    @property
    def max_values(self):
        return self.metadata.max_values


@dataclass
class FileSlice:
    FILES_METADATA_SCHEMA = pa.schema(
        [
            ("path", pa.string()),
            ("size_bytes", pa.uint32()),
            ("num_records", pa.uint32()),
            ("partition_path", pa.string()),
        ]
    )

    file_group_id: str
    partition_path: str
    base_instant_time: str
    base_file: BaseFile

    @property
    def files_metadata(self):
        return self.base_file.path, self.base_file.size, self.base_file.num_records, self.partition_path

    @property
    def colstats_min_max(self):
        return self.base_file.min_values, self.base_file.max_values

    @property
    def colstats_schema(self):
        return self.base_file.colstats_schema


@dataclass
class FileGroup:
    file_group_id: str
    partition_path: str
    file_slices: SortedDict[str, FileSlice] = field(default_factory=SortedDict)

    def add_base_file(self, base_file: BaseFile):
        ct = base_file.commit_time
        if ct in self.file_slices:
            self.file_slices.get(ct).base_file = base_file
        else:
            self.file_slices[ct] = FileSlice(self.file_group_id, self.partition_path, ct, base_file)

    def get_latest_file_slice(self) -> FileSlice | None:
        if not self.file_slices:
            return None

        return self.file_slices.peekitem(-1)[1]
