from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa
from fsspec import AbstractFileSystem
from sortedcontainers import SortedDict


@dataclass(init=False)
class BaseFile:
    def __init__(self, path: str, size: int, num_records: int, fs: AbstractFileSystem):
        self.path = path
        self.size = size
        self.num_records = num_records
        file_name = path.rsplit(fs.sep, 1)[-1]
        self.file_name = file_name
        file_group_id, _, commit_time_ext = file_name.split("_")
        self.file_group_id = file_group_id
        self.commit_time = commit_time_ext.split(".")[0]


@dataclass
class FileSlice:
    METADATA_SCHEMA = pa.schema(
        [
            ("path", pa.string()),
            ("size", pa.uint32()),
            ("num_records", pa.uint32()),
            ("partition_path", pa.string()),
            # TODO(Shiyan): support column stats
        ]
    )

    file_group_id: str
    partition_path: str
    base_instant_time: str
    base_file: BaseFile | None

    @property
    def metadata(self):
        return (self.base_file.path, self.base_file.size, self.base_file.num_records, self.partition_path)


@dataclass
class FileGroup:
    file_group_id: str
    partition_path: str
    file_slices: SortedDict[str, FileSlice] = field(default_factory=SortedDict)

    def add_base_file(self, base_file: BaseFile):
        ct = base_file.commit_time
        if ct not in self.file_slices:
            self.file_slices[ct] = FileSlice(self.file_group_id, self.partition_path, ct, base_file=None)

        self.file_slices.get(ct).base_file = base_file

    def get_latest_file_slice(self) -> FileSlice | None:
        if not self.file_slices:
            return None

        return self.file_slices.peekitem(-1)[1]
