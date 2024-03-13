from dataclasses import dataclass, field

from fsspec import AbstractFileSystem
from sortedcontainers import SortedDict


@dataclass(init=False)
class BaseFile:
    def __init__(self, path: str, fs: AbstractFileSystem):
        self.path = path
        file_name = path.rsplit(fs.sep, 1)[-1]
        self.file_name = file_name
        file_group_id, _, commit_time_ext = file_name.split("_")
        self.file_group_id = file_group_id
        self.commit_time = commit_time_ext.split(".")[0]


@dataclass
class FileSlice:
    file_group_id: str
    base_instant_time: str
    base_file: BaseFile = None


@dataclass
class FileGroup:
    file_group_id: str
    partition_path: str
    file_slices: SortedDict[str, FileSlice] = field(default_factory=SortedDict)

    def add_base_file(self, base_file: BaseFile):
        if base_file.commit_time not in self.file_slices:
            self.file_slices[base_file.commit_time] = FileSlice(self.file_group_id, base_file.commit_time)

        self.file_slices.get(base_file.commit_time).base_file = base_file

    def get_latest_file_slice(self) -> FileSlice | None:
        if not self.file_slices:
            return None

        return self.file_slices.peekitem(-1)[1]
