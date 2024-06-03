from __future__ import annotations

import json
import os
from dataclasses import dataclass
from enum import Enum

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from daft.filesystem import join_path


class State(Enum):
    REQUESTED = 0
    INFLIGHT = 1
    COMPLETED = 2


@dataclass
class Instant:
    state: State
    action: str
    timestamp: str

    @property
    def file_name(self):
        state = "" if self.state == State.COMPLETED else f".{self.state.name.lower()}"
        return f"{self.timestamp}.{self.action}{state}"

    def __lt__(self, other: Instant):
        return [self.timestamp, self.state] < [other.timestamp, other.state]


@dataclass(init=False)
class Timeline:
    base_path: str
    fs: pafs.FileSystem
    completed_commit_instants: list[Instant]

    def __init__(self, base_path: str, fs: pafs.FileSystem):
        self.base_path = base_path
        self.fs = fs
        self._load_completed_commit_instants()

    @property
    def has_completed_commit(self) -> bool:
        return len(self.completed_commit_instants) > 0

    def _load_completed_commit_instants(self):
        timeline_path = join_path(self.fs, self.base_path, ".hoodie")
        write_action_exts = {".commit"}
        commit_instants = []
        for file_info in self.fs.get_file_info(pafs.FileSelector(timeline_path)):
            ext = os.path.splitext(file_info.base_name)[1]
            if ext in write_action_exts:
                timestamp = file_info.base_name[: -len(ext)]
                instant = Instant(state=State.COMPLETED, action=ext[1:], timestamp=timestamp)
                commit_instants.append(instant)
        self.completed_commit_instants = sorted(commit_instants)

    def get_latest_commit_metadata(self) -> dict:
        if not self.has_completed_commit:
            return {}
        latest_instant_file_path = join_path(
            self.fs, self.base_path, ".hoodie", self.completed_commit_instants[-1].file_name
        )
        with self.fs.open_input_file(latest_instant_file_path) as f:
            return json.load(f)

    def get_latest_commit_schema(self) -> pa.Schema:
        latest_commit_metadata = self.get_latest_commit_metadata()
        if not latest_commit_metadata.get("partitionToWriteStats"):
            return pa.schema([])

        _, write_stats = next(iter(latest_commit_metadata["partitionToWriteStats"].items()))
        base_file_path = join_path(self.fs, self.base_path, write_stats[0]["path"])
        with self.fs.open_input_file(base_file_path) as f:
            return pq.read_schema(f)
