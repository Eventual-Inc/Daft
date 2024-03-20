from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum

import pyarrow as pa
import pyarrow.parquet as pq
from fsspec import AbstractFileSystem


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
    fs: AbstractFileSystem
    instants: list[Instant]

    def __init__(self, base_path: str, fs: AbstractFileSystem):
        self.base_path = base_path
        self.fs = fs
        self._load_completed_commit_instants()

    def _load_completed_commit_instants(self):
        timeline_path = self.fs.sep.join([self.base_path, ".hoodie"])
        action = "commit"
        ext = ".commit"
        instants = []
        for timeline_file_path in filter(lambda p: p.endswith(ext), self.fs.ls(timeline_path, detail=False)):
            timestamp = timeline_file_path[len(timeline_path) + 1 : -len(ext)]
            instants.append(Instant(state=State.COMPLETED, action=action, timestamp=timestamp))
        self.instants = sorted(instants)

    def get_latest_commit_metadata(self) -> dict:
        latest_instant_file_path = self.fs.sep.join([self.base_path, ".hoodie", self.instants[-1].file_name])
        with self.fs.open(latest_instant_file_path) as f:
            return json.load(f)

    def get_latest_commit_schema(self) -> pa.Schema:
        latest_commit_metadata = self.get_latest_commit_metadata()
        _, write_stats = next(iter(latest_commit_metadata["partitionToWriteStats"].items()))
        base_file_path = self.fs.sep.join([self.base_path, write_stats[0]["path"]])
        return pq.read_schema(base_file_path)
