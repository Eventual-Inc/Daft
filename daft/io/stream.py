from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterator

from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema, DataType, schema


def stream(path: str) -> StreamSource:
    pass


class StreamSource(DataSource, ABC):
    pass


class GlobSource(StreamSource):
    """The GlobSource produces tasks from a glob path match."""

    _pattern: str

    def __init__(self, pattern: str):
        self._pattern = pattern

    def name(self) -> str:
        return "GlobSource"

    def schema(self) -> Schema:
        return schema({
            "path": DataType.string(),
            "size": DataType.uint64(),
        })

    def get_tasks(self, pushdowns: Pushdowns) -> Iterator[DataSourceTask]:
        raise NotImplementedError


@dataclass
class GlobSourceTask(DataSourceTask):
    path: str
    size: int

    def schema(self) -> Schema:
        return schema({
            "path": DataType.string(),
            "size": DataType.uint64(),
        })

    def get_batches(self) -> Iterator[RecordBatch]:
        raise NotImplementedError




