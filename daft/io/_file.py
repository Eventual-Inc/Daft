from __future__ import annotations

import glob
import os

from dataclasses import dataclass
from typing import Iterator

from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import DataType, Schema, schema


class FileSource(DataSource):
    """The FileSource produces tasks from glob matching."""
    _pattern: str

    def __init__(self, pattern: str):
        self._pattern = pattern

    def get_name(self) -> str:
        return "FileSource"

    def get_schema(self) -> Schema:
        return schema(
            {
                "path": DataType.string(),
                "size": DataType.uint64(),
            }
        )

    def get_tasks(self, pushdowns: Pushdowns) -> Iterator[DataSourceTask]:
        for path in glob.glob(self._pattern, recursive=True):
            size = os.path.getsize(path)
            yield FileSourceTask(path, size)


@dataclass
class FileSourceTask(DataSourceTask):
    path: str
    size: int

    def get_schema(self) -> Schema:
        return schema(
            {
                "path": DataType.string(),
                "size": DataType.uint64(),
            }
        )

    def get_batches(self) -> Iterator[RecordBatch]:
        yield RecordBatch.from_pydict({
            "path": [self.path ],
            "size": [self.size ],
        })
