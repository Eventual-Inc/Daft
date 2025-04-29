"""!! JSON SOURCE WORK IN PROGRESS !!

### Experimental

These APIs are current implemented in python, and once stabilized will be
lowered to rust. Any public members should remain, but the protected members
will be internalized.

### Considerations

* pydantic integration?
* type annotated class as the schema?

"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Iterator

import jsonpath_ng

from daft.io._file import FileSource, FileSourceTask
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import DataType, Schema, schema


class JsonSource(DataSource):
    _source: FileSource
    _strategy: JsonStrategy
    _columns: JsonColumns

    def __init__(self, source: FileSource, strategy: JsonStrategy, columns: JsonColumns):
        self._source = source
        self._strategy = strategy
        self._columns = columns

    def get_name(self) -> str:
        return "JsonSource"

    def get_schema(self) -> Schema:
        return self._columns.get_schema()

    def get_tasks(self, pushdowns: Pushdowns) -> Iterator[JsonSourceTask]:
        # TODO APPLY LIMIT
        # TODO APPLY PROJECTIONS
        # TODO when rust, choose task variant here
        s = self._strategy
        c = self._columns
        return [JsonSourceTask(f, s, c) for f in self._source.get_tasks(pushdowns)]


@dataclass
class JsonSourceTask(DataSourceTask):
    _file: FileSourceTask
    _strategy: JsonStrategy
    _columns: JsonColumns

    def get_schema(self) -> Schema:
        return self._columns.get_schema()

    def get_batches(self) -> Iterator[RecordBatch]:
        if self._strategy == JsonStrategy.JSON:
            return self._get_batches_json()
        elif self._strategy == JsonStrategy.JSONL:
            return self._get_batches_jsonl()
        elif self._strategy == JsonStrategy.JSONS:
            return self._get_batches_jsons()
        else:
            raise ValueError(f"Unknown JsonStrategy: {self._strategy}")

    def _get_batches_json(self) -> Iterator[RecordBatch]:
        import orjson

        buf = JsonBuffer(self._columns)

        with open(self._file.path, "rb") as file:
            obj = orjson.loads(file.read())
            buf.append(obj)

        yield buf.to_record_batch()

    def _get_batches_jsonl(self) -> Iterator[RecordBatch]:
        import orjson

        buf = JsonBuffer(self._columns)

        with open(self._file.path, "rb") as file:
            for line in file:
                obj = orjson.loads(line)
                buf.append(obj)

        yield buf.to_record_batch()

    def _get_batches_jsons(self) -> Iterator[RecordBatch]:
        import ijson

        buf = JsonBuffer(self._columns)

        with open(self._file.path, "rb") as file:
            for obj in ijson.items(file, "item"):
                buf.append(obj)

        yield buf.to_record_batch()


class JsonBuffer:
    _columns: JsonColumns
    _paths: dict
    _arrays: dict[str, list]

    def __init__(self, columns: JsonColumns):
        self._columns = columns
        self._paths = {}
        self._arrays = {}
        for column in self._columns:
            k = column._name
            self._paths[k] = jsonpath_ng.parse(column._path)
            self._arrays[k] = []

    def append(self, obj: object):
        for column in self._columns:
            k = column._name
            p = self._paths[k]
            v = next(iter(p.find(obj)), None).value if p.find(obj) else None
            self._arrays[k].append(v)

    def to_record_batch(self) -> RecordBatch:
        dest = {}
        for column in self._columns:
            dest[column._name] = self._arrays[column._name]
        return RecordBatch.from_pydict(dest)


class JsonStrategy(Enum):
    """Defines the strategy for parsing some JSON-based file.

    Attributes:
        JSON: Single JSON value in the entire file.
        JSONL: Newline-delimited JSON, where each line contains a complete JSON value.
        JSONS: Incrementally parsed JSON stream, allowing for parsing of large JSON files.
    """

    JSON = auto()
    JSONL = auto()
    JSONS = auto()


class JsonMapping:
    _view: JsonColumns


class JsonColumns:
    _columns: list[JsonColumn]

    def __init__(self, columns: list[JsonColumn]) -> None:
        self._columns = columns

    def get_schema(self) -> JsonColumns:
        return schema({c._name: c._dtype for c in self._columns})

    def __iter__(self):
        return iter(self._columns)

    def __getitem__(self, index):
        return self._columns[index]

    def __len__(self):
        return len(self._columns)


class JsonColumn:
    _name: str
    _dtype: DataType
    _path: str

    def __init__(self) -> None:
        raise ValueError

    @classmethod
    def ordinality(cls, name: str) -> JsonColumn:
        raise NotImplementedError

    @classmethod
    def path(cls, name: str, dtype: DataType, path: str | None = None) -> JsonColumn:
        c = cls.__new__(cls)
        c._name = name
        c._dtype = dtype
        c._path = path or name
        return c

    @classmethod
    def exists(cls, name: str, path: str | None = None) -> JsonColumn:
        raise NotImplementedError
