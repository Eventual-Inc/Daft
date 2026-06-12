from __future__ import annotations

from collections.abc import AsyncIterator

import pyarrow as pa
import pytest

import daft
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema


class _TableTask(DataSourceTask):
    def __init__(self, table: pa.Table) -> None:
        self._table = table

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._table.schema)

    async def read(self) -> AsyncIterator[RecordBatch]:
        for batch in self._table.to_batches():
            yield RecordBatch.from_arrow_record_batches([batch], self._table.schema)


class _SimpleDataSource(DataSource):
    def __init__(self, rows: int = 2) -> None:
        self._rows = rows

    @property
    def name(self) -> str:
        return "simple"

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(pa.schema([("id", pa.int64()), ("value", pa.string())]))

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        del pushdowns
        yield _TableTask(pa.table({"id": list(range(self._rows)), "value": [f"v{i}" for i in range(self._rows)]}))


class _OtherDataSource(DataSource):
    def __init__(self, x: int = 1) -> None:
        self._x = x

    @property
    def name(self) -> str:
        return "other"

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(pa.schema([("x", pa.int64())]))

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        del pushdowns
        yield _TableTask(pa.table({"x": [self._x]}))


def test_attach_and_read_source():
    with daft.session() as sess:
        source = _SimpleDataSource(rows=3)
        assert sess.attach_data_source(source) is source

        assert sess.list_data_sources() == ["simple"]
        assert sess.has_data_source("simple")
        assert sess.get_data_source("simple") is source
        assert sess.read_source("simple").to_pydict() == {"id": [0, 1, 2], "value": ["v0", "v1", "v2"]}


def test_attach_data_source_with_alias():
    with daft.session() as sess:
        sess.attach_data_source(_SimpleDataSource(), alias="my_source")

        assert sess.has_data_source("my_source")
        assert not sess.has_data_source("simple")
        assert sess.read_source("my_source").to_pydict() == {"id": [0, 1], "value": ["v0", "v1"]}


def test_attach_dispatches_data_source():
    with daft.session() as sess:
        sess.attach(_SimpleDataSource())

        assert sess.has_data_source("simple")


def test_attach_duplicate_data_source_errors():
    with daft.session() as sess:
        sess.attach_data_source(_SimpleDataSource())

        with pytest.raises(Exception, match="already exists"):
            sess.attach_data_source(_SimpleDataSource())


def test_detach_data_source():
    with daft.session() as sess:
        sess.attach_data_source(_SimpleDataSource())
        sess.detach_data_source("simple")

        assert sess.list_data_sources() == []
        with pytest.raises(Exception, match="not found"):
            sess.read_source("simple")


def test_detach_unknown_data_source_errors():
    with daft.session() as sess, pytest.raises(Exception, match="not found"):
        sess.detach_data_source("missing")


def test_get_unknown_data_source_errors():
    with daft.session() as sess, pytest.raises(Exception, match="not found"):
        sess.get_data_source("missing")


def test_list_data_sources_pattern():
    with daft.session() as sess:
        sess.attach_data_source(_SimpleDataSource())
        sess.attach_data_source(_OtherDataSource())

        assert sorted(sess.list_data_sources()) == ["other", "simple"]
        assert sess.list_data_sources("sim") == ["simple"]


def test_data_source_functions_use_current_session():
    with daft.session():
        daft.attach_data_source(_SimpleDataSource())

        assert daft.list_data_sources() == ["simple"]
        assert daft.has_data_source("simple")
        assert daft.read_source("simple").to_pydict() == {"id": [0, 1], "value": ["v0", "v1"]}

        daft.detach_data_source("simple")
        assert not daft.has_data_source("simple")


def test_data_sources_are_session_scoped():
    with daft.session() as outer:
        outer.attach_data_source(_SimpleDataSource())

        with daft.session() as inner:
            assert inner.list_data_sources() == []
            inner.attach_data_source(_OtherDataSource())
            assert inner.list_data_sources() == ["other"]

        assert outer.list_data_sources() == ["simple"]
