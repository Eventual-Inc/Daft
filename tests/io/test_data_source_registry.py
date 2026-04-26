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
    @classmethod
    def name(cls) -> str:
        return "simple"

    def __init__(self, rows: int = 2) -> None:
        self._rows = rows

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(pa.schema([("id", pa.int64()), ("value", pa.string())]))

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        del pushdowns
        yield _TableTask(pa.table({"id": list(range(self._rows)), "value": [f"v{i}" for i in range(self._rows)]}))


class _PropertyNameDataSource(DataSource):
    def __init__(self, x: int = 1) -> None:
        self._x = x

    @property
    def name(self) -> str:
        return "property_name"

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(pa.schema([("x", pa.int64())]))

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        del pushdowns
        yield _TableTask(pa.table({"x": [self._x]}))


class _RequiredArgPropertyNameDataSource(DataSource):
    def __init__(self, x: int) -> None:
        self._x = x

    @property
    def name(self) -> str:
        return "required_arg_property_name"

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(pa.schema([("x", pa.int64())]))

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        del pushdowns
        yield _TableTask(pa.table({"x": [self._x]}))


def test_register_and_read_source():
    with daft.session() as sess:
        sess.register_data_source(_SimpleDataSource)

        assert sess.list_data_sources() == ["simple"]
        assert sess.get_data_source("simple") is _SimpleDataSource
        assert sess.read_source("simple", rows=3).to_pydict() == {"id": [0, 1, 2], "value": ["v0", "v1", "v2"]}


def test_data_sources_module_uses_current_session():
    with daft.session():
        daft.data_sources.register(_SimpleDataSource)

        assert daft.data_sources.list() == ["simple"]
        assert daft.data_sources.read("simple", rows=1).to_pydict() == {"id": [0], "value": ["v0"]}
        assert daft.read_source("simple", rows=1).to_pydict() == {"id": [0], "value": ["v0"]}


def test_register_duplicate_requires_replace():
    with daft.session() as sess:
        sess.register_data_source(_SimpleDataSource)

        with pytest.raises(ValueError, match="already registered"):
            sess.register_data_source(_SimpleDataSource)

        sess.register_data_source(_SimpleDataSource, replace=True)


def test_property_name_source_can_be_registered_when_default_constructible():
    with daft.session() as sess:
        sess.register_data_source(_PropertyNameDataSource)

        assert sess.read_source("property_name", x=2).to_pydict() == {"x": [2]}


def test_property_name_source_with_required_args_uses_explicit_registration_name():
    with daft.session() as sess:
        with pytest.raises(ValueError, match="pass name= explicitly"):
            sess.register_data_source(_RequiredArgPropertyNameDataSource)

        sess.register_data_source(_RequiredArgPropertyNameDataSource, name="required_arg_property_name")
        assert sess.read_source("required_arg_property_name", x=3).to_pydict() == {"x": [3]}


def test_unregister_source():
    with daft.session() as sess:
        sess.register_data_source(_SimpleDataSource)
        sess.unregister_data_source("simple")

        assert sess.list_data_sources() == []
        with pytest.raises(ValueError, match="not registered"):
            sess.read_source("simple")


def test_get_unknown_source_errors():
    with daft.session() as sess:
        with pytest.raises(ValueError, match="not registered"):
            sess.get_data_source("missing")


def test_data_source_registry_is_session_scoped():
    with daft.session() as outer:
        outer.register_data_source(_SimpleDataSource)

        with daft.session() as inner:
            assert inner.list_data_sources() == []
            inner.register_data_source(_PropertyNameDataSource)
            assert inner.list_data_sources() == ["property_name"]

        assert outer.list_data_sources() == ["simple"]
