from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pytest

from daft.io.lance.lance_data_sink import LanceDataSink
from daft.recordbatch import MicroPartition
from daft.schema import Schema


class FakeLanceModule:
    def __init__(self):
        self.calls = []
        # fragment namespace with write_fragments
        self.fragment = SimpleNamespace(write_fragments=self.write_fragments)

    def dataset(self, *args, **kwargs):
        # Simulate non-existent dataset for create mode
        return None

    def write_fragments(self, *args, **kwargs):
        # Back-compat if called as lance.fragment.write_fragments(table, ...)
        raise NotImplementedError

    def __getattr__(self, name):
        # Allow access to LanceOperation, LanceDataset in other contexts if needed
        return SimpleNamespace()

    # Actual function used via fragment.write_fragments
    def _record_write(self, table: pa.Table, **kwargs):
        self.calls.append(table)
        # Return a fake list of fragment metadata objects
        return [SimpleNamespace(path="/tmp/fake.lance", rows=table.num_rows)]

    # Bind the method to fragment namespace during init
    def _bind(self):
        self.fragment.write_fragments = lambda table, *a, **kw: self._record_write(table, **kw)
        return self


@pytest.fixture
def schema() -> Schema:
    return Schema.from_pyarrow_schema(pa.schema([pa.field("a", pa.int64())]))


def _make_mp(n: int) -> MicroPartition:
    return MicroPartition.from_pydict({"a": list(range(n))})


def test_accumulate_small_micropartitions(schema, tmp_path):
    fake = FakeLanceModule()._bind()

    with patch.object(LanceDataSink, "_import_lance", return_value=fake):
        sink = LanceDataSink(uri=str(tmp_path / "tbl"), schema=schema, mode="create", max_rows_per_file=25)
        mps = [_make_mp(10), _make_mp(20), _make_mp(30)]
        results = list(sink.write(iter(mps)))

    # Expect two writes: 10+20 -> 30, and 30 alone
    assert len(fake.calls) == 2
    assert [t.num_rows for t in fake.calls] == [30, 30]
    assert sum(r.rows_written for r in results) == 60


def test_flush_remaining_at_end(schema, tmp_path):
    fake = FakeLanceModule()._bind()

    with patch.object(LanceDataSink, "_import_lance", return_value=fake):
        sink = LanceDataSink(uri=str(tmp_path / "tbl"), schema=schema, mode="create", max_rows_per_file=25)
        mps = [_make_mp(10), _make_mp(5)]
        results = list(sink.write(iter(mps)))

    # Expect one write with 15 rows
    assert len(fake.calls) == 1
    assert fake.calls[0].num_rows == 15
    assert sum(r.rows_written for r in results) == 15


def test_large_micropartition_writes_directly(schema, tmp_path):
    fake = FakeLanceModule()._bind()

    with patch.object(LanceDataSink, "_import_lance", return_value=fake):
        sink = LanceDataSink(uri=str(tmp_path / "tbl"), schema=schema, mode="create", max_rows_per_file=25)
        mps = [_make_mp(30)]
        results = list(sink.write(iter(mps)))

    # Expect one direct write of 30 rows
    assert len(fake.calls) == 1
    assert fake.calls[0].num_rows == 30
    assert sum(r.rows_written for r in results) == 30


def test_no_accumulation_when_param_missing(schema, tmp_path):
    fake = FakeLanceModule()._bind()

    with patch.object(LanceDataSink, "_import_lance", return_value=fake):
        sink = LanceDataSink(uri=str(tmp_path / "tbl"), schema=schema, mode="create")
        mps = [_make_mp(10), _make_mp(5)]
        results = list(sink.write(iter(mps)))

    # Expect two separate writes corresponding to each micropartition
    assert len(fake.calls) == 2
    assert [t.num_rows for t in fake.calls] == [10, 5]
    assert [r.rows_written for r in results] == [10, 5]
