from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pytest

import daft
from daft.io import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema
from daft.subscribers import Subscriber

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from daft.io.pushdowns import Pushdowns
    from daft.subscribers.events import (
        ExecutionFinished,
        ExecutionStarted,
        OperatorFinished,
        OperatorStarted,
        OptimizationCompleted,
        QueryFinished,
        QueryStarted,
        Stats,
    )


class StatsCollector(Subscriber):
    """Records the last value of every stat, keyed by query id and node id."""

    def __init__(self) -> None:
        self.query_ids: list[str] = []
        self.node_stats: defaultdict[str, defaultdict[int, dict[str, Any]]] = defaultdict(lambda: defaultdict(dict))
        self.node_names: defaultdict[str, dict[int, str]] = defaultdict(dict)

    def on_query_started(self, event: QueryStarted) -> None:
        self.query_ids.append(event.query_id)

    def on_query_finished(self, event: QueryFinished) -> None:
        pass

    def on_optimization_completed(self, event: OptimizationCompleted) -> None:
        pass

    def on_execution_started(self, event: ExecutionStarted) -> None:
        pass

    def on_operator_start(self, event: OperatorStarted) -> None:
        self.node_names[event.query_id][event.node_id] = event.name

    def on_stats(self, event: Stats) -> None:
        for node_id, stats in event.stats.items():
            for stat_name, (_, stat_value) in stats.items():
                self.node_stats[event.query_id][node_id][stat_name] = stat_value

    def on_operator_end(self, event: OperatorFinished) -> None:
        pass

    def on_execution_finished(self, event: ExecutionFinished) -> None:
        pass

    def scan_node_stats(self, query_id: str) -> list[dict[str, Any]]:
        return [stats for stats in self.node_stats[query_id].values() if "bytes.read" in stats]


def _batch(start: int, stop: int) -> RecordBatch:
    return RecordBatch.from_arrow_table(pa.table({"x": list(range(start, stop))}))


class CountingTask(DataSourceTask):
    """Task that yields batches and reports how many bytes it 'read' via stats()."""

    def __init__(self, schema: Schema, batches: list[tuple[int, int]], bytes_per_batch: int, extra: dict | None):
        self._schema = schema
        self._batches = batches
        self._bytes_per_batch = bytes_per_batch
        self._bytes_read = 0
        self._requests = 0
        self._extra = extra or {}
        self.stats_calls = 0

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        for start, stop in self._batches:
            self._requests += 1
            self._bytes_read += self._bytes_per_batch
            yield _batch(start, stop)

    def stats(self) -> dict[str, int]:
        self.stats_calls += 1
        return {"bytes.read": self._bytes_read, "io.requests": self._requests, **self._extra}


class CountingSource(DataSource):
    def __init__(self, tasks: list[list[tuple[int, int]]], bytes_per_batch: int = 1000, extra: dict | None = None):
        self._schema = Schema.from_pyarrow_schema(pa.schema([("x", pa.int64())]))
        self._tasks = tasks
        self._bytes_per_batch = bytes_per_batch
        self._extra = extra

    @property
    def name(self) -> str:
        return "counting_source"

    @property
    def schema(self) -> Schema:
        return self._schema

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        for batches in self._tasks:
            yield CountingTask(self._schema, batches, self._bytes_per_batch, self._extra)


class NoStatsTask(DataSourceTask):
    def __init__(self, schema: Schema):
        self._schema = schema

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        yield _batch(0, 3)


class NoStatsSource(DataSource):
    def __init__(self):
        self._schema = Schema.from_pyarrow_schema(pa.schema([("x", pa.int64())]))

    @property
    def name(self) -> str:
        return "no_stats_source"

    @property
    def schema(self) -> Schema:
        return self._schema

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        yield NoStatsTask(self._schema)


def test_data_source_task_stats_reported_on_scan_node():
    subscriber = StatsCollector()
    daft.attach_subscriber("data_source_stats", subscriber)
    try:
        # Two tasks, three and two batches, 1000 bytes per batch => 5000 bytes for the scan node.
        source = CountingSource(tasks=[[(0, 2), (2, 4), (4, 6)], [(6, 8), (8, 10)]], bytes_per_batch=1000)
        df = source.read().collect()
        assert sorted(df.to_pydict()["x"]) == list(range(10))

        query_id = subscriber.query_ids[-1]
        scan_stats = subscriber.scan_node_stats(query_id)
        assert len(scan_stats) == 1, f"expected exactly one scan node with bytes.read, got {scan_stats}"
        assert scan_stats[0]["bytes.read"] == 5000
        assert scan_stats[0]["io.requests"] == 5
        assert scan_stats[0]["rows.out"] == 10
    finally:
        daft.detach_subscriber("data_source_stats")


def test_data_source_task_stats_are_cumulative_not_double_counted():
    """stats() is polled after every batch; the engine must fold in deltas, not re-add totals."""
    subscriber = StatsCollector()
    daft.attach_subscriber("data_source_stats", subscriber)
    try:
        source = CountingSource(tasks=[[(0, 1), (1, 2), (2, 3), (3, 4)]], bytes_per_batch=7)
        source.read().collect()

        query_id = subscriber.query_ids[-1]
        scan_stats = subscriber.scan_node_stats(query_id)
        assert len(scan_stats) == 1
        assert scan_stats[0]["bytes.read"] == 4 * 7
        assert scan_stats[0]["io.requests"] == 4
    finally:
        daft.detach_subscriber("data_source_stats")


def test_data_source_task_default_stats_is_noop():
    subscriber = StatsCollector()
    daft.attach_subscriber("data_source_stats", subscriber)
    try:
        df = NoStatsSource().read().collect()
        assert df.to_pydict() == {"x": [0, 1, 2]}

        query_id = subscriber.query_ids[-1]
        scan_stats = subscriber.scan_node_stats(query_id)
        assert len(scan_stats) == 1
        assert scan_stats[0]["bytes.read"] == 0
        assert scan_stats[0]["io.requests"] == 0
    finally:
        daft.detach_subscriber("data_source_stats")


def test_data_source_task_stats_ignores_unknown_keys():
    subscriber = StatsCollector()
    daft.attach_subscriber("data_source_stats", subscriber)
    try:
        source = CountingSource(tasks=[[(0, 2)]], bytes_per_batch=42, extra={"custom.counter": 99, "none": None})
        source.read().collect()

        query_id = subscriber.query_ids[-1]
        scan_stats = subscriber.scan_node_stats(query_id)
        assert len(scan_stats) == 1
        assert scan_stats[0]["bytes.read"] == 42
        assert "custom.counter" not in scan_stats[0]
    finally:
        daft.detach_subscriber("data_source_stats")


def test_data_source_task_stats_bad_type_raises():
    class BadStatsTask(NoStatsTask):
        def stats(self) -> dict[str, Any]:
            return {"bytes.read": "not-an-int"}

    class BadStatsSource(NoStatsSource):
        async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
            yield BadStatsTask(self._schema)

    with pytest.raises(Exception, match="bytes.read.*must be an int"):
        BadStatsSource().read().collect()
