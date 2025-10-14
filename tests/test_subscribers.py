from __future__ import annotations

import io
from collections import defaultdict
from collections.abc import Mapping
from typing import Any

import pytest

import daft
from daft.daft import PyMicroPartition, PyNodeInfo, PyQueryMetadata
from daft.recordbatch import MicroPartition
from daft.subscribers import StatType, Subscriber
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="Only Native Runner supports subscribers right now"
)


class MockSubscriber(Subscriber):
    query_metadata: dict[str, PyQueryMetadata]
    query_optimized_plan: dict[str, str]
    query_node_stats: defaultdict[str, defaultdict[int, dict[str, Any]]]
    query_results: defaultdict[str, list[PyMicroPartition]]

    def __init__(self):
        self.query_metadata = {}
        self.query_optimized_plan = {}
        self.query_node_stats = defaultdict(lambda: defaultdict(dict))
        self.query_results = defaultdict(list)

    def on_query_start(self, query_id: str, metadata: PyQueryMetadata) -> None:
        self.query_metadata[query_id] = metadata

    def on_query_end(self, query_id: str) -> None:
        pass

    def on_result_out(self, query_id: str, result: PyMicroPartition) -> None:
        self.query_results[query_id].append(result)

    def on_optimization_start(self, query_id: str) -> None:
        pass

    def on_optimization_end(self, query_id: str, optimized_plan: str) -> None:
        self.query_optimized_plan[query_id] = optimized_plan

    def on_exec_start(self, query_id: str, node_infos: list[PyNodeInfo]) -> None:
        pass

    def on_exec_operator_start(self, query_id: str, node_id: int) -> None:
        pass

    def on_exec_emit_stats(self, query_id: str, all_stats: Mapping[int, Mapping[str, tuple[StatType, Any]]]) -> None:
        for node_id, stats in all_stats.items():
            for stat_name, (_, stat_value) in stats.items():
                self.query_node_stats[query_id][node_id][stat_name] = stat_value

    def on_exec_operator_end(self, query_id: str, node_id: int) -> None:
        pass

    def on_exec_end(self, query_id: str) -> None:
        pass


def test_subscriber_template():
    subscriber = MockSubscriber()
    ctx = daft.context.get_context()
    ctx.attach_subscriber("mock", subscriber)

    df = daft.from_pydict({"x": [1, 2, 3]})
    df = df.with_column("y", df["x"] + 1)
    df = df.limit(5)

    string_io = io.StringIO()
    df.explain(file=string_io)
    unoptimized_plan = string_io.getvalue().removeprefix("== Unoptimized Logical Plan ==\n\n")
    output_schema = df.schema()
    df = df.collect()

    query_id = next(iter(subscriber.query_metadata.keys()))
    assert subscriber.query_metadata[query_id].unoptimized_plan in unoptimized_plan
    assert subscriber.query_metadata[query_id].output_schema == output_schema._schema
    assert subscriber.query_optimized_plan[query_id] not in unoptimized_plan

    # Test output
    mps = [MicroPartition._from_pymicropartition(mp) for mp in subscriber.query_results[query_id]]
    assert daft.DataFrame._from_micropartitions(*mps).to_pydict() == df.to_pydict()

    # Test stats
    for _, stats in subscriber.query_node_stats[query_id].items():
        for stat_name, stat_value in stats.items():
            if stat_name == "rows_in" or stat_name == "rows_out":
                assert stat_value == 3

    # Verify detach
    ctx.detach_subscriber("mock")
    df = daft.from_pydict({"x": [1, 2, 3]})
    df = df.with_column("y", df["x"] + 1)
    df.collect()
    # Should only have the previous query
    assert len(subscriber.query_metadata) == 1
