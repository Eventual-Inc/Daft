from __future__ import annotations

import io
import signal
import threading
import time
from collections import defaultdict
from collections.abc import Mapping
from typing import Any

import pytest

import daft
from daft.daft import PyMicroPartition, PyNodeInfo, PyQueryMetadata, PyQueryResult, QueryEndState
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
    end_states: dict[str, QueryEndState]
    end_messages: dict[str, str]
    query_ids: list[str]

    def __init__(self):
        self.query_metadata = {}
        self.query_optimized_plan = {}
        self.query_node_stats = defaultdict(lambda: defaultdict(dict))
        self.query_results = defaultdict(list)
        self.end_states = {}
        self.end_messages = {}
        self.query_ids = []

    def on_query_start(self, query_id: str, metadata: PyQueryMetadata) -> None:
        self.query_ids.append(query_id)
        self.query_metadata[query_id] = metadata

    def on_query_end(self, query_id: str, result: PyQueryResult) -> None:
        self.end_states[query_id] = result.end_state
        self.end_messages[query_id] = result.error_message

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


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_capture_states(monkeypatch):
    subscriber = MockSubscriber()
    ctx = daft.context.get_context()
    ctx.attach_subscriber("mock", subscriber)

    def inject_keyboard_interrupt():
        threading.Timer(2.0, lambda: signal.raise_signal(signal.SIGINT)).start()

    @daft.func(return_dtype=daft.DataType.string())
    def failing_udf(s):
        raise ValueError("This UDF will fail forever")

    @daft.func(return_dtype=daft.DataType.int64())
    def success_udf(s):
        return s

    @daft.func(return_dtype=daft.DataType.int64())
    def long_running_udf(s):
        time.sleep(10)
        return s

    # 1. Finished state

    df = daft.from_pydict({"x": [1, 2, 3]})
    df = df.with_column("y", success_udf(df["x"]))

    df.collect()

    # Get keys from the map
    query_id = subscriber.query_ids[-1]
    assert subscriber.end_states[query_id] == QueryEndState.Finished
    assert subscriber.end_messages[query_id] == ""
    # contains the Value Error message

    # 2. Failed state

    df = daft.from_pydict({"x": ["1", "2", "3"]})
    df = df.with_column("y", failing_udf(df["x"]))

    with pytest.raises(Exception):
        df.collect()

    query_id = subscriber.query_ids[-1]
    assert subscriber.end_states[query_id] == QueryEndState.Failed
    # contains the Value Error message
    assert "This UDF will fail forever" in subscriber.end_messages[query_id]

    # 3. Canceled State

    inject_keyboard_interrupt()

    df = daft.from_pydict({"x": ["1", "2", "3"]})
    df = df.with_column("y", long_running_udf(df["x"]))

    with pytest.raises(KeyboardInterrupt):
        df.collect()

    query_id = subscriber.query_ids[-1]
    assert subscriber.end_states[query_id] == QueryEndState.Canceled
    # contains the Value Error message
    assert "Query canceled by the user" in subscriber.end_messages[query_id]


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
