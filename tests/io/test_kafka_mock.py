"""Broker-free tests for `daft.read_kafka` argument handling and exports."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import daft


def test_read_kafka_is_exported() -> None:
    """`read_kafka` is exported from `daft` and `daft.io`."""
    assert hasattr(daft, "read_kafka"), "Expected `daft.read_kafka` to be exported"
    assert hasattr(daft.io, "read_kafka"), "Expected `daft.io.read_kafka` to be exported"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"bootstrap_servers": "localhost:9092", "topics": "topic-a"},
        {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "chunk_size": 1},
        {
            "bootstrap_servers": ["localhost:9092", "localhost:9093"],
            "topics": ["topic-a", "topic-b"],
        },
        {
            "bootstrap_servers": ("localhost:9092", "localhost:9093"),
            "topics": "topic-a",
        },
        {
            "bootstrap_servers": "localhost:9092",
            "topics": ["topic-a", "topic-b"],
            "start": {"topic-a": {0: 0}, "topic-b": {0: 0}},
            "end": "latest",
        },
        {
            "bootstrap_servers": "localhost:9092",
            "topics": "topic-a",
            "start": datetime(2026, 1, 6, 12, 0, 0, tzinfo=timezone.utc),
            "end": "2026-01-06T13:00:00Z",
        },
    ],
)
def test_read_kafka_accepts_inputs(kwargs: dict) -> None:
    df = daft.read_kafka(**kwargs)
    assert isinstance(df, daft.DataFrame)


@pytest.mark.parametrize(
    "kwargs,exc,match",
    [
        ({"bootstrap_servers": "localhost:9092", "topics": []}, ValueError, "topics must be non-empty"),
        (
            {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "timeout_ms": 0},
            ValueError,
            "timeout_ms must be > 0",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "chunk_size": 0},
            ValueError,
            "chunk_size must be > 0",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topics": ["topic-a", "topic-b"], "start": {0: 0}, "end": "latest"},
            ValueError,
            "multiple topics",
        ),
        (
            {
                "bootstrap_servers": "localhost:9092",
                "topics": ["topic-a", "topic-b"],
                "start": "earliest",
                "end": {0: 0},
            },
            ValueError,
            "multiple topics",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "start": "earliest", "end": None},
            ValueError,
            "cannot be None for bounded read",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "start": None, "end": "latest"},
            ValueError,
            "cannot be None for bounded read",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "start": {}, "end": "latest"},
            ValueError,
            "partition offset map must be non-empty",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "start": "earliest", "end": {}},
            ValueError,
            "partition offset map must be non-empty",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "start": {"topic-a": {}}, "end": "latest"},
            ValueError,
            "partition offset map must be non-empty",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "start": "earliest", "end": {"topic-a": {}}},
            ValueError,
            "partition offset map must be non-empty",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "start": {"topic-a": 1}, "end": "latest"},
            TypeError,
            "topic offset map values must be dicts",
        ),
        (
            {
                "bootstrap_servers": "localhost:9092",
                "topics": "topic-a",
                "start": {"topic-a": {0: 0}, 0: 0},
                "end": "latest",
            },
            TypeError,
            "partition offset maps must be either",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "start": 1.23, "end": "latest"},
            TypeError,
            "start/end must be one of",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "start": "not-a-timestamp", "end": "latest"},
            ValueError,
            None,
        ),
    ],
)
def test_read_kafka_rejects_inputs(kwargs: dict, exc: type[Exception], match: str | None) -> None:
    if match is None:
        with pytest.raises(exc):
            daft.read_kafka(**kwargs)
    else:
        with pytest.raises(exc, match=match):
            daft.read_kafka(**kwargs)


@pytest.mark.parametrize(
    "kwargs,match",
    [
        (
            {"start": {"topic-a": {0: 0}}, "end": "latest"},
            "offsets must be provided for exactly the topics",
        ),
        (
            {
                "start": {"topic-a": {0: 0}, "topic-b": {0: 0}, "topic-c": {0: 0}},
                "end": "latest",
            },
            "offsets must be provided for exactly the topics",
        ),
        (
            {"start": "earliest", "end": {"topic-a": {0: 0}}},
            "offsets must be provided for exactly the topics",
        ),
        (
            {"start": "earliest", "end": {"topic-a": {0: 0}, "topic-b": {0: 0}, "topic-c": {0: 0}}},
            "offsets must be provided for exactly the topics",
        ),
    ],
)
def test_read_kafka_rejects_offset_topic_key_mismatch(kwargs: dict, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        daft.read_kafka(
            bootstrap_servers="localhost:9092",
            topics=["topic-a", "topic-b"],
            **kwargs,
        )


def test_to_timestamp_ms_rejects_unsupported_type() -> None:
    from daft.io._kafka import _to_timestamp_ms

    with pytest.raises(TypeError, match="expected int\\(ms\\), datetime, or ISO-8601 string"):
        _to_timestamp_ms(object())


def test_make_consumer_config_preserves_value_types() -> None:
    from daft.io._kafka import _make_consumer_config

    cfg = _make_consumer_config(
        bootstrap_servers="localhost:9092",
        group_id="gid",
        kafka_client_config={
            "enable.partition.eof": True,
            "socket.timeout.ms": 1234,
            "client.id": "daft",
        },
    )
    assert cfg["enable.partition.eof"] is True
    assert cfg["socket.timeout.ms"] == 1234
    assert cfg["client.id"] == "daft"


def test_kafka_source_task_terminates_on_empty_consume(monkeypatch: pytest.MonkeyPatch) -> None:
    from daft.dependencies import confluent_kafka as lazy_confluent_kafka
    from daft.io import _kafka

    # Broker-free simulation:
    # We inject a minimal `confluent_kafka` module into `sys.modules` so that `_kafka.py` imports this stub
    # at runtime inside `KafkaSourceTask.get_micro_partitions()`.
    #
    # Behavior under test: if `consume()` returns an empty list immediately, the task should terminate
    # without yielding any MicroPartitions and without retrying/spinning.
    class _Consumer:
        consume_calls: int = 0

        def __init__(self, cfg: object) -> None:
            return None

        def get_watermark_offsets(self, tp: object, *, timeout: float, cached: bool) -> tuple[int, int]:
            return (0, 10)

        def assign(self, tps: list[object]) -> None:
            return None

        def consume(self, num_messages: int, *, timeout: float) -> list[object]:
            type(self).consume_calls += 1
            return []

        def close(self) -> None:
            return None

    stub = SimpleNamespace(
        Consumer=_Consumer,
        TopicPartition=lambda topic, partition, offset=None: (topic, partition, offset),
    )
    monkeypatch.setattr(lazy_confluent_kafka, "_module", None)
    monkeypatch.setitem(__import__("sys").modules, "confluent_kafka", stub)

    task = _kafka.KafkaSourceTask(
        _bootstrap_servers="localhost:9092",
        _group_id="gid",
        _topic="t",
        _partition=0,
        _start_offset=0,
        _end_offset=5,
        _kafka_client_config=None,
        _timeout_ms=1,
        _chunk_size=10,
        _limit=None,
    )

    assert list(task.get_micro_partitions()) == []
    assert _Consumer.consume_calls == 1


def test_kafka_source_task_ignores_partition_eof(monkeypatch: pytest.MonkeyPatch) -> None:
    from daft.dependencies import confluent_kafka as lazy_confluent_kafka
    from daft.io import _kafka

    # Broker-free simulation:
    # `enable.partition.eof=True` can surface an EOF event as a message whose `error().code()`
    # matches `KafkaError._PARTITION_EOF`.
    #
    # Behavior under test: encountering the EOF event should not raise and should simply end the task.
    class _Err:
        def __init__(self, code: int) -> None:
            self._code = code

        def code(self) -> int:
            return self._code

    class _Msg:
        def __init__(self, err: _Err) -> None:
            self._err = err

        def error(self) -> _Err:
            return self._err

    class _Consumer:
        def __init__(self, cfg: object) -> None:
            self.calls = 0

        def get_watermark_offsets(self, tp: object, *, timeout: float, cached: bool) -> tuple[int, int]:
            return (0, 10)

        def assign(self, tps: list[object]) -> None:
            return None

        def consume(self, num_messages: int, *, timeout: float) -> list[_Msg]:
            self.calls += 1
            if self.calls == 1:
                return [_Msg(_Err(-191))]
            return []

        def close(self) -> None:
            return None

    stub = SimpleNamespace(
        Consumer=_Consumer,
        TopicPartition=lambda topic, partition, offset=None: (topic, partition, offset),
        KafkaError=SimpleNamespace(_PARTITION_EOF=-191),
        KafkaException=Exception,
        TIMESTAMP_NOT_AVAILABLE=0,
    )
    monkeypatch.setattr(lazy_confluent_kafka, "_module", None)
    monkeypatch.setitem(__import__("sys").modules, "confluent_kafka", stub)

    task = _kafka.KafkaSourceTask(
        _bootstrap_servers="localhost:9092",
        _group_id="gid",
        _topic="t",
        _partition=0,
        _start_offset=0,
        _end_offset=5,
        _kafka_client_config={"enable.partition.eof": True},
        _timeout_ms=1,
        _chunk_size=10,
        _limit=None,
    )

    assert list(task.get_micro_partitions()) == []


def test_kafka_source_task_respects_chunk_size(monkeypatch: pytest.MonkeyPatch) -> None:
    from daft.dependencies import confluent_kafka as lazy_confluent_kafka
    from daft.io import _kafka

    class _Msg:
        def __init__(self, offset: int) -> None:
            self._offset = offset

        def error(self) -> None:
            return None

        def offset(self) -> int:
            return self._offset

        def timestamp(self) -> tuple[int, int]:
            return (1, 0)

        def key(self) -> bytes:
            return b"k"

        def value(self) -> bytes:
            return b"v"

    class _Consumer:
        def __init__(self, cfg: object) -> None:
            self.calls = 0

        def get_watermark_offsets(self, tp: object, *, timeout: float, cached: bool) -> tuple[int, int]:
            return (0, 10)

        def assign(self, tps: list[object]) -> None:
            return None

        def consume(self, num_messages: int, *, timeout: float) -> list[_Msg]:
            self.calls += 1
            if self.calls == 1:
                return [_Msg(0)]
            if self.calls == 2:
                return [_Msg(1)]
            if self.calls == 3:
                return [_Msg(2)]
            return []

        def close(self) -> None:
            return None

    stub = SimpleNamespace(
        Consumer=_Consumer,
        TopicPartition=lambda topic, partition, offset=None: (topic, partition, offset),
        TIMESTAMP_NOT_AVAILABLE=0,
    )
    monkeypatch.setattr(lazy_confluent_kafka, "_module", None)
    monkeypatch.setitem(__import__("sys").modules, "confluent_kafka", stub)

    task = _kafka.KafkaSourceTask(
        _bootstrap_servers="localhost:9092",
        _group_id="gid",
        _topic="t",
        _partition=0,
        _start_offset=0,
        _end_offset=3,
        _kafka_client_config=None,
        _timeout_ms=1,
        _chunk_size=1,
        _limit=None,
    )

    micro_partitions = list(task.get_micro_partitions())
    assert len(micro_partitions) == 3
    assert [rows[0]["offset"] for rows in (mp.to_pylist() for mp in micro_partitions)] == [0, 1, 2]
