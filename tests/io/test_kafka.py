"""Broker-free tests for `daft.read_kafka` argument handling and exports."""

from __future__ import annotations

from datetime import datetime, timezone

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
            {"bootstrap_servers": "localhost:9092", "topics": ["topic-a", "topic-b"], "start": {0: 0}, "end": "latest"},
            ValueError,
            "multiple topics",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topics": ["topic-a", "topic-b"], "start": "earliest", "end": {0: 0}},
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
            {"bootstrap_servers": "localhost:9092", "topics": "topic-a", "start": {"topic-a": {0: 0}, 0: 0}, "end": "latest"},
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
            "start offsets must be provided for exactly the topics",
        ),
        (
            {
                "start": {"topic-a": {0: 0}, "topic-b": {0: 0}, "topic-c": {0: 0}},
                "end": "latest",
            },
            "start offsets must be provided for exactly the topics",
        ),
        (
            {"start": "earliest", "end": {"topic-a": {0: 0}}},
            "end offsets must be provided for exactly the topics",
        ),
        (
            {"start": "earliest", "end": {"topic-a": {0: 0}, "topic-b": {0: 0}, "topic-c": {0: 0}}},
            "end offsets must be provided for exactly the topics",
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

    with pytest.raises(TypeError, match="Expected timestamp as int\\(ms\\), datetime, or ISO-8601 string"):
        _to_timestamp_ms(object())
