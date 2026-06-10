from __future__ import annotations

import pytest

import daft


def test_write_kafka_is_exported_on_dataframe() -> None:
    df = daft.from_pydict({"value": [b"a"]})
    assert hasattr(df, "write_kafka")


@pytest.mark.parametrize(
    "kwargs,exc,match",
    [
        (
            {"bootstrap_servers": "localhost:9092"},
            ValueError,
            "exactly one of topic or topic_col",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topic": "topic-a", "topic_col": "topic"},
            ValueError,
            "exactly one of topic or topic_col",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topic": ""},
            ValueError,
            "topic must be non-empty",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topic": "topic-a", "partition": -1},
            ValueError,
            "partition must be >= 0",
        ),
        (
            {
                "bootstrap_servers": "localhost:9092",
                "topic": "topic-a",
                "partition": 0,
                "partition_col": "partition",
            },
            ValueError,
            "partition and partition_col are mutually exclusive",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topic": "topic-a", "timeout_ms": 0},
            ValueError,
            "timeout_ms must be > 0",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topic": "topic-a", "value_format": "avro"},
            ValueError,
            "value_format must be one of",
        ),
        (
            {"bootstrap_servers": "localhost:9092", "topic": "topic-a", "key_format": "json"},
            ValueError,
            "key_format must be one of",
        ),
        (
            {
                "bootstrap_servers": "localhost:9092",
                "topic": "topic-a",
                "kafka_client_config": {"bootstrap.servers": "other:9092"},
            },
            ValueError,
            "must not override managed key",
        ),
        (
            {
                "bootstrap_servers": "localhost:9092",
                "topic": "topic-a",
                "kafka_client_config": {"transactional.id": "tx-1"},
            },
            NotImplementedError,
            "transactional.id is not supported",
        ),
        (
            {
                "bootstrap_servers": "localhost:9092",
                "topic": "topic-a",
                "kafka_client_config": {"acks": {"nested": "invalid"}},
            },
            TypeError,
            "must be a scalar",
        ),
    ],
)
def test_write_kafka_rejects_invalid_inputs(
    kwargs: dict[str, object],
    exc: type[Exception],
    match: str,
) -> None:
    df = daft.from_pydict({"value": [b"a"]})
    with pytest.raises(exc, match=match):
        df.write_kafka(**kwargs)


def test_write_kafka_normalization_helpers_accept_valid_values() -> None:
    from daft.dataframe.dataframe import _normalize_kafka_bootstrap_servers, _validate_kafka_client_config

    assert _normalize_kafka_bootstrap_servers("localhost:9092") == "localhost:9092"
    assert _normalize_kafka_bootstrap_servers(["a:9092", "b:9092"]) == "a:9092,b:9092"
    assert _validate_kafka_client_config(
        {
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 10,
            "queue.buffering.max.kbytes": 1024.5,
            "client.id": None,
        }
    ) == {
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 10,
        "queue.buffering.max.kbytes": 1024.5,
        "client.id": None,
    }
