from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone

import pytest

import daft


def _wait_for_kafka(bootstrap: str, timeout_s: int = 60) -> None:
    pytest.importorskip("kafka")
    from kafka import KafkaAdminClient

    deadline = time.time() + timeout_s
    last: Exception | None = None
    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(bootstrap_servers=[bootstrap], client_id="daft-kafka-it")
            admin.close()
            return
        except Exception as e:
            last = e
            time.sleep(1)
    raise RuntimeError(f"Kafka was not ready after {timeout_s}s: {last}")


def _ensure_topic(*, bootstrap: str, topic: str, num_partitions: int) -> None:
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=[bootstrap], client_id="daft-kafka-it-admin")
    try:
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)],
            validate_only=False,
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def _produce_messages(
    *,
    bootstrap: str,
    topic: str,
    partition: int,
    start_id: int,
    count: int,
    base_ts_ms: int,
    label: str,
) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap],
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for i in range(count):
        msg_id = start_id + i
        producer.send(
            topic,
            partition=partition,
            key=f"key-{msg_id}",
            value={"id": msg_id, "label": label},
            timestamp_ms=base_ts_ms + i,
        )
    producer.flush()
    producer.close()


def _decode_rows(rows: list[dict]) -> list[dict]:
    decoded: list[dict] = []
    for r in rows:
        key = r.get("key")
        if isinstance(key, (bytes, bytearray, memoryview)):
            key = bytes(key).decode("utf-8")
        value = r.get("value")
        if isinstance(value, (bytes, bytearray, memoryview)):
            value = json.loads(bytes(value).decode("utf-8"))
        decoded.append(
            {
                "topic": r.get("topic"),
                "partition": r.get("partition"),
                "offset": r.get("offset"),
                "timestamp_ms": r.get("timestamp_ms"),
                "key": key,
                "value": value,
            }
        )
    return decoded


@pytest.mark.integration()
def test_read_kafka_end_to_end() -> None:
    pytest.importorskip("kafka")
    bootstrap = "127.0.0.1:9092"
    _wait_for_kafka(bootstrap)

    topic = f"daft-kafka-it-{uuid.uuid4().hex[:8]}"
    _ensure_topic(bootstrap=bootstrap, topic=topic, num_partitions=2)

    now_ms = int(time.time() * 1000)
    batch1_ts = now_ms
    batch2_ts = now_ms + 60_000
    boundary_ts = (batch1_ts + batch2_ts) // 2
    boundary_dt = datetime.fromtimestamp(boundary_ts / 1000, tz=timezone.utc)

    _produce_messages(
        bootstrap=bootstrap,
        topic=topic,
        partition=0,
        start_id=0,
        count=10,
        base_ts_ms=batch1_ts,
        label="batch1",
    )
    _produce_messages(
        bootstrap=bootstrap,
        topic=topic,
        partition=0,
        start_id=10,
        count=10,
        base_ts_ms=batch2_ts,
        label="batch2",
    )
    _produce_messages(
        bootstrap=bootstrap,
        topic=topic,
        partition=1,
        start_id=100,
        count=10,
        base_ts_ms=batch2_ts,
        label="p1",
    )

    df_all_p0 = daft.read_kafka(
        bootstrap_servers=bootstrap,
        topics=topic,
        group_id="daft-kafka-integration",
        timeout_ms=20_000,
        start="earliest",
        end="latest",
        partitions=[0],
    ).collect()
    decoded = _decode_rows(df_all_p0.to_pylist())
    assert len(decoded) == 20
    assert {r["topic"] for r in decoded} == {topic}
    assert {r["partition"] for r in decoded} == {0}
    assert {r["value"]["id"] for r in decoded} == set(range(20))

    df_ts = daft.read_kafka(
        bootstrap_servers=bootstrap,
        topics=topic,
        group_id="daft-kafka-integration",
        timeout_ms=20_000,
        start=boundary_dt,
        end="latest",
        partitions=[0],
    ).collect()
    decoded = _decode_rows(df_ts.to_pylist())
    assert len(decoded) == 10
    assert {r["value"]["label"] for r in decoded} == {"batch2"}
    assert {r["value"]["id"] for r in decoded} == set(range(10, 20))

    df_offsets = daft.read_kafka(
        bootstrap_servers=bootstrap,
        topics=topic,
        group_id="daft-kafka-integration",
        timeout_ms=20_000,
        start={0: 0},
        end={0: 5},
        partitions=[0],
    ).collect()
    decoded = _decode_rows(df_offsets.to_pylist())
    assert len(decoded) == 5
    assert {r["partition"] for r in decoded} == {0}

    df_p1 = daft.read_kafka(
        bootstrap_servers=bootstrap,
        topics=topic,
        group_id="daft-kafka-integration",
        timeout_ms=20_000,
        start="earliest",
        end="latest",
        partitions=[1],
    ).collect()
    decoded = _decode_rows(df_p1.to_pylist())
    assert len(decoded) == 10
    assert {r["partition"] for r in decoded} == {1}
    assert {r["value"]["label"] for r in decoded} == {"p1"}
