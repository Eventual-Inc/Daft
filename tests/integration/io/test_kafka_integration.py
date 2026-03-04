from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone

import pytest

import daft


def _wait_for_kafka_ready(bootstrap: str, timeout_s: int = 60) -> None:
    confluent_kafka = pytest.importorskip("confluent_kafka")

    deadline = time.time() + timeout_s
    last: Exception | None = None
    while time.time() < deadline:
        try:
            consumer = confluent_kafka.Consumer(
                {
                    "bootstrap.servers": bootstrap,
                    "group.id": "daft-kafka-it-healthcheck",
                    "enable.auto.commit": "false",
                }
            )
            consumer.list_topics(timeout=5)
            consumer.close()
            return
        except Exception as e:
            last = e
            time.sleep(1)
    raise RuntimeError(f"Kafka was not ready after {timeout_s}s: {last}")


def _ensure_topic(*, bootstrap: str, topic: str, num_partitions: int) -> None:
    confluent_kafka = pytest.importorskip("confluent_kafka")
    admin = pytest.importorskip("confluent_kafka.admin")

    client = admin.AdminClient({"bootstrap.servers": bootstrap})
    futures = client.create_topics([admin.NewTopic(topic, num_partitions=num_partitions, replication_factor=1)])
    try:
        futures[topic].result(timeout=30)
    except confluent_kafka.KafkaException as e:
        if e.args and hasattr(e.args[0], "code") and callable(e.args[0].code):
            if e.args[0].code() == confluent_kafka.KafkaError.TOPIC_ALREADY_EXISTS:
                return
        raise


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
    confluent_kafka = pytest.importorskip("confluent_kafka")
    producer = confluent_kafka.Producer({"bootstrap.servers": bootstrap})
    for i in range(count):
        msg_id = start_id + i
        producer.produce(
            topic,
            partition=partition,
            key=f"key-{msg_id}".encode(),
            value=json.dumps({"id": msg_id, "label": label}).encode("utf-8"),
            timestamp=base_ts_ms + i,
        )
    producer.flush(30)


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


@pytest.fixture(scope="module")
def kafka_context() -> Iterator[dict[str, object]]:
    pytest.importorskip("confluent_kafka")

    bootstrap = "127.0.0.1:9092"
    _wait_for_kafka_ready(bootstrap)

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

    topic2 = f"{topic}-extra"
    _ensure_topic(bootstrap=bootstrap, topic=topic2, num_partitions=1)
    _produce_messages(
        bootstrap=bootstrap,
        topic=topic2,
        partition=0,
        start_id=1000,
        count=5,
        base_ts_ms=batch2_ts,
        label="t2",
    )

    yield {
        "bootstrap": bootstrap,
        "topic": topic,
        "topic2": topic2,
        "boundary_dt": boundary_dt,
        "batch2_ts": batch2_ts,
    }


@pytest.mark.integration()
def test_read_kafka_partition_filter(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])

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


@pytest.mark.integration()
def test_read_kafka_chunk_size_one(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])

    df_all_p0 = daft.read_kafka(
        bootstrap_servers=bootstrap,
        topics=topic,
        group_id="daft-kafka-integration",
        timeout_ms=20_000,
        start="earliest",
        end="latest",
        partitions=[0],
        chunk_size=1,
    ).collect()
    assert len(list(df_all_p0.iter_partitions())) == 20
    decoded = _decode_rows(df_all_p0.to_pylist())
    assert len(decoded) == 20
    assert {r["topic"] for r in decoded} == {topic}
    assert {r["partition"] for r in decoded} == {0}
    assert {r["value"]["id"] for r in decoded} == set(range(20))


@pytest.mark.integration()
def test_read_kafka_timestamp_start(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])
    boundary_dt = kafka_context["boundary_dt"]
    assert isinstance(boundary_dt, datetime)

    df_ts = daft.read_kafka(
        bootstrap_servers=bootstrap,
        topics=topic,
        group_id="daft-kafka-integration",
        timeout_ms=20_000,
        start=boundary_dt,
        end="latest",
        partitions=[0],
    ).collect()
    assert len(list(df_ts.iter_partitions())) == 1
    decoded = _decode_rows(df_ts.to_pylist())
    assert len(decoded) == 10
    assert {r["value"]["label"] for r in decoded} == {"batch2"}
    assert {r["value"]["id"] for r in decoded} == set(range(10, 20))


@pytest.mark.integration()
def test_read_kafka_offset_range(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])

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
    assert {r["offset"] for r in decoded} == set(range(5))


@pytest.mark.integration()
def test_read_kafka_offset_map_start(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])

    df_offsets_start = daft.read_kafka(
        bootstrap_servers=bootstrap,
        topics=topic,
        group_id="daft-kafka-integration",
        timeout_ms=20_000,
        start={0: 10},
        end="latest",
        partitions=[0],
    ).collect()
    decoded = _decode_rows(df_offsets_start.to_pylist())
    assert len(decoded) == 10
    assert {r["partition"] for r in decoded} == {0}
    assert {r["value"]["id"] for r in decoded} == set(range(10, 20))


@pytest.mark.integration()
def test_read_kafka_partition1(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])

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


@pytest.mark.integration()
def test_read_kafka_multi_topic_offset_map(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])
    topic2 = str(kafka_context["topic2"])

    df_multi = daft.read_kafka(
        bootstrap_servers=bootstrap,
        topics=[topic, topic2],
        group_id="daft-kafka-integration",
        timeout_ms=20_000,
        start={topic: {0: 10, 1: 0}, topic2: {0: 2}},
        end="latest",
    ).collect()
    decoded = _decode_rows(df_multi.to_pylist())
    assert len(decoded) == (20 - 10) + 10 + (5 - 2)


@pytest.mark.integration()
def test_read_kafka_multi_topic_offset_map_topic_mismatch_raises(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])
    topic2 = str(kafka_context["topic2"])

    with pytest.raises(ValueError, match="offsets must be provided for exactly the topics"):
        daft.read_kafka(
            bootstrap_servers=bootstrap,
            topics=[topic, topic2],
            group_id="daft-kafka-integration",
            timeout_ms=20_000,
            start={topic: {0: 0, 1: 0}},
            end="latest",
        )


@pytest.mark.integration()
def test_read_kafka_multiple_topics_shorthand_offset_map_raises(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])
    topic2 = str(kafka_context["topic2"])

    with pytest.raises(ValueError, match="multiple topics"):
        daft.read_kafka(
            bootstrap_servers=bootstrap,
            topics=[topic, topic2],
            group_id="daft-kafka-integration",
            timeout_ms=20_000,
            start={0: 0},
            end="latest",
        )


@pytest.mark.integration()
def test_read_kafka_end_before_start_raises(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])

    with pytest.raises(ValueError, match="end must be >= start"):
        daft.read_kafka(
            bootstrap_servers=bootstrap,
            topics=topic,
            group_id="daft-kafka-integration",
            timeout_ms=20_000,
            start={0: 10},
            end={0: 5},
            partitions=[0],
        ).collect()


@pytest.mark.integration()
def test_read_kafka_missing_partition_offset_raises(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])

    with pytest.raises(ValueError, match="missing offset for partition"):
        daft.read_kafka(
            bootstrap_servers=bootstrap,
            topics=topic,
            group_id="daft-kafka-integration",
            timeout_ms=20_000,
            start={0: 0},
            end={0: 5},
        ).collect()
