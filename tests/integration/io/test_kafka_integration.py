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
        except confluent_kafka.KafkaException as e:
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
        if (
            e.args
            and hasattr(e.args[0], "code")
            and callable(e.args[0].code)
            and e.args[0].code() == confluent_kafka.KafkaError.TOPIC_ALREADY_EXISTS
        ):
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
            value_text = bytes(value).decode("utf-8")
            try:
                value = json.loads(value_text)
            except json.JSONDecodeError:
                value = value_text
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


def _write_summary_counts(summary: daft.DataFrame) -> tuple[int, int]:
    rows = summary.to_pylist()
    return (
        sum(int(row["messages_delivered"]) for row in rows),
        sum(int(row["messages_failed"]) for row in rows),
    )


def _read_topic(*, bootstrap: str, topic: str) -> list[dict]:
    return _decode_rows(
        daft.read_kafka(
            bootstrap_servers=bootstrap,
            topics=topic,
            group_id=f"daft-kafka-integration-{uuid.uuid4().hex}",
            timeout_ms=20_000,
            start="earliest",
            end="latest",
        )
        .collect()
        .to_pylist()
    )


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
def test_read_kafka_timestamp_start_after_last_message_returns_empty(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])
    batch2_ts = int(kafka_context["batch2_ts"])

    late_dt = datetime.fromtimestamp((batch2_ts + 3_600_000) / 1000, tz=timezone.utc)

    df = daft.read_kafka(
        bootstrap_servers=bootstrap,
        topics=topic,
        group_id="daft-kafka-integration",
        timeout_ms=20_000,
        start=late_dt,
        end="latest",
        partitions=[0],
    ).collect()
    decoded = _decode_rows(df.to_pylist())
    assert decoded == []


@pytest.mark.integration()
def test_read_kafka_timestamp_end_after_last_message_reads_all(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])
    batch2_ts = int(kafka_context["batch2_ts"])

    late_dt = datetime.fromtimestamp((batch2_ts + 3_600_000) / 1000, tz=timezone.utc)

    df = daft.read_kafka(
        bootstrap_servers=bootstrap,
        topics=topic,
        group_id="daft-kafka-integration",
        timeout_ms=20_000,
        start="earliest",
        end=late_dt,
        partitions=[0],
    ).collect()
    decoded = _decode_rows(df.to_pylist())
    assert len(decoded) == 20
    assert {r["topic"] for r in decoded} == {topic}
    assert {r["partition"] for r in decoded} == {0}
    assert {r["value"]["id"] for r in decoded} == set(range(20))


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
def test_read_kafka_limit(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = str(kafka_context["topic"])

    df = (
        daft.read_kafka(
            bootstrap_servers=bootstrap,
            topics=topic,
            group_id="daft-kafka-integration",
            timeout_ms=20_000,
            start="earliest",
            end="latest",
            partitions=[0],
        )
        .limit(5)
        .collect()
    )
    decoded = _decode_rows(df.to_pylist())
    assert len(decoded) <= 5


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


@pytest.mark.integration()
def test_write_kafka_raw_roundtrip(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = f"daft-kafka-write-raw-{uuid.uuid4().hex[:8]}"
    _ensure_topic(bootstrap=bootstrap, topic=topic, num_partitions=2)

    summary = daft.from_pydict(
        {
            "key": [b"k1", b"k2", b"k3"],
            "value": [b"v1", b"v2", b"v3"],
            "partition": [0, 1, 0],
        }
    ).write_kafka(
        bootstrap_servers=bootstrap,
        topic=topic,
        key_col="key",
        value_col="value",
        partition_col="partition",
        kafka_client_config={"acks": "all", "enable.idempotence": True},
        timeout_ms=20_000,
    )

    delivered, failed = _write_summary_counts(summary)
    assert delivered == 3
    assert failed == 0

    decoded = _read_topic(bootstrap=bootstrap, topic=topic)
    assert {r["key"] for r in decoded} == {"k1", "k2", "k3"}
    assert {r["value"] for r in decoded} == {"v1", "v2", "v3"}
    assert {r["partition"] for r in decoded} == {0, 1}


@pytest.mark.integration()
def test_write_kafka_json_dynamic_topic(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic_a = f"daft-kafka-write-json-a-{uuid.uuid4().hex[:8]}"
    topic_b = f"daft-kafka-write-json-b-{uuid.uuid4().hex[:8]}"
    _ensure_topic(bootstrap=bootstrap, topic=topic_a, num_partitions=1)
    _ensure_topic(bootstrap=bootstrap, topic=topic_b, num_partitions=1)

    payload_a = {"id": 1, "kind": "a", "tags": ["red", None]}
    payload_b = {"id": 2, "kind": "b", "tags": ["blue"]}
    summary = daft.from_pylist(
        [
            {"topic": topic_a, "key": "ka", "value": payload_a},
            {"topic": topic_b, "key": "kb", "value": payload_b},
        ]
    ).write_kafka(
        bootstrap_servers=bootstrap,
        topic_col="topic",
        key_col="key",
        value_col="value",
        key_format="utf8",
        value_format="json",
        kafka_client_config={"acks": "all"},
        timeout_ms=20_000,
    )

    delivered, failed = _write_summary_counts(summary)
    assert delivered == 2
    assert failed == 0

    decoded_a = _read_topic(bootstrap=bootstrap, topic=topic_a)
    decoded_b = _read_topic(bootstrap=bootstrap, topic=topic_b)
    assert [r["key"] for r in decoded_a] == ["ka"]
    assert [r["value"] for r in decoded_a] == [payload_a]
    assert [r["key"] for r in decoded_b] == ["kb"]
    assert [r["value"] for r in decoded_b] == [payload_b]


@pytest.mark.integration()
def test_write_kafka_headers(kafka_context: dict[str, object]) -> None:
    confluent_kafka = pytest.importorskip("confluent_kafka")

    bootstrap = str(kafka_context["bootstrap"])
    topic = f"daft-kafka-write-headers-{uuid.uuid4().hex[:8]}"
    _ensure_topic(bootstrap=bootstrap, topic=topic, num_partitions=1)

    summary = daft.from_pylist(
        [
            {
                "key": b"k",
                "value": b"v",
                "headers": [{"key": "trace", "value": b"a"}, {"key": "trace", "value": b"b"}],
            }
        ]
    ).write_kafka(
        bootstrap_servers=bootstrap,
        topic=topic,
        key_col="key",
        value_col="value",
        headers_col="headers",
        kafka_client_config={"acks": "all"},
        timeout_ms=20_000,
    )
    delivered, failed = _write_summary_counts(summary)
    assert delivered == 1
    assert failed == 0

    consumer = confluent_kafka.Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"daft-kafka-integration-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        }
    )
    consumer.subscribe([topic])
    deadline = time.time() + 20
    try:
        while time.time() < deadline:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                raise confluent_kafka.KafkaException(msg.error())

            assert msg.headers() == [("trace", b"a"), ("trace", b"b")]
            return
    finally:
        consumer.close()

    pytest.fail("timed out waiting for header test message")
