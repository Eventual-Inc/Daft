# Kafka Write Local E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in local Docker E2E test that validates Daft native `write_kafka` against a real Redpanda broker.

**Architecture:** Create one local-only pytest module under `tests/local_e2e` with module-level environment gating so it is never run by default. Reuse the existing Redpanda Docker Compose service and local helper functions for broker readiness, topic creation, seed production, exact consumption, Daft readback, summary assertions, and message normalization.

**Tech Stack:** pytest, Daft Python API, `confluent_kafka`, existing Redpanda Docker Compose service, native Daft runner.

---

## File Structure

- Create `tests/local_e2e/test_kafka_write_e2e.py`
  - Owns the full opt-in local Kafka write E2E.
  - Contains local helpers only; do not import private helpers from existing integration tests.
  - Skips at module import unless `DAFT_RUN_KAFKA_LOCAL_E2E=1`.

- Modify `tests/conftest.py`
  - Register the `local_e2e` marker next to the existing `integration` marker.
  - Do not change default `addopts`; the environment gate plus explicit opt-in marker selection are the safety mechanisms.

## Task 1: Opt-In Test Module Skeleton

**Files:**
- Create: `tests/local_e2e/test_kafka_write_e2e.py`

- [ ] **Step 1: Create the local E2E test directory**

Run:

```bash
mkdir -p tests/local_e2e
```

Expected: directory exists and no source files are modified.

- [ ] **Step 2: Add a gated module skeleton**

Create `tests/local_e2e/test_kafka_write_e2e.py` with:

```python
from __future__ import annotations

import os

import pytest


if os.environ.get("DAFT_RUN_KAFKA_LOCAL_E2E") != "1":
    pytest.skip("local Kafka E2E is opt-in", allow_module_level=True)


pytestmark = [pytest.mark.integration, pytest.mark.local_e2e]


def test_kafka_write_local_e2e_placeholder() -> None:
    assert True
```

- [ ] **Step 3: Verify the module is skipped by default**

Run:

```bash
.venv/bin/python -m pytest tests/local_e2e/test_kafka_write_e2e.py -q -rs
```

Expected: one skipped module and exit code `0`.

- [ ] **Step 4: Verify the module runs when opted in**

Run:

```bash
DAFT_RUN_KAFKA_LOCAL_E2E=1 .venv/bin/python -m pytest tests/local_e2e/test_kafka_write_e2e.py -m "integration and local_e2e" -q
```

Expected: one placeholder test passes. The explicit `-m` expression overrides the repository default that excludes integration tests.

## Task 2: Marker Registration

**Files:**
- Modify: `tests/conftest.py`

- [ ] **Step 1: Register `local_e2e` marker**

Modify `pytest_configure` in `tests/conftest.py` to keep the existing `integration` marker and add:

```python
config.addinivalue_line(
    "markers",
    "local_e2e: mark opt-in local end-to-end tests",
)
```

Keep the existing default in `pyproject.toml` unchanged:

```toml
addopts = "-m 'not (integration or benchmark or hypothesis)'"
```

- [ ] **Step 2: Re-run the default skip check**

Run:

```bash
.venv/bin/python -m pytest tests/local_e2e/test_kafka_write_e2e.py -q -rs
```

Expected: module skipped and no unknown marker warning.

## Task 3: Kafka Helper Layer

**Files:**
- Modify: `tests/local_e2e/test_kafka_write_e2e.py`

- [ ] **Step 1: Replace the placeholder module with helper imports and constants**

Replace the entire placeholder module with:

```python
from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Iterable, Iterator
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import pytest

import daft


if os.environ.get("DAFT_RUN_KAFKA_LOCAL_E2E") != "1":
    pytest.skip("local Kafka E2E is opt-in", allow_module_level=True)


pytestmark = [pytest.mark.integration, pytest.mark.local_e2e]

BOOTSTRAP = "127.0.0.1:9092"
WRITE_TIMEOUT_MS = 20_000


@dataclass(frozen=True)
class SeedRecord:
    event_id: int
    key: bytes
    value: bytes
    partition: int
    timestamp_ms: int
```

- [ ] **Step 2: Add broker readiness and topic helpers**

Append:

```python
def wait_for_kafka_ready(bootstrap: str, timeout_s: int = 60) -> None:
    confluent_kafka = pytest.importorskip("confluent_kafka")

    readiness_errors = (confluent_kafka.KafkaException, OSError, RuntimeError, TimeoutError)
    deadline = time.time() + timeout_s
    last: Exception | None = None
    while time.time() < deadline:
        consumer = None
        try:
            consumer = confluent_kafka.Consumer(
                {
                    "bootstrap.servers": bootstrap,
                    "group.id": "daft-kafka-local-e2e-healthcheck",
                    "enable.auto.commit": "false",
                }
            )
            consumer.list_topics(timeout=5)
            return
        except readiness_errors as exc:
            last = exc
            time.sleep(1)
        finally:
            if consumer is not None:
                with suppress(*readiness_errors):
                    consumer.close()
    raise RuntimeError(f"Kafka was not ready after {timeout_s}s: {last}")


def create_topic(*, bootstrap: str, topic: str, partitions: int, config: dict[str, str] | None = None) -> None:
    confluent_kafka = pytest.importorskip("confluent_kafka")
    admin = pytest.importorskip("confluent_kafka.admin")

    client = admin.AdminClient({"bootstrap.servers": bootstrap})
    futures = client.create_topics(
        [admin.NewTopic(topic, num_partitions=partitions, replication_factor=1, config=config)]
    )
    try:
        futures[topic].result(timeout=30)
    except confluent_kafka.KafkaException as exc:
        if (
            exc.args
            and hasattr(exc.args[0], "code")
            and callable(exc.args[0].code)
            and exc.args[0].code() == confluent_kafka.KafkaError.TOPIC_ALREADY_EXISTS
        ):
            return
        raise
```

- [ ] **Step 3: Add producer and consumer helpers**

Append:

```python
def produce_seed_records(*, bootstrap: str, topic: str, records: Iterable[SeedRecord]) -> None:
    confluent_kafka = pytest.importorskip("confluent_kafka")

    producer = confluent_kafka.Producer({"bootstrap.servers": bootstrap})
    for record in records:
        producer.produce(
            topic,
            key=record.key,
            value=record.value,
            partition=record.partition,
            timestamp=record.timestamp_ms,
        )
    remaining = producer.flush(30)
    assert remaining == 0


def consume_exactly(*, bootstrap: str, topic: str, count: int, timeout_s: int = 20) -> list[Any]:
    confluent_kafka = pytest.importorskip("confluent_kafka")

    consumer = confluent_kafka.Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"daft-kafka-local-e2e-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        }
    )
    consumer.subscribe([topic])
    deadline = time.time() + timeout_s
    messages: list[Any] = []
    try:
        while time.time() < deadline and len(messages) < count:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                raise confluent_kafka.KafkaException(msg.error())
            messages.append(msg)
    finally:
        consumer.close()

    if len(messages) != count:
        pytest.fail(f"timed out waiting for {count} message(s) from topic {topic!r}; got {len(messages)}")
    return messages
```

- [ ] **Step 4: Add Daft read and assertion helpers**

Append:

```python
def read_all_with_daft(*, bootstrap: str, topic: str) -> list[dict[str, Any]]:
    return (
        daft.read_kafka(
            bootstrap_servers=bootstrap,
            topics=topic,
            group_id=f"daft-kafka-local-e2e-{uuid.uuid4().hex}",
            timeout_ms=WRITE_TIMEOUT_MS,
            start="earliest",
            end="latest",
        )
        .collect()
        .to_pylist()
    )


def assert_summary(summary: daft.DataFrame, *, attempted: int, delivered: int, failed: int = 0) -> None:
    rows = summary.to_pylist()
    assert rows
    assert sum(int(row["messages_attempted"]) for row in rows) == attempted
    assert sum(int(row["messages_delivered"]) for row in rows) == delivered
    assert sum(int(row["messages_failed"]) for row in rows) == failed
    assert sum(int(row["bytes_delivered"]) for row in rows) > 0
    assert all(isinstance(row["task_id"], int) for row in rows)


def normalize_message(msg: Any) -> dict[str, Any]:
    return {
        "key": msg.key(),
        "value": msg.value(),
        "partition": msg.partition(),
        "timestamp_ms": msg.timestamp()[1],
        "headers": msg.headers(),
    }
```

- [ ] **Step 5: Add run context fixture**

Append:

```python
@dataclass(frozen=True)
class KafkaE2EContext:
    bootstrap: str
    run_id: str
    base_ts_ms: int
    source_topic: str
    raw_topic: str
    user_topic: str
    order_topic: str
    headers_topic: str
    compact_topic: str


@pytest.fixture(scope="module")
def kafka_e2e_context() -> Iterator[KafkaE2EContext]:
    pytest.importorskip("confluent_kafka")
    wait_for_kafka_ready(BOOTSTRAP)

    run_id = uuid.uuid4().hex[:8]
    ctx = KafkaE2EContext(
        bootstrap=BOOTSTRAP,
        run_id=run_id,
        base_ts_ms=int(time.time() * 1000),
        source_topic=f"daft-e2e-src-{run_id}",
        raw_topic=f"daft-e2e-raw-out-{run_id}",
        user_topic=f"daft-e2e-json-user-{run_id}",
        order_topic=f"daft-e2e-json-order-{run_id}",
        headers_topic=f"daft-e2e-headers-{run_id}",
        compact_topic=f"daft-e2e-compact-{run_id}",
    )

    create_topic(bootstrap=ctx.bootstrap, topic=ctx.source_topic, partitions=3)
    create_topic(bootstrap=ctx.bootstrap, topic=ctx.raw_topic, partitions=3)
    create_topic(bootstrap=ctx.bootstrap, topic=ctx.user_topic, partitions=2)
    create_topic(bootstrap=ctx.bootstrap, topic=ctx.order_topic, partitions=2)
    create_topic(bootstrap=ctx.bootstrap, topic=ctx.headers_topic, partitions=1)
    create_topic(
        bootstrap=ctx.bootstrap,
        topic=ctx.compact_topic,
        partitions=1,
        config={"cleanup.policy": "compact"},
    )

    yield ctx
```

- [ ] **Step 6: Verify helper-only module still imports and runs no tests**

Run:

```bash
DAFT_RUN_KAFKA_LOCAL_E2E=1 .venv/bin/python -m pytest tests/local_e2e/test_kafka_write_e2e.py -m "integration and local_e2e" -q
```

Expected: pytest collects zero tests or only fixture import passes. If pytest exits with "no tests ran" as code `5`, continue to Task 4 without treating this as a functional failure.

## Task 4: Raw Mirror E2E Scenario

**Files:**
- Modify: `tests/local_e2e/test_kafka_write_e2e.py`

- [ ] **Step 1: Add source record builder**

Append before tests:

```python
def build_source_records(base_ts_ms: int) -> list[SeedRecord]:
    rows = [
        (1, b"user-001", {"event_id": 1, "kind": "signup", "amount": 0, "note": "hello"}, 0),
        (2, b"order-001", {"event_id": 2, "kind": "purchase", "amount": 19.99, "note": "zstd-path"}, 1),
        (3, b"user-001", {"event_id": 3, "kind": "update", "amount": 0, "note": "duplicate-key"}, 0),
        (4, "user-004".encode("utf-8"), {"event_id": 4, "kind": "unicode", "note": "hello unicode"}, 2),
        (5, b"user-005", {"event_id": 5, "kind": "logout", "amount": 0, "note": "small"}, 0),
        (6, b"order-006", {"event_id": 6, "kind": "purchase", "amount": 42.5, "note": "medium"}, 1),
        (7, b"user-007", {"event_id": 7, "kind": "profile", "amount": 0, "note": "large-" + "x" * 128}, 2),
        (8, b"user-008", {"event_id": 8, "kind": "signup", "amount": 0, "note": "p0"}, 0),
        (9, b"order-009", {"event_id": 9, "kind": "refund", "amount": -3.5, "note": "p1"}, 1),
        (10, b"user-010", {"event_id": 10, "kind": "update", "amount": 0, "note": "p2"}, 2),
        (11, b"user-011", {"event_id": 11, "kind": "signup", "amount": 0, "note": "p0-second"}, 0),
        (12, b"order-012", {"event_id": 12, "kind": "purchase", "amount": 7.25, "note": "p1-second"}, 1),
    ]
    return [
        SeedRecord(
            event_id=event_id,
            key=key,
            value=json.dumps(value, sort_keys=True).encode("utf-8"),
            partition=partition,
            timestamp_ms=base_ts_ms + event_id,
        )
        for event_id, key, value, partition in rows
    ]
```

- [ ] **Step 2: Add raw mirror test**

Append:

```python
def test_raw_read_transform_write_roundtrip(kafka_e2e_context: KafkaE2EContext) -> None:
    ctx = kafka_e2e_context
    source_records = build_source_records(ctx.base_ts_ms)
    produce_seed_records(bootstrap=ctx.bootstrap, topic=ctx.source_topic, records=source_records)

    source_df = daft.read_kafka(
        bootstrap_servers=ctx.bootstrap,
        topics=ctx.source_topic,
        group_id=f"daft-kafka-local-e2e-{uuid.uuid4().hex}",
        timeout_ms=WRITE_TIMEOUT_MS,
        start="earliest",
        end="latest",
    ).collect()
    mirror_df = source_df.select("key", "value", "partition", "timestamp_ms")

    summary = mirror_df.write_kafka(
        bootstrap_servers=ctx.bootstrap,
        topic=ctx.raw_topic,
        key_col="key",
        value_col="value",
        partition_col="partition",
        timestamp_ms_col="timestamp_ms",
        kafka_client_config={
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "client.id": f"daft-e2e-raw-{ctx.run_id}",
        },
        timeout_ms=WRITE_TIMEOUT_MS,
    )

    assert_summary(summary, attempted=len(source_records), delivered=len(source_records))

    consumed = [normalize_message(msg) for msg in consume_exactly(
        bootstrap=ctx.bootstrap,
        topic=ctx.raw_topic,
        count=len(source_records),
    )]
    expected = [
        {
            "key": record.key,
            "value": record.value,
            "partition": record.partition,
            "timestamp_ms": record.timestamp_ms,
        }
        for record in source_records
    ]
    consumed_tuples = sorted(
        (row["partition"], row["timestamp_ms"], row["key"], row["value"]) for row in consumed
    )
    expected_tuples = sorted(
        (row["partition"], row["timestamp_ms"], row["key"], row["value"]) for row in expected
    )
    assert consumed_tuples == expected_tuples

    daft_rows = read_all_with_daft(bootstrap=ctx.bootstrap, topic=ctx.raw_topic)
    assert len(daft_rows) == len(source_records)
```

- [ ] **Step 3: Run the raw mirror test against Redpanda**

Run:

```bash
docker compose -f tests/integration/io/docker-compose/docker-compose.yml up -d redpanda
DAFT_RUN_KAFKA_LOCAL_E2E=1 DAFT_RUNNER=native \
  .venv/bin/python -m pytest tests/local_e2e/test_kafka_write_e2e.py::test_raw_read_transform_write_roundtrip \
  -m "integration and local_e2e" -q -s
```

Expected: test passes and summary counters match the 12 seeded records.

## Task 5: JSON Dynamic Topic And Tombstone Scenario

**Files:**
- Modify: `tests/local_e2e/test_kafka_write_e2e.py`

- [ ] **Step 1: Add JSON dynamic topic test**

Append:

```python
def test_json_dynamic_topic_and_tombstone(kafka_e2e_context: KafkaE2EContext) -> None:
    ctx = kafka_e2e_context
    json_rows = [
        {
            "topic": ctx.user_topic,
            "key": "user-001",
            "value": {"op": "upsert", "name": "alice", "tags": ["new", None]},
            "partition": 0,
            "timestamp_ms": ctx.base_ts_ms + 100,
        },
        {
            "topic": ctx.order_topic,
            "key": "order-001",
            "value": {"op": "created", "amount": 19.99, "ok": True},
            "partition": 1,
            "timestamp_ms": ctx.base_ts_ms + 101,
        },
        {
            "topic": ctx.user_topic,
            "key": "user-002",
            "value": None,
            "partition": 1,
            "timestamp_ms": ctx.base_ts_ms + 102,
        },
    ]

    summary = daft.from_pylist(json_rows).write_kafka(
        bootstrap_servers=ctx.bootstrap,
        topic_col="topic",
        key_col="key",
        value_col="value",
        partition_col="partition",
        timestamp_ms_col="timestamp_ms",
        key_format="utf8",
        value_format="json",
        kafka_client_config={"acks": "all", "compression.type": "zstd"},
        timeout_ms=WRITE_TIMEOUT_MS,
    )

    assert_summary(summary, attempted=3, delivered=3)

    user_messages = {
        msg.key().decode("utf-8"): normalize_message(msg)
        for msg in consume_exactly(bootstrap=ctx.bootstrap, topic=ctx.user_topic, count=2)
    }
    order_messages = {
        msg.key().decode("utf-8"): normalize_message(msg)
        for msg in consume_exactly(bootstrap=ctx.bootstrap, topic=ctx.order_topic, count=1)
    }

    assert json.loads(user_messages["user-001"]["value"].decode("utf-8")) == json_rows[0]["value"]
    assert user_messages["user-002"]["value"] is None
    assert json.loads(order_messages["order-001"]["value"].decode("utf-8")) == json_rows[1]["value"]
    assert user_messages["user-001"]["partition"] == 0
    assert user_messages["user-002"]["partition"] == 1
    assert order_messages["order-001"]["partition"] == 1
```

- [ ] **Step 2: Run the JSON dynamic topic test**

Run:

```bash
DAFT_RUN_KAFKA_LOCAL_E2E=1 DAFT_RUNNER=native \
  .venv/bin/python -m pytest tests/local_e2e/test_kafka_write_e2e.py::test_json_dynamic_topic_and_tombstone \
  -m "integration and local_e2e" -q -s
```

Expected: test passes; top-level `None` is consumed as Kafka null value.

## Task 6: Headers And Compact Tombstone Scenarios

**Files:**
- Modify: `tests/local_e2e/test_kafka_write_e2e.py`

- [ ] **Step 1: Add headers test**

Append:

```python
def test_headers_preserve_duplicate_order_and_null(kafka_e2e_context: KafkaE2EContext) -> None:
    ctx = kafka_e2e_context
    summary = daft.from_pylist(
        [
            {
                "key": b"k1",
                "value": b"v1",
                "headers": [
                    {"key": "trace", "value": b"a"},
                    {"key": "trace", "value": b"b"},
                    {"key": "nullable", "value": None},
                ],
            }
        ]
    ).write_kafka(
        bootstrap_servers=ctx.bootstrap,
        topic=ctx.headers_topic,
        key_col="key",
        value_col="value",
        headers_col="headers",
        kafka_client_config={"acks": "all"},
        timeout_ms=WRITE_TIMEOUT_MS,
    )

    assert_summary(summary, attempted=1, delivered=1)
    [message] = consume_exactly(bootstrap=ctx.bootstrap, topic=ctx.headers_topic, count=1)
    assert message.headers() == [("trace", b"a"), ("trace", b"b"), ("nullable", None)]
```

- [ ] **Step 2: Add compact tombstone test**

Append:

```python
def test_compacted_topic_receives_tombstone(kafka_e2e_context: KafkaE2EContext) -> None:
    ctx = kafka_e2e_context
    summary = daft.from_pylist(
        [
            {"key": "user-compact-001", "value": {"status": "active"}},
            {"key": "user-compact-001", "value": None},
        ]
    ).write_kafka(
        bootstrap_servers=ctx.bootstrap,
        topic=ctx.compact_topic,
        key_col="key",
        value_col="value",
        key_format="utf8",
        value_format="json",
        kafka_client_config={"acks": "all"},
        timeout_ms=WRITE_TIMEOUT_MS,
    )

    assert_summary(summary, attempted=2, delivered=2)
    messages = consume_exactly(bootstrap=ctx.bootstrap, topic=ctx.compact_topic, count=2)
    assert [msg.key().decode("utf-8") for msg in messages] == ["user-compact-001", "user-compact-001"]
    assert json.loads(messages[0].value().decode("utf-8")) == {"status": "active"}
    assert messages[1].value() is None
```

- [ ] **Step 3: Run headers and compact tests**

Run:

```bash
DAFT_RUN_KAFKA_LOCAL_E2E=1 DAFT_RUNNER=native \
  .venv/bin/python -m pytest \
  tests/local_e2e/test_kafka_write_e2e.py::test_headers_preserve_duplicate_order_and_null \
  tests/local_e2e/test_kafka_write_e2e.py::test_compacted_topic_receives_tombstone \
  -m "integration and local_e2e" -q -s
```

Expected: both tests pass.

## Task 7: Validation Guardrails

**Files:**
- Modify: `tests/local_e2e/test_kafka_write_e2e.py`

- [ ] **Step 1: Add validation tests**

Append:

```python
def test_validation_guardrails(kafka_e2e_context: KafkaE2EContext) -> None:
    ctx = kafka_e2e_context
    df = daft.from_pydict({"value": [b"x"]})

    with pytest.raises(NotImplementedError, match="transactional.id"):
        df.write_kafka(
            bootstrap_servers=ctx.bootstrap,
            topic=ctx.raw_topic,
            kafka_client_config={"transactional.id": "tx-1"},
        )

    with pytest.raises(Exception, match="topic"):
        daft.from_pylist([{"topic": None, "value": b"x"}]).write_kafka(
            bootstrap_servers=ctx.bootstrap,
            topic_col="topic",
            value_col="value",
            timeout_ms=WRITE_TIMEOUT_MS,
        )

    with pytest.raises(Exception, match="topic"):
        daft.from_pylist([{"topic": "", "value": b"x"}]).write_kafka(
            bootstrap_servers=ctx.bootstrap,
            topic_col="topic",
            value_col="value",
            timeout_ms=WRITE_TIMEOUT_MS,
        )
```

- [ ] **Step 2: Run validation guardrails**

Run:

```bash
DAFT_RUN_KAFKA_LOCAL_E2E=1 DAFT_RUNNER=native \
  .venv/bin/python -m pytest tests/local_e2e/test_kafka_write_e2e.py::test_validation_guardrails \
  -m "integration and local_e2e" -q -s
```

Expected: test passes without producing misleading success summaries.

## Task 8: Full Verification

**Files:**
- Verify: `tests/local_e2e/test_kafka_write_e2e.py`
- Verify: `tests/conftest.py`

- [ ] **Step 1: Run Python syntax check**

Run:

```bash
.venv/bin/python -m py_compile tests/local_e2e/test_kafka_write_e2e.py
```

Expected: exit code `0`.

- [ ] **Step 2: Run targeted ruff check**

Run:

```bash
.venv/bin/python -m ruff check tests/local_e2e/test_kafka_write_e2e.py
```

Expected: `All checks passed!`

- [ ] **Step 3: Verify default skip behavior**

Run:

```bash
.venv/bin/python -m pytest tests/local_e2e/test_kafka_write_e2e.py -q -rs
```

Expected: module skipped because `DAFT_RUN_KAFKA_LOCAL_E2E` is not set.

- [ ] **Step 4: Run full local E2E**

Run:

```bash
docker compose -f tests/integration/io/docker-compose/docker-compose.yml up -d redpanda
DAFT_RUN_KAFKA_LOCAL_E2E=1 DAFT_RUNNER=native \
  .venv/bin/python -m pytest tests/local_e2e/test_kafka_write_e2e.py \
  -m "integration and local_e2e" -q -s
```

Expected: all local E2E tests pass against Redpanda.

- [ ] **Step 5: Confirm repository diff hygiene**

Run:

```bash
git diff --check
git status --short
```

Expected: no whitespace errors; only expected files are modified.

## Self-Review Checklist

- Spec coverage: Tasks cover opt-in invocation, Redpanda readiness, unique topics, source seed data, raw mirror, Daft readback, JSON dynamic topics, tombstones, headers, compact topic tombstone, null and empty dynamic topic guardrails, and verification commands.
- Placeholder handling: The placeholder is limited to Task 1 bootstrap and is explicitly replaced before any E2E scenario is added; no task uses `TBD`, `TODO`, `fill in`, or unspecified test commands.
- Type consistency: Helper names in tests match helper definitions: `wait_for_kafka_ready`, `create_topic`, `produce_seed_records`, `consume_exactly`, `read_all_with_daft`, `assert_summary`, and `normalize_message`.
- Scope: TLS/SASL and broker-dependent delivery failure remain future extensions, matching the approved v1 design.
