# Kafka Write Local E2E Design

## Purpose

This design adds an opt-in local end-to-end test flow for Daft's native
`DataFrame.write_kafka(...)` path. The test is intended for Daft Kafka write
development confidence, not for default community CI. It uses a real Redpanda
broker through Docker, writes real Kafka records through Daft's native Rust
producer, and verifies results with both Kafka's Python client and
`daft.read_kafka(...)`.

The E2E should prove that Daft can:

- Read records from a real Kafka-compatible broker.
- Transform and write those records back through native `write_kafka`.
- Preserve Kafka record fields: topic, partition, key, value, timestamp, and
  headers.
- Encode JSON values correctly, including top-level tombstones and nested JSON
  nulls.
- Pass through common librdkafka producer configuration such as `acks`,
  idempotence, compression, and `client.id`.
- Return task-level write summaries with useful counters.
- Surface expected validation failures without reporting success.

## Non-Goals

- This test should not become a default required upstream CI check.
- This test should not require TLS, SASL, Kerberos, or a production-like Kafka
  security setup in v1.
- This test should not wait for Kafka log compaction to occur. It only verifies
  that Daft writes tombstone records correctly.
- This test should not replace the existing mock/unit/integration tests.

## Location And Invocation

Add the local-only test at:

```text
tests/local_e2e/test_kafka_write_e2e.py
```

Use the existing Redpanda service in:

```text
tests/integration/io/docker-compose/docker-compose.yml
```

Recommended manual invocation:

```bash
docker compose -f tests/integration/io/docker-compose/docker-compose.yml up -d redpanda
DAFT_RUN_KAFKA_LOCAL_E2E=1 DAFT_RUNNER=native \
  .venv/bin/python -m pytest tests/local_e2e/test_kafka_write_e2e.py -q -s
```

The test module should skip unless `DAFT_RUN_KAFKA_LOCAL_E2E=1` is present:

```python
if os.environ.get("DAFT_RUN_KAFKA_LOCAL_E2E") != "1":
    pytest.skip("local Kafka E2E is opt-in", allow_module_level=True)
```

Use module-level markers:

```python
pytestmark = [pytest.mark.local_e2e, pytest.mark.integration]
```

## Broker And Topic Setup

Use `127.0.0.1:9092` as the bootstrap address, matching the existing Redpanda
Docker Compose service. The test should wait for broker readiness with
`confluent_kafka.Consumer.list_topics(timeout=...)`.

Each run creates a unique `run_id` and all topic names include that id:

```text
daft-e2e-src-{run_id}
daft-e2e-raw-out-{run_id}
daft-e2e-json-user-{run_id}
daft-e2e-json-order-{run_id}
daft-e2e-headers-{run_id}
daft-e2e-compact-{run_id}
daft-e2e-bad-{run_id}
```

Topic configuration:

| Topic role | Partitions | Notes |
| --- | ---: | --- |
| `src` | 3 | Seeded by Kafka producer, read by Daft |
| `raw-out` | 3 | Written by Daft raw mirror flow |
| `json-user` | 2 | Dynamic topic write target |
| `json-order` | 2 | Dynamic topic write target |
| `headers` | 1 | Header ordering assertions are easier on one partition |
| `compact` | 1 | Set `cleanup.policy=compact`; do not wait for compaction |
| `bad` | 0 | Do not create by default; reserved for failure testing |

## Test Data

### Source Records

Seed at least 12 records into the source topic with Kafka's Python producer.
Records should cover:

- All 3 source partitions.
- Duplicate keys.
- Unicode keys and values.
- Different payload sizes.
- Explicit `timestamp_ms`.
- Raw bytes that contain JSON text.

Representative rows:

```python
source_records = [
    {
        "event_id": 1,
        "key": b"user-001",
        "value": b'{"event_id":1,"kind":"signup","amount":0,"note":"hello"}',
        "partition": 0,
        "timestamp_ms": base_ts_ms + 1,
    },
    {
        "event_id": 2,
        "key": b"order-001",
        "value": b'{"event_id":2,"kind":"purchase","amount":19.99,"note":"zstd-path"}',
        "partition": 1,
        "timestamp_ms": base_ts_ms + 2,
    },
    {
        "event_id": 3,
        "key": b"user-001",
        "value": b'{"event_id":3,"kind":"update","amount":0,"note":"duplicate-key"}',
        "partition": 0,
        "timestamp_ms": base_ts_ms + 3,
    },
    {
        "event_id": 4,
        "key": "user-004".encode("utf-8"),
        "value": json.dumps(
            {"event_id": 4, "kind": "unicode", "note": "hello unicode"},
            ensure_ascii=False,
        ).encode("utf-8"),
        "partition": 2,
        "timestamp_ms": base_ts_ms + 4,
    },
]
```

Avoid non-ASCII literals in the file unless the repo style already permits them;
use escaped or ASCII-only examples when possible.

### JSON Dynamic Topic Rows

Use Daft `from_pylist` rows:

```python
json_rows = [
    {
        "topic": user_topic,
        "key": "user-001",
        "value": {"op": "upsert", "name": "alice", "tags": ["new", None]},
        "partition": 0,
        "timestamp_ms": base_ts_ms + 100,
    },
    {
        "topic": order_topic,
        "key": "order-001",
        "value": {"op": "created", "amount": 19.99, "ok": True},
        "partition": 1,
        "timestamp_ms": base_ts_ms + 101,
    },
    {
        "topic": user_topic,
        "key": "user-002",
        "value": None,
        "partition": 1,
        "timestamp_ms": base_ts_ms + 102,
    },
]
```

This verifies dynamic topic routing, UTF-8 key encoding, JSON serialization,
nested JSON nulls, top-level tombstones, explicit partition, and explicit
timestamp.

### Header Rows

Use one single-partition topic for deterministic header assertions:

```python
header_rows = [
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
```

This verifies duplicate header keys, ordering, and null header values.

### Compacted Topic Rows

Use a compacted topic to verify tombstone compatibility:

```python
compact_rows = [
    {"key": "user-compact-001", "value": {"status": "active"}},
    {"key": "user-compact-001", "value": None},
]
```

Do not wait for compaction. Assert that Kafka contains both records and that the
second record has `msg.value() is None`.

## E2E Scenarios

### Scenario 1: Kafka Source To Daft To Kafka Raw Mirror

Flow:

1. Create source and raw output topics.
2. Seed source records using `confluent_kafka.Producer`.
3. Read source topic with `daft.read_kafka(start="earliest", end="latest")`.
4. Select `key`, `value`, `partition`, and `timestamp_ms`.
5. Write the selected DataFrame with `write_kafka(...)`.

Producer config:

```python
kafka_client_config={
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "zstd",
    "client.id": f"daft-e2e-raw-{run_id}",
}
```

Assertions:

- `messages_attempted` sum equals source row count.
- `messages_delivered` sum equals source row count.
- `messages_failed` sum is `0`.
- `bytes_delivered` sum is greater than `0`.
- `task_id` values are integers; do not require them to start at zero.
- Kafka consumer reads the exact expected keys, values, partitions, and
  timestamps from `raw-out`.
- `daft.read_kafka(...)` can read all raw output rows back.

Order should only be asserted within a single partition. For multi-partition
topics, compare normalized sets or group by partition.

### Scenario 2: JSON Dynamic Topic And Tombstone

Flow:

1. Create `json-user` and `json-order` topics.
2. Build `json_rows` with Daft `from_pylist`.
3. Write with:

```python
write_kafka(
    bootstrap_servers=bootstrap,
    topic_col="topic",
    key_col="key",
    value_col="value",
    partition_col="partition",
    timestamp_ms_col="timestamp_ms",
    key_format="utf8",
    value_format="json",
    kafka_client_config={"acks": "all", "compression.type": "zstd"},
    timeout_ms=20_000,
)
```

Assertions:

- User topic receives the user upsert and user tombstone.
- Order topic receives the order event.
- Nested Python `None` serializes as JSON `null`.
- Top-level Python `None` writes Kafka null value.
- UTF-8 keys decode to the expected strings.
- Explicit partitions and timestamps are visible on consumed records.

### Scenario 3: Headers

Flow:

1. Create headers topic with one partition.
2. Write `header_rows` with `headers_col="headers"`.
3. Consume with Kafka's Python consumer.

Assertions:

- Headers equal `[("trace", b"a"), ("trace", b"b"), ("nullable", None)]`.
- Duplicate keys and order are preserved.
- Summary reports one attempted and one delivered message.

### Scenario 4: Compacted Topic Tombstone

Flow:

1. Create compact topic with `cleanup.policy=compact`.
2. Write `compact_rows` using `key_format="utf8"` and `value_format="json"`.
3. Consume the two records with Kafka's Python consumer.

Assertions:

- First record value decodes to `{"status": "active"}`.
- Second record has the same key and `msg.value() is None`.
- Summary reports two attempted and two delivered messages.

### Scenario 5: Validation Guardrails

Include stable validation failures:

- `kafka_client_config={"transactional.id": "tx-1"}` raises
  `NotImplementedError`.
- Dynamic topic `None` or empty string raises an error containing
  `[write_kafka]` and topic context.

Do not include broker-dependent delivery failure in v1 unless it is proven
stable with Redpanda. A future P1 test can try writing to a non-existent topic
with `allow.auto.create.topics=False` and short `message.timeout.ms`.

## Helper Functions

The test module should keep helper functions local and explicit:

```python
wait_for_kafka_ready(bootstrap)
create_topic(bootstrap, topic, partitions, config=None)
produce_seed_records(bootstrap, topic, records)
consume_exactly(bootstrap, topic, count, timeout_s=20)
read_all_with_daft(bootstrap, topic)
assert_summary(summary, attempted, delivered, failed=0)
normalize_kafka_message(msg)
```

Implementation notes:

- Use a fresh consumer group id for each consume/read helper.
- Use bounded deadlines and clear failure messages.
- Prefer exact count consumption when the expected count is known.
- Close consumers in `finally`.
- Do not rely on cross-partition ordering.
- Print topic names and consumed counts in failures.
- Keep topic cleanup best-effort; unique topic names already avoid collisions.

## Acceptance Criteria

The local E2E passes when:

- Redpanda is reachable through Docker on `127.0.0.1:9092`.
- The raw mirror flow writes all source records and preserves key, value,
  partition, and timestamp.
- Daft can read back the raw mirror topic.
- Dynamic topic routing writes user and order records to distinct topics.
- JSON serialization preserves nested nulls and writes top-level nulls as Kafka
  tombstones.
- Headers preserve duplicate keys, order, and null values.
- The compacted topic receives an upsert followed by a tombstone.
- Summary rows report expected attempted, delivered, failed, and bytes counters.
- Validation failures are raised without creating misleading success summaries.

## Future Extensions

P1:

- Broker delivery failure with topic auto-creation disabled and short delivery
  timeout.
- Larger batch, for example 5,000 rows, to exercise multiple Daft partitions and
  task summaries.
- Optional Ray runner run if the local environment has Ray available.

P2:

- Separate TLS-enabled Redpanda profile with generated CA and client
  certificate material.
- SASL/PLAIN profile if the broker image supports a lightweight local setup.
- More producer config coverage for retries, linger, batching, and queue limits.
