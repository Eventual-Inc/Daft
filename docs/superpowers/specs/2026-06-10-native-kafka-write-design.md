# Native Kafka Write Design

## Summary

Daft currently supports bounded Kafka reads through `daft.read_kafka(...)`, but does not provide a native Kafka write path. This design adds `DataFrame.write_kafka(...)` as a Daft-native Kafka producer sink backed by Rust and `rdkafka`/`librdkafka`.

The target is a full Kafka producer sink for batch DataFrames:

- Static and dynamic topic routing.
- Raw, UTF-8, and JSON value encoding.
- Raw and UTF-8 key encoding.
- Static and dynamic partition selection.
- Kafka message timestamps.
- Kafka headers with duplicate key preservation.
- Task-level write summary rows for logging and diagnosis.
- `librdkafka` producer configuration passthrough.
- Delivery-result based accounting.
- Optional producer metrics/statistics hooks.

The first version is explicitly at-least-once. It does not support transactions or exactly-once semantics.

Related issue: https://github.com/Eventual-Inc/Daft/issues/7101

## Context

Current Kafka support lives in `daft/io/_kafka.py` and exposes:

```python
daft.read_kafka(
    bootstrap_servers: str | Sequence[str],
    topics: str | Sequence[str],
    *,
    start: object = "earliest",
    end: object = "latest",
    group_id: str = "daft-bounded-kafka-reader",
    partitions: Sequence[int] | None = None,
    chunk_size: int = 1024,
    kafka_client_config: Mapping[str, object] | None = None,
    timeout_ms: int = 10_000,
)
```

`read_kafka` returns the fixed schema:

```text
topic: string
partition: int32
offset: int64
timestamp_ms: int64
key: binary
value: binary
```

The write implementation should preserve useful naming alignment with `read_kafka`, especially `bootstrap_servers`, `kafka_client_config`, `timeout_ms`, and default `key`/`value`/`timestamp_ms` column names.

Daft native writes currently flow through:

```text
DataFrame.write_*
  -> LogicalPlanBuilder
  -> SinkInfo
  -> LocalPhysicalPlan
  -> daft-local-execution WriteSink
  -> daft-writers WriterFactory
```

Kafka write should join this native path rather than use Python `DataSink`.

## Goals

- Provide a first-class `DataFrame.write_kafka(...)` API.
- Keep Kafka write native to Daft planner/execution/writer crates.
- Use `rdkafka` as the Rust Kafka client.
- Preserve broad `librdkafka` producer configuration access.
- Return task-level summary rows by default.
- Count delivered messages from Kafka delivery results, not from producer statistics.
- Support native and Ray/distributed execution.
- Keep no-default-feature Rust builds working.

## Non-Goals

- No transaction or exactly-once support in the first version.
- No Schema Registry integration.
- No Avro or Protobuf encoding.
- No per-message delivery-report DataFrame by default.
- No streaming or unbounded DataFrame sink.
- No rewrite of `read_kafka` from Python to Rust as part of this feature.

## Contribution Constraints

This is a non-trivial Daft contribution. PRs should:

- Link to issue `#7101`.
- Include tests or explain gaps.
- Keep changes reviewable.
- Use Conventional Commit titles.
- Disclose material AI assistance and verification.
- Expect core owner review for logical plan, local plan, local execution, distributed execution, writers, and Cargo changes.

## User API

Proposed API:

```python
df.write_kafka(
    bootstrap_servers: str | Sequence[str],
    topic: str | None = None,
    *,
    topic_col: str | None = None,
    value_col: str = "value",
    key_col: str | None = "key",
    headers_col: str | None = None,
    partition: int | None = None,
    partition_col: str | None = None,
    timestamp_ms_col: str | None = "timestamp_ms",
    value_format: Literal["raw", "utf8", "json"] = "raw",
    key_format: Literal["raw", "utf8"] = "raw",
    kafka_client_config: Mapping[str, object] | None = None,
    timeout_ms: int = 10_000,
) -> DataFrame
```

Rules:

- `topic` and `topic_col` are mutually exclusive. Exactly one must be set.
- `partition` and `partition_col` are mutually exclusive.
- `bootstrap_servers`, `kafka_client_config`, and `timeout_ms` mirror `read_kafka`.
- `value_col` is required semantically and defaults to `value`.
- `key_col` defaults to opportunistic `key`: use it if present, otherwise write unkeyed messages.
- `timestamp_ms_col` defaults to opportunistic `timestamp_ms`: use it if present, otherwise let Kafka assign timestamps.
- Explicitly provided `key_col` or `timestamp_ms_col` must exist.
- `key_col=None` disables keys.
- `timestamp_ms_col=None` disables explicit timestamps.
- `headers_col` uses Kafka-native ordered headers and should preserve duplicate keys.

## Return Schema

`write_kafka` returns task-level summary rows by default. This is more useful for logs and distributed diagnosis than a single global summary.

Suggested schema:

```text
task_id: int64
messages_attempted: int64
messages_delivered: int64
messages_failed: int64
bytes_delivered: int64
first_error: utf8
```

If Daft cannot reliably expose a numeric task id at the writer boundary, use `writer_id: int64` first and keep the same semantics.

Do not return per-message offsets by default. For large writes, per-message delivery metadata would produce an output DataFrame as large as the input. A future `delivery_report_mode` can be designed separately if users need that level of detail.

## Field Semantics

| Kafka field | API source | Supported types | Null behavior |
| --- | --- | --- | --- |
| topic | `topic` or `topic_col` | static `str` or `Utf8` column | null dynamic topic is an error |
| value | `value_col` | binary, utf8, or JSON-serializable value | null value sends Kafka tombstone |
| key | `key_col` | binary or utf8 | null key writes unkeyed message |
| partition | `partition` or `partition_col` | non-negative int32/int64 | null lets Kafka choose partition |
| timestamp | `timestamp_ms_col` | int64 epoch milliseconds | null lets Kafka assign timestamp |
| headers | `headers_col` | list of key/value structs | null means no headers |

### Value Format

`value_format="raw"`:

- Requires `value_col` to be binary.
- Sends bytes unchanged.
- This is the default and best matches `read_kafka` round trips.

`value_format="utf8"`:

- Requires `value_col` to be string/utf8.
- Encodes strings as UTF-8 bytes.

`value_format="json"`:

- Serializes the selected Daft value to UTF-8 JSON bytes.
- Supports JSON-representable scalar, struct, list, and map values.
- A Daft null value sends a Kafka tombstone.
- A non-null JSON value that serializes to JSON `null` sends bytes `b"null"`.
- Binary values should not be implicitly base64 encoded in the first version.

### Key Format

`key_format="raw"`:

- Requires binary key values.

`key_format="utf8"`:

- Requires string/utf8 key values.

Do not add `key_format="json"` in the first version. Kafka keys are commonly used for partitioning and compaction, and raw/utf8 keeps behavior predictable.

### Headers

Canonical type:

```text
list[struct<key: utf8, value: binary | utf8 | null>>
```

Kafka headers allow duplicate keys and preserve ordering. A map type should not be the canonical representation because it cannot preserve duplicate keys. A future convenience path may accept maps and convert them to ordered headers, but the native representation should remain list-of-struct.

## Logical and Physical Plan Design

Add a native Kafka sink variant:

```rust
pub enum SinkInfo<E = ExprRef> {
    OutputFileInfo(OutputFileInfo<E>),
    KafkaInfo(KafkaWriteInfo<E>),
    #[cfg(feature = "python")]
    CatalogInfo(CatalogInfo<E>),
    #[cfg(feature = "python")]
    DataSinkInfo(DataSinkInfo),
}
```

Core plan object:

```rust
pub struct KafkaWriteInfo<E = ExprRef> {
    pub bootstrap_servers: String,
    pub topic: KafkaTopic<E>,
    pub value: KafkaValue<E>,
    pub key: Option<KafkaColumn<E>>,
    pub headers: Option<KafkaHeaders<E>>,
    pub partition: Option<KafkaPartition<E>>,
    pub timestamp_ms: Option<E>,
    pub value_format: KafkaValueFormat,
    pub key_format: KafkaKeyFormat,
    pub kafka_client_config: BTreeMap<String, KafkaConfigValue>,
    pub timeout_ms: u64,
}
```

Supporting enums:

```rust
pub enum KafkaTopic<E> {
    Static(String),
    Dynamic(E),
}

pub enum KafkaPartition<E> {
    Static(i32),
    Dynamic(E),
}

pub enum KafkaValueFormat {
    Raw,
    Utf8,
    Json,
}

pub enum KafkaKeyFormat {
    Raw,
    Utf8,
}

pub enum KafkaConfigValue {
    String(String),
    Int(i64),
    Float(HashableFloatWrapper),
    Bool(bool),
    Null,
}
```

`KafkaWriteInfo` must:

- Derive or implement `Clone`, `Serialize`, `Deserialize`, `PartialEq`, `Eq`, and `Hash`.
- Avoid raw `f64` in hashable plan types. Use Daft's hashable float wrapper, or normalize Kafka config values to strings before storing them in the plan.
- Use generic `E = ExprRef | BoundExpr`, like existing sink info types.
- Bind expressions once against the input schema.
- Avoid any direct dependency on `rdkafka` types.

Physical flow:

```text
DataFrame.write_kafka(...)
  -> LogicalPlanBuilder::kafka_write(...)
  -> SinkInfo::KafkaInfo(KafkaWriteInfo<ExprRef>)
  -> SinkInfo::bind(schema)
  -> KafkaWriteInfo<BoundExpr>
  -> LocalPhysicalPlan::KafkaWrite
  -> WriteSink with WriteFormat::Kafka
  -> daft_writers::kafka::KafkaWriterFactory
```

Kafka write should not use `CommitWrite`. File writes need a commit phase for files/manifests. Kafka is an external side-effect sink where delivery success is the task completion condition.

## Rust Writer Design

Add:

```text
src/daft-writers/src/kafka/
  mod.rs
  config.rs
  record.rs
  headers.rs
  producer.rs
  accounting.rs
  metrics.rs
  writer.rs
```

Responsibilities:

- `config.rs`: normalize Daft config into `rdkafka::ClientConfig`.
- `record.rs`: project Daft rows into Kafka outgoing records.
- `headers.rs`: parse list-of-struct Kafka headers.
- `producer.rs`: define a small producer trait and the `rdkafka` adapter.
- `accounting.rs`: delivery counters and summary RecordBatch creation.
- `metrics.rs`: optional `rdkafka` statistics snapshot handling.
- `writer.rs`: `KafkaWriterFactory` and `KafkaWriter`.

Core internal types:

```rust
pub struct KafkaOutgoingRecord {
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<KafkaHeader>,
    pub partition: Option<i32>,
    pub timestamp_ms: Option<i64>,
}

pub struct KafkaHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

pub struct KafkaDelivery {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
}
```

Use a producer trait for testability:

```rust
#[async_trait]
trait KafkaProducer: Send + Sync {
    async fn send(&self, record: KafkaOutgoingRecord) -> DaftResult<KafkaDelivery>;
    async fn flush(&self, timeout: Duration) -> DaftResult<()>;

    fn metrics_snapshot(&self) -> Option<KafkaProducerMetrics> {
        None
    }
}
```

`RdkafkaProducer` should wrap `rdkafka::producer::FutureProducer`.

`FutureProducer` is preferred because Daft writer execution is async and delivery futures compose naturally with delivery-result accounting. `ThreadedProducer` is callback-oriented and would make error propagation and testing less direct.

## Write and Close Behavior

`KafkaWriter.write(input)`:

```text
1. Iterate rows from the input MicroPartition.
2. Build KafkaOutgoingRecord from topic/key/value/headers/partition/timestamp.
3. Increment messages_attempted only after record construction succeeds.
4. Send with FutureProducer.
5. Await bounded in-flight delivery futures.
6. On delivery success, increment messages_delivered and bytes_delivered.
7. On enqueue or delivery failure, increment messages_failed, store first_error, and return DaftError.
8. Return WriteResult with delivered row and byte counts.
```

`KafkaWriter.close()`:

```text
1. Drain all remaining in-flight delivery futures.
2. Flush producer with timeout_ms.
3. Return task-level summary RecordBatch.
```

Use bounded in-flight futures internally so producer throughput is not fully serial. Do not expose a Daft-level `max_in_flight_messages` parameter in the first API unless needed. Let users tune librdkafka batching and queue behavior through `kafka_client_config`.

## Delivery Semantics

First version semantics:

```text
at-least-once
delivery-result confirmed
fail on delivery error
no transaction
no exactly-once
```

Important behavior:

- Delivery success is based on Kafka delivery result.
- Any task delivery error fails the Daft job.
- Already delivered Kafka messages are not rolled back.
- User retries or Daft task retries can duplicate messages.
- Kafka producer idempotence can reduce retry duplicates but is not Daft-level exactly-once.

## Config Passthrough

`kafka_client_config` should preserve `librdkafka` flexibility:

- Accept scalar values: `str`, `int`, `bool`, `float`, and `None`.
- Convert keys to strings.
- Convert values into `rdkafka::ClientConfig` values.
- Reject non-scalar values.
- Do not log config values.

Managed or restricted keys:

```text
bootstrap.servers  # managed by Daft
transactional.id   # rejected in first version
```

`client.id` should remain user-overridable. Daft may provide a default such as `daft-write-kafka`, but should not prevent user override.

Supported through passthrough:

```text
acks
enable.idempotence
compression.type
linger.ms
batch.size
message.timeout.ms
request.timeout.ms
queue.buffering.max.messages
queue.buffering.max.kbytes
security.protocol
sasl.*
ssl.*
statistics.interval.ms
delivery.timeout.ms
retries
```

Do not whitelist only these keys. They are examples. Unknown librdkafka keys should generally pass through to `rdkafka::ClientConfig` so librdkafka can validate them.

## Idempotence and Transactions

Users may enable producer idempotence:

```python
df.write_kafka(
    ...,
    kafka_client_config={
        "acks": "all",
        "enable.idempotence": True,
    },
)
```

Document clearly that this is not exactly-once semantics for the Daft operation. It does not protect against user reruns, job retries, or task retries outside the producer session.

Reject:

```python
kafka_client_config={"transactional.id": "..."}
```

with:

```text
NotImplemented("[write_kafka] transactional.id is not supported yet")
```

Future transactional support should use explicit Daft-owned API, for example:

```python
df.write_kafka(
    ...,
    delivery_semantics="at_least_once" | "transactional",
    transactional_id_prefix="daft-job-...",
)
```

Distributed transaction boundaries must be controlled by Daft, not by arbitrary user-provided shared `transactional.id` values.

## Metrics

Default correctness summary comes from delivery results:

```text
messages_attempted
messages_delivered
messages_failed
bytes_delivered
first_error
```

`rdkafka`/`librdkafka` statistics are observability data, not the source of correctness summary:

- They are only emitted when `statistics.interval.ms` is set.
- They are producer/client lifecycle snapshots.
- They are asynchronous and periodic.
- They should not replace delivery-result accounting.

Internal optional snapshot:

```rust
pub struct KafkaProducerMetrics {
    pub txmsgs: i64,
    pub txmsg_bytes: i64,
    pub outbuf_msg_cnt: i64,
    pub msg_cnt: i64,
    pub msg_max: i64,
}
```

Do not add these fields to the public return schema in the first version. Prefer Daft runtime metrics or a future explicit metrics mode.

## Error Model

Use existing Daft errors with clear prefixes:

| Scenario | Error |
| --- | --- |
| invalid public argument | `DaftError::ValueError` |
| invalid config value type | `DaftError::TypeError` |
| record projection/type error | `DaftError::ComputeError` |
| Kafka enqueue/delivery/flush failure | `DaftError::External` |
| `transactional.id` | `DaftError::NotImplemented` |

All messages should start with `[write_kafka]`.

Never include secret config values in logs or errors. Sensitive keys include:

```text
sasl.password
ssl.key.password
sasl.oauthbearer.config
```

## Feature Gating

`rdkafka` should be optional and localized to `daft-writers`.

Suggested shape:

```toml
[dependencies]
rdkafka = { workspace = true, optional = true, default-features = false, features = ["tokio"] }

[features]
kafka = ["dep:rdkafka"]
```

Plan and execution crates can compile Kafka plan/config types without depending directly on `rdkafka`. Only the writer implementation should need the external client.

No-default-feature CI must continue to pass:

```bash
cargo test --no-default-features --workspace
```

Open decision: whether the root `python` feature should include Kafka write by default for wheels, or whether Kafka write should be behind a separate build feature to avoid `librdkafka` build/link complexity.

## Testing

### Python Broker-Free Tests

Add tests in `tests/io/test_kafka_write_mock.py` or extend `tests/io/test_kafka_mock.py`:

- `DataFrame.write_kafka` exists.
- `bootstrap_servers` accepts string and sequence.
- `topic`/`topic_col` mutual exclusion.
- `partition`/`partition_col` mutual exclusion.
- `timeout_ms <= 0` fails.
- `kafka_client_config` rejects non-scalar values.
- `bootstrap.servers` cannot be overridden.
- `transactional.id` is rejected.
- `key_col=None` and `timestamp_ms_col=None` are accepted.
- invalid `value_format` and `key_format` fail.

### Rust Unit Tests

Use `FakeProducer` to test without a broker:

- Config normalization.
- Sensitive config redaction.
- Raw, UTF-8, and JSON record projection.
- Null value tombstone behavior.
- Null JSON distinction from bytes `b"null"`.
- Static and dynamic topic routing.
- Static and dynamic partition selection.
- Timestamp handling.
- Ordered headers and duplicate key preservation.
- Delivery accounting.
- First-error capture.
- Delivery failure and flush failure propagation.
- Bounded in-flight future draining.

### Redpanda Integration Tests

Reuse `tests/integration/io/docker-compose/docker-compose.yml`.

Add integration tests for:

- Raw binary key/value round trip using `read_kafka`.
- UTF-8 value writes.
- JSON value writes.
- Static partition and timestamp.
- Dynamic topic routing.
- Headers, verified through a Kafka consumer because `read_kafka` does not currently return headers.

Avoid flaky integration failure tests that depend on broker shutdown. Use fake producer tests for failure behavior.

### Native and Ray

Run at least one native and one Ray write/read round trip:

```bash
DAFT_RUNNER=native pytest tests/integration/io/test_kafka_integration.py -m integration
DAFT_RUNNER=ray pytest tests/integration/io/test_kafka_integration.py -m integration
```

If Ray coverage is expensive, keep one minimal Ray round trip and run richer type/header cases with the native runner.

## Documentation

Update:

```text
docs/connectors/kafka.md
docs/api/io.md or generated API docs entry
```

Document:

- Kafka connector supports read and write.
- `write_kafka` is experimental.
- At-least-once semantics.
- Null value sends tombstone.
- Idempotence recommended config.
- `transactional.id` and EOS unsupported.
- `kafka_client_config` passes through librdkafka producer options.
- Headers use ordered list-of-struct.
- Return value is task-level summary.

Example:

```python
df.write_kafka(
    bootstrap_servers="localhost:9092",
    topic="events",
    key_col="key",
    value_col="value",
    kafka_client_config={
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
    },
)
```

JSON example:

```python
df.write_kafka(
    bootstrap_servers="localhost:9092",
    topic="events-json",
    value_col="payload",
    value_format="json",
)
```

## Suggested Implementation Boundary

First implementation includes:

- Static topic.
- Dynamic topic.
- Raw key/value.
- UTF-8 key/value.
- JSON value.
- Static/dynamic partition.
- Timestamp.
- Headers.
- Task-level summary.
- Delivery-result accounting.
- Bounded in-flight sends.
- `librdkafka` config passthrough.
- Optional statistics capture hook.
- Idempotence through config.

First implementation excludes:

- Transactional writes.
- Exactly-once semantics.
- Schema Registry.
- Avro and Protobuf.
- Per-message delivery report output.
- Rust rewrite of `read_kafka`.
- Streaming/unbounded sink.

## Review Strategy

Even though the feature is a single Kafka write capability, it touches multiple Daft layers. If maintainers prefer smaller review units, split PRs as:

1. API, logical plan, local plan, return schema, validation, and docs skeleton.
2. `rdkafka` writer, full record construction, task summary, and tests.
3. Integration hardening, Ray coverage, docs polish, and metrics hooks.

If maintainers accept one PR, keep commits modular by layer so review remains tractable.

## Open Decisions

- Whether `python` should enable Kafka write by default in published wheels.
- Whether the public return field should be `task_id` or `writer_id`.
- Whether Daft should expose a public `max_in_flight_messages` tuning parameter in the first release.
- Whether optional `rdkafka` statistics should be integrated into Daft runtime metrics immediately or deferred.
