# Native Kafka Write Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `DataFrame.write_kafka(...)` as a Daft-native, Rust-backed Kafka producer sink with task-level summaries and broad `librdkafka` config passthrough.

**Architecture:** Add a native `KafkaInfo` sink to Daft logical planning, translate it to a `KafkaWrite` local physical plan, and execute it through the existing `WriteSink`/`WriterFactory` path. Keep Kafka client code localized in `daft-writers/src/kafka`, using `rdkafka::producer::FutureProducer` behind a testable producer trait.

**Tech Stack:** Python public DataFrame API, PyO3 logical builder bindings, Daft logical/local/distributed plan crates, Daft local execution, `daft-writers`, `rdkafka`, Redpanda integration tests, pytest, cargo.

---

## File Structure

- Modify `daft/dataframe/dataframe.py`
  - Public `DataFrame.write_kafka(...)` API.
  - Python-side argument normalization for easy failures.
  - Blocking write execution pattern matching `write_parquet`, `write_csv`, and `write_json`.

- Modify `daft/logical/builder.py`
  - Python wrapper method `LogicalPlanBuilder.write_kafka(...)`.
  - Convert optional column names into Python expressions before calling the Rust builder.

- Modify `daft/daft/__init__.pyi`
  - Type stub for the PyO3 `LogicalPlanBuilder.kafka_write(...)` method.

- Leave `src/daft-logical-plan/Cargo.toml` unchanged for float hashing
  - `common-hashable-float-wrapper` is already present in this crate.

- Modify `src/daft-logical-plan/src/sink_info.rs`
  - Add `KafkaInfo(KafkaWriteInfo<E>)`.
  - Add Kafka plan config types.
  - Add bind/display helpers.

- Modify `src/daft-logical-plan/src/ops/sink.rs`
  - Add Kafka task summary output schema.
  - Add display branch for Kafka sinks.

- Modify `src/daft-logical-plan/src/builder/mod.rs`
  - Add Rust `LogicalPlanBuilder::kafka_write(...)`.
  - Add PyO3 `PyLogicalPlanBuilder.kafka_write(...)`.

- Modify `src/daft-local-plan/src/plan.rs`
  - Add `LocalPhysicalPlan::KafkaWrite(KafkaWrite)`.
  - Add constructor, schema access, context access, child traversal, and replacement handling.

- Modify `src/daft-local-plan/src/translate.rs`
  - Translate `SinkInfo::KafkaInfo` into `LocalPhysicalPlan::KafkaWrite`.

- Modify `src/daft-distributed/src/pipeline_node/sink.rs`
  - Translate distributed `SinkInfo::KafkaInfo` into local Kafka write plans.

- Modify `src/daft-local-execution/src/sinks/write.rs`
  - Add `WriteFormat::Kafka`.
  - Return `"Kafka Write"` from `name()`.

- Modify `src/daft-local-execution/src/pipeline.rs`
  - Build `daft_writers::make_kafka_writer_factory(...)`.
  - Create `WriteSink::new(WriteFormat::Kafka, ...)`.

- Modify `src/daft-writers/Cargo.toml`
  - Add optional `rdkafka`.
  - Add a `kafka` feature.
  - Add `futures` for bounded in-flight delivery handling.

- Modify `src/daft-writers/src/lib.rs`
  - Gate and export the Kafka writer factory.

- Create `src/daft-writers/src/kafka/mod.rs`
  - Module exports.

- Create `src/daft-writers/src/kafka/config.rs`
  - Config normalization and protected key validation.

- Create `src/daft-writers/src/kafka/record.rs`
  - Row projection to `KafkaOutgoingRecord`.

- Create `src/daft-writers/src/kafka/headers.rs`
  - Header extraction and duplicate key preservation.

- Create `src/daft-writers/src/kafka/producer.rs`
  - `KafkaProducer` trait, `RdkafkaProducer`, and `FakeProducer` for tests.

- Create `src/daft-writers/src/kafka/accounting.rs`
  - Counters, first error capture, byte accounting, summary `RecordBatch`.

- Create `src/daft-writers/src/kafka/metrics.rs`
  - Optional statistics snapshot data shape.

- Create `src/daft-writers/src/kafka/writer.rs`
  - `KafkaWriterFactory` and `KafkaWriter`.

- Create `tests/io/test_kafka_write_mock.py`
  - Broker-free Python validation tests.

- Modify `tests/integration/io/test_kafka_integration.py`
  - Redpanda write/read round trips.

- Modify `docs/connectors/kafka.md`
  - Document write support.

- Modify `docs/api/io.md`
  - Add API docs entry only after confirming this page lists DataFrame write APIs.

---

### Task 1: Python API Validation Tests

**Files:**
- Create: `tests/io/test_kafka_write_mock.py`
- Modify in Task 2: `daft/dataframe/dataframe.py`
- Modify in Task 2: `daft/logical/builder.py`

- [ ] **Step 1: Write failing export and validation tests**

Create `tests/io/test_kafka_write_mock.py` with:

```python
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
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run:

```bash
pytest tests/io/test_kafka_write_mock.py -q
```

Expected: tests fail because `DataFrame.write_kafka` is not implemented.

- [ ] **Step 3: Commit failing tests**

```bash
git add tests/io/test_kafka_write_mock.py
git commit -m "test: define kafka write api validation contract" \
  -m "Constraint: Kafka write is a public API and should fail fast before planning invalid producer work." \
  -m "Confidence: high" \
  -m "Scope-risk: narrow" \
  -m "Tested: pytest tests/io/test_kafka_write_mock.py -q (expected failure before implementation)"
```

---

### Task 2: Python API and Builder Wrapper

**Files:**
- Modify: `daft/dataframe/dataframe.py`
- Modify: `daft/logical/builder.py`
- Modify: `daft/daft/__init__.pyi`
- Test: `tests/io/test_kafka_write_mock.py`

- [ ] **Step 1: Add Python helper constants and validation in `daft/dataframe/dataframe.py`**

Add near the other module-level helpers:

```python
_KAFKA_VALUE_FORMATS = {"raw", "utf8", "json"}
_KAFKA_KEY_FORMATS = {"raw", "utf8"}
_KAFKA_MANAGED_CONFIG_KEYS = {"bootstrap.servers"}
_KAFKA_UNSUPPORTED_CONFIG_KEYS = {"transactional.id"}


def _normalize_kafka_bootstrap_servers(bootstrap_servers: str | typing.Sequence[str]) -> str:
    if isinstance(bootstrap_servers, str):
        return bootstrap_servers
    return ",".join(str(server) for server in bootstrap_servers)


def _validate_kafka_client_config(kafka_client_config: Mapping[str, object] | None) -> dict[str, object] | None:
    if kafka_client_config is None:
        return None

    normalized: dict[str, object] = {}
    for raw_key, value in kafka_client_config.items():
        key = str(raw_key)
        if key in _KAFKA_MANAGED_CONFIG_KEYS:
            raise ValueError(f"[write_kafka] kafka_client_config must not override managed key: {key!r}")
        if key in _KAFKA_UNSUPPORTED_CONFIG_KEYS:
            raise NotImplementedError(f"[write_kafka] {key} is not supported yet")
        if isinstance(value, (str, int, bool, float)) or value is None:
            normalized[key] = value
            continue
        raise TypeError(
            f"[write_kafka] kafka_client_config value for key {key!r} must be a scalar "
            f"(str, int, bool, float, or None), got {type(value).__name__}"
        )
    return normalized
```

- [ ] **Step 2: Add `DataFrame.write_kafka`**

Add after `write_json(...)` and before catalog-specific writes:

```python
    @DataframePublicAPI
    def write_kafka(
        self,
        bootstrap_servers: str | typing.Sequence[str],
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
    ) -> "DataFrame":
        """Writes the DataFrame to Kafka, returning task-level write summaries.

        This call is blocking and executes the DataFrame.
        """
        if (topic is None) == (topic_col is None):
            raise ValueError("[write_kafka] exactly one of topic or topic_col must be provided")
        if topic is not None and topic == "":
            raise ValueError("[write_kafka] topic must be non-empty")
        if partition is not None and partition < 0:
            raise ValueError("[write_kafka] partition must be >= 0")
        if partition is not None and partition_col is not None:
            raise ValueError("[write_kafka] partition and partition_col are mutually exclusive")
        if timeout_ms <= 0:
            raise ValueError("[write_kafka] timeout_ms must be > 0")
        if value_format not in _KAFKA_VALUE_FORMATS:
            raise ValueError("[write_kafka] value_format must be one of: raw, utf8, json")
        if key_format not in _KAFKA_KEY_FORMATS:
            raise ValueError("[write_kafka] key_format must be one of: raw, utf8")

        bootstrap_servers_str = _normalize_kafka_bootstrap_servers(bootstrap_servers)
        normalized_client_config = _validate_kafka_client_config(kafka_client_config)

        builder = self._builder.write_kafka(
            bootstrap_servers=bootstrap_servers_str,
            topic=topic,
            topic_col=topic_col,
            value_col=value_col,
            key_col=key_col,
            headers_col=headers_col,
            partition=partition,
            partition_col=partition_col,
            timestamp_ms_col=timestamp_ms_col,
            value_format=value_format,
            key_format=key_format,
            kafka_client_config=normalized_client_config,
            timeout_ms=timeout_ms,
        )

        write_df = DataFrame(builder)
        write_df.collect()
        assert write_df._result is not None

        result_df = DataFrame(write_df._get_current_builder())
        result_df._result_cache = write_df._result_cache
        result_df._preview = write_df._preview
        result_df._metadata = write_df._metadata
        return result_df
```

- [ ] **Step 3: Add wrapper method in `daft/logical/builder.py`**

Add near `write_tabular`:

```python
    def write_kafka(
        self,
        *,
        bootstrap_servers: str,
        topic: str | None,
        topic_col: str | None,
        value_col: str,
        key_col: str | None,
        headers_col: str | None,
        partition: int | None,
        partition_col: str | None,
        timestamp_ms_col: str | None,
        value_format: str,
        key_format: str,
        kafka_client_config: dict[str, object] | None,
        timeout_ms: int,
    ) -> LogicalPlanBuilder:
        builder = self._builder.kafka_write(
            bootstrap_servers,
            topic,
            topic_col,
            value_col,
            key_col,
            headers_col,
            partition,
            partition_col,
            timestamp_ms_col,
            value_format,
            key_format,
            kafka_client_config,
            timeout_ms,
        )
        return LogicalPlanBuilder(builder)
```

- [ ] **Step 4: Add PyO3 stub in `daft/daft/__init__.pyi`**

Add to `class LogicalPlanBuilder` near other write methods:

```python
    def kafka_write(
        self,
        bootstrap_servers: str,
        topic: str | None,
        topic_col: str | None,
        value_col: str,
        key_col: str | None,
        headers_col: str | None,
        partition: int | None,
        partition_col: str | None,
        timestamp_ms_col: str | None,
        value_format: str,
        key_format: str,
        kafka_client_config: dict[str, Any] | None,
        timeout_ms: int,
    ) -> LogicalPlanBuilder: ...
```

- [ ] **Step 5: Run validation tests to verify remaining failure moves into Rust binding**

Run:

```bash
pytest tests/io/test_kafka_write_mock.py -q
```

Expected: failures now mention missing `LogicalPlanBuilder.kafka_write` on the PyO3 object, or Rust builder missing the method.

- [ ] **Step 6: Commit Python API shell**

```bash
git add daft/dataframe/dataframe.py daft/logical/builder.py daft/daft/__init__.pyi
git commit -m "feat: add kafka write dataframe api shell" \
  -m "Constraint: Keep public argument failures in Python before entering native planning." \
  -m "Confidence: medium" \
  -m "Scope-risk: narrow" \
  -m "Tested: pytest tests/io/test_kafka_write_mock.py -q (expected Rust binding failure)"
```

---

### Task 3: Logical Plan Kafka Sink Types

**Files:**
- Modify: `src/daft-logical-plan/src/sink_info.rs`
- Modify: `src/daft-logical-plan/src/ops/sink.rs`
- Modify: `src/daft-logical-plan/src/builder/mod.rs`
- Test: `cargo check -p daft-logical-plan --no-default-features`

- [ ] **Step 1: Add Kafka plan types in `sink_info.rs`**

Add imports:

```rust
use std::collections::BTreeMap;

use common_hashable_float_wrapper::FloatWrapper;
```

Add types after `OutputFileInfo`:

```rust
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct KafkaWriteInfo<E = ExprRef> {
    pub bootstrap_servers: String,
    pub topic: KafkaTopic<E>,
    pub value_col: E,
    pub key_col: Option<E>,
    pub headers_col: Option<E>,
    pub partition: Option<KafkaPartition<E>>,
    pub timestamp_ms_col: Option<E>,
    pub value_format: KafkaValueFormat,
    pub key_format: KafkaKeyFormat,
    pub kafka_client_config: BTreeMap<String, KafkaConfigValue>,
    pub timeout_ms: u64,
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum KafkaTopic<E = ExprRef> {
    Static(String),
    Dynamic(E),
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum KafkaPartition<E = ExprRef> {
    Static(i32),
    Dynamic(E),
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum KafkaValueFormat {
    Raw,
    Utf8,
    Json,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum KafkaKeyFormat {
    Raw,
    Utf8,
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub enum KafkaConfigValue {
    String(String),
    Int(i64),
    Float(FloatWrapper<f64>),
    Bool(bool),
    Null,
}
```

- [ ] **Step 2: Add `SinkInfo::KafkaInfo`**

Update the enum:

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

- [ ] **Step 3: Add bind helpers in `sink_info.rs`**

Add implementations:

```rust
impl KafkaWriteInfo {
    pub fn bind(&self, schema: &Schema) -> DaftResult<KafkaWriteInfo<BoundExpr>> {
        Ok(KafkaWriteInfo {
            bootstrap_servers: self.bootstrap_servers.clone(),
            topic: self.topic.clone().bind(schema)?,
            value_col: BoundExpr::try_new(self.value_col.clone(), schema)?,
            key_col: self
                .key_col
                .clone()
                .map(|expr| BoundExpr::try_new(expr, schema))
                .transpose()?,
            headers_col: self
                .headers_col
                .clone()
                .map(|expr| BoundExpr::try_new(expr, schema))
                .transpose()?,
            partition: self
                .partition
                .clone()
                .map(|partition| partition.bind(schema))
                .transpose()?,
            timestamp_ms_col: self
                .timestamp_ms_col
                .clone()
                .map(|expr| BoundExpr::try_new(expr, schema))
                .transpose()?,
            value_format: self.value_format,
            key_format: self.key_format,
            kafka_client_config: self.kafka_client_config.clone(),
            timeout_ms: self.timeout_ms,
        })
    }

    pub fn multiline_display(&self) -> Vec<String> {
        vec![
            format!("Bootstrap servers = {}", self.bootstrap_servers),
            format!("Topic = {}", self.topic),
            format!("Value format = {:?}", self.value_format),
            format!("Key format = {:?}", self.key_format),
        ]
    }
}

impl KafkaTopic {
    fn bind(&self, schema: &Schema) -> DaftResult<KafkaTopic<BoundExpr>> {
        match self {
            Self::Static(topic) => Ok(KafkaTopic::Static(topic.clone())),
            Self::Dynamic(expr) => Ok(KafkaTopic::Dynamic(BoundExpr::try_new(expr.clone(), schema)?)),
        }
    }
}

impl KafkaPartition {
    fn bind(&self, schema: &Schema) -> DaftResult<KafkaPartition<BoundExpr>> {
        match self {
            Self::Static(partition) => Ok(KafkaPartition::Static(*partition)),
            Self::Dynamic(expr) => Ok(KafkaPartition::Dynamic(BoundExpr::try_new(expr.clone(), schema)?)),
        }
    }
}
```

Add `Display` implementations:

```rust
impl<E> std::fmt::Display for KafkaTopic<E>
where
    E: ToString,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static(topic) => write!(f, "{}", topic),
            Self::Dynamic(expr) => write!(f, "{}", expr.to_string()),
        }
    }
}
```

Update `SinkInfo::bind`:

```rust
Self::KafkaInfo(kafka_info) => Ok(SinkInfo::KafkaInfo(kafka_info.clone().bind(schema)?)),
```

- [ ] **Step 4: Add Kafka return schema in `ops/sink.rs`**

Add branch:

```rust
SinkInfo::KafkaInfo(_) => vec![
    Field::new("task_id", DataType::Int64),
    Field::new("messages_attempted", DataType::Int64),
    Field::new("messages_delivered", DataType::Int64),
    Field::new("messages_failed", DataType::Int64),
    Field::new("bytes_delivered", DataType::Int64),
    Field::new("first_error", DataType::Utf8),
],
```

Add display branch:

```rust
SinkInfo::KafkaInfo(kafka_info) => {
    res.push("Sink: Kafka".to_string());
    res.extend(kafka_info.multiline_display());
}
```

- [ ] **Step 5: Add Rust builder method in `builder/mod.rs`**

Add imports for Kafka types:

```rust
use crate::sink_info::{
    KafkaConfigValue, KafkaKeyFormat, KafkaPartition, KafkaTopic, KafkaValueFormat, KafkaWriteInfo,
};
```

Add helper parsers:

```rust
fn parse_kafka_value_format(value_format: &str) -> DaftResult<KafkaValueFormat> {
    match value_format {
        "raw" => Ok(KafkaValueFormat::Raw),
        "utf8" => Ok(KafkaValueFormat::Utf8),
        "json" => Ok(KafkaValueFormat::Json),
        other => Err(DaftError::ValueError(format!(
            "[write_kafka] value_format must be one of raw, utf8, json; got {other}"
        ))),
    }
}

fn parse_kafka_key_format(key_format: &str) -> DaftResult<KafkaKeyFormat> {
    match key_format {
        "raw" => Ok(KafkaKeyFormat::Raw),
        "utf8" => Ok(KafkaKeyFormat::Utf8),
        other => Err(DaftError::ValueError(format!(
            "[write_kafka] key_format must be one of raw, utf8; got {other}"
        ))),
    }
}
```

Add method:

```rust
#[allow(clippy::too_many_arguments)]
pub fn kafka_write(
    &self,
    bootstrap_servers: String,
    topic: Option<String>,
    topic_col: Option<ExprRef>,
    value_col: ExprRef,
    key_col: Option<ExprRef>,
    headers_col: Option<ExprRef>,
    partition: Option<i32>,
    partition_col: Option<ExprRef>,
    timestamp_ms_col: Option<ExprRef>,
    value_format: String,
    key_format: String,
    kafka_client_config: BTreeMap<String, KafkaConfigValue>,
    timeout_ms: u64,
) -> DaftResult<Self> {
    let topic = match (topic, topic_col) {
        (Some(topic), None) => KafkaTopic::Static(topic),
        (None, Some(topic_col)) => KafkaTopic::Dynamic(topic_col),
        _ => {
            return Err(DaftError::ValueError(
                "[write_kafka] exactly one of topic or topic_col must be provided".to_string(),
            ));
        }
    };
    let partition = match (partition, partition_col) {
        (Some(partition), None) => Some(KafkaPartition::Static(partition)),
        (None, Some(partition_col)) => Some(KafkaPartition::Dynamic(partition_col)),
        (None, None) => None,
        (Some(_), Some(_)) => {
            return Err(DaftError::ValueError(
                "[write_kafka] partition and partition_col are mutually exclusive".to_string(),
            ));
        }
    };

    let sink_info = SinkInfo::KafkaInfo(KafkaWriteInfo {
        bootstrap_servers,
        topic,
        value_col,
        key_col,
        headers_col,
        partition,
        timestamp_ms_col,
        value_format: parse_kafka_value_format(&value_format)?,
        key_format: parse_kafka_key_format(&key_format)?,
        kafka_client_config,
        timeout_ms,
    });
    let logical_plan: LogicalPlan = ops::Sink::try_new(self.plan.clone(), sink_info.into())?.into();
    Ok(self.with_new_plan(logical_plan))
}
```

- [ ] **Step 6: Run logical-plan check**

Run:

```bash
cargo check -p daft-logical-plan --no-default-features
```

Expected: pass for native Rust logical plan code.

- [ ] **Step 7: Commit logical plan changes**

```bash
git add src/daft-logical-plan/src/sink_info.rs src/daft-logical-plan/src/ops/sink.rs src/daft-logical-plan/src/builder/mod.rs
git commit -m "feat: add kafka sink logical plan types" \
  -m "Constraint: Kafka write planning must remain serializable and independent of rdkafka." \
  -m "Confidence: medium" \
  -m "Scope-risk: moderate" \
  -m "Tested: cargo check -p daft-logical-plan --no-default-features"
```

---

### Task 4: PyO3 Kafka Builder Binding

**Files:**
- Modify: `src/daft-logical-plan/src/builder/mod.rs`
- Test: `tests/io/test_kafka_write_mock.py`

- [ ] **Step 1: Add Python config converter in `builder/mod.rs`**

Inside the `#[cfg(feature = "python")]` section, add:

```rust
#[cfg(feature = "python")]
fn py_kafka_config_to_rust(
    _py: Python,
    config: Option<pyo3::Bound<'_, pyo3::types::PyDict>>,
) -> PyResult<BTreeMap<String, KafkaConfigValue>> {
    let mut out = BTreeMap::new();
    let Some(config) = config else {
        return Ok(out);
    };

    for (key, value) in config.iter() {
        let key = key.extract::<String>()?;
        let value = if value.is_none() {
            KafkaConfigValue::Null
        } else if let Ok(v) = value.extract::<bool>() {
            KafkaConfigValue::Bool(v)
        } else if let Ok(v) = value.extract::<i64>() {
            KafkaConfigValue::Int(v)
        } else if let Ok(v) = value.extract::<f64>() {
            KafkaConfigValue::Float(FloatWrapper(v))
        } else if let Ok(v) = value.extract::<String>() {
            KafkaConfigValue::String(v)
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "[write_kafka] kafka_client_config value for key {key:?} must be a scalar"
            )));
        };
        out.insert(key, value);
    }
    Ok(out)
}
```

Add imports for `FloatWrapper`, `Python`, and `PyDict` in the same import block that already contains the PyO3 builder methods.

- [ ] **Step 2: Add PyO3 method**

Add near other write PyO3 methods:

```rust
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (
    bootstrap_servers,
    topic,
    topic_col,
    value_col,
    key_col,
    headers_col,
    partition,
    partition_col,
    timestamp_ms_col,
    value_format,
    key_format,
    kafka_client_config=None,
    timeout_ms=10000
))]
pub fn kafka_write(
    &self,
    py: Python,
    bootstrap_servers: String,
    topic: Option<String>,
    topic_col: Option<String>,
    value_col: String,
    key_col: Option<String>,
    headers_col: Option<String>,
    partition: Option<i32>,
    partition_col: Option<String>,
    timestamp_ms_col: Option<String>,
    value_format: String,
    key_format: String,
    kafka_client_config: Option<pyo3::Bound<'_, pyo3::types::PyDict>>,
    timeout_ms: u64,
) -> PyResult<Self> {
    let topic_col = topic_col.map(|name| unresolved_col(name.as_str()));
    let value_col = unresolved_col(value_col.as_str());
    let key_col = key_col.map(|name| unresolved_col(name.as_str()));
    let headers_col = headers_col.map(|name| unresolved_col(name.as_str()));
    let partition_col = partition_col.map(|name| unresolved_col(name.as_str()));
    let timestamp_ms_col = timestamp_ms_col.map(|name| unresolved_col(name.as_str()));
    let kafka_client_config = py_kafka_config_to_rust(py, kafka_client_config)?;

    Ok(self
        .builder
        .kafka_write(
            bootstrap_servers,
            topic,
            topic_col,
            value_col,
            key_col,
            headers_col,
            partition,
            partition_col,
            timestamp_ms_col,
            value_format,
            key_format,
            kafka_client_config,
            timeout_ms,
        )?
        .into())
}
```

Add `unresolved_col` to the `daft_dsl` import list before compiling this method.

- [ ] **Step 3: Run Python validation tests**

Run:

```bash
pytest tests/io/test_kafka_write_mock.py -q
```

Expected: invalid input tests pass and valid helper tests pass. The public `write_kafka` path can still fail during execution until Task 10 wires local execution.

- [ ] **Step 4: Commit PyO3 binding**

```bash
git add src/daft-logical-plan/src/builder/mod.rs daft/daft/__init__.pyi
git commit -m "feat: expose kafka write in logical builder bindings" \
  -m "Constraint: Python write_kafka must lower to native logical planning rather than DataSink." \
  -m "Confidence: medium" \
  -m "Scope-risk: moderate" \
  -m "Tested: pytest tests/io/test_kafka_write_mock.py -q"
```

---

### Task 5: Local and Distributed Plan Plumbing

**Files:**
- Modify: `src/daft-local-plan/src/plan.rs`
- Modify: `src/daft-local-plan/src/translate.rs`
- Modify: `src/daft-distributed/src/pipeline_node/sink.rs`
- Test: `cargo check -p daft-local-plan --no-default-features`
- Test: `cargo check -p daft-distributed --features python`

- [ ] **Step 1: Add local physical enum variant and struct**

In `src/daft-local-plan/src/plan.rs`, add enum variant:

```rust
KafkaWrite(KafkaWrite),
```

Add struct near other write structs:

```rust
#[derive(Serialize, Deserialize)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct KafkaWrite {
    pub input: LocalPhysicalPlanRef,
    pub kafka_info: daft_logical_plan::KafkaWriteInfo<BoundExpr>,
    pub file_schema: SchemaRef,
    pub stats_state: StatsState,
    pub context: LocalNodeContext,
}
```

- [ ] **Step 2: Add constructor**

Add to `impl LocalPhysicalPlan` near other write constructors:

```rust
pub fn kafka_write(
    input: LocalPhysicalPlanRef,
    kafka_info: daft_logical_plan::KafkaWriteInfo<BoundExpr>,
    file_schema: SchemaRef,
    stats_state: StatsState,
    context: LocalNodeContext,
) -> LocalPhysicalPlanRef {
    Self::KafkaWrite(KafkaWrite {
        input,
        kafka_info,
        file_schema,
        stats_state,
        context,
    })
    .arced()
}
```

- [ ] **Step 3: Add match arms throughout `plan.rs`**

Add `KafkaWrite` to each match that handles write-like unary nodes:

```rust
Self::KafkaWrite(KafkaWrite { stats_state, .. }) => stats_state,
Self::KafkaWrite(KafkaWrite { context, .. }) => context,
Self::KafkaWrite(KafkaWrite { context, .. }) => context,
Self::KafkaWrite(KafkaWrite { file_schema, .. }) => file_schema,
Self::KafkaWrite(KafkaWrite { input, .. }) => vec![input.clone()],
```

In `with_new_children`, add:

```rust
Self::KafkaWrite(KafkaWrite {
    kafka_info,
    file_schema,
    stats_state,
    context,
    ..
}) => Self::kafka_write(
    new_children[0].clone(),
    kafka_info.clone(),
    file_schema.clone(),
    stats_state.clone(),
    context.clone(),
),
```

- [ ] **Step 4: Translate logical sink to local Kafka write**

In `src/daft-local-plan/src/translate.rs`, add:

```rust
SinkInfo::KafkaInfo(info) => LocalPhysicalPlan::kafka_write(
    input_plan,
    info.clone().bind(&data_schema)?,
    sink.schema.clone(),
    sink.stats_state.clone(),
    LocalNodeContext::default(),
),
```

- [ ] **Step 5: Translate distributed sink to local Kafka write**

In `src/daft-distributed/src/pipeline_node/sink.rs`, add this branch. `SinkNode` stores a bound `SinkInfo`, so `info.clone()` has the `BoundExpr` shape expected by `LocalPhysicalPlan::kafka_write`.

```rust
SinkInfo::KafkaInfo(info) => LocalPhysicalPlan::kafka_write(
    input,
    info.clone(),
    file_schema,
    StatsState::NotMaterialized,
    LocalNodeContext::new(Some(node_id as usize)),
),
```

- [ ] **Step 6: Run cargo checks**

Run:

```bash
cargo check -p daft-local-plan --no-default-features
cargo check -p daft-distributed --features python
```

Expected: both pass.

- [ ] **Step 7: Commit plan plumbing**

```bash
git add src/daft-local-plan/src/plan.rs src/daft-local-plan/src/translate.rs src/daft-distributed/src/pipeline_node/sink.rs
git commit -m "feat: add kafka write physical plan plumbing" \
  -m "Constraint: Kafka writes are direct external sinks and do not use CommitWrite." \
  -m "Confidence: medium" \
  -m "Scope-risk: moderate" \
  -m "Tested: cargo check -p daft-local-plan --no-default-features; cargo check -p daft-distributed --features python"
```

---

### Task 6: Kafka Writer Config and Accounting Units

**Files:**
- Modify: `src/daft-writers/Cargo.toml`
- Modify: `Cargo.toml`
- Modify: `src/daft-writers/src/lib.rs`
- Create: `src/daft-writers/src/kafka/mod.rs`
- Create: `src/daft-writers/src/kafka/config.rs`
- Create: `src/daft-writers/src/kafka/accounting.rs`
- Create: `src/daft-writers/src/kafka/metrics.rs`
- Test: `cargo test -p daft-writers --no-default-features kafka::config kafka::accounting`

- [ ] **Step 1: Add dependencies and features**

In root `Cargo.toml` workspace dependencies, add:

```toml
rdkafka = { version = "0.38.0", default-features = false, features = ["tokio"] }
```

In `src/daft-writers/Cargo.toml`, add dependencies:

```toml
futures = {workspace = true}
rdkafka = {workspace = true, optional = true}
common-hashable-float-wrapper = {path = "../common/hashable-float-wrapper", default-features = false}
```

Add feature:

```toml
kafka = ["dep:rdkafka"]
python = ["dep:pyo3", "kafka", "common-file-formats/python", "common-error/python", "daft-dsl/python", "daft-io/python", "daft-logical-plan/python", "daft-micropartition/python"]
```

- [ ] **Step 2: Wire module exports**

In `src/daft-writers/src/lib.rs`, add:

```rust
#[cfg(feature = "kafka")]
mod kafka;
#[cfg(feature = "kafka")]
pub use kafka::writer::make_kafka_writer_factory;
```

- [ ] **Step 3: Create `kafka/mod.rs`**

```rust
pub mod accounting;
pub mod config;
pub mod metrics;
pub mod writer;
pub mod headers;
pub mod producer;
pub mod record;
```

- [ ] **Step 4: Create config conversion**

Create `src/daft-writers/src/kafka/config.rs`:

```rust
use common_error::{DaftError, DaftResult};
use daft_logical_plan::KafkaConfigValue;

const MANAGED_KEYS: &[&str] = &["bootstrap.servers"];
const UNSUPPORTED_KEYS: &[&str] = &["transactional.id"];
const SENSITIVE_MARKERS: &[&str] = &["password", "sasl.oauthbearer.config"];

pub fn config_value_to_string(value: &KafkaConfigValue) -> String {
    match value {
        KafkaConfigValue::String(v) => v.clone(),
        KafkaConfigValue::Int(v) => v.to_string(),
        KafkaConfigValue::Float(v) => v.0.to_string(),
        KafkaConfigValue::Bool(v) => v.to_string(),
        KafkaConfigValue::Null => String::new(),
    }
}

pub fn validate_config_key(key: &str) -> DaftResult<()> {
    if MANAGED_KEYS.contains(&key) {
        return Err(DaftError::ValueError(format!(
            "[write_kafka] kafka_client_config must not override managed key: {key:?}"
        )));
    }
    if UNSUPPORTED_KEYS.contains(&key) {
        return Err(DaftError::NotImplemented(format!(
            "[write_kafka] {key} is not supported yet"
        )));
    }
    Ok(())
}

pub fn redact_config_value(key: &str, value: &str) -> String {
    let lowered = key.to_ascii_lowercase();
    if SENSITIVE_MARKERS.iter().any(|marker| lowered.contains(marker)) {
        "<redacted>".to_string()
    } else {
        value.to_string()
    }
}

#[cfg(feature = "kafka")]
pub fn build_client_config(
    bootstrap_servers: &str,
    kafka_client_config: &std::collections::BTreeMap<String, KafkaConfigValue>,
) -> DaftResult<rdkafka::ClientConfig> {
    let mut config = rdkafka::ClientConfig::new();
    config.set("bootstrap.servers", bootstrap_servers);
    config.set("client.id", "daft-write-kafka");
    for (key, value) in kafka_client_config {
        validate_config_key(key)?;
        let value = config_value_to_string(value);
        config.set(key, value);
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common_hashable_float_wrapper::FloatWrapper;

    #[test]
    fn converts_scalar_values_to_strings() {
        assert_eq!(config_value_to_string(&KafkaConfigValue::String("all".to_string())), "all");
        assert_eq!(config_value_to_string(&KafkaConfigValue::Int(10)), "10");
        assert_eq!(config_value_to_string(&KafkaConfigValue::Float(FloatWrapper(1.5))), "1.5");
        assert_eq!(config_value_to_string(&KafkaConfigValue::Bool(true)), "true");
        assert_eq!(config_value_to_string(&KafkaConfigValue::Null), "");
    }

    #[test]
    fn rejects_managed_and_transactional_keys() {
        assert!(validate_config_key("bootstrap.servers").is_err());
        assert!(validate_config_key("transactional.id").is_err());
        assert!(validate_config_key("acks").is_ok());
    }

    #[test]
    fn redacts_sensitive_values() {
        assert_eq!(redact_config_value("sasl.password", "secret"), "<redacted>");
        assert_eq!(redact_config_value("acks", "all"), "all");
    }
}
```

- [ ] **Step 5: Create accounting**

Create `src/daft-writers/src/kafka/accounting.rs`:

```rust
use common_error::DaftResult;
use daft_core::prelude::{DataType, Field, Schema};
use daft_recordbatch::RecordBatch;
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub struct KafkaWriteAccounting {
    pub task_id: i64,
    pub messages_attempted: i64,
    pub messages_delivered: i64,
    pub messages_failed: i64,
    pub bytes_delivered: i64,
    pub first_error: Option<String>,
}

impl KafkaWriteAccounting {
    pub fn new(task_id: i64) -> Self {
        Self {
            task_id,
            ..Self::default()
        }
    }

    pub fn record_attempt(&mut self) {
        self.messages_attempted += 1;
    }

    pub fn record_delivery(&mut self, bytes: usize) {
        self.messages_delivered += 1;
        self.bytes_delivered += bytes as i64;
    }

    pub fn record_failure(&mut self, err: impl Into<String>) {
        self.messages_failed += 1;
        if self.first_error.is_none() {
            self.first_error = Some(err.into());
        }
    }

    pub fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("task_id", DataType::Int64),
            Field::new("messages_attempted", DataType::Int64),
            Field::new("messages_delivered", DataType::Int64),
            Field::new("messages_failed", DataType::Int64),
            Field::new("bytes_delivered", DataType::Int64),
            Field::new("first_error", DataType::Utf8),
        ]))
    }

    pub fn to_record_batch(&self) -> DaftResult<RecordBatch> {
        RecordBatch::from_pydict(Arc::new(vec![
            ("task_id".into(), vec![self.task_id.into()]),
            ("messages_attempted".into(), vec![self.messages_attempted.into()]),
            ("messages_delivered".into(), vec![self.messages_delivered.into()]),
            ("messages_failed".into(), vec![self.messages_failed.into()]),
            ("bytes_delivered".into(), vec![self.bytes_delivered.into()]),
            ("first_error".into(), vec![self.first_error.clone().into()]),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_first_error_and_counts_delivery() {
        let mut accounting = KafkaWriteAccounting::new(7);
        accounting.record_attempt();
        accounting.record_delivery(11);
        accounting.record_attempt();
        accounting.record_failure("first");
        accounting.record_failure("second");

        assert_eq!(accounting.task_id, 7);
        assert_eq!(accounting.messages_attempted, 2);
        assert_eq!(accounting.messages_delivered, 1);
        assert_eq!(accounting.messages_failed, 2);
        assert_eq!(accounting.bytes_delivered, 11);
        assert_eq!(accounting.first_error.as_deref(), Some("first"));
    }
}
```

Use the exact `RecordBatch` constructor that already exists in this repository. Before coding `to_record_batch`, run `rg -n "RecordBatch::from_|Int64Array|Utf8Array" src/daft-writers src/daft-recordbatch src/daft-core` and mirror the simplest existing Rust-side summary batch construction pattern.

- [ ] **Step 6: Create metrics shape**

Create `src/daft-writers/src/kafka/metrics.rs`:

```rust
#[derive(Debug, Clone, Default)]
pub struct KafkaProducerMetrics {
    pub txmsgs: i64,
    pub txmsg_bytes: i64,
    pub outbuf_msg_cnt: i64,
    pub msg_cnt: i64,
    pub msg_max: i64,
}
```

- [ ] **Step 7: Run writer unit tests**

Run:

```bash
cargo test -p daft-writers --features kafka kafka::config kafka::accounting
cargo test -p daft-writers --no-default-features
```

Expected: Kafka feature tests pass with `--features kafka`; no-default-features build still passes.

- [ ] **Step 8: Commit writer units**

```bash
git add Cargo.toml src/daft-writers/Cargo.toml src/daft-writers/src/lib.rs src/daft-writers/src/kafka
git commit -m "feat: add kafka writer config and accounting units" \
  -m "Constraint: Keep rdkafka optional and keep correctness accounting independent of producer statistics." \
  -m "Confidence: medium" \
  -m "Scope-risk: moderate" \
  -m "Tested: cargo test -p daft-writers --features kafka kafka::config kafka::accounting; cargo test -p daft-writers --no-default-features"
```

---

### Task 7: Record Projection, Headers, and Fake Producer

**Files:**
- Create: `src/daft-writers/src/kafka/headers.rs`
- Create: `src/daft-writers/src/kafka/record.rs`
- Create: `src/daft-writers/src/kafka/producer.rs`
- Test: `cargo test -p daft-writers --features kafka kafka::record kafka::headers kafka::producer`

- [ ] **Step 1: Define producer data types and fake producer**

Create `src/daft-writers/src/kafka/producer.rs`:

```rust
use std::{collections::VecDeque, sync::Mutex, time::Duration};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};

use super::metrics::KafkaProducerMetrics;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaOutgoingRecord {
    pub topic: String,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<KafkaHeader>,
    pub partition: Option<i32>,
    pub timestamp_ms: Option<i64>,
}

impl KafkaOutgoingRecord {
    pub fn delivered_bytes(&self) -> usize {
        let key_bytes = self.key.as_ref().map_or(0, Vec::len);
        let value_bytes = self.value.as_ref().map_or(0, Vec::len);
        let header_bytes = self
            .headers
            .iter()
            .map(|header| header.key.len() + header.value.as_ref().map_or(0, Vec::len))
            .sum::<usize>();
        key_bytes + value_bytes + header_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaDelivery {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
}

#[async_trait]
pub trait KafkaProducer: Send + Sync {
    async fn send(&self, record: KafkaOutgoingRecord) -> DaftResult<KafkaDelivery>;
    async fn flush(&self, timeout: Duration) -> DaftResult<()>;

    fn metrics_snapshot(&self) -> Option<KafkaProducerMetrics> {
        None
    }
}

pub struct FakeProducer {
    deliveries: Mutex<VecDeque<DaftResult<KafkaDelivery>>>,
    sent: Mutex<Vec<KafkaOutgoingRecord>>,
    flush_result: Mutex<Option<DaftResult<()>>>,
}

impl FakeProducer {
    pub fn succeeding() -> Self {
        Self {
            deliveries: Mutex::new(VecDeque::new()),
            sent: Mutex::new(Vec::new()),
            flush_result: Mutex::new(Some(Ok(()))),
        }
    }

    pub fn with_delivery_result(result: DaftResult<KafkaDelivery>) -> Self {
        let mut deliveries = VecDeque::new();
        deliveries.push_back(result);
        Self {
            deliveries: Mutex::new(deliveries),
            sent: Mutex::new(Vec::new()),
            flush_result: Mutex::new(Some(Ok(()))),
        }
    }

    pub fn sent_records(&self) -> Vec<KafkaOutgoingRecord> {
        self.sent.lock().expect("fake producer mutex poisoned").clone()
    }
}

#[async_trait]
impl KafkaProducer for FakeProducer {
    async fn send(&self, record: KafkaOutgoingRecord) -> DaftResult<KafkaDelivery> {
        self.sent.lock().expect("fake producer mutex poisoned").push(record.clone());
        if let Some(result) = self.deliveries.lock().expect("fake producer mutex poisoned").pop_front() {
            return result;
        }
        Ok(KafkaDelivery {
            topic: record.topic,
            partition: record.partition.unwrap_or(0),
            offset: 0,
            timestamp_ms: record.timestamp_ms,
        })
    }

    async fn flush(&self, _timeout: Duration) -> DaftResult<()> {
        self.flush_result
            .lock()
            .expect("fake producer mutex poisoned")
            .take()
            .unwrap_or_else(|| Ok(()))
    }
}

pub fn delivery_error(message: &str) -> DaftResult<KafkaDelivery> {
    Err(DaftError::External(format!("[write_kafka] {message}")))
}
```

- [ ] **Step 2: Implement headers extraction tests first**

Create `src/daft-writers/src/kafka/headers.rs`:

```rust
use common_error::DaftResult;

use super::producer::KafkaHeader;

pub fn validate_header_key(key: &str) -> DaftResult<()> {
    if key.is_empty() {
        return Err(common_error::DaftError::ValueError(
            "[write_kafka] Kafka header key must be non-empty".to_string(),
        ));
    }
    Ok(())
}

pub fn make_header(key: &str, value: Option<&[u8]>) -> DaftResult<KafkaHeader> {
    validate_header_key(key)?;
    Ok(KafkaHeader {
        key: key.to_string(),
        value: value.map(|bytes| bytes.to_vec()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_duplicate_header_keys() {
        let headers = vec![
            make_header("trace", Some(b"a")).unwrap(),
            make_header("trace", Some(b"b")).unwrap(),
            make_header("empty", None).unwrap(),
        ];
        assert_eq!(headers[0].key, "trace");
        assert_eq!(headers[0].value.as_deref(), Some(&b"a"[..]));
        assert_eq!(headers[1].key, "trace");
        assert_eq!(headers[1].value.as_deref(), Some(&b"b"[..]));
        assert_eq!(headers[2].value, None);
    }

    #[test]
    fn rejects_empty_header_key() {
        assert!(make_header("", Some(b"value")).is_err());
    }
}
```

- [ ] **Step 3: Implement record projection scaffold**

Create `src/daft-writers/src/kafka/record.rs`:

```rust
use common_error::{DaftError, DaftResult};

use super::producer::KafkaOutgoingRecord;

pub fn validate_topic(topic: &str) -> DaftResult<()> {
    if topic.is_empty() {
        return Err(DaftError::ComputeError(
            "[write_kafka] Kafka topic must be non-empty".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_partition(partition: i32) -> DaftResult<()> {
    if partition < 0 {
        return Err(DaftError::ComputeError(
            "[write_kafka] Kafka partition must be >= 0".to_string(),
        ));
    }
    Ok(())
}

pub fn make_test_record(
    topic: &str,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    partition: Option<i32>,
    timestamp_ms: Option<i64>,
) -> DaftResult<KafkaOutgoingRecord> {
    validate_topic(topic)?;
    if let Some(partition) = partition {
        validate_partition(partition)?;
    }
    Ok(KafkaOutgoingRecord {
        topic: topic.to_string(),
        key: key.map(|bytes| bytes.to_vec()),
        value: value.map(|bytes| bytes.to_vec()),
        headers: vec![],
        partition,
        timestamp_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_raw_record_and_counts_bytes() {
        let record = make_test_record("events", Some(b"k"), Some(b"value"), Some(1), Some(123)).unwrap();
        assert_eq!(record.topic, "events");
        assert_eq!(record.key.as_deref(), Some(&b"k"[..]));
        assert_eq!(record.value.as_deref(), Some(&b"value"[..]));
        assert_eq!(record.partition, Some(1));
        assert_eq!(record.timestamp_ms, Some(123));
        assert_eq!(record.delivered_bytes(), 6);
    }

    #[test]
    fn supports_tombstone_value() {
        let record = make_test_record("events", Some(b"k"), None, None, None).unwrap();
        assert_eq!(record.value, None);
    }

    #[test]
    fn rejects_empty_topic_and_negative_partition() {
        assert!(make_test_record("", None, Some(b"value"), None, None).is_err());
        assert!(make_test_record("events", None, Some(b"value"), Some(-1), None).is_err());
    }
}
```

In later implementation, replace `make_test_record` with full row projection from `MicroPartition` while keeping these validation helpers.

- [ ] **Step 4: Run unit tests**

Run:

```bash
cargo test -p daft-writers --features kafka kafka::record kafka::headers kafka::producer
```

Expected: pass.

- [ ] **Step 5: Commit record and producer units**

```bash
git add src/daft-writers/src/kafka/headers.rs src/daft-writers/src/kafka/record.rs src/daft-writers/src/kafka/producer.rs
git commit -m "feat: add kafka record and producer test seams" \
  -m "Constraint: Keep delivery accounting testable without a Kafka broker." \
  -m "Confidence: medium" \
  -m "Scope-risk: moderate" \
  -m "Tested: cargo test -p daft-writers --features kafka kafka::record kafka::headers kafka::producer"
```

---

### Task 8: rdkafka Adapter and Kafka Writer

**Files:**
- Modify: `src/daft-writers/src/kafka/producer.rs`
- Create: `src/daft-writers/src/kafka/writer.rs`
- Modify: `src/daft-writers/src/lib.rs`
- Test: `cargo test -p daft-writers --features kafka kafka::writer`

- [ ] **Step 1: Add `RdkafkaProducer` adapter**

In `producer.rs`, add under `#[cfg(feature = "kafka")]`:

```rust
#[cfg(feature = "kafka")]
pub struct RdkafkaProducer {
    inner: rdkafka::producer::FutureProducer,
}

#[cfg(feature = "kafka")]
impl RdkafkaProducer {
    pub fn new(inner: rdkafka::producer::FutureProducer) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "kafka")]
#[async_trait]
impl KafkaProducer for RdkafkaProducer {
    async fn send(&self, record: KafkaOutgoingRecord) -> DaftResult<KafkaDelivery> {
        use rdkafka::producer::FutureRecord;
        use std::time::Duration;

        let mut future_record = FutureRecord::to(&record.topic);
        if let Some(key) = &record.key {
            future_record = future_record.key(key);
        }
        if let Some(value) = &record.value {
            future_record = future_record.payload(value);
        }
        if let Some(partition) = record.partition {
            future_record = future_record.partition(partition);
        }
        if let Some(timestamp_ms) = record.timestamp_ms {
            future_record = future_record.timestamp(timestamp_ms);
        }

        let delivery = self
            .inner
            .send(future_record, Duration::from_secs(0))
            .await
            .map_err(|(err, _)| common_error::DaftError::External(format!("[write_kafka] delivery failed: {err}")))?;

        Ok(KafkaDelivery {
            topic: record.topic,
            partition: delivery.partition,
            offset: delivery.offset,
            timestamp_ms: record.timestamp_ms,
        })
    }

    async fn flush(&self, timeout: Duration) -> DaftResult<()> {
        use rdkafka::producer::Producer;
        self.inner
            .flush(timeout)
            .map_err(|err| common_error::DaftError::External(format!("[write_kafka] flush failed: {err}")))
    }
}
```

Add headers support by converting `KafkaHeader` into `rdkafka::message::OwnedHeaders` if compiler confirms the API shape. Use:

```rust
use rdkafka::message::{Header, OwnedHeaders};
```

and attach headers before send.

- [ ] **Step 2: Implement writer factory and writer**

Create `src/daft-writers/src/kafka/writer.rs`:

```rust
use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use daft_logical_plan::KafkaWriteInfo;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{AsyncFileWriter, WriteResult, WriterFactory};

use super::{
    accounting::KafkaWriteAccounting,
    config::build_client_config,
    producer::{KafkaProducer, RdkafkaProducer},
};

pub fn make_kafka_writer_factory(
    kafka_info: KafkaWriteInfo<daft_dsl::expr::bound_expr::BoundExpr>,
) -> Arc<dyn WriterFactory<Input = MicroPartition, Result = Vec<RecordBatch>>> {
    Arc::new(KafkaWriterFactory { kafka_info })
}

pub struct KafkaWriterFactory {
    kafka_info: KafkaWriteInfo<daft_dsl::expr::bound_expr::BoundExpr>,
}

impl WriterFactory for KafkaWriterFactory {
    type Input = MicroPartition;
    type Result = Vec<RecordBatch>;

    fn create_writer(
        &self,
        file_idx: usize,
        _partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        let client_config = build_client_config(
            &self.kafka_info.bootstrap_servers,
            &self.kafka_info.kafka_client_config,
        )?;
        let producer = client_config
            .create()
            .map_err(|err| DaftError::External(format!("[write_kafka] failed to create producer: {err}")))?;

        Ok(Box::new(KafkaWriter::new(
            self.kafka_info.clone(),
            RdkafkaProducer::new(producer),
            file_idx as i64,
        )))
    }
}

pub struct KafkaWriter<P: KafkaProducer> {
    kafka_info: KafkaWriteInfo<daft_dsl::expr::bound_expr::BoundExpr>,
    producer: P,
    accounting: KafkaWriteAccounting,
}

impl<P: KafkaProducer> KafkaWriter<P> {
    pub fn new(
        kafka_info: KafkaWriteInfo<daft_dsl::expr::bound_expr::BoundExpr>,
        producer: P,
        task_id: i64,
    ) -> Self {
        Self {
            kafka_info,
            producer,
            accounting: KafkaWriteAccounting::new(task_id),
        }
    }
}

#[async_trait]
impl<P: KafkaProducer + 'static> AsyncFileWriter for KafkaWriter<P> {
    type Input = MicroPartition;
    type Result = Vec<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<WriteResult> {
        let _ = &self.kafka_info;
        let rows = data.len();
        for _ in 0..rows {
            self.accounting.record_attempt();
        }
        Err(DaftError::NotImplemented(
            "[write_kafka] row projection is not wired into KafkaWriter yet".to_string(),
        ))
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        self.producer
            .flush(Duration::from_millis(self.kafka_info.timeout_ms))
            .await?;
        Ok(vec![self.accounting.to_record_batch()?])
    }

    fn bytes_written(&self) -> usize {
        self.accounting.bytes_delivered as usize
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.accounting.bytes_delivered as usize]
    }
}
```

This intermediate writer compiles the factory path and deliberately returns `NotImplemented` until Task 9 wires full row projection. The public API should not be considered complete until Task 9 and integration tests pass.

- [ ] **Step 3: Run writer check**

Run:

```bash
cargo check -p daft-writers --features kafka
```

Expected: pass after adapting API details for `RecordBatch` construction and `rdkafka` headers.

- [ ] **Step 4: Commit adapter and writer shell**

```bash
git add src/daft-writers/src/kafka/producer.rs src/daft-writers/src/kafka/writer.rs src/daft-writers/src/lib.rs
git commit -m "feat: add rdkafka writer factory shell" \
  -m "Constraint: Isolate rdkafka behind daft-writers and a testable producer trait." \
  -m "Confidence: medium" \
  -m "Scope-risk: moderate" \
  -m "Tested: cargo check -p daft-writers --features kafka"
```

---

### Task 9: Full Row Projection and Delivery Accounting

**Files:**
- Modify: `src/daft-writers/src/kafka/record.rs`
- Modify: `src/daft-writers/src/kafka/headers.rs`
- Modify: `src/daft-writers/src/kafka/writer.rs`
- Test: `cargo test -p daft-writers --features kafka kafka::writer kafka::record kafka::headers`

- [ ] **Step 1: Add record projection tests with `FakeProducer`**

In `writer.rs` tests, add a test that constructs a simple `MicroPartition` with binary `key` and `value`, uses `KafkaWriter<FakeProducer>`, and asserts:

```rust
assert_eq!(accounting.messages_attempted, 1);
assert_eq!(accounting.messages_delivered, 1);
assert_eq!(accounting.messages_failed, 0);
```

Create a local helper using `RecordBatch` and `MicroPartition::new_loaded` so the test owns its binary `key` and `value` columns.

- [ ] **Step 2: Implement projection from `MicroPartition` rows**

In `record.rs`, add:

```rust
pub fn records_from_micropartition(
    input: &MicroPartition,
    info: &daft_logical_plan::KafkaWriteInfo<daft_dsl::expr::bound_expr::BoundExpr>,
) -> DaftResult<Vec<KafkaOutgoingRecord>> {
    let tables = input.get_tables()?;
    let mut records = Vec::with_capacity(input.len());
    for table in tables {
        for row_idx in 0..table.len() {
            records.push(record_from_row(&table, row_idx, info)?);
        }
    }
    Ok(records)
}
```

Implement `record_from_row` using Daft series accessors for each bound expression. The concrete accessors must match Daft's current `RecordBatch` APIs. Preserve these semantics:

- Static topic uses the plan topic string.
- Dynamic topic reads utf8 and rejects null.
- Raw value reads binary and sends `None` for null.
- UTF-8 value reads utf8 and encodes to bytes.
- JSON value serializes a non-null Daft value to JSON bytes and sends tombstone for Daft null.
- Raw key reads binary and sends `None` for null.
- UTF-8 key reads utf8 and sends `None` for null.
- Partition validates non-negative int32/int64.
- Timestamp reads nullable int64.
- Headers preserves list order and duplicate keys.

- [ ] **Step 3: Replace writer `NotImplemented` with send loop**

In `writer.rs`, replace `write` body:

```rust
async fn write(&mut self, data: Self::Input) -> DaftResult<WriteResult> {
    let records = super::record::records_from_micropartition(&data, &self.kafka_info)?;
    let mut rows_written = 0usize;
    let mut bytes_written = 0usize;

    for record in records {
        self.accounting.record_attempt();
        let delivered_bytes = record.delivered_bytes();
        match self.producer.send(record).await {
            Ok(_) => {
                self.accounting.record_delivery(delivered_bytes);
                rows_written += 1;
                bytes_written += delivered_bytes;
            }
            Err(err) => {
                self.accounting.record_failure(err.to_string());
                return Err(err);
            }
        }
    }

    Ok(WriteResult {
        rows_written,
        bytes_written,
    })
}
```

After this passes, replace serial sends with bounded in-flight futures:

```rust
const MAX_IN_FLIGHT_PER_TASK: usize = 1024;
```

Use `futures::stream::FuturesUnordered` and drain when the set reaches the bound.

- [ ] **Step 4: Run writer tests**

Run:

```bash
cargo test -p daft-writers --features kafka kafka::writer kafka::record kafka::headers
```

Expected: pass.

- [ ] **Step 5: Commit full writer projection**

```bash
git add src/daft-writers/src/kafka/record.rs src/daft-writers/src/kafka/headers.rs src/daft-writers/src/kafka/writer.rs
git commit -m "feat: implement kafka record projection and delivery accounting" \
  -m "Constraint: write_kafka correctness summary must come from delivery results." \
  -m "Confidence: medium" \
  -m "Scope-risk: broad" \
  -m "Tested: cargo test -p daft-writers --features kafka kafka::writer kafka::record kafka::headers"
```

---

### Task 10: Local Execution Pipeline Integration

**Files:**
- Modify: `src/daft-local-execution/src/sinks/write.rs`
- Modify: `src/daft-local-execution/src/pipeline.rs`
- Test: `cargo check -p daft-local-execution --features python`
- Test: `pytest tests/io/test_kafka_write_mock.py -q`

- [ ] **Step 1: Add `WriteFormat::Kafka`**

In `write.rs`:

```rust
#[derive(Debug)]
pub enum WriteFormat {
    Parquet,
    PartitionedParquet,
    Csv,
    PartitionedCsv,
    Json,
    PartitionedJson,
    Iceberg,
    PartitionedIceberg,
    Deltalake,
    PartitionedDeltalake,
    Lance,
    Kafka,
    DataSink(String),
}
```

Add to `name()`:

```rust
WriteFormat::Kafka => "Kafka Write".into(),
```

- [ ] **Step 2: Add pipeline branch**

In `pipeline.rs`, add branch near other write branches:

```rust
LocalPhysicalPlan::KafkaWrite(daft_local_plan::KafkaWrite {
    input,
    kafka_info,
    file_schema,
    stats_state,
    context,
}) => {
    let child_node = physical_plan_to_pipeline(input, cfg, ctx, input_senders)?;
    let writer_factory = daft_writers::make_kafka_writer_factory(kafka_info.clone());
    let write_sink = WriteSink::new(
        WriteFormat::Kafka,
        writer_factory,
        None,
        file_schema.clone(),
    );
    BlockingSinkNode::new(
        Arc::new(write_sink),
        child_node,
        stats_state.clone(),
        ctx,
        context,
    )
    .boxed()
}
```

- [ ] **Step 3: Run checks and Python validation**

Run:

```bash
cargo check -p daft-local-execution --features python
pytest tests/io/test_kafka_write_mock.py -q
```

Expected: broker-free invalid-input tests and normalization helper tests pass. Broker-free tests do not assert successful delivery because public `write_kafka` is blocking and requires a broker.

- [ ] **Step 4: Commit pipeline integration**

```bash
git add src/daft-local-execution/src/sinks/write.rs src/daft-local-execution/src/pipeline.rs
git commit -m "feat: route kafka writes through local execution" \
  -m "Constraint: Kafka write should reuse Daft WriteSink and WriterFactory execution semantics." \
  -m "Confidence: medium" \
  -m "Scope-risk: moderate" \
  -m "Tested: cargo check -p daft-local-execution --features python; pytest tests/io/test_kafka_write_mock.py -q"
```

---

### Task 11: Redpanda Integration Tests

**Files:**
- Modify: `tests/integration/io/test_kafka_integration.py`
- Test: `DAFT_RUNNER=native pytest tests/integration/io/test_kafka_integration.py -m integration -q`

- [ ] **Step 1: Add write round-trip test**

Append:

```python
@pytest.mark.integration()
def test_write_kafka_raw_roundtrip(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic = f"daft-kafka-write-{uuid.uuid4().hex[:8]}"
    _ensure_topic(bootstrap=bootstrap, topic=topic, num_partitions=2)

    input_df = daft.from_pydict(
        {
            "key": [b"k1", b"k2", b"k3"],
            "value": [b"v1", b"v2", b"v3"],
            "partition": [0, 1, 0],
        }
    )
    summary = input_df.write_kafka(
        bootstrap_servers=bootstrap,
        topic=topic,
        key_col="key",
        value_col="value",
        partition_col="partition",
        kafka_client_config={"acks": "all", "enable.idempotence": True},
        timeout_ms=20_000,
    ).collect()

    rows = summary.to_pylist()
    assert sum(row["messages_delivered"] for row in rows) == 3
    assert sum(row["messages_failed"] for row in rows) == 0

    out = daft.read_kafka(
        bootstrap_servers=bootstrap,
        topics=topic,
        start="earliest",
        end="latest",
        timeout_ms=20_000,
    ).collect()
    decoded = _decode_rows(out.to_pylist())
    assert {row["key"] for row in decoded} == {"k1", "k2", "k3"}
    assert {row["value"] for row in decoded} == {"v1", "v2", "v3"}
    assert {row["partition"] for row in decoded} == {0, 1}
```

Update `_decode_rows` so non-JSON values decode as UTF-8 when JSON parsing fails:

```python
try:
    value = json.loads(bytes(value).decode("utf-8"))
except json.JSONDecodeError:
    value = bytes(value).decode("utf-8")
```

- [ ] **Step 2: Add JSON and dynamic topic test**

Append:

```python
@pytest.mark.integration()
def test_write_kafka_json_dynamic_topic(kafka_context: dict[str, object]) -> None:
    bootstrap = str(kafka_context["bootstrap"])
    topic_a = f"daft-kafka-json-a-{uuid.uuid4().hex[:8]}"
    topic_b = f"daft-kafka-json-b-{uuid.uuid4().hex[:8]}"
    _ensure_topic(bootstrap=bootstrap, topic=topic_a, num_partitions=1)
    _ensure_topic(bootstrap=bootstrap, topic=topic_b, num_partitions=1)

    daft.from_pydict(
        {
            "topic": [topic_a, topic_b],
            "key": ["a", "b"],
            "payload": [{"id": 1, "kind": "a"}, {"id": 2, "kind": "b"}],
        }
    ).write_kafka(
        bootstrap_servers=bootstrap,
        topic_col="topic",
        key_col="key",
        key_format="utf8",
        value_col="payload",
        value_format="json",
        timeout_ms=20_000,
    )

    out_a = daft.read_kafka(bootstrap, topic_a, timeout_ms=20_000).collect()
    out_b = daft.read_kafka(bootstrap, topic_b, timeout_ms=20_000).collect()
    decoded_a = _decode_rows(out_a.to_pylist())
    decoded_b = _decode_rows(out_b.to_pylist())
    assert decoded_a[0]["value"] == {"id": 1, "kind": "a"}
    assert decoded_b[0]["value"] == {"id": 2, "kind": "b"}
```

- [ ] **Step 3: Add headers test with direct consumer**

Append:

```python
@pytest.mark.integration()
def test_write_kafka_headers(kafka_context: dict[str, object]) -> None:
    confluent_kafka = pytest.importorskip("confluent_kafka")
    bootstrap = str(kafka_context["bootstrap"])
    topic = f"daft-kafka-headers-{uuid.uuid4().hex[:8]}"
    _ensure_topic(bootstrap=bootstrap, topic=topic, num_partitions=1)

    daft.from_pydict(
        {
            "value": [b"payload"],
            "headers": [[{"key": "trace", "value": b"a"}, {"key": "trace", "value": b"b"}]],
        }
    ).write_kafka(
        bootstrap_servers=bootstrap,
        topic=topic,
        value_col="value",
        headers_col="headers",
        timeout_ms=20_000,
    )

    consumer = confluent_kafka.Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"daft-kafka-headers-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    msg = consumer.poll(20)
    consumer.close()
    assert msg is not None
    assert msg.error() is None
    assert msg.headers() == [("trace", b"a"), ("trace", b"b")]
```

- [ ] **Step 4: Run integration tests**

Start the Redpanda service:

```bash
docker compose -f tests/integration/io/docker-compose/docker-compose.yml up -d redpanda
```

Run:

```bash
DAFT_RUNNER=native pytest tests/integration/io/test_kafka_integration.py -m integration -q
```

Expected: Kafka read and write integration tests pass against Redpanda.

- [ ] **Step 5: Commit integration tests**

```bash
git add tests/integration/io/test_kafka_integration.py
git commit -m "test: cover kafka write redpanda round trips" \
  -m "Constraint: Kafka write needs broker-backed coverage for delivery and read-back behavior." \
  -m "Confidence: medium" \
  -m "Scope-risk: moderate" \
  -m "Tested: DAFT_RUNNER=native pytest tests/integration/io/test_kafka_integration.py -m integration -q"
```

---

### Task 12: Documentation and Final Verification

**Files:**
- Modify: `docs/connectors/kafka.md`
- Modify: `docs/api/io.md`
- Test: `make docs` if docs dependencies are available
- Test: `cargo test --no-default-features --workspace`
- Test: `make build`
- Test: `DAFT_RUNNER=native make test EXTRA_ARGS="tests/io/test_kafka_write_mock.py tests/integration/io/test_kafka_integration.py -m 'integration or not integration'"`

- [ ] **Step 1: Update Kafka connector docs**

In `docs/connectors/kafka.md`, change the title:

```markdown
# Kafka
```

Update limitations:

```markdown
!!! warning "Experimental"

    This connector is experimental and the API may change in future releases.

    - Reads are bounded batch reads only.
    - Consumer group offsets are not committed by `read_kafka`.
    - Writes are at-least-once and do not support transactions or exactly-once semantics.
```

Add write section:

```markdown
## Writing Messages

Use `DataFrame.write_kafka()` to produce one Kafka message per input row.

=== "Python"

    ```python
    import daft

    df = daft.from_pydict(
        {
            "key": [b"user-1", b"user-2"],
            "value": [b"login", b"logout"],
        }
    )

    summary = df.write_kafka(
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
    summary.show()
    ```

`write_kafka` returns task-level summary rows with:

| Column | Type | Description |
| --- | --- | --- |
| `task_id` | `int64` | Writer task identifier |
| `messages_attempted` | `int64` | Records constructed for Kafka |
| `messages_delivered` | `int64` | Records confirmed by Kafka delivery reports |
| `messages_failed` | `int64` | Records that failed enqueue or delivery |
| `bytes_delivered` | `int64` | Delivered key/value/header payload bytes |
| `first_error` | `string` | First sanitized error, or null on success |
```

Add null semantics:

```markdown
!!! note "Null values and tombstones"

    A null `value_col` writes a Kafka null payload, also known as a tombstone for compacted topics.
    With `value_format="json"`, non-null values are serialized to JSON bytes.
```

Add config note:

```markdown
`kafka_client_config` is passed to librdkafka producer configuration. Daft manages
`bootstrap.servers` through the `bootstrap_servers` argument and rejects `transactional.id`
until transactional writes are explicitly supported.
```

- [ ] **Step 2: Check API docs index**

Run:

```bash
rg -n "DataFrame.write_|write_parquet|write_csv|write_json|read_kafka" docs/api docs/connectors
```

When `docs/api/io.md` lists DataFrame write APIs, add:

```markdown
:::: daft.DataFrame.write_kafka
```

When it lists only module-level IO functions, leave `docs/api/io.md` unchanged and rely on generated DataFrame API docs.

Check the generated docs locally before deciding.

- [ ] **Step 3: Run format and targeted tests**

Run:

```bash
make format
cargo test -p daft-writers --features kafka kafka
cargo test --no-default-features --workspace
make build
DAFT_RUNNER=native make test EXTRA_ARGS="tests/io/test_kafka_write_mock.py tests/integration/io/test_kafka_integration.py -m 'integration or not integration'"
```

Expected: all pass before opening a PR.

- [ ] **Step 4: Run Ray smoke test**

Run:

```bash
DAFT_RUNNER=ray pytest tests/integration/io/test_kafka_integration.py::test_write_kafka_raw_roundtrip -m integration -q
```

Expected: pass.

- [ ] **Step 5: Commit docs and verification fixes**

```bash
git add docs/connectors/kafka.md docs/api/io.md
git commit -m "docs: document native kafka write support" \
  -m "Constraint: Kafka write semantics must be explicit about at-least-once delivery and unsupported transactions." \
  -m "Confidence: high" \
  -m "Scope-risk: narrow" \
  -m "Tested: make format; cargo test -p daft-writers --features kafka kafka; cargo test --no-default-features --workspace; make build; DAFT_RUNNER=native make test EXTRA_ARGS=\"tests/io/test_kafka_write_mock.py tests/integration/io/test_kafka_integration.py -m 'integration or not integration'\"; DAFT_RUNNER=ray pytest tests/integration/io/test_kafka_integration.py::test_write_kafka_raw_roundtrip -m integration -q"
```

---

## Final PR Checklist

- [ ] The issue link `Fixes #7101` or `Refs #7101` is in the PR body.
- [ ] PR title follows Conventional Commits.
- [ ] PR body includes `Changes Made` and `Related Issues`.
- [ ] PR body discloses AI assistance and verification performed.
- [ ] `cargo test --no-default-features --workspace` passes.
- [ ] `make build` passes after Rust changes.
- [ ] Python broker-free tests pass.
- [ ] Redpanda native integration tests pass.
- [ ] At least one Ray Kafka write/read round trip passes.
- [ ] Docs mention at-least-once semantics, tombstones, config passthrough, and unsupported transactions.
