# Kafka

!!! warning "Experimental"

    This connector is experimental and the API may change in future releases. Current limitations:

    - **Bounded batch reads only** — no streaming or unbounded read mode
    - **No offset commit management** — consumer group offsets are not committed
    - **At-least-once writes only** — already delivered messages are not rolled back after a later failure
    - **No transactional writes** — `transactional.id` is rejected

[Apache Kafka](https://kafka.apache.org/) is a distributed event streaming platform. Daft can read bounded ranges of messages from Kafka topics into DataFrames using [`daft.read_kafka()`][daft.io.read_kafka] and can write DataFrames to Kafka topics using [`df.write_kafka()`][daft.dataframe.DataFrame.write_kafka].

Daft currently supports:

1. **Bounded batch reads**: Read a fixed range of messages defined by offsets, timestamps, or earliest/latest markers
2. **Multi-topic reads**: Read from one or more topics in a single call
3. **Partition filtering**: Read from specific partitions within a topic
4. **Flexible bounds**: Specify start/end using `"earliest"`/`"latest"`, timestamps (datetime, ISO-8601, or epoch ms), or explicit per-partition offset maps
5. **Limit pushdown**: Use `.limit(N)` to stop reading early once enough rows are collected
6. **Native batch writes**: Produce DataFrame rows to a static or per-row topic with optional keys, partitions, timestamps, and headers

## Installing Daft with Kafka Support

Daft reads from Kafka through the [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) package:

```bash
pip install -U "daft[kafka]"
```

Or install the dependency manually:

```bash
pip install confluent-kafka
```

Kafka writes use Daft's native producer path and do not require the Python `confluent-kafka` package.

## Read Output Schema

Every `read_kafka` call returns a DataFrame with the following fixed schema:

| Column         | Type     | Description                                           |
|----------------|----------|-------------------------------------------------------|
| `topic`        | `string` | The topic the message was read from                   |
| `partition`    | `int32`  | The partition ID within the topic                     |
| `offset`       | `int64`  | The offset of the message within the partition        |
| `timestamp_ms` | `int64`  | Message timestamp in milliseconds (null if unavailable) |
| `key`          | `binary` | The message key as raw bytes (null if not present)    |
| `value`        | `binary` | The message value as raw bytes                        |

## Reading Messages

### Basic Usage

Read all messages from a topic (earliest to latest):

=== "🐍 Python"

    ```python
    import daft

    df = daft.read_kafka(bootstrap_servers="localhost:9092", topics="my-topic")
    df.show()
    ```

### Timestamp Bounds

Read messages within a time range:

=== "🐍 Python"

    ```python
    import daft
    from datetime import datetime, timezone

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)

    df = daft.read_kafka(
        bootstrap_servers="localhost:9092",
        topics="my-topic",
        start=start,
        end=end,
    )
    ```

You can also use ISO-8601 strings or epoch milliseconds:

=== "🐍 Python"

    ```python
    df = daft.read_kafka(
        bootstrap_servers="localhost:9092",
        topics="my-topic",
        start="2024-01-01T00:00:00Z",
        end=1704153600000,  # epoch ms
    )
    ```

### Offset Bounds

Read a specific offset range from a partition:

=== "🐍 Python"

    ```python
    df = daft.read_kafka(
        bootstrap_servers="localhost:9092",
        topics="my-topic",
        start={0: 100},   # partition 0, offset 100
        end={0: 200},     # partition 0, offset 200 (exclusive)
    )
    ```

### Partition Filtering

Read only from specific partitions:

=== "🐍 Python"

    ```python
    df = daft.read_kafka(
        bootstrap_servers="localhost:9092",
        topics="my-topic",
        partitions=[0, 2],
    )
    ```

### Multiple Topics

Read from multiple topics at once:

=== "🐍 Python"

    ```python
    df = daft.read_kafka(
        bootstrap_servers="localhost:9092",
        topics=["topic-a", "topic-b"],
    )
    ```

With per-topic offset maps:

=== "🐍 Python"

    ```python
    df = daft.read_kafka(
        bootstrap_servers="localhost:9092",
        topics=["topic-a", "topic-b"],
        start={"topic-a": {0: 100}, "topic-b": {0: 50}},
        end="latest",
    )
    ```

### Using Limit

Stop reading early once enough rows are collected:

=== "🐍 Python"

    ```python
    df = daft.read_kafka(bootstrap_servers="localhost:9092", topics="my-topic").limit(1000)
    df.show()
    ```

## Kafka Client Configuration

Pass additional [librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) options via `kafka_client_config`:

=== "🐍 Python"

    ```python
    df = daft.read_kafka(
        bootstrap_servers="localhost:9092",
        topics="my-topic",
        kafka_client_config={
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "my-api-key",
            "sasl.password": "my-api-secret",
        },
    )
    ```

!!! note

    The following keys are managed internally and cannot be overridden via `kafka_client_config`:
    `bootstrap.servers`, `group.id`, `enable.auto.commit`, `enable.auto.offset.store`.
    These are configured through the dedicated `bootstrap_servers` and `group_id` parameters of `read_kafka()` instead.

## Writing Messages

Use `DataFrame.write_kafka()` to produce each input row as a Kafka message. The call is blocking and returns a DataFrame with task-level write summaries.

### Basic Usage

Write binary keys and values to a single topic:

=== "🐍 Python"

    ```python
    import daft

    df = daft.from_pydict(
        {
            "key": [b"user-1", b"user-2"],
            "value": [b"created", b"updated"],
        }
    )

    summary = df.write_kafka(
        bootstrap_servers="localhost:9092",
        topic="events",
        key_col="key",
        value_col="value",
    )
    summary.show()
    ```

### JSON Values

Set `value_format="json"` to serialize JSON-compatible Daft values. A top-level null value is written as a Kafka null value, which is commonly used as a tombstone in compacted topics. Nested nulls inside structs or lists remain JSON `null`.

=== "🐍 Python"

    ```python
    df = daft.from_pylist(
        [
            {"key": "user-1", "value": {"op": "upsert", "tags": ["new", None]}},
            {"key": "user-2", "value": None},
        ]
    )

    df.write_kafka(
        bootstrap_servers="localhost:9092",
        topic="user-events",
        key_col="key",
        value_col="value",
        key_format="utf8",
        value_format="json",
    )
    ```

### Dynamic Topics and Partitions

Use `topic_col` to choose the topic per row. Use either `partition` for a fixed partition or `partition_col` for a per-row partition. Null partition values let Kafka choose the partition.

=== "🐍 Python"

    ```python
    df = daft.from_pydict(
        {
            "topic": ["events-a", "events-b"],
            "value": [b"a", b"b"],
            "partition": [0, None],
        }
    )

    df.write_kafka(
        bootstrap_servers=["broker-1:9092", "broker-2:9092"],
        topic_col="topic",
        value_col="value",
        key_col=None,
        partition_col="partition",
    )
    ```

### Timestamps and Headers

`timestamp_ms_col` defaults to `"timestamp_ms"` when that column is present. Header values should be a list of structs with `key` and `value` fields. This representation preserves duplicate header keys and ordering.

=== "🐍 Python"

    ```python
    df = daft.from_pylist(
        [
            {
                "key": b"k",
                "value": b"v",
                "timestamp_ms": 1710000000000,
                "headers": [
                    {"key": "trace", "value": b"a"},
                    {"key": "trace", "value": b"b"},
                ],
            }
        ]
    )

    df.write_kafka(
        bootstrap_servers="localhost:9092",
        topic="events",
        key_col="key",
        value_col="value",
        headers_col="headers",
        timestamp_ms_col="timestamp_ms",
    )
    ```

## Write Summary Schema

`write_kafka` returns one row per write task:

| Column               | Type     | Description                                             |
|----------------------|----------|---------------------------------------------------------|
| `task_id`            | `int64`  | Daft write task identifier                              |
| `messages_attempted` | `int64`  | Number of messages attempted by the task                |
| `messages_delivered` | `int64`  | Number of messages acknowledged by Kafka                |
| `messages_failed`    | `int64`  | Number of messages that failed delivery                 |
| `bytes_delivered`    | `int64`  | Delivered key, value, and header bytes counted by Daft  |
| `first_error`        | `string` | First delivery or flush error for the task, if any      |

Delivery counts come from Kafka delivery results. `write_kafka` is at-least-once: if a task errors after some messages are delivered, those messages remain in Kafka.

## Write Formats

| Field       | Parameter          | Formats                         | Null behavior                                  |
|-------------|--------------------|---------------------------------|------------------------------------------------|
| topic       | `topic`/`topic_col` | UTF-8 string                    | Dynamic topics must not be null                |
| key         | `key_col`          | `raw` binary or `utf8` string   | Null key is allowed                            |
| value       | `value_col`        | `raw`, `utf8`, or `json`        | Null value sends a Kafka null value            |
| partition   | `partition`/`partition_col` | non-negative `int32`/`int64` | Null dynamic partition lets Kafka choose       |
| timestamp   | `timestamp_ms_col` | `int64` epoch milliseconds      | Null timestamp lets Kafka assign the timestamp |
| headers     | `headers_col`      | list of `{key, value}` structs  | Null headers are treated as no headers         |

## Kafka Producer Configuration

Pass additional [librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) options via `kafka_client_config`:

=== "🐍 Python"

    ```python
    df.write_kafka(
        bootstrap_servers="localhost:9092",
        topic="events",
        kafka_client_config={
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "client.id": "daft-writer",
        },
    )
    ```

`timeout_ms` is used for Kafka enqueue, delivery, and flush timeouts. If `kafka_client_config` does not include `delivery.timeout.ms` or `message.timeout.ms`, Daft passes `timeout_ms` through as librdkafka's `delivery.timeout.ms`.

!!! note

    `bootstrap.servers` is managed by the `bootstrap_servers` parameter and cannot be overridden through `kafka_client_config`. `transactional.id` is not supported yet and will raise an error.

Native Kafka writes are built with librdkafka support for TLS and common compression codecs including zlib and zstd. Kerberos/GSSAPI builds are not enabled by default.
