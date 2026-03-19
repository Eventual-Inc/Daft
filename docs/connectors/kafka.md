# Reading from Kafka

!!! warning "Experimental"

    This connector is experimental and the API may change in future releases. Current limitations:

    - **Bounded batch reads only** — no streaming or unbounded mode
    - **No offset commit management** — consumer group offsets are not committed
    - **No write support** — Daft cannot produce messages to Kafka

[Apache Kafka](https://kafka.apache.org/) is a distributed event streaming platform. Daft can read bounded ranges of messages from Kafka topics into DataFrames using [`daft.read_kafka()`][daft.io.read_kafka].

Daft currently supports:

1. **Bounded batch reads**: Read a fixed range of messages defined by offsets, timestamps, or earliest/latest markers
2. **Multi-topic reads**: Read from one or more topics in a single call
3. **Partition filtering**: Read from specific partitions within a topic
4. **Flexible bounds**: Specify start/end using `"earliest"`/`"latest"`, timestamps (datetime, ISO-8601, or epoch ms), or explicit per-partition offset maps
5. **Limit pushdown**: Use `.limit(N)` to stop reading early once enough rows are collected

## Installing Daft with Kafka Support

Daft integrates with Kafka through the [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) package:

```bash
pip install -U "daft[kafka]"
```

Or install the dependency manually:

```bash
pip install confluent-kafka
```

## Output Schema

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
