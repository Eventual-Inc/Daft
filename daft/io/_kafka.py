from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from daft import DataType
from daft.api_annotations import PublicAPI
from daft.dependencies import confluent_kafka
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.io.pushdowns import Pushdowns


logger = logging.getLogger(__name__)


_KAFKA_SCHEMA = Schema.from_pydict(
    {
        "topic": DataType.string(),
        "partition": DataType.int32(),
        "offset": DataType.int64(),
        "timestamp_ms": DataType.int64(),
        "key": DataType.binary(),
        "value": DataType.binary(),
    }
)

_KIND_EARLIEST = "earliest"
_KIND_LATEST = "latest"
_KIND_TIMESTAMP_MS = "timestamp_ms"
_KIND_TOPIC_PARTITION_OFFSETS = "topic_partition_offsets"


def _to_timestamp_ms(value: object) -> int:
    """Convert a timestamp bound into epoch milliseconds (UTC).

    Accepts:
    - int: treated as milliseconds since epoch
    - datetime: converted to UTC milliseconds (naive datetimes assumed UTC)
    - str: parsed as ISO-8601; supports a trailing 'Z'
    """
    if isinstance(value, int):
        return value

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        s = value
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
    else:
        raise TypeError("[read_kafka] invalid timestamp bound; expected int(ms), datetime, or ISO-8601 string")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.timestamp() * 1000)


def _normalize_bound(
    bound: object,
    topics: list[str],
) -> tuple[str, int | None, dict[str, dict[int, int]] | None]:
    """Normalize user-facing start/end bound into an internal representation.

    Returns a 3-tuple of:
    - kind: One of {_KIND_EARLIEST, _KIND_LATEST, _KIND_TIMESTAMP_MS, _KIND_TOPIC_PARTITION_OFFSETS}
    - timestamp_ms: Only set when kind == _KIND_TIMESTAMP_MS
    - offsets_by_topic: Only set when kind == _KIND_TOPIC_PARTITION_OFFSETS and shaped as {topic: {partition: offset}}

    For offset-map bounds, the user may pass either {partition: offset} (single-topic only) or
    {topic: {partition: offset}} (multi-topic). Both are normalized into offsets_by_topic.
    """
    if bound is None:
        raise ValueError("[read_kafka] start/end bound cannot be None for bounded read")

    if isinstance(bound, str):
        lowered = bound.lower()
        if lowered in {_KIND_EARLIEST, _KIND_LATEST}:
            return (lowered, None, None)
        return (_KIND_TIMESTAMP_MS, _to_timestamp_ms(bound), None)

    if isinstance(bound, datetime):
        return (_KIND_TIMESTAMP_MS, _to_timestamp_ms(bound), None)

    if isinstance(bound, int):
        return (_KIND_TIMESTAMP_MS, bound, None)

    if isinstance(bound, Mapping):
        items = list(bound.items())
        if not items:
            raise ValueError("[read_kafka] partition offset map must be non-empty")

        # Accept either:
        # - {topic: {partition: offset}} for multi-topic reads, or
        # - {partition: offset} for single-topic reads (rewritten to {topic: {partition: offset}}).
        if all(isinstance(k, str) for k, _ in items):
            offsets_by_topic: dict[str, dict[int, int]] = {}
            for topic, per_topic in items:
                if not isinstance(per_topic, Mapping):
                    raise TypeError("[read_kafka] topic offset map values must be dicts of partition->offset")
                offsets_for_topic: dict[int, int] = {}
                for pk, pv in per_topic.items():
                    if not isinstance(pk, int) or not isinstance(pv, int):
                        raise TypeError("[read_kafka] partition offset map values must be dicts of partition->offset")
                    offsets_for_topic[pk] = pv
                if not offsets_for_topic:
                    raise ValueError("[read_kafka] partition offset map must be non-empty")
                offsets_by_topic[str(topic)] = offsets_for_topic

            provided = set(offsets_by_topic.keys())
            expected = set(topics)
            extra = provided - expected
            missing = expected - provided
            if extra or missing:
                raise ValueError(
                    "[read_kafka] offsets must be provided for exactly the topics being read; "
                    f"expected={sorted(expected)}, provided={sorted(provided)}, extra={sorted(extra)}, missing={sorted(missing)}"
                )
            return (_KIND_TOPIC_PARTITION_OFFSETS, None, offsets_by_topic)

        if all(not isinstance(k, str) for k, _ in items):
            # Single-topic shorthand: {partition: offset}
            if len(topics) != 1:
                raise ValueError(
                    "[read_kafka] {partition: offset} start/end bounds require a single topic; "
                    f"got topics={topics}; use {{topic: {{partition: offset}}}} for multiple topics"
                )
            offsets_by_partition: dict[int, int] = {}
            for k, v in items:
                if not isinstance(k, int) or not isinstance(v, int):
                    raise TypeError(
                        "[read_kafka] partition offset maps must be either {partition: offset} or {topic: {partition: offset}}"
                    )
                offsets_by_partition[k] = v
            return (_KIND_TOPIC_PARTITION_OFFSETS, None, {topics[0]: offsets_by_partition})

        raise TypeError(
            "[read_kafka] partition offset maps must be either {partition: offset} or {topic: {partition: offset}}"
        )

    raise TypeError(
        "[read_kafka] start/end must be one of: 'earliest'/'latest', int(timestamp_ms), datetime/ISO string, "
        "or an offset map ({partition: offset} or {topic: {partition: offset}})"
    )


def _make_consumer_config(
    *,
    bootstrap_servers: str,
    group_id: str,
    kafka_client_config: Mapping[str, object] | None,
) -> dict[str, object]:
    cfg: dict[str, object] = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    if kafka_client_config is not None:
        for k, v in kafka_client_config.items():
            key = str(k)
            if isinstance(v, (str, int, bool, float)) or v is None:
                cfg[key] = v
            else:
                cfg[key] = str(v)
    return cfg


def _resolve_bound(
    *,
    consumer: Any,
    kind: str,
    timestamp_ms: int | None,
    offsets_by_topic: dict[str, dict[int, int]] | None,
    low: int,
    high: int,
    topic: str,
    partition: int,
    timeout_s: float,
) -> int:
    """Resolve a normalized bound into a concrete Kafka offset, clamped to [low, high]."""
    if kind == _KIND_EARLIEST:
        return low
    if kind == _KIND_LATEST:
        return high
    if kind == _KIND_TIMESTAMP_MS:
        if timestamp_ms is None:
            raise ValueError("[read_kafka] internal error: timestamp_ms is required to resolve a timestamp bound")
        tp = confluent_kafka.TopicPartition(topic, partition, timestamp_ms)
        [resolved] = consumer.offsets_for_times([tp], timeout=timeout_s)
        if resolved is None or resolved.offset is None:
            raise ValueError(
                "[read_kafka] failed to resolve timestamp to offset; "
                f"topic={topic}, partition={partition}, timestamp_ms={timestamp_ms}"
            )
        if resolved.offset < 0:
            # offsets_for_times returns -1 when the timestamp is after the last message in the partition; fall back to `high`.
            logger.warning(
                "read_kafka: offsets_for_times returned invalid offset (-1) for timestamp_ms=%s; falling back to end-of-partition (high=%s). topic=%s partition=%s",
                timestamp_ms,
                high,
                topic,
                partition,
            )
            return high
        return max(low, min(high, resolved.offset))
    if kind == _KIND_TOPIC_PARTITION_OFFSETS:
        if offsets_by_topic is None:
            raise ValueError(
                "[read_kafka] internal error: topic/partition offset map is required to resolve an offset-map bound"
            )
        per_topic = offsets_by_topic.get(topic)
        if per_topic is None:
            raise ValueError(f"[read_kafka] missing offsets for topic {topic!r}")
        configured = per_topic.get(partition)
        if configured is None:
            raise ValueError(f"[read_kafka] missing offset for partition {partition} of topic {topic!r}")
        if configured < 0:
            raise ValueError("[read_kafka] partition offsets must be >= 0")
        return max(low, min(high, configured))
    raise ValueError(f"[read_kafka] unsupported bound kind: {kind}")


class KafkaSource(DataSource):
    _bootstrap_servers: str
    _group_id: str
    _topics: list[str]
    _start_kind: str
    _start_timestamp_ms: int | None
    _start_topic_partition_offsets: dict[str, dict[int, int]] | None
    _end_kind: str
    _end_timestamp_ms: int | None
    _end_topic_partition_offsets: dict[str, dict[int, int]] | None
    _partitions: Sequence[int] | None
    _kafka_client_config: Mapping[str, object] | None
    _timeout_ms: int
    _chunk_size: int

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        start_kind: str,
        start_timestamp_ms: int | None,
        start_topic_partition_offsets: dict[str, dict[int, int]] | None,
        end_kind: str,
        end_timestamp_ms: int | None,
        end_topic_partition_offsets: dict[str, dict[int, int]] | None,
        partitions: Sequence[int] | None,
        kafka_client_config: Mapping[str, object] | None,
        timeout_ms: int,
        chunk_size: int = 1024,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._topics = topics
        self._start_kind = start_kind
        self._start_timestamp_ms = start_timestamp_ms
        self._start_topic_partition_offsets = start_topic_partition_offsets
        self._end_kind = end_kind
        self._end_timestamp_ms = end_timestamp_ms
        self._end_topic_partition_offsets = end_topic_partition_offsets
        self._partitions = partitions
        self._kafka_client_config = kafka_client_config
        self._timeout_ms = timeout_ms
        self._chunk_size = chunk_size

    @property
    def name(self) -> str:
        return "KafkaSource"

    @property
    def schema(self) -> Schema:
        return _KAFKA_SCHEMA

    def get_tasks(self, pushdowns: Pushdowns) -> Iterator[KafkaSourceTask]:
        timeout_s = self._timeout_ms / 1000.0

        cfg = _make_consumer_config(
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            kafka_client_config=self._kafka_client_config,
        )
        consumer = confluent_kafka.Consumer(cfg)

        try:
            partition_filter = set(self._partitions) if self._partitions is not None else None

            for topic in self._topics:
                metadata = consumer.list_topics(topic=topic, timeout=timeout_s)
                topic_metadata = metadata.topics.get(topic)
                if topic_metadata is None or topic_metadata.error is not None:
                    raise ValueError(f"[read_kafka] topic not found: {topic}")

                for partition_id in topic_metadata.partitions.keys():
                    if partition_filter is not None and partition_id not in partition_filter:
                        continue

                    low, high = consumer.get_watermark_offsets(
                        confluent_kafka.TopicPartition(topic, partition_id),
                        timeout=timeout_s,
                        cached=False,
                    )

                    start_offset = _resolve_bound(
                        consumer=consumer,
                        kind=self._start_kind,
                        timestamp_ms=self._start_timestamp_ms,
                        offsets_by_topic=self._start_topic_partition_offsets,
                        low=low,
                        high=high,
                        topic=topic,
                        partition=partition_id,
                        timeout_s=timeout_s,
                    )
                    end_offset = _resolve_bound(
                        consumer=consumer,
                        kind=self._end_kind,
                        timestamp_ms=self._end_timestamp_ms,
                        offsets_by_topic=self._end_topic_partition_offsets,
                        low=low,
                        high=high,
                        topic=topic,
                        partition=partition_id,
                        timeout_s=timeout_s,
                    )

                    if end_offset < start_offset:
                        raise ValueError(
                            "[read_kafka] end must be >= start; "
                            f"topic={topic}, partition={partition_id}, start_offset={start_offset}, end_offset={end_offset}"
                        )
                    if start_offset == end_offset:
                        continue

                    yield KafkaSourceTask(
                        _bootstrap_servers=self._bootstrap_servers,
                        _group_id=self._group_id,
                        _topic=topic,
                        _partition=partition_id,
                        _start_offset=start_offset,
                        _end_offset=end_offset,
                        _kafka_client_config=self._kafka_client_config,
                        _timeout_ms=self._timeout_ms,
                        _chunk_size=self._chunk_size,
                        _limit=pushdowns.limit,
                    )
        finally:
            consumer.close()


@dataclass(frozen=True)
class KafkaSourceTask(DataSourceTask):
    _bootstrap_servers: str
    _group_id: str
    _topic: str
    _partition: int
    _start_offset: int
    _end_offset: int
    _kafka_client_config: Mapping[str, object] | None
    _timeout_ms: int
    _chunk_size: int
    _limit: int | None

    @property
    def schema(self) -> Schema:
        return _KAFKA_SCHEMA

    def get_micro_partitions(self) -> Iterator[MicroPartition]:
        timeout_s = self._timeout_ms / 1000.0

        cfg = _make_consumer_config(
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            kafka_client_config=self._kafka_client_config,
        )
        consumer = confluent_kafka.Consumer(cfg)

        try:
            if self._start_offset >= self._end_offset:
                return

            topic_partition = confluent_kafka.TopicPartition(self._topic, self._partition, self._start_offset)
            consumer.assign([topic_partition])

            remaining = self._limit
            if remaining is not None and remaining <= 0:
                return

            while True:
                # Build up to `chunk_size` rows for the next MicroPartition.
                topics: list[str] = []
                partitions: list[int] = []
                offsets: list[int] = []
                timestamps: list[int | None] = []
                keys: list[bytes | None] = []
                values: list[bytes | None] = []

                stop = False

                # `consume(n)` may return fewer than `n` messages, so we keep consuming until we either
                # fill a chunk (for larger MicroPartitions) or hit a stop condition.
                while len(topics) < self._chunk_size and remaining != 0:
                    fetch_n = self._chunk_size - len(topics)
                    if remaining is not None:
                        fetch_n = min(fetch_n, remaining)

                    msgs = consumer.consume(fetch_n, timeout=timeout_s)
                    if not msgs:
                        # Avoid spinning indefinitely when no messages are returned: flush the current chunk (if any),
                        # otherwise terminate this KafkaSourceTask generator (no more MicroPartitions will be yielded
                        # for this partition).
                        break

                    for msg in msgs:
                        err = msg.error()
                        if err is not None:
                            if err.code() == confluent_kafka.KafkaError._PARTITION_EOF:
                                stop = True
                                break
                            raise confluent_kafka.KafkaException(err)

                        offset = msg.offset()
                        if offset >= self._end_offset:
                            # Stop once we hit the bounded end offset for this task.
                            stop = True
                            break

                        topics.append(self._topic)
                        partitions.append(self._partition)
                        offsets.append(offset)

                        ts_type, ts_ms = msg.timestamp()
                        if ts_type == confluent_kafka.TIMESTAMP_NOT_AVAILABLE or ts_ms is None or ts_ms < 0:
                            timestamps.append(None)
                        else:
                            timestamps.append(ts_ms)

                        keys.append(msg.key())
                        values.append(msg.value())

                        if remaining is not None:
                            remaining -= 1
                            if remaining == 0:
                                stop = True
                                break

                        if offset == self._end_offset - 1:
                            # Early exit: this is the last offset in range, skip one more consume() round-trip.
                            stop = True
                            break

                    if stop:
                        break

                if not topics:
                    # No rows accumulated for this chunk (e.g. consume() returned empty immediately), so terminate.
                    return

                yield MicroPartition.from_pydict(
                    {
                        "topic": topics,
                        "partition": partitions,
                        "offset": offsets,
                        "timestamp_ms": timestamps,
                        "key": keys,
                        "value": values,
                    }
                )

                if stop:
                    return
        finally:
            consumer.close()


@PublicAPI
def read_kafka(
    bootstrap_servers: str | Sequence[str],
    topics: str | Sequence[str],
    *,
    start: object = _KIND_EARLIEST,
    end: object = _KIND_LATEST,
    group_id: str = "daft-bounded-kafka-reader",
    partitions: Sequence[int] | None = None,
    chunk_size: int = 1024,
    kafka_client_config: Mapping[str, object] | None = None,
    timeout_ms: int = 10_000,
) -> DataFrame:
    """Creates a DataFrame by reading messages from Kafka topic(s).

    This function reads bounded ranges of messages from one or more Kafka topics. It supports
    multiple ways to specify the start and end bounds: earliest/latest, timestamp, or explicit
    partition offsets.

    Args:
        bootstrap_servers (str | Sequence[str]): Kafka bootstrap server(s) to connect to.
            Can be a single server string (e.g., "localhost:9092") or a sequence of servers.
        topics (str | Sequence[str]): Kafka topic(s) to read from. Can be a single topic string
            or a sequence of topics.
        start (object): The start bound for reading messages. Defaults to "earliest".
            Supported values:
            - "earliest": Start from the earliest available offset for each partition.
            - "latest": Start from the latest offset for each partition.
            - int: Timestamp in milliseconds since epoch.
            - datetime: A timezone-aware or naive datetime (naive datetimes are assumed UTC).
            - str: An ISO-8601 timestamp string (e.g., "2024-01-01T00:00:00Z").
            - dict: For single topic: ``{partition: offset}``. For multiple topics: ``{topic: {partition: offset}}``.
        end (object): The end bound for reading messages. Defaults to "latest".
            Supports the same value types as ``start``. The end offset is exclusive.
        group_id (str): Consumer group ID used for the Kafka consumer. Defaults to
            "daft-bounded-kafka-reader".
        partitions (Sequence[int] | None): Optional sequence of partition IDs to read from.
            If None, reads from all partitions of the specified topic(s). Defaults to None.
        chunk_size (int): Maximum number of messages per MicroPartition. Defaults to 1024.
        kafka_client_config (Mapping[str, object] | None): Optional additional configuration
            options passed directly to the underlying Kafka consumer. These are merged with
            the default configuration. Defaults to None.
        timeout_ms (int): Timeout in milliseconds for Kafka operations (metadata queries,
            message consumption, etc.). Defaults to 10_000 (10 seconds).

    Returns:
        DataFrame: A DataFrame with the following schema:
            - topic (string): The topic the message was read from.
            - partition (int32): The partition ID within the topic.
            - offset (int64): The offset of the message within the partition.
            - timestamp_ms (int64): The timestamp of the message in milliseconds since epoch,
              or null if not available.
            - key (binary): The message key as raw bytes, or null if not present.
            - value (binary): The message value as raw bytes.

    Examples:
        Read from a single topic with default bounds (earliest to latest):
        >>> df = daft.read_kafka("localhost:9092", "my-topic")

        Read from multiple topics:
        >>> df = daft.read_kafka("localhost:9092", ["topic-a", "topic-b"])

        Read from specific partitions:
        >>> df = daft.read_kafka("localhost:9092", "my-topic", partitions=[0, 1])

        Read from a timestamp range:
        >>> from datetime import datetime, timezone
        >>> start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        >>> end_dt = datetime(2024, 1, 2, tzinfo=timezone.utc)
        >>> df = daft.read_kafka("localhost:9092", "my-topic", start=start_dt, end=end_dt)

        Read from specific partition offsets:
        >>> df = daft.read_kafka("localhost:9092", "my-topic", start={0: 100, 1: 200})

        Read from multiple topics with per-topic offsets:
        >>> df = daft.read_kafka(
        ...     "localhost:9092",
        ...     ["topic-a", "topic-b"],
        ...     start={"topic-a": {0: 100}, "topic-b": {0: 50}},
        ... )

        Configure Kafka client options:
        >>> df = daft.read_kafka(
        ...     "localhost:9092",
        ...     "my-topic",
        ...     kafka_client_config={"enable.partition.eof": True, "session.timeout.ms": 30000},
        ... )

    Note:
        This function requires the ``confluent-kafka`` package. Install it with:
        ``pip install confluent-kafka``

        Timestamp bounds use Kafka message timestamps. If your cluster uses CreateTime, producers can publish
        late/out-of-order timestamps; timestamp bounds are not a safe exactly-once checkpoint. Prefer offset-based
        checkpoints (e.g., partition offset maps or committed consumer offsets)
    """
    if isinstance(bootstrap_servers, str):
        bootstrap_servers_str = bootstrap_servers
    else:
        bootstrap_servers_str = ",".join([str(s) for s in bootstrap_servers])

    if isinstance(topics, str):
        topics = [topics]
    else:
        topics = list(topics)

    if not topics:
        raise ValueError("[read_kafka] topics must be non-empty")

    if timeout_ms <= 0:
        raise ValueError("[read_kafka] timeout_ms must be > 0")

    if chunk_size <= 0:
        raise ValueError("[read_kafka] chunk_size must be > 0")

    start_kind, start_timestamp_ms, start_topic_partition_offsets = _normalize_bound(start, topics)
    end_kind, end_timestamp_ms, end_topic_partition_offsets = _normalize_bound(end, topics)

    return KafkaSource(
        bootstrap_servers=bootstrap_servers_str,
        group_id=group_id,
        topics=topics,
        start_kind=start_kind,
        start_timestamp_ms=start_timestamp_ms,
        start_topic_partition_offsets=start_topic_partition_offsets,
        end_kind=end_kind,
        end_timestamp_ms=end_timestamp_ms,
        end_topic_partition_offsets=end_topic_partition_offsets,
        partitions=partitions,
        kafka_client_config=kafka_client_config,
        timeout_ms=timeout_ms,
        chunk_size=chunk_size,
    ).read()
