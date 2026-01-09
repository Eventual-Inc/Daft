from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import cast

from daft.api_annotations import PublicAPI
from daft.daft import ScanOperatorHandle
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder


def _to_timestamp_ms(value: object) -> int:
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
        raise TypeError("Expected timestamp as int(ms), datetime, or ISO-8601 string")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.timestamp() * 1000)


def _normalize_bound(
    bound: object,
    *,
    allow_none: bool,
) -> tuple[str, int | None, dict[int, int] | dict[str, dict[int, int]] | None]:
    if bound is None:
        if allow_none:
            return ("none", None, None)
        raise ValueError("bound cannot be None for bounded read")

    if isinstance(bound, str):
        lowered = bound.lower()
        if lowered in {"earliest", "latest"}:
            return (lowered, None, None)
        return ("timestamp_ms", _to_timestamp_ms(bound), None)

    if isinstance(bound, datetime):
        return ("timestamp_ms", _to_timestamp_ms(bound), None)

    if isinstance(bound, int):
        return ("timestamp_ms", bound, None)

    if isinstance(bound, Mapping):
        items = list(bound.items())
        if not items:
            raise ValueError("partition offset map must be non-empty")

        if all(isinstance(k, str) for k, _ in items):
            offsets_by_topic: dict[str, dict[int, int]] = {}
            for topic, per_topic in items:
                if not isinstance(per_topic, Mapping):
                    raise TypeError("topic offset map values must be dicts of partition->offset")
                offsets_for_topic: dict[int, int] = {}
                for pk, pv in per_topic.items():
                    offsets_for_topic[int(pk)] = int(pv)
                if not offsets_for_topic:
                    raise ValueError("partition offset map must be non-empty")
                offsets_by_topic[str(topic)] = offsets_for_topic
            return ("topic_partition_offsets", None, offsets_by_topic)

        if all(not isinstance(k, str) for k, _ in items):
            offsets_by_partition: dict[int, int] = {}
            for k, v in items:
                offsets_by_partition[int(k)] = int(v)
            return ("partition_offsets", None, offsets_by_partition)

        raise TypeError("partition offset maps must be either {partition: offset} or {topic: {partition: offset}}")

    raise TypeError(
        "start/end must be one of: 'earliest'/'latest', int(timestamp_ms), datetime/ISO string, or a dict of partition->offset"
    )


def _normalize_topic_partition_offsets(
    kind: str,
    offsets_raw: dict[int, int] | dict[str, dict[int, int]] | None,
    topics: list[str],
) -> tuple[str, dict[str, dict[int, int]] | None]:
    if kind == "partition_offsets":
        if len(topics) != 1:
            raise ValueError(
                "{partition: offset} start/end bounds require a single topic; use {topic: {partition: offset}} for multiple topics"
            )
        return ("topic_partition_offsets", {topics[0]: cast("dict[int, int]", offsets_raw)})
    if kind == "topic_partition_offsets":
        return ("topic_partition_offsets", cast("dict[str, dict[int, int]]", offsets_raw))
    return (kind, None)


def _validate_offsets_cover_topics(
    offsets_by_topic: dict[str, dict[int, int]] | None,
    topics: list[str],
) -> None:
    if offsets_by_topic is None:
        return
    provided = set(offsets_by_topic.keys())
    expected = set(topics)
    extra = provided - expected
    missing = expected - provided
    if extra or missing:
        raise ValueError(
            "offsets must be provided for exactly the topics being read; "
            f"extra={sorted(extra)}, missing={sorted(missing)}"
        )


@PublicAPI
def read_kafka(
    bootstrap_servers: str | Sequence[str],
    topics: str | Sequence[str],
    *,
    start: object = "earliest",
    end: object = "latest",
    group_id: str = "daft-bounded-kafka-reader",
    partitions: Sequence[int] | None = None,
    kafka_client_config: Mapping[str, object] | None = None,
    timeout_ms: int = 10_000,
) -> DataFrame:
    if isinstance(bootstrap_servers, str):
        bootstrap_servers_str = bootstrap_servers
    else:
        bootstrap_servers_str = ",".join([str(s) for s in bootstrap_servers])

    if isinstance(topics, str):
        topics = [topics]
    else:
        topics = list(topics)

    if not topics:
        raise ValueError("topics must be non-empty")

    if timeout_ms <= 0:
        raise ValueError("timeout_ms must be > 0")

    partitions_i32: list[int] | None
    if partitions is None:
        partitions_i32 = None
    else:
        partitions_i32 = [int(p) for p in partitions]

    kafka_client_config_str: dict[str, str] | None
    if kafka_client_config is None:
        kafka_client_config_str = None
    else:
        kafka_client_config_str = {str(k): str(v) for k, v in kafka_client_config.items()}

    start_kind, start_timestamp_ms, start_offsets_raw = _normalize_bound(start, allow_none=False)
    end_kind, end_timestamp_ms, end_offsets_raw = _normalize_bound(end, allow_none=False)

    start_kind, start_topic_partition_offsets = _normalize_topic_partition_offsets(
        start_kind, start_offsets_raw, topics
    )
    end_kind, end_topic_partition_offsets = _normalize_topic_partition_offsets(end_kind, end_offsets_raw, topics)
    _validate_offsets_cover_topics(start_topic_partition_offsets, topics)
    _validate_offsets_cover_topics(end_topic_partition_offsets, topics)

    handle = ScanOperatorHandle.kafka_scan_bounded(
        bootstrap_servers=bootstrap_servers_str,
        group_id=group_id,
        topics=topics,
        start_kind=start_kind,
        start_timestamp_ms=start_timestamp_ms,
        start_topic_partition_offsets=start_topic_partition_offsets,
        end_kind=end_kind,
        end_timestamp_ms=end_timestamp_ms,
        end_topic_partition_offsets=end_topic_partition_offsets,
        partitions=partitions_i32,
        kafka_client_config=kafka_client_config_str,
        timeout_ms=timeout_ms,
    )

    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
