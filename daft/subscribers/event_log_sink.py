from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    import ray
except ImportError:
    raise

if TYPE_CHECKING:
    from io import TextIOWrapper

logger = logging.getLogger(__name__)

SINK_ACTOR_NAME_PREFIX = "daft-event-log-sink"
SINK_ACTOR_NAMESPACE = "daft"


def _json_default(obj: object) -> object:
    if isinstance(obj, timedelta):
        return round(obj.total_seconds() * 1000)
    return str(obj)


@ray.remote(num_cpus=0)
class EventLogSink:
    """Ray actor that writes received event records to per-query JSONL files.

    One file per query: ``<log_dir>/<query_id>/events.jsonl``. Records from
    multiple processes interleave but carry role/hostname/pid so consumers can
    demultiplex.
    """

    def __init__(self, log_dir: str) -> None:
        self._log_dir = Path(log_dir).expanduser().resolve()
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._files: dict[str, TextIOWrapper] = {}

    def append(self, record: dict[str, Any]) -> None:
        query_id = record.get("query_id")
        if not query_id:
            return
        f = self._get_file(query_id)
        if f is None:
            return
        try:
            f.write(json.dumps(record, default=_json_default) + "\n")
        except OSError:
            pass

    def _get_file(self, query_id: str) -> TextIOWrapper | None:
        existing = self._files.get(query_id)
        if existing is not None:
            return existing
        query_dir = self._log_dir / query_id
        try:
            query_dir.mkdir(parents=True, exist_ok=True)
            f = open(query_dir / "events.jsonl", "a", buffering=1)
        except OSError:
            return None
        self._files[query_id] = f
        return f

    def shutdown(self) -> None:
        for f in self._files.values():
            try:
                f.close()
            except OSError:
                pass
        self._files.clear()


def get_sink_actor_name(job_id: str | None) -> str:
    return f"{SINK_ACTOR_NAME_PREFIX}-{job_id or 'default'}"


def create_or_get_sink(
    log_dir: str,
    job_id: str | None,
    head_node_id: str | None,
) -> Any:
    name = get_sink_actor_name(job_id)
    options: dict[str, Any] = {
        "name": name,
        "namespace": SINK_ACTOR_NAMESPACE,
        "get_if_exists": True,
        "lifetime": "detached",
    }
    if head_node_id:
        options["scheduling_strategy"] = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
            node_id=head_node_id,
            soft=False,
        )
    return EventLogSink.options(**options).remote(log_dir)  # type: ignore[attr-defined]


def get_sink(job_id: str | None) -> Any | None:
    name = get_sink_actor_name(job_id)
    try:
        return ray.get_actor(name, namespace=SINK_ACTOR_NAMESPACE)
    except ValueError:
        return None


def teardown_sink(job_id: str | None, timeout_sec: float = 5.0) -> None:
    """Flush and kill the sink actor for the given job. Idempotent.

    Safe to register via ``atexit``. Swallows errors because teardown is
    best-effort; Ray cluster teardown may race with this.
    """
    sink = get_sink(job_id)
    if sink is None:
        return
    try:
        ray.get(sink.shutdown.remote(), timeout=timeout_sec)
    except Exception:
        pass
    try:
        ray.kill(sink)
    except Exception:
        pass


__all__ = [
    "SINK_ACTOR_NAMESPACE",
    "EventLogSink",
    "create_or_get_sink",
    "get_sink",
    "get_sink_actor_name",
    "teardown_sink",
]
