from __future__ import annotations

import atexit
import json
import os
import socket
import time
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from io import TextIOWrapper

    from daft.subscribers.events import (
        ExecutionFinished,
        ExecutionStarted,
        OperatorFinished,
        OperatorStarted,
        OptimizationCompleted,
        OptimizationStarted,
        ProcessStats,
        QueryFinished,
        QueryHeartbeat,
        QueryStarted,
        ResultProduced,
        Stats,
    )

from typing import TYPE_CHECKING

from daft.context import get_context
from daft.subscribers.abc import Subscriber

if TYPE_CHECKING:
    from daft.daft import QueryEndState

_EVENT_LOG_ALIAS = "_daft_event_log"
_DEFAULT_EVENT_LOG_DIR = Path("~/.daft/events").expanduser()
_EVENT_LOG_ATEXIT_REGISTERED = False


def _json_default(obj: object) -> object:
    """JSON serializer for types not handled by the default encoder."""
    if isinstance(obj, timedelta):
        return round(obj.total_seconds() * 1000)
    return str(obj)


def _epoch_now() -> float:
    return time.time()


def _mono_ms() -> float:
    """Monotonic clock in milliseconds for duration measurement."""
    return time.monotonic() * 1000


class EventLogSubscriber(Subscriber):
    """Experimental subscriber that writes query lifecycle events to a JSONL log.

    Events follow the schema documented in the diagnostics plan:
    one JSON object per line with ``event``, ``ts``, and event-specific fields.
    """

    def __init__(
        self,
        log_dir: str | Path,
        component: str = "driver",
        node_role: str = "head",
    ) -> None:
        self._log_dir = Path(log_dir).expanduser().resolve()
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._closed = False
        self._query_files: dict[str, TextIOWrapper] = {}

        # Track start times locally for events that do not carry durations.
        self._query_starts: dict[str, float] = {}
        self._optimization_starts: dict[str, float] = {}
        self._exec_starts: dict[str, float] = {}
        self._operator_starts: dict[tuple[str, int], float] = {}

        # Per-process identity tags. Attached to every record so consumers can
        # demultiplex interleaved output from driver + workers.
        self._component = component
        self._node_role = node_role
        self._hostname = socket.gethostname()
        self._pid = os.getpid()

    def _clear_query_state(self, query_id: str) -> None:
        """Remove any leftover timing state for the given query."""
        self._query_starts.pop(query_id, None)
        self._optimization_starts.pop(query_id, None)
        self._exec_starts.pop(query_id, None)
        stale_operator_keys = [key for key in self._operator_starts if key[0] == query_id]
        for key in stale_operator_keys:
            self._operator_starts.pop(key, None)

    def _query_dir(self, query_id: str) -> Path:
        return self._log_dir / query_id

    def _events_path(self, query_id: str) -> Path:
        return self._query_dir(query_id) / "events.jsonl"

    def _get_query_file(self, query_id: str) -> TextIOWrapper | None:
        if self._closed:
            return None
        query_file = self._query_files.get(query_id)
        if query_file is not None:
            return query_file

        query_dir = self._query_dir(query_id)
        query_dir.mkdir(parents=True, exist_ok=True)
        try:
            query_file = open(self._events_path(query_id), "a", buffering=1)  # line-buffered
        except OSError:
            return None

        self._query_files[query_id] = query_file
        self._write_session_header(query_id)
        return query_file

    def _close_query_file(self, query_id: str) -> None:
        query_file = self._query_files.pop(query_id, None)
        if query_file is None:
            return
        query_file.close()

    def _write_session_header(self, query_id: str) -> None:
        from daft import __version__ as daft_version

        self._write_event(
            query_id,
            "event_log_started",
            {
                "daft_version": daft_version,
            },
        )

    def _write_event(self, query_id: str, event_name: str, payload: dict[str, Any]) -> None:
        record: dict[str, Any] = {
            "event": event_name,
            "timestamp": _epoch_now(),
            "query_id": query_id,
            "component": self._component,
            "node_role": self._node_role,
            "hostname": self._hostname,
            "pid": self._pid,
        }
        record.update(payload)
        self._emit_record(query_id, record)

    def _emit_record(self, query_id: str, record: dict[str, Any]) -> None:
        """Write a record to its sink. Default: per-query JSONL file. Subclasses override."""
        query_file = self._get_query_file(query_id)
        if query_file is None:
            return
        try:
            query_file.write(json.dumps(record, default=_json_default) + "\n")
        except OSError:
            pass  # Don't let logging failures affect query execution

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for query_file in self._query_files.values():
            try:
                query_file.close()
            except OSError:
                pass
        self._query_files.clear()

    # Query lifecycle

    def on_query_started(self, event: QueryStarted) -> None:
        self._query_starts[event.query_id] = _mono_ms()
        payload = {
            "plan": event.metadata.unoptimized_plan,
            "runner": event.metadata.runner,
            "entrypoint": event.metadata.entrypoint,
            "daft_version": event.metadata.daft_version,
            "python_version": event.metadata.python_version,
        }
        if event.metadata.ray_version is not None:
            payload["runner_version"] = event.metadata.ray_version

        self._write_event(
            event.query_id,
            "query_started",
            payload,
        )

    def on_query_heartbeat(self, event: QueryHeartbeat) -> None:
        """Don't log out heartbeats, too verbose."""
        pass

    def on_query_finished(self, event: QueryFinished) -> None:
        duration_ms = event.duration_ms
        if duration_ms is None:
            start = self._query_starts.pop(event.query_id, None)
            duration_ms = _mono_ms() - start if start is not None else None

        payload: dict[str, Any] = {}
        if duration_ms is not None:
            payload["duration_ms"] = round(duration_ms)
        payload["state"] = query_state_str(event.result.end_state)

        self._write_event(event.query_id, "query_ended", payload)
        self._clear_query_state(event.query_id)
        self._close_query_file(event.query_id)

    # Result

    def on_result_produced(self, event: ResultProduced) -> None:
        self._write_event(
            event.query_id,
            "result_produced",
            {
                "rows": event.num_rows,
            },
        )

    # Optimization

    def on_optimization_started(self, event: OptimizationStarted) -> None:
        self._optimization_starts[event.query_id] = _mono_ms()
        self._write_event(event.query_id, "optimization_started", {})

    def on_optimization_completed(self, event: OptimizationCompleted) -> None:
        start = self._optimization_starts.pop(event.query_id, None)
        duration_ms = round(_mono_ms() - start) if start is not None else None

        payload: dict[str, Any] = {"plan": event.optimized_plan}
        if duration_ms is not None:
            payload["duration_ms"] = duration_ms

        self._write_event(event.query_id, "optimization_ended", payload)

    # Execution

    def on_execution_started(self, event: ExecutionStarted) -> None:
        self._exec_starts[event.query_id] = _mono_ms()
        self._write_event(event.query_id, "execution_started", {"physical_plan": event.physical_plan})

    def on_operator_start(self, event: OperatorStarted) -> None:
        query_id = event.query_id
        node_id = event.node_id
        self._operator_starts[(query_id, node_id)] = _mono_ms()
        self._write_event(
            query_id,
            "operator_started",
            {
                "node_id": node_id,
                "name": event.name,
            },
        )

    def on_stats(self, event: Stats) -> None:
        for node_id, node_stats in event.stats.items():
            metrics: dict[str, Any] = {}
            for name, (_stat_type, value) in node_stats.items():
                metrics[name] = value
            self._write_event(
                event.query_id,
                "stats",
                {
                    "node_id": node_id,
                    "metrics": metrics,
                },
            )

    def on_process_stats(self, event: ProcessStats) -> None:
        metrics: dict[str, Any] = {}
        for name, (_stat_type, value) in event.stats.items():
            metrics[name] = value
        self._write_event(event.query_id, "process_stats", {"metrics": metrics})

    def on_operator_end(self, event: OperatorFinished) -> None:
        query_id = event.query_id
        node_id = event.node_id
        start = self._operator_starts.pop((query_id, node_id), None)
        duration_ms = round(_mono_ms() - start) if start is not None else None

        payload: dict[str, Any] = {"node_id": node_id, "name": event.name}
        if duration_ms is not None:
            payload["duration_ms"] = duration_ms

        self._write_event(query_id, "operator_ended", payload)

    def on_execution_finished(self, event: ExecutionFinished) -> None:
        duration_ms = event.duration_ms
        if duration_ms is None:
            start = self._exec_starts.pop(event.query_id, None)
            duration_ms = _mono_ms() - start if start is not None else None

        payload: dict[str, Any] = {}
        if duration_ms is not None:
            payload["duration_ms"] = round(duration_ms)

        self._write_event(event.query_id, "execution_ended", payload)


class RemoteEventLogSubscriber(EventLogSubscriber):
    """Event log subscriber that ships records to a centralized Ray actor sink.

    instead of writing to a local file.

    Used on flotilla workers (and optionally the driver) to aggregate events
    from every process into a single per-query JSONL file on the head node.
    """

    def __init__(self, sink_actor: Any, component: str, node_role: str) -> None:
        # Intentionally bypass parent __init__ — this subscriber never touches
        # the local filesystem, so there is no log_dir to create.
        self._log_dir = None  # type: ignore[assignment]
        self._closed = False
        self._query_files = {}
        self._query_starts = {}
        self._optimization_starts = {}
        self._exec_starts = {}
        self._operator_starts = {}
        self._component = component
        self._node_role = node_role
        self._hostname = socket.gethostname()
        self._pid = os.getpid()
        self._sink = sink_actor

    def _emit_record(self, query_id: str, record: dict[str, Any]) -> None:
        if self._closed:
            return
        try:
            self._sink.append.remote(record)
        except Exception:
            pass  # sink unreachable; drop event rather than failing the query

    def close(self) -> None:
        self._closed = True


def query_state_str(state: QueryEndState) -> str:
    return state.__str__().removeprefix("QueryEndState.")


_EVENT_LOG_SUBSCRIBER: EventLogSubscriber | None = None


def enable_event_log(log_dir: str | Path | None = None) -> None:
    """Experimental helper that attaches an event-log subscriber.

    This API is currently intended for local event-log capture through
    `enable_event_log()` / `disable_event_log()`.
    """
    global _EVENT_LOG_ATEXIT_REGISTERED, _EVENT_LOG_SUBSCRIBER
    if _EVENT_LOG_SUBSCRIBER is not None:
        disable_event_log()
    if not _EVENT_LOG_ATEXIT_REGISTERED:
        atexit.register(disable_event_log)
        _EVENT_LOG_ATEXIT_REGISTERED = True

    subscriber = EventLogSubscriber(log_dir or _DEFAULT_EVENT_LOG_DIR)
    try:
        get_context().attach_subscriber(_EVENT_LOG_ALIAS, subscriber)
    except Exception:
        subscriber.close()
        raise
    _EVENT_LOG_SUBSCRIBER = subscriber


def disable_event_log() -> None:
    """Detach the global EventLogSubscriber from the Daft context."""
    global _EVENT_LOG_SUBSCRIBER
    subscriber = _EVENT_LOG_SUBSCRIBER
    if subscriber is None:
        return
    _EVENT_LOG_SUBSCRIBER = None
    try:
        get_context().detach_subscriber(_EVENT_LOG_ALIAS)
    except Exception:
        pass
    subscriber.close()


__all__ = ["EventLogSubscriber", "disable_event_log", "enable_event_log"]
