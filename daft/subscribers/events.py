from __future__ import annotations

import atexit
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

from daft.context import get_context
from daft.daft import PyMicroPartition, PyQueryMetadata, PyQueryResult, QueryEndState, StatType
from daft.subscribers.abc import Subscriber

_EVENT_LOG_ALIAS = "_daft_event_log"
_DEFAULT_EVENT_LOG_DIR = Path("~/.daft/events").expanduser()
_EVENT_LOG_ATEXIT_REGISTERED = False


def _json_default(obj: object) -> object:
    """JSON serializer for types not handled by the default encoder."""
    if isinstance(obj, timedelta):
        return round(obj.total_seconds() * 1000)
    return str(obj)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _mono_ms() -> float:
    """Monotonic clock in milliseconds for duration measurement."""
    return time.monotonic() * 1000


def _generate_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:4]
    return f"run_{timestamp}_{suffix}"


class EventLogSubscriber(Subscriber):
    """Experimental subscriber that writes query lifecycle events to a JSONL log.

    Events follow the schema documented in the diagnostics plan:
    one JSON object per line with ``event``, ``ts``, and event-specific fields.
    """

    def __init__(self, log_dir: str | Path, run_id: str | None = None) -> None:
        self._log_dir = Path(log_dir).expanduser().resolve()
        self._run_id = run_id or _generate_run_id()
        self._run_dir = self._log_dir / self._run_id
        self._events_path = self._run_dir / "events.jsonl"
        self._run_dir.mkdir(parents=True, exist_ok=True)
        self._file = open(self._events_path, "a", buffering=1)  # line-buffered
        self._closed = False

        # Track start times for duration computation (monotonic ms).
        # TODO update the framework to pass this information
        self._query_starts: dict[str, float] = {}
        self._optimization_starts: dict[str, float] = {}
        self._exec_starts: dict[str, float] = {}
        self._operator_starts: dict[tuple[str, int], float] = {}

        self._write_session_header()

    def _clear_query_state(self, query_id: str) -> None:
        """Remove any leftover timing state for the given query."""
        self._optimization_starts.pop(query_id, None)
        self._exec_starts.pop(query_id, None)

        stale_operator_keys = [key for key in self._operator_starts if key[0] == query_id]
        for key in stale_operator_keys:
            self._operator_starts.pop(key, None)

    def _write_session_header(self) -> None:
        from daft import __version__ as daft_version

        self._write_event(
            "session_started",
            {
                "daft_version": daft_version,
            },
        )

    def _write_event(self, event_name: str, payload: dict[str, Any]) -> None:
        if self._closed:
            return
        record: dict[str, Any] = {"event": event_name, "ts": _iso_now()}
        record.update(payload)
        try:
            self._file.write(json.dumps(record, default=_json_default) + "\n")
        except OSError:
            pass  # Don't let logging failures affect query execution

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._file.close()

    # Query lifecycle

    def on_query_start(self, query_id: str, metadata: PyQueryMetadata) -> None:
        self._query_starts[query_id] = _mono_ms()
        self._write_event("query_started", {"query_id": query_id})
        self._write_event("plan_unoptimized", {"query_id": query_id, "plan": metadata.unoptimized_plan})

    def on_query_end(self, query_id: str, result: PyQueryResult) -> None:
        start = self._query_starts.pop(query_id, None)
        duration_ms = round(_mono_ms() - start) if start is not None else None

        payload: dict[str, Any] = {"query_id": query_id}
        if duration_ms is not None:
            payload["duration_ms"] = duration_ms

        if result.end_state == QueryEndState.Finished:
            payload["status"] = "ok"
        elif result.end_state == QueryEndState.Failed:
            payload["status"] = "failed"
            if result.error_message:
                payload["error_message"] = result.error_message
        elif result.end_state == QueryEndState.Canceled:
            payload["status"] = "canceled"
        else:
            payload["status"] = "dead"

        self._write_event("query_ended", payload)
        self._clear_query_state(query_id)

    # Result

    def on_result_out(self, query_id: str, result: PyMicroPartition) -> None:
        self._write_event(
            "result_out",
            {
                "query_id": query_id,
                "rows": len(result),
            },
        )

    # Optimization

    def on_optimization_start(self, query_id: str) -> None:
        self._optimization_starts[query_id] = _mono_ms()
        self._write_event("optimization_started", {"query_id": query_id})

    def on_optimization_end(self, query_id: str, optimized_plan: str) -> None:
        start = self._optimization_starts.pop(query_id, None)
        duration_ms = round(_mono_ms() - start) if start is not None else None

        payload: dict[str, Any] = {"query_id": query_id}
        if duration_ms is not None:
            payload["duration_ms"] = duration_ms

        self._write_event("optimization_ended", payload)
        self._write_event("plan_optimized", {"query_id": query_id, "plan": optimized_plan})

    # Execution

    def on_exec_start(self, query_id: str, physical_plan: str) -> None:
        self._exec_starts[query_id] = _mono_ms()
        self._write_event("execution_started", {"query_id": query_id})
        self._write_event("plan_physical", {"query_id": query_id, "plan": physical_plan})

    def on_exec_operator_start(self, query_id: str, node_id: int) -> None:
        self._operator_starts[(query_id, node_id)] = _mono_ms()
        self._write_event(
            "operator_started",
            {
                "query_id": query_id,
                "node_id": node_id,
            },
        )

    def on_exec_emit_stats(self, query_id: str, stats: Mapping[int, Mapping[str, tuple[StatType, Any]]]) -> None:
        for node_id, node_stats in stats.items():
            metrics: dict[str, Any] = {}
            for name, (_stat_type, value) in node_stats.items():
                metrics[name] = value
            self._write_event(
                "stats",
                {
                    "query_id": query_id,
                    "node_id": node_id,
                    "metrics": metrics,
                },
            )

    def on_exec_operator_end(self, query_id: str, node_id: int) -> None:
        start = self._operator_starts.pop((query_id, node_id), None)
        duration_ms = round(_mono_ms() - start) if start is not None else None

        payload: dict[str, Any] = {
            "query_id": query_id,
            "node_id": node_id,
        }
        if duration_ms is not None:
            payload["duration_ms"] = duration_ms

        self._write_event("operator_ended", payload)

    def on_exec_end(self, query_id: str) -> None:
        start = self._exec_starts.pop(query_id, None)
        duration_ms = round(_mono_ms() - start) if start is not None else None

        payload: dict[str, Any] = {"query_id": query_id}
        if duration_ms is not None:
            payload["duration_ms"] = duration_ms

        self._write_event("execution_ended", payload)


_EVENT_LOG_SUBSCRIBER: EventLogSubscriber | None = None


def enable_event_log(dir: str | Path | None = None) -> None:
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

    subscriber = EventLogSubscriber(dir or _DEFAULT_EVENT_LOG_DIR)
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
