from __future__ import annotations

import atexit
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping
    from io import TextIOWrapper

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


class EventLogSubscriber(Subscriber):
    """Experimental subscriber that writes query lifecycle events to a JSONL log.

    Events follow the schema documented in the diagnostics plan:
    one JSON object per line with ``event``, ``ts``, and event-specific fields.
    """

    def __init__(self, log_dir: str | Path) -> None:
        self._log_dir = Path(log_dir).expanduser().resolve()
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._closed = False
        self._query_files: dict[str, TextIOWrapper] = {}

        # Track start times for duration computation (monotonic ms).
        # TODO update the framework to pass this information
        self._query_starts: dict[str, float] = {}
        self._optimization_starts: dict[str, float] = {}
        self._exec_starts: dict[str, float] = {}
        self._operator_starts: dict[tuple[str, int], float] = {}

    def _clear_query_state(self, query_id: str) -> None:
        """Remove any leftover timing state for the given query."""
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
        query_file = self._get_query_file(query_id)
        if query_file is None:
            return
        record: dict[str, Any] = {"event": event_name, "ts": _iso_now(), "query_id": query_id}
        record.update(payload)
        try:
            query_file.write(json.dumps(record, default=_json_default) + "\n")
        except OSError:
            pass  # Don't let logging failures affect query execution

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for query_file in self._query_files.values():
            query_file.close()
        self._query_files.clear()

    # Query lifecycle

    def on_query_start(self, query_id: str, metadata: PyQueryMetadata) -> None:
        self._query_starts[query_id] = _mono_ms()
        self._write_event(query_id, "query_started", {})
        self._write_event(query_id, "plan_unoptimized", {"plan": metadata.unoptimized_plan})

    def on_query_end(self, query_id: str, result: PyQueryResult) -> None:
        start = self._query_starts.pop(query_id, None)
        duration_ms = round(_mono_ms() - start) if start is not None else None

        payload: dict[str, Any] = {}
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

        self._write_event(query_id, "query_ended", payload)
        self._clear_query_state(query_id)
        self._close_query_file(query_id)

    # Result

    def on_result_out(self, query_id: str, result: PyMicroPartition) -> None:
        self._write_event(
            query_id,
            "result_out",
            {
                "rows": len(result),
            },
        )

    # Optimization

    def on_optimization_start(self, query_id: str) -> None:
        self._optimization_starts[query_id] = _mono_ms()
        self._write_event(query_id, "optimization_started", {})

    def on_optimization_end(self, query_id: str, optimized_plan: str) -> None:
        start = self._optimization_starts.pop(query_id, None)
        duration_ms = round(_mono_ms() - start) if start is not None else None

        payload: dict[str, Any] = {}
        if duration_ms is not None:
            payload["duration_ms"] = duration_ms

        self._write_event(query_id, "optimization_ended", payload)
        self._write_event(query_id, "plan_optimized", {"plan": optimized_plan})

    # Execution

    def on_exec_start(self, query_id: str, physical_plan: str) -> None:
        self._exec_starts[query_id] = _mono_ms()
        self._write_event(query_id, "execution_started", {})
        self._write_event(query_id, "plan_physical", {"plan": physical_plan})

    def on_exec_operator_start(self, query_id: str, node_id: int) -> None:
        self._operator_starts[(query_id, node_id)] = _mono_ms()
        self._write_event(
            query_id,
            "operator_started",
            {
                "node_id": node_id,
            },
        )

    def on_exec_emit_stats(self, query_id: str, stats: Mapping[int, Mapping[str, tuple[StatType, Any]]]) -> None:
        for node_id, node_stats in stats.items():
            metrics: dict[str, Any] = {}
            for name, (_stat_type, value) in node_stats.items():
                metrics[name] = value
            self._write_event(
                query_id,
                "stats",
                {
                    "node_id": node_id,
                    "metrics": metrics,
                },
            )

    def on_exec_operator_end(self, query_id: str, node_id: int) -> None:
        start = self._operator_starts.pop((query_id, node_id), None)
        duration_ms = round(_mono_ms() - start) if start is not None else None

        payload: dict[str, Any] = {
            "node_id": node_id,
        }
        if duration_ms is not None:
            payload["duration_ms"] = duration_ms

        self._write_event(query_id, "operator_ended", payload)

    def on_exec_end(self, query_id: str) -> None:
        start = self._exec_starts.pop(query_id, None)
        duration_ms = round(_mono_ms() - start) if start is not None else None

        payload: dict[str, Any] = {}
        if duration_ms is not None:
            payload["duration_ms"] = duration_ms

        self._write_event(query_id, "execution_ended", payload)


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
