from __future__ import annotations

import base64
import datetime
import functools
import json
import logging
import os
import pathlib
import platform
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any, Callable

from daft import context

logger = logging.getLogger(__name__)

_ANALYTICS_CLIENT = None
_WRITE_KEY = "ebFETjqH70OOvtDvrlBC902iljBZGvPU"
_PUBLISHER_THREAD_DEFAULT_SLEEP_INTERVAL_SECONDS = 5.0
_LIMIT_READLINES_BYTES = 250 * 1024
_SEGMENT_BATCH_ENDPOINT = "https://api.segment.io/v1/batch"


def _build_segment_batch_payload(data: list[dict[str, Any]], daft_version: str, daft_build_type: str) -> dict[str, Any]:
    return {
        "batch": [
            {
                "type": "track",
                "anonymousId": d["session_id"],
                "event": d["event_name"],
                "properties": d["data"],
                "timestamp": d["event_time"],
                "context": {
                    "app": {
                        "name": "getdaft",
                        "version": daft_version,
                        "build": daft_build_type,
                    },
                },
            }
            for d in data
        ],
    }


def _post_segment_track_endpoint(payload: dict[str, Any]) -> None:
    """Posts a batch of JSON data to Segment"""
    req = urllib.request.Request(
        _SEGMENT_BATCH_ENDPOINT,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "User-Agent": "daft-analytics",
            "Authorization": f"Basic {base64.b64encode(f'{_WRITE_KEY}:'.encode()).decode('utf-8')}",
        },
        data=json.dumps(payload).encode("utf-8"),
    )
    resp = urllib.request.urlopen(req)
    if resp.status != 200:
        raise RuntimeError(f"HTTP request to segment returned status code: {resp.status}")


def _publisher_thread_target(
    watch_file: pathlib.Path,
    daft_version: str,
    daft_build_type: str,
    sleep_interval_seconds: float,
    publish: Callable[[dict[str, Any]], None],
):
    """Thread target for the publisher thread, which reads new JSON lines from the specified file and sends analytics to Segment"""
    logger.debug(f"Watching file for analytics: {watch_file}")
    with open(watch_file, "rb") as f:
        while True:
            try:
                lines = f.readlines(_LIMIT_READLINES_BYTES)
                if lines:
                    data = [json.loads(l) for l in lines]
                    publish(_build_segment_batch_payload(data, daft_version, daft_build_type))
                time.sleep(sleep_interval_seconds)
            except Exception as e:
                # No-op on failure to avoid crashing the publisher thread - TODO: add retries for more robust log
                logger.debug(f"Error in analytics publisher thread: {e}")


class AnalyticsClient:
    """Client for sending analytics events, which is a singleton for each Python process"""

    def __init__(
        self,
        daft_version: str,
        daft_build_type: str,
        publisher_thread_sleep_interval_seconds: float = _PUBLISHER_THREAD_DEFAULT_SLEEP_INTERVAL_SECONDS,
        publish_payload_function: Callable[[dict[str, Any]], None] = _post_segment_track_endpoint,
    ) -> None:
        self._session_key = str(uuid.uuid4())
        self._append_logfile = open(self._get_session_analytics_file(), "a")
        self._publisher_thread = threading.Thread(
            target=_publisher_thread_target,
            # daemon=True makes this thread non-blocking to program exit
            daemon=True,
            args=(
                self._get_session_analytics_file(),
                daft_version,
                daft_build_type,
                publisher_thread_sleep_interval_seconds,
                publish_payload_function,
            ),
        )
        self._publisher_thread.start()

    def _get_session_analytics_file(self) -> pathlib.Path:
        session_path = pathlib.Path(tempfile.gettempdir()) / "daft" / self._session_key
        session_path.mkdir(parents=True, exist_ok=True)
        log_path = session_path / "analytics.log"
        if not log_path.exists():
            log_path.touch()
        return log_path

    def _append_to_log(self, event_name: str, data: dict[str, Any]) -> None:
        current_time = datetime.datetime.utcnow().isoformat()
        self._append_logfile.write(
            json.dumps(
                {"session_id": self._session_key, "event_name": event_name, "event_time": current_time, "data": data}
            )
        )
        self._append_logfile.write("\n")
        self._append_logfile.flush()

    def track_import(self) -> None:
        self._append_to_log(
            "Imported Daft",
            {
                "runner": context.get_context().runner_config.name,
                "platform": platform.platform(),
                "python_version": platform.python_version(),
                "DAFT_ANALYTICS_ENABLED": os.getenv("DAFT_ANALYTICS_ENABLED"),
            },
        )

    def track_df_method_call(self, method_name: str, duration_seconds: float, error: str | None = None) -> None:
        optionals = {}
        if error is not None:
            optionals["error"] = error
        self._append_to_log(
            "DataFrame Method Call",
            {
                "method_name": method_name,
                "duration_seconds": duration_seconds,
                **optionals,
            },
        )


def init_analytics(daft_version: str, daft_build_type: str) -> AnalyticsClient:
    """Initialize the analytics module

    Returns:
        AnalyticsClient: initialized singleton AnalyticsClient
    """
    global _ANALYTICS_CLIENT

    if _ANALYTICS_CLIENT is not None:
        return _ANALYTICS_CLIENT

    _ANALYTICS_CLIENT = AnalyticsClient(daft_version, daft_build_type)
    return _ANALYTICS_CLIENT


def time_df_method(method):
    """Decorator to track metrics about Dataframe method calls"""

    @functools.wraps(method)
    def tracked_method(*args, **kwargs):

        if _ANALYTICS_CLIENT is None:
            return method(*args, **kwargs)

        start = time.time()
        try:
            result = method(*args, **kwargs)
        except Exception as e:
            _ANALYTICS_CLIENT.track_df_method_call(
                method_name=method.__name__, duration_seconds=time.time() - start, error=str(type(e).__name__)
            )
            raise

        _ANALYTICS_CLIENT.track_df_method_call(
            method_name=method.__name__,
            duration_seconds=time.time() - start,
        )
        return result

    return tracked_method
