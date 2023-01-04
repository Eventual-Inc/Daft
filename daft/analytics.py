from __future__ import annotations

import functools
import os
import platform
import time
import uuid

import segment.analytics as analytics

from daft import context

_WRITE_KEY = "ebFETjqH70OOvtDvrlBC902iljBZGvPU"
_ANALYTICS_CLIENT = None


class AnalyticsClient:
    """Client for sending analytics events, which is a singleton for each Python process"""

    def __init__(self) -> None:
        self._session_key = str(uuid.uuid4())
        self._segment_client = analytics.Client(_WRITE_KEY)

    def track_import(self, daft_version: str) -> None:
        self._segment_client.track(
            self._session_key,
            "Imported Daft",
            {
                "runner": context.get_context().runner_config.name,
                "daft_version": daft_version,
                "platform": platform.platform(),
                "python_version": platform.python_version(),
            },
        )

    def track_df_method_call(self, method_name: str, duration_seconds: float, error: str | None = None) -> None:
        optionals = {}
        if error is not None:
            optionals["error"] = error
        self._segment_client.track(
            self._session_key,
            "DataFrame Method Call",
            {
                "method_name": method_name,
                "duration_seconds": duration_seconds,
                **optionals,
            },
        )


def init_analytics(release_build: bool) -> AnalyticsClient | None:
    """Initialize the analytics module

    Args:
        daft_version (str): version of Daft, defaulting to "0.0.0" which indicates a dev/local build

    Returns:
        AnalyticsClient: initialized singleton AnalyticsClient
    """
    global _ANALYTICS_CLIENT

    user_opted_out = os.getenv("DAFT_ANALYTICS_ENABLED") == "0"
    if not release_build or user_opted_out:
        return None

    if _ANALYTICS_CLIENT is not None:
        return _ANALYTICS_CLIENT

    _ANALYTICS_CLIENT = AnalyticsClient()
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
