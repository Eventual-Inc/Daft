from __future__ import annotations

import datetime
import os
import platform
import time
from unittest.mock import MagicMock, patch

import pytest

import daft
from daft import context
from daft.analytics import AnalyticsClient

PUBLISHER_THREAD_SLEEP_INTERVAL_SECONDS = 0.1
MOCK_DATETIME = datetime.datetime(2021, 1, 1, 0, 0, 0)


@pytest.fixture(scope="function")
def mock_analytics() -> tuple[AnalyticsClient, MagicMock]:
    mock_publish = MagicMock()
    client = AnalyticsClient(
        daft.get_version(),
        daft.get_build_type(),
        publish_payload_function=mock_publish,
        buffer_capacity=1,
    )

    # Patch the publish method
    client._post_segment_track_endpoint = MagicMock()

    return client, mock_publish


@patch("daft.analytics.datetime")
def test_analytics_client_track_import(mock_datetime: MagicMock, mock_analytics: tuple[AnalyticsClient, MagicMock]):
    mock_datetime.datetime.utcnow.return_value = MOCK_DATETIME
    analytics_client, mock_publish = mock_analytics

    # Run track_import
    analytics_client.track_import()

    # Sleep to allow publisher thread to poll events
    time.sleep(PUBLISHER_THREAD_SLEEP_INTERVAL_SECONDS + 0.5)

    mock_publish.assert_called_once_with(
        {
            "batch": [
                {
                    "type": "track",
                    "anonymousId": analytics_client._session_key,
                    "event": "Imported Daft",
                    "properties": {
                        "runner": context.get_context().runner_config.name,
                        "platform": platform.platform(),
                        "python_version": platform.python_version(),
                        "DAFT_ANALYTICS_ENABLED": os.getenv("DAFT_ANALYTICS_ENABLED"),
                    },
                    "timestamp": MOCK_DATETIME.isoformat(),
                    "context": {
                        "app": {
                            "name": "getdaft",
                            "version": daft.get_version(),
                            "build": daft.get_build_type(),
                        },
                    },
                }
            ],
        }
    )


@patch("daft.analytics.datetime")
def test_analytics_client_track_dataframe_method(
    mock_datetime: MagicMock, mock_analytics: tuple[AnalyticsClient, MagicMock]
):
    mock_datetime.datetime.utcnow.return_value = MOCK_DATETIME
    analytics_client, mock_publish = mock_analytics

    # Run track_df_method_call
    analytics_client.track_df_method_call(
        "foo",
        4.32,
        "err",
    )

    # Sleep to allow publisher thread to poll events
    time.sleep(PUBLISHER_THREAD_SLEEP_INTERVAL_SECONDS + 0.5)

    mock_publish.assert_called_once_with(
        {
            "batch": [
                {
                    "type": "track",
                    "anonymousId": analytics_client._session_key,
                    "event": "DataFrame Method Call",
                    "properties": {
                        "method_name": "foo",
                        "duration_seconds": 4.32,
                        "error": "err",
                    },
                    "timestamp": MOCK_DATETIME.isoformat(),
                    "context": {
                        "app": {
                            "name": "getdaft",
                            "version": daft.get_version(),
                            "build": daft.get_build_type(),
                        },
                    },
                }
            ],
        }
    )
