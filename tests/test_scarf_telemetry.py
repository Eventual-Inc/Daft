from __future__ import annotations

import os
import socket
import urllib
from unittest.mock import MagicMock, patch

from daft.scarf_telemetry import scarf_analytics

PUBLISHER_THREAD_SLEEP_INTERVAL_SECONDS = 0.1


@patch("urllib.request.urlopen")
def test_scarf_analytics_no_request_when_disabled(mock_urlopen: MagicMock):
    # Test when SCARF_NO_ANALYTICS is set to "true"
    os.environ["SCARF_NO_ANALYTICS"] = "true"
    response_status, runner_type = scarf_analytics()
    assert response_status is None
    assert runner_type is None

    # Test when DO_NOT_TRACK is set to "true"
    os.environ["SCARF_NO_ANALYTICS"] = "false"  # Reset to false
    os.environ["DO_NOT_TRACK"] = "true"
    response_status, runner_type = scarf_analytics()
    assert response_status is None
    assert runner_type is None


@patch("urllib.request.urlopen")
def test_scarf_analytics_makes_request(mock_urlopen: MagicMock):
    # Test when both environment variables are not set to "true"
    os.environ["SCARF_NO_ANALYTICS"] = "false"
    os.environ["DO_NOT_TRACK"] = "false"

    mock_response = MagicMock()
    mock_response.status = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response  # Mock the context manager
    response_status, runner_type = scarf_analytics()  # Should make a request
    assert response_status == "Response status: 200"
    assert runner_type is not None  # Ensure a runner type is returned


@patch("urllib.request.urlopen")
def test_scarf_analytics_timeout(mock_urlopen: MagicMock):
    mock_urlopen.side_effect = urllib.error.URLError(socket.timeout("Timeout"))
    os.environ["SCARF_NO_ANALYTICS"] = "false"
    os.environ["DO_NOT_TRACK"] = "false"

    # Call `scarf_analytics` and ensure that it handles the timeout
    response_status, runner_type = scarf_analytics()
    assert response_status.startswith("Analytics error:")
    assert runner_type is None  # Ensure no runner type is returned


@patch("urllib.request.urlopen")
def test_scarf_analytics_disabled(mock_urlopen: MagicMock):
    os.environ["SCARF_NO_ANALYTICS"] = "false"
    os.environ["DO_NOT_TRACK"] = "false"

    # Disable analytics
    os.environ["SCARF_NO_ANALYTICS"] = "true"
    response_status, runner_type = scarf_analytics()
    assert response_status is None  # Verify no request was made
    assert runner_type is None


@patch("urllib.request.urlopen")
def test_scarf_analytics_py_runner(mock_urlopen: MagicMock):
    os.environ["DAFT_RUNNER"] = "py"
    os.environ["SCARF_NO_ANALYTICS"] = "false"
    os.environ["DO_NOT_TRACK"] = "false"

    mock_response = MagicMock()
    mock_response.status = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response  # Mock the context manager
    response_status, runner_type = scarf_analytics()  # Should make a request for the Python runner
    expected_url = "https://daft.gateway.scarf.sh/daft-runner-py?"
    assert expected_url in mock_urlopen.call_args[0][0], "URL for Python runner not called correctly"
    assert response_status == "Response status: 200"
    assert runner_type == "py"


@patch("urllib.request.urlopen")
def test_scarf_analytics_ray_runner(mock_urlopen: MagicMock):
    os.environ["DAFT_RUNNER"] = "ray"
    os.environ["SCARF_NO_ANALYTICS"] = "false"
    os.environ["DO_NOT_TRACK"] = "false"

    mock_response = MagicMock()
    mock_response.status = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response  # Mock the context manager
    response_status, runner_type = scarf_analytics()  # Should make a request for the Ray runner
    expected_url = "https://daft.gateway.scarf.sh/daft-runner-ray?"
    assert expected_url in mock_urlopen.call_args[0][0], "URL for Ray runner not called correctly"
    assert response_status == "Response status: 200"
    assert runner_type == "ray"


@patch("urllib.request.urlopen")
def test_scarf_analytics_native_runner(mock_urlopen: MagicMock):
    os.environ["DAFT_RUNNER"] = "native"
    os.environ["SCARF_NO_ANALYTICS"] = "false"
    os.environ["DO_NOT_TRACK"] = "false"

    mock_response = MagicMock()
    mock_response.status = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response  # Mock the context manager
    response_status, runner_type = scarf_analytics()  # Should make a request for the Native runner
    expected_url = "https://daft.gateway.scarf.sh/daft-runner-native?"
    assert expected_url in mock_urlopen.call_args[0][0], "URL for Native runner not called correctly"
    assert response_status == "Response status: 200"
    assert runner_type == "native"
