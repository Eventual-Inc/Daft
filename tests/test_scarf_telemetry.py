from __future__ import annotations

import os
import socket
import urllib
from unittest.mock import MagicMock, patch

import pytest

from daft.scarf_telemetry import track_import_on_scarf, track_runner_on_scarf


@pytest.fixture
def ensure_analytics_enabled():
    """Ensure DAFT_ANALYTICS_ENABLED is not set to 0 for tests."""
    # Store original value
    original_value = os.environ.get("DAFT_ANALYTICS_ENABLED")

    # Set to a non-zero value
    if original_value == "0":
        os.environ.pop("DAFT_ANALYTICS_ENABLED", None)

    yield

    # Restore original value after test
    if original_value is not None:
        os.environ["DAFT_ANALYTICS_ENABLED"] = original_value
    else:
        os.environ.pop("DAFT_ANALYTICS_ENABLED", None)


@pytest.mark.parametrize(
    "telemetry_fn,endpoint,extra_params",
    [
        (track_runner_on_scarf, "daft-runner", {"runner": "ray"}),
        (track_import_on_scarf, "daft-import", None),
    ],
)
@patch("daft.get_build_type")
@patch("daft.get_version")
@patch("urllib.request.urlopen")
def test_scarf_telemetry_basic(
    mock_urlopen: MagicMock,
    mock_version: MagicMock,
    mock_build_type: MagicMock,
    telemetry_fn,
    endpoint,
    extra_params,
    ensure_analytics_enabled,
):
    # Test basic functionality of scarf_telemetry verify that analytics are successfully sent and url is properly formatted with all required parameters

    # Set up mocks for version and build_type
    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"
    mock_response = MagicMock()
    mock_response.status = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response

    # Test basic analytics call
    if extra_params:
        request_thread, result_container = telemetry_fn(**extra_params)
    else:
        request_thread, result_container = telemetry_fn()

    request_thread.join()

    assert result_container["response_status"] == "Response status: 200"
    if extra_params and "runner" in extra_params:
        assert result_container["extra_value"] == extra_params["runner"]
    else:
        assert result_container["extra_value"] is None

    # Verify URL format and parameters
    called_url = mock_urlopen.call_args[0][0]
    assert called_url.startswith(f"https://daft.gateway.scarf.sh/{endpoint}?")
    assert "version=0.0.0" in called_url
    if extra_params and "runner" in extra_params:
        assert f"runner={extra_params['runner']}" in called_url


@pytest.mark.parametrize(
    "telemetry_fn,extra_params",
    [
        (track_runner_on_scarf, {"runner": "ray"}),
        (track_import_on_scarf, None),
    ],
)
@patch("daft.get_build_type")
@patch("daft.get_version")
def test_scarf_telemetry_dev_build(
    mock_version: MagicMock,
    mock_build_type: MagicMock,
    telemetry_fn,
    extra_params,
    ensure_analytics_enabled,
):
    # Test that analytics are not sent for dev builds, function returns None for both status and runner type

    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "dev"

    if extra_params:
        request_thread, result_container = telemetry_fn(**extra_params)
    else:
        request_thread, result_container = telemetry_fn()

    assert request_thread is None
    assert result_container["response_status"] is None
    assert result_container["extra_value"] is None


@pytest.mark.parametrize(
    "telemetry_fn,extra_params",
    [
        (track_runner_on_scarf, {"runner": "ray"}),
        (track_import_on_scarf, None),
    ],
)
@patch("daft.get_build_type")
@patch("daft.get_version")
@patch("urllib.request.urlopen")
def test_scarf_telemetry_opt_out_with_scarf_analytics(
    mock_urlopen: MagicMock,
    mock_version: MagicMock,
    mock_build_type: MagicMock,
    telemetry_fn,
    extra_params,
    ensure_analytics_enabled,
):
    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"

    # Clean and set environment
    os.environ.pop("DO_NOT_TRACK", None)
    os.environ["SCARF_NO_ANALYTICS"] = "true"

    if extra_params:
        request_thread, result_container = telemetry_fn(**extra_params)
    else:
        request_thread, result_container = telemetry_fn()

    assert request_thread is None
    assert result_container["response_status"] is None
    assert result_container["extra_value"] is None
    mock_urlopen.assert_not_called()


@pytest.mark.parametrize(
    "telemetry_fn,extra_params",
    [
        (track_runner_on_scarf, {"runner": "ray"}),
        (track_import_on_scarf, None),
    ],
)
@patch("daft.get_build_type")
@patch("daft.get_version")
@patch("urllib.request.urlopen")
def test_scarf_telemetry_opt_out_with_do_not_track(
    mock_urlopen: MagicMock,
    mock_version: MagicMock,
    mock_build_type: MagicMock,
    telemetry_fn,
    extra_params,
    ensure_analytics_enabled,
):
    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"

    # Clean and set environment
    os.environ.pop("SCARF_NO_ANALYTICS", None)
    os.environ["DO_NOT_TRACK"] = "true"

    if extra_params:
        request_thread, result_container = telemetry_fn(**extra_params)
    else:
        request_thread, result_container = telemetry_fn()

    assert request_thread is None
    assert result_container["response_status"] is None
    assert result_container["extra_value"] is None
    mock_urlopen.assert_not_called()


@pytest.mark.parametrize(
    "telemetry_fn,extra_params",
    [
        (track_runner_on_scarf, {"runner": "ray"}),
        (track_import_on_scarf, None),
    ],
)
@patch("daft.get_build_type")
@patch("daft.get_version")
@patch("urllib.request.urlopen")
def test_scarf_telemetry_error_handling(
    mock_urlopen: MagicMock,
    mock_version: MagicMock,
    mock_build_type: MagicMock,
    telemetry_fn,
    extra_params,
    ensure_analytics_enabled,
):
    # Test error handling in scarf_telemetry, verifies that network errors are caught, function returns error message and None for runner type

    # Clean environment
    os.environ.pop("SCARF_NO_ANALYTICS", None)
    os.environ.pop("DO_NOT_TRACK", None)

    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"
    mock_urlopen.side_effect = urllib.error.URLError(socket.timeout("Timeout"))

    if extra_params:
        request_thread, result_container = telemetry_fn(**extra_params)
    else:
        request_thread, result_container = telemetry_fn()

    request_thread.join()

    assert result_container["response_status"].startswith("Analytics error:")
