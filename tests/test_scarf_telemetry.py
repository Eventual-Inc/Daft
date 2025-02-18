from __future__ import annotations

import os
import socket
import urllib
from unittest.mock import MagicMock, patch

from daft.scarf_telemetry import scarf_telemetry


@patch("daft.scarf_telemetry.get_build_type")
@patch("daft.scarf_telemetry.get_version")
@patch("urllib.request.urlopen")
def test_scarf_telemetry_basic(mock_urlopen: MagicMock, mock_version: MagicMock, mock_build_type: MagicMock):
    # Test basic functionality of scarf_telemetry verify that analytics are successfully sent and url is properly formatted with all required parameters

    # Set up mocks for version and build_type
    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"
    mock_response = MagicMock()
    mock_response.status = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response

    # Test basic analytics call
    response_status, runner_type = scarf_telemetry(runner="ray")

    assert response_status == "Response status: 200"
    assert runner_type == "ray"

    # Verify URL format and parameters
    called_url = mock_urlopen.call_args[0][0]
    assert called_url.startswith("https://daft.gateway.scarf.sh/daft-runner?")
    assert "version=0.0.0" in called_url
    assert "runner=ray" in called_url


@patch("daft.scarf_telemetry.get_build_type")
@patch("daft.scarf_telemetry.get_version")
def test_scarf_telemetry_dev_build(mock_version: MagicMock, mock_build_type: MagicMock):
    # Test that analytics are not sent for dev builds, function returns None for both status and runner type

    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "dev"

    response_status, runner_type = scarf_telemetry(runner="ray")

    assert response_status is None
    assert runner_type is None


@patch("daft.scarf_telemetry.get_build_type")
@patch("daft.scarf_telemetry.get_version")
@patch("urllib.request.urlopen")
def test_scarf_telemetry_opt_out_with_scarf_analytics(
    mock_urlopen: MagicMock,
    mock_version: MagicMock,
    mock_build_type: MagicMock,
):
    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"

    # Clean and set environment
    os.environ.pop("DO_NOT_TRACK", None)
    os.environ["SCARF_NO_ANALYTICS"] = "true"

    response_status, runner_type = scarf_telemetry(runner="ray")

    assert response_status is None
    assert runner_type is None
    mock_urlopen.assert_not_called()


@patch("daft.scarf_telemetry.get_build_type")
@patch("daft.scarf_telemetry.get_version")
@patch("urllib.request.urlopen")
def test_scarf_telemetry_opt_out_with_do_not_track(
    mock_urlopen: MagicMock,
    mock_version: MagicMock,
    mock_build_type: MagicMock,
):
    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"

    # Clean and set environment
    os.environ.pop("SCARF_NO_ANALYTICS", None)
    os.environ["DO_NOT_TRACK"] = "true"

    response_status, runner_type = scarf_telemetry(runner="ray")

    assert response_status is None
    assert runner_type is None
    mock_urlopen.assert_not_called()


@patch("daft.scarf_telemetry.get_build_type")
@patch("daft.scarf_telemetry.get_version")
@patch("urllib.request.urlopen")
def test_scarf_telemetry_error_handling(
    mock_urlopen: MagicMock,
    mock_version: MagicMock,
    mock_build_type: MagicMock,
):
    # Test error handling in scarf_telemetry, verifies that network errors are caught, function returns error message and None for runner type

    # Clean environment
    os.environ.pop("SCARF_NO_ANALYTICS", None)
    os.environ.pop("DO_NOT_TRACK", None)

    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"
    mock_urlopen.side_effect = urllib.error.URLError(socket.timeout("Timeout"))

    response_status, runner_type = scarf_telemetry(runner="ray")

    assert response_status.startswith("Analytics error:")
    assert runner_type is None
