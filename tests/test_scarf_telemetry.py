from __future__ import annotations

import socket
import urllib
from unittest.mock import MagicMock, patch

from daft.scarf_telemetry import scarf_telemetry


@patch("daft.scarf_telemetry.get_build_type")
@patch("daft.scarf_telemetry.get_version")
@patch("urllib.request.urlopen")
def test_scarf_telemetry_basic(
    mock_urlopen: MagicMock,
    mock_version: MagicMock,
    mock_build_type: MagicMock,
):
    # Test basic functionality of scarf_telemetry verify that analytics are successfully sent and url is properly formatted with all required paramters

    # Set up mocks for version and build_type
    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"
    mock_response = MagicMock()
    mock_response.status = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response

    # Test basic analytics call
    response_status, runner_type = scarf_telemetry(scarf_opt_out=False, runner="ray")

    assert response_status == "Response status: 200"
    assert runner_type == "ray"

    # Verify URL format and paramters
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

    response_status, runner_type = scarf_telemetry(scarf_opt_out=False, runner="ray")

    assert response_status is None
    assert runner_type is None


@patch("daft.scarf_telemetry.get_build_type")
@patch("daft.scarf_telemetry.get_version")
def test_scarf_telemetry_opt_out(mock_version: MagicMock, mock_build_type: MagicMock):
    # Test that analytics respect the opt-out flags, function returns None for both status and runner type
    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"

    response_status, runner_type = scarf_telemetry(scarf_opt_out=True, runner="ray")

    assert response_status is None
    assert runner_type is None


@patch("daft.scarf_telemetry.get_build_type")
@patch("daft.scarf_telemetry.get_version")
@patch("urllib.request.urlopen")
def test_scarf_telemetry_error_handling(
    mock_urlopen: MagicMock,
    mock_version: MagicMock,
    mock_build_type: MagicMock,
):
    # Test error handling in scarf_telemetry, verifies that network errors are caught, function returns error message and None for runner type

    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"
    mock_urlopen.side_effect = urllib.error.URLError(socket.timeout("Timeout"))

    response_status, runner_type = scarf_telemetry(scarf_opt_out=False, runner="ray")

    assert response_status.startswith("Analytics error:")
    assert runner_type is None


# Tests for runner integration with scarf_telemetry, commented out because cannot set runner more than once
# @patch("daft.context.scarf_telemetry")
# def test_runner_ray_analytics(mock_scarf_telemetry: MagicMock):
#     # Test Ray runner integration with analytics, verifies that set_runner_ray calls scarf_telemetry and correct parameters are passed

#     os.environ.pop("SCARF_NO_ANALYTICS", None)
#     os.environ.pop("DO_NOT_TRACK", None)

#     set_runner_ray()

#     mock_scarf_telemetry.assert_called_once_with(False, runner="ray")

# @patch("daft.context.scarf_telemetry")
# def test_runner_py_analytics(mock_scarf_telemetry: MagicMock):
#     # Test Python runner integration with analytics, verifies that set_runner_py calls scarf_telemetry and correct parameters are passed

#     os.environ.pop("SCARF_NO_ANALYTICS", None)
#     os.environ.pop("DO_NOT_TRACK", None)

#     set_runner_py()

#     mock_scarf_telemetry.assert_called_once_with(False, runner="py")

# @patch("daft.context.scarf_telemetry")
# def test_runner_native_analytics(mock_scarf_telemetry: MagicMock):
#     # Test native runner integration with analytics, verifies that set_runner_py calls scarf_telemetry and correct parameters are passed

#     os.environ.pop("SCARF_NO_ANALYTICS", None)
#     os.environ.pop("DO_NOT_TRACK", None)

#     set_runner_native()

#     mock_scarf_telemetry.assert_called_once_with(False, runner="native")

# @patch("daft.context.scarf_telemetry")
# def test_runner_analytics_with_opt_out_1(mock_scarf_telemetry: MagicMock):
#     """Test runner integration with opt-out settings.

#     This test verifies:
#     1. SCARF_NO_ANALYTICS environment variable triggers opt-out
#     2. scarf_analytics is called with correct opt-out parameter in both cases
#     3. Runner type is still correctly passed even when opted out
#     """
#     # Test SCARF_NO_ANALYTICS opt-out
#     os.environ.pop("DO_NOT_TRACK", None)
#     os.environ["SCARF_NO_ANALYTICS"] = "true"
#     set_runner_ray()
#     mock_scarf_telemetry.assert_called_once_with(True, runner="ray")

# @patch("daft.context.scarf_telemetry")
# def test_runner_analytics_with_opt_out_2(mock_scarf_telemetry: MagicMock):
#     """Test runner integration with opt-out settings.

#     This test verifies:
#     1. DO_NOT_TRACK environment variable triggers opt-out
#     2. scarf_analytics is called with correct opt-out parameter in both cases
#     3. Runner type is still correctly passed even when opted out
#     """

#     # Test DO_NOT_TRACK opt-out
#     os.environ.pop("SCARF_NO_ANALYTICS", None)
#     os.environ["DO_NOT_TRACK"] = "true"
#     mock_scarf_telemetry.reset_mock()

#     set_runner_ray()
#     mock_scarf_telemetry.assert_called_once_with(True, runner="ray")
