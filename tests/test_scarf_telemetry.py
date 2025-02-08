from __future__ import annotations

import os
import socket
import urllib
from unittest.mock import MagicMock, patch

from daft import get_runner
from daft.scarf_telemetry import scarf_analytics

# PUBLISHER_THREAD_SLEEP_INTERVAL_SECONDS = 0.1


@patch("urllib.request.urlopen")
def test_scarf_analytics_dev(mock_urlopen: MagicMock):
    # Test that analytics are not sent for dev builds
    response_status, runner_type = scarf_analytics(
        scarf_opt_out=False, build_type="dev", version="0.0.0", runner="native"
    )

    assert response_status is None
    assert runner_type is None
    mock_urlopen.assert_not_called()


@patch("urllib.request.urlopen")
def test_scarf_analytics_scarf_opt_out(mock_urlopen: MagicMock):
    # Test that analytics are not sent when user opts out.
    response_status, runner_type = scarf_analytics(
        scarf_opt_out=True, build_type="release", version="0.0.0", runner="native"
    )

    assert response_status is None
    assert runner_type is None
    mock_urlopen.assert_not_called()


@patch("urllib.request.urlopen")
def test_scarf_analytics_for_each_runner(mock_urlopen: MagicMock):
    # Test analytics with each valid runner type.
    mock_response = MagicMock()
    mock_response.status = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response

    runners = ["py", "ray", "native"]

    for runner in runners:
        mock_urlopen.reset_mock()

        response_status, runner_type = scarf_analytics(
            scarf_opt_out=False, build_type="release", version="0.0.0", runner=runner
        )

        assert response_status == "Response status: 200"
        assert runner_type == runner

        # Verify URL contains correct parameters
        called_url = mock_urlopen.call_args[0][0]
        assert f"runner={runner}" in called_url
        assert "version=0.0.0" in called_url


@patch("urllib.request.urlopen")
def test_scarf_analytics_environment_vars(mock_urlopen: MagicMock):
    # Test that environment variables still work with new implementation.
    os.environ["SCARF_NO_ANALYTICS"] = "true"
    response_status, runner_type = scarf_analytics(
        scarf_opt_out=False, build_type="release", version="0.0.0", runner="native"
    )

    assert response_status is None
    assert runner_type is None
    mock_urlopen.assert_not_called()

    os.environ["SCARF_NO_ANALYTICS"] = "false"
    os.environ["DO_NOT_TRACK"] = "true"
    response_status, runner_type = scarf_analytics(
        scarf_opt_out=False, build_type="release", version="0.0.0", runner="native"
    )

    assert response_status is None
    assert runner_type is None
    mock_urlopen.assert_not_called()


@patch("urllib.request.urlopen")
def test_scarf_analytics_error_handling(mock_urlopen: MagicMock):
    # Test error handling in analytics.
    mock_urlopen.side_effect = urllib.error.URLError(socket.timeout("Timeout"))

    os.environ["SCARF_NO_ANALYTICS"] = "false"
    os.environ["DO_NOT_TRACK"] = "false"

    response_status, runner_type = scarf_analytics(
        scarf_opt_out=False, build_type="release", version="0.0.0", runner="native"
    )

    assert response_status is not None, "Expected an error message but got None"
    assert response_status.startswith("Analytics error:")
    assert runner_type is None


@patch("urllib.request.urlopen")
def test_scarf_analytics_url_format(mock_urlopen: MagicMock):
    # Test that the URL is correctly formatted.
    mock_response = MagicMock()
    mock_response.status = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response

    scarf_analytics(scarf_opt_out=False, build_type="release", version="0.0.0", runner="native")

    called_url = mock_urlopen.call_args[0][0]
    assert called_url.startswith("https://daft.gateway.scarf.sh/daft-runner?")
    assert "platform=" in called_url
    assert "python=" in called_url
    assert "arch=" in called_url
    assert "version=0.0.0" in called_url
    assert "runner=native" in called_url


def test_scarf_analytics_no_exception_leak():
    # Test that no exceptions leak from the analytics function.
    with patch("urllib.request.urlopen", side_effect=Exception("Test error")):
        response_status, runner_type = scarf_analytics(
            scarf_opt_out=False, build_type="release", version="0.0.0", runner="native"
        )
        assert response_status.startswith("Analytics error:")
        assert runner_type is None


def test_get_runner():
    # Test the get_runner function with different environment variables.

    # Test default value
    os.environ.pop("DAFT_RUNNER", None)  # Remove if exists
    assert get_runner() == "native"

    # Test valid values
    for runner in ["py", "ray", "native"]:
        os.environ["DAFT_RUNNER"] = runner
        assert get_runner() == runner

    # Test invalid value defaults to py
    os.environ["DAFT_RUNNER"] = "invalid"
    assert get_runner() == "native"

    # Test case insensitivity
    os.environ["DAFT_RUNNER"] = "RAY"
    assert get_runner() == "ray"
