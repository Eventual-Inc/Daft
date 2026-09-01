from __future__ import annotations

import os
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

    # Verify Scarf URL format and parameters (first call)
    scarf_url = mock_urlopen.call_args_list[0][0][0]
    assert scarf_url.startswith(f"https://daft.gateway.scarf.sh/{endpoint}?")
    assert "version=0.0.0" in scarf_url
    if extra_params and "runner" in extra_params:
        assert f"runner={extra_params['runner']}" in scarf_url

    # Verify osstelemetry.io URL format and parameters (second call)
    ot_url = mock_urlopen.call_args_list[1][0][0]
    assert ot_url.startswith("https://osstelemetry.io/data?")
    assert f"activity_type={endpoint}" in ot_url
    assert "package=daft" in ot_url
    assert "version=0.0.0" in ot_url


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
    mock_urlopen.side_effect = urllib.error.URLError(TimeoutError("Timeout"))

    if extra_params:
        request_thread, result_container = telemetry_fn(**extra_params)
    else:
        request_thread, result_container = telemetry_fn()

    request_thread.join()

    assert result_container["response_status"].startswith("Analytics error:")


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
def test_scarf_telemetry_reuses_shared_ssl_context(
    mock_urlopen: MagicMock,
    mock_version: MagicMock,
    mock_build_type: MagicMock,
    telemetry_fn,
    extra_params,
    ensure_analytics_enabled,
):
    # Every request must reuse the shared SSLContext built on the main thread, so
    # the daemon worker never initializes OpenSSL global state itself and thus
    # cannot race the interpreter's OpenSSL teardown at shutdown (which can
    # SIGSEGV after all user work has completed).
    os.environ.pop("SCARF_NO_ANALYTICS", None)
    os.environ.pop("DO_NOT_TRACK", None)

    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"
    mock_response = MagicMock()
    mock_response.status = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response

    if extra_params:
        request_thread, _ = telemetry_fn(**extra_params)
    else:
        request_thread, _ = telemetry_fn()
    request_thread.join()

    contexts = [call.kwargs.get("context") for call in mock_urlopen.call_args_list]
    assert contexts, "expected telemetry requests to be issued"
    assert all(ctx is not None for ctx in contexts), "telemetry request missing shared SSLContext"
    assert len(set(id(ctx) for ctx in contexts)) == 1, "telemetry requests should reuse one SSLContext"


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
def test_scarf_telemetry_requests_use_timeout(
    mock_urlopen: MagicMock,
    mock_version: MagicMock,
    mock_build_type: MagicMock,
    telemetry_fn,
    extra_params,
    ensure_analytics_enabled,
):
    # Every request must pass a timeout so a slow/unreachable endpoint cannot keep
    # the worker thread alive inside the SSL stack indefinitely.
    os.environ.pop("SCARF_NO_ANALYTICS", None)
    os.environ.pop("DO_NOT_TRACK", None)

    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"
    mock_response = MagicMock()
    mock_response.status = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response

    if extra_params:
        request_thread, _ = telemetry_fn(**extra_params)
    else:
        request_thread, _ = telemetry_fn()
    request_thread.join()

    assert mock_urlopen.call_args_list, "expected telemetry requests to be issued"
    for call in mock_urlopen.call_args_list:
        assert call.kwargs.get("timeout") is not None, "telemetry request missing timeout"


@pytest.mark.parametrize(
    "telemetry_fn,extra_params",
    [
        (track_runner_on_scarf, {"runner": "ray"}),
        (track_import_on_scarf, None),
    ],
)
@patch("daft.get_build_type")
@patch("daft.get_version")
@patch("daft.scarf_telemetry._get_ssl_context")
def test_scarf_telemetry_ssl_setup_failure_is_best_effort(
    mock_get_ssl_context: MagicMock,
    mock_version: MagicMock,
    mock_build_type: MagicMock,
    telemetry_fn,
    extra_params,
    ensure_analytics_enabled,
):
    # Telemetry must stay best-effort: if building the SSLContext on the main
    # thread fails, the event is skipped instead of propagating into the caller's
    # import (track_import_on_scarf) or query execution (track_runner_on_scarf).
    os.environ.pop("SCARF_NO_ANALYTICS", None)
    os.environ.pop("DO_NOT_TRACK", None)

    mock_version.return_value = "0.0.0"
    mock_build_type.return_value = "release"
    mock_get_ssl_context.side_effect = OSError("cannot load CA certificates")

    if extra_params:
        request_thread, result_container = telemetry_fn(**extra_params)
    else:
        request_thread, result_container = telemetry_fn()

    assert request_thread is None
    assert result_container["response_status"] is None
    assert result_container["extra_value"] is None
