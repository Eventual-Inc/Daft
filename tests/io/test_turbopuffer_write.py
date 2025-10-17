from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
import turbopuffer

import daft
from tests.conftest import get_tests_daft_runner_name


@pytest.fixture
def sample_df() -> daft.DataFrame:
    """Create a sample table for testing."""
    return daft.from_pydict(
        {
            "id": [1, 2, 3],
            "text": ["hello", "world", "test"],
            "vector": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]],
        }
    )


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    reason="Mocking the data sink's dependencies doesn't work in Ray execution environment",
)
@pytest.mark.parametrize(
    "error_class,status_code,error_message",
    [
        (turbopuffer.RateLimitError, 429, "Rate limit exceeded"),
        (turbopuffer.APIConnectionError, None, "Connection failed"),
        (turbopuffer.InternalServerError, 500, "Internal server error"),
        (turbopuffer.ConflictError, 409, "Conflict error"),
        (turbopuffer.APITimeoutError, None, "Request timed out"),
    ],
)
def test_resilience_to_transient_errors(sample_df, error_class, status_code, error_message):
    """Test that transient errors are handled gracefully and don't cause the write to fail."""
    mock_namespace = Mock()
    mock_response = Mock()
    mock_request = Mock()

    if error_class == turbopuffer.APIConnectionError or error_class == turbopuffer.APITimeoutError:
        # APIConnectionError and APITimeoutError require a request parameter.
        error = error_class(request=mock_request)
    elif status_code is not None:
        # Errors with HTTP responses.
        mock_response.status_code = status_code
        error = error_class(message=error_message, response=mock_response, body={"error": error_message})
    else:
        error = error_class(message=error_message)

    mock_namespace.write.side_effect = error

    # Create mock Turbopuffer instance that returns the mock namespace.
    mock_tpuf_instance = Mock()
    mock_tpuf_instance.namespace.return_value = mock_namespace

    # Create a mock turbopuffer module with real exception classes.
    mock_turbopuffer = Mock()
    mock_turbopuffer.Turbopuffer.return_value = mock_tpuf_instance
    mock_turbopuffer.APIError = turbopuffer.APIError
    mock_turbopuffer.RateLimitError = turbopuffer.RateLimitError
    mock_turbopuffer.APIConnectionError = turbopuffer.APIConnectionError
    mock_turbopuffer.InternalServerError = turbopuffer.InternalServerError
    mock_turbopuffer.ConflictError = turbopuffer.ConflictError
    mock_turbopuffer.APITimeoutError = turbopuffer.APITimeoutError

    # Patch _import_turbopuffer to return our mock module.
    with patch(
        "daft.io.turbopuffer.turbopuffer_data_sink.TurbopufferDataSink._import_turbopuffer",
        return_value=mock_turbopuffer,
    ):
        # Write should complete without raising an exception.
        results = sample_df.write_turbopuffer(
            namespace="test_namespace", api_key="test_api_key", region="test_region"
        ).collect()
        rows_not_written = 0
        for result in results:
            write_result = result["write_responses"].result
            assert write_result["status"] == "failed"
            assert "error" in write_result
            rows_not_written += write_result["rows_not_written"]

        # All rows should be marked as not written.
        assert rows_not_written == sample_df.count_rows()


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    reason="Mocking the data sink's dependencies doesn't work in Ray execution environment",
)
@pytest.mark.parametrize(
    "error_class,status_code,error_message",
    [
        (turbopuffer.AuthenticationError, 401, "Invalid API key"),
        (turbopuffer.BadRequestError, 400, "Bad request"),
        (turbopuffer.PermissionDeniedError, 403, "Permission denied"),
        (turbopuffer.NotFoundError, 404, "Namespace not found"),
        (turbopuffer.UnprocessableEntityError, 422, "Unprocessable entity"),
    ],
)
def test_fail_fast_on_non_transient_errors(sample_df, error_class, status_code, error_message):
    """Test that non-transient errors cause immediate failure (fail fast)."""
    mock_namespace = Mock()
    mock_response = Mock()
    mock_response.status_code = status_code
    error = error_class(message=error_message, response=mock_response, body={"error": error_message})
    mock_namespace.write.side_effect = error
    mock_tpuf_instance = Mock()
    mock_tpuf_instance.namespace.return_value = mock_namespace

    # Create a mock turbopuffer module with real exception classes.
    mock_turbopuffer = Mock()
    mock_turbopuffer.Turbopuffer.return_value = mock_tpuf_instance
    mock_turbopuffer.APIError = turbopuffer.APIError
    mock_turbopuffer.AuthenticationError = turbopuffer.AuthenticationError
    mock_turbopuffer.BadRequestError = turbopuffer.BadRequestError
    mock_turbopuffer.PermissionDeniedError = turbopuffer.PermissionDeniedError
    mock_turbopuffer.NotFoundError = turbopuffer.NotFoundError
    mock_turbopuffer.UnprocessableEntityError = turbopuffer.UnprocessableEntityError

    # Patch _import_turbopuffer to return our mock module.
    with patch(
        "daft.io.turbopuffer.turbopuffer_data_sink.TurbopufferDataSink._import_turbopuffer",
        return_value=mock_turbopuffer,
    ):
        # Write should raise the exception (fail fast).
        # Non-transient errors propagate through and get wrapped in a RuntimeError by safe_write.
        with pytest.raises(RuntimeError) as exc_info:
            sample_df.write_turbopuffer(
                namespace="test_namespace", api_key="test_api_key", region="test_region"
            ).collect()

        # Verify the original exception is the non-transient error we expect.
        assert isinstance(exc_info.value.__cause__, error_class)
        assert error_message in str(exc_info.value.__cause__)
