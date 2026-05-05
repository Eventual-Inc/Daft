from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
import qdrant_client
from qdrant_client.http.exceptions import ApiException, ResponseHandlingException, UnexpectedResponse

import daft
from tests.conftest import get_tests_daft_runner_name


@pytest.fixture
def sample_df() -> daft.DataFrame:
    return daft.from_pydict(
        {
            "id": [1, 2, 3],
            "text": ["hello", "world", "test"],
            "vector": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]],
        }
    )


def _make_unexpected_response(status_code: int, message: str) -> UnexpectedResponse:
    return UnexpectedResponse(
        status_code=status_code,
        reason_phrase=message,
        content=message.encode(),
        headers=None,
    )


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    reason="Mocking the data sink's dependencies doesn't work in Ray execution environment",
)
@pytest.mark.parametrize(
    "error",
    [
        _make_unexpected_response(429, "Too Many Requests"),
        _make_unexpected_response(500, "Internal Server Error"),
        _make_unexpected_response(502, "Bad Gateway"),
        _make_unexpected_response(503, "Service Unavailable"),
        _make_unexpected_response(504, "Gateway Timeout"),
        ResponseHandlingException("connection reset"),
    ],
)
def test_resilience_to_transient_errors(sample_df, error):
    mock_client = Mock()
    mock_client.upsert.side_effect = error

    with (
        patch(
            "daft.io.qdrant.qdrant_data_sink.QdrantDataSink._import_qdrant",
            return_value=qdrant_client,
        ),
        patch("qdrant_client.QdrantClient", return_value=mock_client),
    ):
        results = sample_df.write_qdrant(
            collection_name="test_collection", url="http://localhost:6333", api_key="test_api_key"
        ).collect()
        rows_not_written = 0
        for result in results:
            write_result = result["write_responses"].result
            assert write_result["status"] == "failed"
            assert "error" in write_result
            rows_not_written += write_result["rows_not_written"]

        assert rows_not_written == sample_df.count_rows()


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    reason="Mocking the data sink's dependencies doesn't work in Ray execution environment",
)
@pytest.mark.parametrize(
    "error",
    [
        _make_unexpected_response(400, "Bad Request"),
        _make_unexpected_response(401, "Unauthorized"),
        _make_unexpected_response(403, "Forbidden"),
        _make_unexpected_response(404, "Not Found"),
        _make_unexpected_response(422, "Unprocessable Entity"),
        ApiException("validation failed"),
    ],
)
def test_fail_fast_on_non_transient_errors(sample_df, error):
    mock_client = Mock()
    mock_client.upsert.side_effect = error

    with (
        patch(
            "daft.io.qdrant.qdrant_data_sink.QdrantDataSink._import_qdrant",
            return_value=qdrant_client,
        ),
        patch("qdrant_client.QdrantClient", return_value=mock_client),
    ):
        with pytest.raises(RuntimeError) as exc_info:
            sample_df.write_qdrant(
                collection_name="test_collection", url="http://localhost:6333", api_key="test_api_key"
            ).collect()

        assert isinstance(exc_info.value.__cause__, type(error))
