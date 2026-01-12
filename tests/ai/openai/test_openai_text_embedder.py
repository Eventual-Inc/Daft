from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("openai")

from unittest.mock import AsyncMock, Mock, patch

import numpy as np
from openai import RateLimitError
from openai._types import omit
from openai.types.create_embedding_response import CreateEmbeddingResponse, Usage
from openai.types.embedding import Embedding as OpenAIEmbedding

from daft import DataType
from daft.ai.openai.protocols.text_embedder import (
    OpenAITextEmbedder,
    OpenAITextEmbedderDescriptor,
    _models,
    chunk_text,
)
from daft.ai.protocols import TextEmbedder
from daft.ai.typing import EmbeddingDimensions


def new_input(approx_tokens: int) -> str:
    # ~4 chars per token, the batcher uses 3 to be conservative.
    return "x" * 4 * approx_tokens


@pytest.fixture
def mock_client():
    client = Mock()
    client.embeddings = Mock()
    client.embeddings.create = AsyncMock()
    return client


@pytest.fixture
def mock_text_embedder(mock_client) -> TextEmbedder:
    embedder = OpenAITextEmbedder(
        provider_options={"api_key": "test-key"},
        model="text-embedding-3-small",
        embed_options={},
    )
    embedder._client = mock_client
    return embedder


def run(coro):
    return asyncio.run(coro)


def test_valid_model_names():
    """Test that valid model names are accepted."""
    for model_name in _models.keys():
        descriptor = OpenAITextEmbedderDescriptor(
            provider_name="openai",
            provider_options={"api_key": "test-key"},
            model_name=model_name,
            dimensions=None,
            embed_options={},
        )
        assert descriptor.get_provider() == "openai"
        assert descriptor.get_model() == model_name
        assert descriptor.get_options() == {}
        assert descriptor.get_dimensions() == _models[model_name].dimensions


def test_invalid_model_name():
    """Test that invalid model names raise ValueError."""
    with pytest.raises(ValueError, match="Unsupported OpenAI embedding model"):
        OpenAITextEmbedderDescriptor(
            provider_name="openai",
            provider_options={"api_key": "test-key"},
            model_name="invalid-model",
            dimensions=None,
            embed_options={},
        )


def test_custom_base_url_dimensions_none_probes_for_non_native_model():
    model = "BAAI/bge-m3"
    base_url = "http://localhost:1234/v1"

    from openai.types.create_embedding_response import CreateEmbeddingResponse
    from openai.types.embedding import Embedding as OpenAIEmbedding

    # Patch the module-local import used by OpenAITextEmbedderDescriptor.
    with patch("daft.ai.openai.protocols.text_embedder.OpenAIClient") as mock_openai_sync_client:
        mock_sync_client = Mock()
        mock_sync_client.embeddings = Mock()
        mock_sync_client.embeddings.create = Mock(
            return_value=CreateEmbeddingResponse(
                data=[
                    OpenAIEmbedding(
                        embedding=[0.1] * 1024,
                        index=0,
                        object="embedding",
                    )
                ],
                model=model,
                object="list",
                usage={"prompt_tokens": 0, "total_tokens": 0},
            )
        )
        mock_openai_sync_client.return_value = mock_sync_client

        descriptor = OpenAITextEmbedderDescriptor(
            provider_name="openai",
            provider_options={"api_key": "test-key", "base_url": base_url},
            model_name=model,
            dimensions=None,
        )

        assert descriptor.get_dimensions().size == 1024


def test_custom_base_url_passes_dimensions_param_through(mock_client):
    embedder = OpenAITextEmbedder(
        provider_options={"api_key": "test-key", "base_url": "http://localhost:1234/v1"},
        model="BAAI/bge-m3",
        embed_options={},
        dimensions=1024,
    )
    embedder._client = mock_client

    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embedding = Mock(spec=OpenAIEmbedding)
    mock_embedding.embedding = np.array([0.1] * 1024, dtype=np.float32)
    mock_response.data = [mock_embedding]
    mock_client.embeddings.create.return_value = mock_response

    result = run(embedder.embed_text(["hello"]))
    assert len(result) == 1
    mock_client.embeddings.create.assert_awaited_once_with(
        input=["hello"],
        model="BAAI/bge-m3",
        encoding_format="float",
        dimensions=1024,
    )


def test_instantiate():
    """Test to instantiate a proper OpenAITextEmbedder with no mocks."""
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="text-embedding-3-small",
        dimensions=None,
        embed_options={},
    )

    # An OpenAI embedding model's dimensions are known without probing.
    assert descriptor.get_dimensions().size == 1536

    embedder = descriptor.instantiate()
    assert isinstance(embedder, OpenAITextEmbedder)
    assert embedder._model == "text-embedding-3-small"


def test_embed_text_single_input(mock_text_embedder, mock_client):
    """Test embedding a single text input."""
    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embedding = Mock(spec=OpenAIEmbedding)
    mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)  # 1536 dimensions
    mock_response.data = [mock_embedding]
    mock_client.embeddings.create.return_value = mock_response

    result = run(mock_text_embedder.embed_text(["Hello world"]))

    assert len(result) == 1
    assert isinstance(result[0], np.ndarray)
    assert result[0].shape == (1536,)
    assert result[0].dtype == np.float32
    mock_client.embeddings.create.assert_awaited_once_with(
        input=["Hello world"],
        model="text-embedding-3-small",
        encoding_format="float",
        dimensions=omit,
    )


def test_embed_text_multiple_inputs(mock_text_embedder, mock_client):
    """Test embedding multiple text inputs in a single batch."""
    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embeddings = []
    for i in range(3):
        mock_embedding = Mock(spec=OpenAIEmbedding)
        mock_embedding.embedding = np.array([float(i), 0.2, 0.3] * 512, dtype=np.float32)  # 1536 dimensions
        mock_embeddings.append(mock_embedding)
    mock_response.data = mock_embeddings
    mock_client.embeddings.create.return_value = mock_response

    result = run(mock_text_embedder.embed_text(["Hello", "World", "Test"]))

    assert len(result) == 3
    for i, embedding in enumerate(result):
        assert isinstance(embedding, np.ndarray)
        assert embedding.shape == (1536,)
        assert embedding.dtype == np.float32
        assert embedding[0] == float(i)  # Check first value matches our mock

    mock_client.embeddings.create.assert_awaited_once_with(
        input=["Hello", "World", "Test"],
        model="text-embedding-3-small",
        encoding_format="float",
        dimensions=omit,
    )


def test_embed_text_batch_splitting(mock_text_embedder, mock_client):
    """Test that large batches are split appropriately."""
    text_sm = new_input(approx_tokens=5_000)
    text_md = new_input(approx_tokens=50_000)
    text_lg = new_input(approx_tokens=100_000)

    async def mock_create_embeddings(*args, **kwargs):
        input_batch = kwargs.get("input", [])
        mock_response = Mock(spec=CreateEmbeddingResponse)
        mock_embeddings = []
        for _ in input_batch:
            mock_embedding = Mock(spec=OpenAIEmbedding)
            mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)
            mock_embeddings.append(mock_embedding)
        mock_response.data = mock_embeddings
        return mock_response

    mock_client.embeddings.create.side_effect = mock_create_embeddings

    result = run(mock_text_embedder.embed_text([text_lg, text_md, text_sm]))

    assert mock_client.embeddings.create.await_count >= 2
    assert len(result) == 3
    for call in mock_client.embeddings.create.await_args_list:
        assert call[1]["model"] == "text-embedding-3-small"
        assert call[1]["encoding_format"] == "float"


def test_embed_text_large_input_chunking(mock_text_embedder, mock_client):
    """Test that very large inputs are chunked appropriately."""
    input_lg = new_input(approx_tokens=10_000)

    async def mock_create_embeddings(*args, **kwargs):
        input_batch = kwargs.get("input", [])
        mock_response = Mock(spec=CreateEmbeddingResponse)
        mock_embeddings = []
        if len(input_batch) == 0:
            pass
        else:
            # return one embedding per chunk
            for i in range(len(input_batch)):
                mock_embedding = Mock(spec=OpenAIEmbedding)
                mock_embedding.embedding = np.array([float(i), 0.2, 0.3] * 512, dtype=np.float32)
                mock_embeddings.append(mock_embedding)

        mock_response.data = mock_embeddings
        return mock_response

    mock_client.embeddings.create.side_effect = mock_create_embeddings

    result = run(mock_text_embedder.embed_text([input_lg]))

    # we only want one result after chunking + weighted average.
    assert len(result) == 1
    assert isinstance(result[0], np.ndarray)
    assert result[0].shape == (1536,)

    # should be called once with the chunked input.
    mock_client.embeddings.create.assert_awaited_once()
    call_args = mock_client.embeddings.create.await_args
    assert len(call_args[1]["input"]) == 2  # called with two chunks


def test_embed_text_mixed_batch_and_chunking(mock_text_embedder, mock_client):
    """Test complex scenario with both batch splitting and input chunking."""
    text_sm = new_input(approx_tokens=5_000)
    text_lg = new_input(approx_tokens=10_000)
    text_xl = new_input(approx_tokens=250_000)

    async def mock_create_embeddings(*args, **kwargs):
        input_batch = kwargs.get("input", [])
        mock_response = Mock(spec=CreateEmbeddingResponse)
        mock_embeddings = []
        for i, _ in enumerate(input_batch):
            mock_embedding = Mock(spec=OpenAIEmbedding)
            mock_embedding.embedding = np.array([float(i), 0.2, 0.3] * 512, dtype=np.float32)
            mock_embeddings.append(mock_embedding)
        mock_response.data = mock_embeddings
        return mock_response

    mock_client.embeddings.create.side_effect = mock_create_embeddings

    result = run(mock_text_embedder.embed_text([text_sm, text_lg, text_xl]))

    assert len(result) == 3
    assert mock_client.embeddings.create.await_count >= 3


def test_embed_text_empty_input(mock_text_embedder, mock_client):
    """Test embedding with empty input list."""
    result = run(mock_text_embedder.embed_text([]))
    assert result == []
    mock_client.embeddings.create.assert_not_awaited()


def test_embed_text_with_none_values(mock_text_embedder, mock_client):
    """Test that None values are handled gracefully and don't crash."""
    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embeddings = []
    for i in range(3):
        mock_embedding = Mock(spec=OpenAIEmbedding)
        mock_embedding.embedding = np.array([float(i), 0.2, 0.3] * 512, dtype=np.float32)  # 1536 dimensions
        mock_embeddings.append(mock_embedding)
    mock_response.data = mock_embeddings
    mock_client.embeddings.create.return_value = mock_response

    # Test with None values mixed with regular strings
    result = run(mock_text_embedder.embed_text([None, "Hello", None]))

    assert len(result) == 3
    for embedding in result:
        assert isinstance(embedding, np.ndarray)
        assert embedding.shape == (1536,)
        assert embedding.dtype == np.float32

    # Verify that None values were converted to empty strings
    mock_client.embeddings.create.assert_called_once()
    call_args = mock_client.embeddings.create.call_args
    assert call_args[1]["input"] == ["", "Hello", ""]
    assert call_args[1]["model"] == "text-embedding-3-small"
    assert call_args[1]["encoding_format"] == "float"


def test_embed_text_failure_with_zero_on_failure(mock_text_embedder, mock_client):
    """Test that failures are handled when zero_on_failure is True."""
    mock_client.embeddings.create.side_effect = Exception("API Error")
    mock_text_embedder._zero_on_failure = True

    result = run(mock_text_embedder._embed_text("Hello world"))

    assert isinstance(result, np.ndarray)
    assert result.shape == (1536,)
    assert np.all(result == 0)  # Should be zero array


def test_embed_text_failure_with_zero_on_failure_and_dimensions(mock_text_embedder, mock_client):
    """Test that failures are handled when zero_on_failure is True."""
    mock_client.embeddings.create.side_effect = Exception("API Error")
    mock_text_embedder._zero_on_failure = True
    mock_text_embedder._dimensions = 256

    result = run(mock_text_embedder._embed_text("Hello world"))

    assert isinstance(result, np.ndarray)
    assert result.shape == (256,)
    assert np.all(result == 0)  # Should be zero array


def test_embed_text_failure_without_zero_on_failure(mock_client):
    """Test that failures are re-raised when zero_on_failure is False."""
    embedder = OpenAITextEmbedder(
        provider_options={"api_key": "test-key"},
        model="text-embedding-3-small",
        embed_options={},
        zero_on_failure=False,
    )
    embedder._client = mock_client
    mock_client.embeddings.create.side_effect = Exception("API Error")

    with pytest.raises(Exception, match="API Error"):
        run(embedder.embed_text(["Hello world"]))


def test_embed_text_batch_method(mock_text_embedder, mock_client):
    """Test the _embed_text_batch method directly."""
    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embeddings = []
    for i in range(2):
        mock_embedding = Mock(spec=OpenAIEmbedding)
        mock_embedding.embedding = np.array([float(i), 0.2, 0.3] * 512, dtype=np.float32)
        mock_embeddings.append(mock_embedding)
    mock_response.data = mock_embeddings
    mock_client.embeddings.create.return_value = mock_response

    result = run(mock_text_embedder._embed_text_batch(["text1", "text2"]))

    assert len(result) == 2
    for i, embedding in enumerate(result):
        assert isinstance(embedding, np.ndarray)
        assert embedding.shape == (1536,)
        assert embedding[0] == float(i)


def test_embed_text_single_method(mock_text_embedder, mock_client):
    """Test the _embed_text method directly."""
    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embedding = Mock(spec=OpenAIEmbedding)
    mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)
    mock_response.data = [mock_embedding]
    mock_client.embeddings.create.return_value = mock_response

    result = run(mock_text_embedder._embed_text("Hello world"))

    assert isinstance(result, np.ndarray)
    assert result.shape == (1536,)
    assert result.dtype == np.float32


def test_different_model_dimensions(mock_client):
    """Test that different models have correct dimensions."""
    # Test text-embedding-3-large which has 3072 dimensions
    embedder = OpenAITextEmbedder(
        provider_options={"api_key": "test-key"},
        model="text-embedding-3-large",
        embed_options={},
        zero_on_failure=True,
    )
    embedder._client = mock_client

    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embedding = Mock(spec=OpenAIEmbedding)
    mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 1024, dtype=np.float32)  # 3072 dimensions
    mock_response.data = [mock_embedding]
    mock_client.embeddings.create.return_value = mock_response

    result = run(embedder.embed_text(["Hello world"]))

    assert len(result) == 1
    assert result[0].shape == (3072,)


def test_chunk():
    assert chunk_text("", 10) == []
    assert chunk_text("hello", 10) == ["hello"]
    assert chunk_text("hello world", 11) == ["hello world"]
    assert chunk_text("hello world", 5) == ["hello", " worl", "d"]
    assert chunk_text("abcdefghijklmnop", 4) == ["abcd", "efgh", "ijkl", "mnop"]
    assert chunk_text("abcdefghijk", 4) == ["abcd", "efgh", "ijk"]


def test_profile_dimensions():
    """Test that all model profiles have correct dimensions."""
    expected_dimensions = {
        "text-embedding-ada-002": 1536,
        "text-embedding-3-small": 1536,
        "text-embedding-3-large": 3072,
    }

    for model_name, expected_size in expected_dimensions.items():
        profile = _models[model_name]
        assert profile.dimensions.size == expected_size
        assert profile.dimensions.dtype == DataType.float32()


def test_profile_immutability():
    """Test that profiles are frozen dataclasses."""
    profile = _models["text-embedding-3-small"]
    with pytest.raises(Exception):  # Should raise when trying to modify
        profile.dimensions = EmbeddingDimensions(size=100, dtype=DataType.float32())


def test_descriptor_to_embedder_workflow():
    """Test the complete workflow from descriptor to embedding."""
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="text-embedding-3-small",
        dimensions=None,
        embed_options={},
    )

    with patch("daft.ai.openai.protocols.text_embedder.AsyncOpenAI") as mock_async_openai_class:
        mock_client = Mock()
        mock_client.embeddings = Mock()
        mock_client.embeddings.create = AsyncMock()
        mock_async_openai_class.return_value = mock_client

        mock_response = Mock(spec=CreateEmbeddingResponse)
        mock_embedding = Mock(spec=OpenAIEmbedding)
        mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)
        mock_response.data = [mock_embedding]
        mock_client.embeddings.create.return_value = mock_response

        text_embedder = descriptor.instantiate()
        result = run(text_embedder.embed_text(["Hello world"]))

        assert len(result) == 1
        assert isinstance(result[0], np.ndarray)
        assert result[0].shape == (1536,)


def test_protocol_compliance():
    """Test that OpenAITextEmbedder implements the TextEmbedder protocol."""
    text_embedder = OpenAITextEmbedder(
        provider_options={"api_key": "test-key"},
        model="text-embedding-3-small",
        embed_options={},
    )

    assert isinstance(text_embedder, TextEmbedder)
    assert hasattr(text_embedder, "embed_text")
    assert callable(text_embedder.embed_text)


def test_embed_text_records_usage_metrics(mock_text_embedder, mock_client):
    """Ensure that token/usage metrics are recorded."""
    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_response.usage = Usage(prompt_tokens=10, total_tokens=12)
    mock_embedding = Mock(spec=OpenAIEmbedding)
    mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)
    mock_response.data = [mock_embedding]
    mock_client.embeddings.create.return_value = mock_response

    with patch("daft.ai.metrics.increment_counter") as mock_counter:
        result = run(mock_text_embedder.embed_text(["Hello world"]))

    assert len(result) == 1
    attrs = {
        "model": "text-embedding-3-small",
        "protocol": "embed",
        "provider": "openai",
    }
    mock_counter.assert_any_call("input tokens", 10, attributes=attrs)
    mock_counter.assert_any_call("total tokens", 12, attributes=attrs)
    mock_counter.assert_any_call("requests", attributes=attrs)
    assert mock_counter.call_count == 3


def test_embed_text_batch_rate_limit_fallback(mock_text_embedder, mock_client):
    """Test that _embed_text_batch falls back to individual calls when rate limited."""
    mock_client.embeddings.create.side_effect = RateLimitError(
        message="Rate limit exceeded",
        response=Mock(),
        body=None,
    )

    with patch.object(
        mock_text_embedder,
        "_embed_text",
        new=AsyncMock(return_value=np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)),
    ) as mock_embed_text:
        result = run(mock_text_embedder._embed_text_batch(["text1", "text2", "text3"]))

        # should have called _embed_text for each input
        assert mock_embed_text.await_count == 3
        assert len(result) == 3
        for embedding in result:
            assert isinstance(embedding, np.ndarray)
            assert embedding.shape == (1536,)
            assert embedding.dtype == np.float32


def test_supports_overriding_dimensions_default_false(mock_client):
    """Test that when supports_overriding_dimensions is False (default), dimensions are NOT included."""
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="text-embedding-3-small",
        dimensions=256,
        embed_options={},  # supports_overriding_dimensions not set, defaults to False
    )
    embedder = descriptor.instantiate()
    embedder._client = mock_client

    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embedding = Mock(spec=OpenAIEmbedding)
    mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)  # 1536 dimensions
    mock_response.data = [mock_embedding]
    mock_client.embeddings.create.return_value = mock_response

    result = run(embedder.embed_text(["Hello world"]))

    assert len(result) == 1
    # Verify dimensions were NOT included in the request (should use omit)
    mock_client.embeddings.create.assert_awaited_once_with(
        input=["Hello world"],
        model="text-embedding-3-small",
        encoding_format="float",
        dimensions=omit,
    )


def test_supports_overriding_dimensions_explicit_false(mock_client):
    """Test that when supports_overriding_dimensions is explicitly False, dimensions are NOT included."""
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="text-embedding-3-small",
        dimensions=256,
        embed_options={"supports_overriding_dimensions": False},
    )
    embedder = descriptor.instantiate()
    embedder._client = mock_client

    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embedding = Mock(spec=OpenAIEmbedding)
    mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)  # 1536 dimensions
    mock_response.data = [mock_embedding]
    mock_client.embeddings.create.return_value = mock_response

    result = run(embedder.embed_text(["Hello world"]))

    assert len(result) == 1
    # Verify dimensions were NOT included in the request (should use omit)
    mock_client.embeddings.create.assert_awaited_once_with(
        input=["Hello world"],
        model="text-embedding-3-small",
        encoding_format="float",
        dimensions=omit,
    )


def test_supports_overriding_dimensions_true(mock_client):
    """Test that when supports_overriding_dimensions is True, dimensions ARE included."""
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="text-embedding-3-small",
        dimensions=256,
        embed_options={"supports_overriding_dimensions": True},
    )
    embedder = descriptor.instantiate()
    embedder._client = mock_client

    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embedding = Mock(spec=OpenAIEmbedding)
    mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 85, dtype=np.float32)  # 256 dimensions
    mock_response.data = [mock_embedding]
    mock_client.embeddings.create.return_value = mock_response

    result = run(embedder.embed_text(["Hello world"]))

    assert len(result) == 1
    # Verify dimensions WERE included in the request
    mock_client.embeddings.create.assert_awaited_once_with(
        input=["Hello world"],
        model="text-embedding-3-small",
        encoding_format="float",
        dimensions=256,
    )


def test_supports_overriding_dimensions_true_batch(mock_client):
    """Test that supports_overriding_dimensions=True works with _embed_text_batch."""
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="text-embedding-3-small",
        dimensions=256,
        embed_options={"supports_overriding_dimensions": True},
    )
    embedder = descriptor.instantiate()
    embedder._client = mock_client

    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embeddings = []
    for i in range(2):
        mock_embedding = Mock(spec=OpenAIEmbedding)
        mock_embedding.embedding = np.array([float(i), 0.2, 0.3] * 85, dtype=np.float32)  # 256 dimensions
        mock_embeddings.append(mock_embedding)
    mock_response.data = mock_embeddings
    mock_client.embeddings.create.return_value = mock_response

    result = run(embedder._embed_text_batch(["text1", "text2"]))

    assert len(result) == 2
    # Verify dimensions WERE included in the batch request
    mock_client.embeddings.create.assert_awaited_once_with(
        input=["text1", "text2"],
        model="text-embedding-3-small",
        encoding_format="float",
        dimensions=256,
    )


def test_supports_overriding_dimensions_false_single_method(mock_client):
    """Test that supports_overriding_dimensions=False works with _embed_text method."""
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="text-embedding-3-small",
        dimensions=256,
        embed_options={"supports_overriding_dimensions": False},
    )
    embedder = descriptor.instantiate()
    embedder._client = mock_client

    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embedding = Mock(spec=OpenAIEmbedding)
    mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)  # 1536 dimensions
    mock_response.data = [mock_embedding]
    mock_client.embeddings.create.return_value = mock_response

    result = run(embedder._embed_text("Hello world"))

    assert isinstance(result, np.ndarray)
    # Verify dimensions were NOT included in the single request (should use omit)
    mock_client.embeddings.create.assert_awaited_once_with(
        input="Hello world",
        model="text-embedding-3-small",
        encoding_format="float",
        dimensions=omit,
    )
