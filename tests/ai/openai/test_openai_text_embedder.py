from __future__ import annotations

import pytest

pytest.importorskip("openai")

from unittest.mock import Mock, patch

import numpy as np
from openai import RateLimitError
from openai.types.create_embedding_response import CreateEmbeddingResponse
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
    return Mock()


@pytest.fixture
def mock_text_embedder(mock_client) -> TextEmbedder:
    return OpenAITextEmbedder(mock_client, "text-embedding-3-small")


def test_valid_model_names():
    """Test that valid model names are accepted."""
    for model_name in _models.keys():
        descriptor = OpenAITextEmbedderDescriptor(
            provider_name="openai",
            provider_options={"api_key": "test-key"},
            model_name=model_name,
            model_options={},
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
            model_options={},
        )


def test_instantiate():
    """Test to instantiate a proper OpenAITextEmbedder with no mocks."""
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="text-embedding-3-small",
        model_options={},
    )

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

    result = mock_text_embedder.embed_text(["Hello world"])

    assert len(result) == 1
    assert isinstance(result[0], np.ndarray)
    assert result[0].shape == (1536,)
    assert result[0].dtype == np.float32
    mock_client.embeddings.create.assert_called_once_with(
        input=["Hello world"],
        model="text-embedding-3-small",
        encoding_format="float",
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

    result = mock_text_embedder.embed_text(["Hello", "World", "Test"])

    assert len(result) == 3
    for i, embedding in enumerate(result):
        assert isinstance(embedding, np.ndarray)
        assert embedding.shape == (1536,)
        assert embedding.dtype == np.float32
        assert embedding[0] == float(i)  # Check first value matches our mock

    mock_client.embeddings.create.assert_called_once_with(
        input=["Hello", "World", "Test"],
        model="text-embedding-3-small",
        encoding_format="float",
    )


def test_embed_text_batch_splitting(mock_text_embedder, mock_client):
    """Test that large batches are split appropriately."""
    text_sm = new_input(approx_tokens=5_000)
    text_md = new_input(approx_tokens=50_000)
    text_lg = new_input(approx_tokens=100_000)

    def mock_create_embeddings(*args, **kwargs):
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

    result = mock_text_embedder.embed_text([text_lg, text_md, text_sm])

    assert mock_client.embeddings.create.call_count >= 2
    assert len(result) == 3
    for call in mock_client.embeddings.create.call_args_list:
        assert call[1]["model"] == "text-embedding-3-small"
        assert call[1]["encoding_format"] == "float"


def test_embed_text_large_input_chunking(mock_text_embedder, mock_client):
    """Test that very large inputs are chunked appropriately."""
    input_lg = new_input(approx_tokens=10_000)

    def mock_create_embeddings(*args, **kwargs):
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

    result = mock_text_embedder.embed_text([input_lg])

    # we only want one result after chunking + weighted average.
    assert len(result) == 1
    assert isinstance(result[0], np.ndarray)
    assert result[0].shape == (1536,)

    # should be called once with the chunked input.
    mock_client.embeddings.create.assert_called_once()
    call_args = mock_client.embeddings.create.call_args
    assert len(call_args[1]["input"]) == 2  # called with two chunks


def test_embed_text_mixed_batch_and_chunking(mock_text_embedder, mock_client):
    """Test complex scenario with both batch splitting and input chunking."""
    text_sm = new_input(approx_tokens=5_000)
    text_lg = new_input(approx_tokens=10_000)
    text_xl = new_input(approx_tokens=250_000)

    def mock_create_embeddings(*args, **kwargs):
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

    result = mock_text_embedder.embed_text([text_sm, text_lg, text_xl])

    assert len(result) == 3
    assert mock_client.embeddings.create.call_count >= 3


def test_embed_text_empty_input(mock_text_embedder, mock_client):
    """Test embedding with empty input list."""
    result = mock_text_embedder.embed_text([])
    assert result == []
    mock_client.embeddings.create.assert_not_called()


def test_embed_text_failure_with_zero_on_failure(mock_text_embedder, mock_client):
    """Test that failures are handled when zero_on_failure is True."""
    mock_client.embeddings.create.side_effect = Exception("API Error")
    mock_text_embedder._zero_on_failure = True

    result = mock_text_embedder._embed_text("Hello world")

    assert isinstance(result, np.ndarray)
    assert result.shape == (1536,)
    assert np.all(result == 0)  # Should be zero array


def test_embed_text_failure_without_zero_on_failure(mock_client):
    """Test that failures are re-raised when zero_on_failure is False."""
    embedder = OpenAITextEmbedder(
        client=mock_client,
        model="text-embedding-3-small",
        zero_on_failure=False,
    )
    mock_client.embeddings.create.side_effect = Exception("API Error")

    with pytest.raises(Exception, match="API Error"):
        embedder.embed_text(["Hello world"])


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

    result = mock_text_embedder._embed_text_batch(["text1", "text2"])

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

    result = mock_text_embedder._embed_text("Hello world")

    assert isinstance(result, np.ndarray)
    assert result.shape == (1536,)
    assert result.dtype == np.float32


def test_different_model_dimensions(mock_client):
    """Test that different models have correct dimensions."""
    # Test text-embedding-3-large which has 3072 dimensions
    embedder = OpenAITextEmbedder(
        client=mock_client,
        model="text-embedding-3-large",
        zero_on_failure=True,
    )

    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embedding = Mock(spec=OpenAIEmbedding)
    mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 1024, dtype=np.float32)  # 3072 dimensions
    mock_response.data = [mock_embedding]
    mock_client.embeddings.create.return_value = mock_response

    result = embedder.embed_text(["Hello world"])

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
        model_options={},
    )

    with patch("daft.ai.openai.protocols.text_embedder.OpenAI") as mock_openai_class:
        mock_client = Mock()
        mock_openai_class.return_value = mock_client

        mock_response = Mock(spec=CreateEmbeddingResponse)
        mock_embedding = Mock(spec=OpenAIEmbedding)
        mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)
        mock_response.data = [mock_embedding]
        mock_client.embeddings.create.return_value = mock_response

        text_embedder = descriptor.instantiate()
        result = text_embedder.embed_text(["Hello world"])

        assert len(result) == 1
        assert isinstance(result[0], np.ndarray)
        assert result[0].shape == (1536,)


def test_protocol_compliance():
    """Test that OpenAITextEmbedder implements the TextEmbedder protocol."""
    text_embedder = OpenAITextEmbedder(
        client=Mock(),
        model="text-embedding-3-small",
    )

    assert isinstance(text_embedder, TextEmbedder)
    assert hasattr(text_embedder, "embed_text")
    assert callable(text_embedder.embed_text)


def test_embed_text_batch_rate_limit_fallback(mock_text_embedder, mock_client):
    """Test that _embed_text_batch falls back to individual calls when rate limited."""
    mock_client.embeddings.create.side_effect = RateLimitError(
        message="Rate limit exceeded",
        response=Mock(),
        body=None,
    )

    def mock_individual_create(*args, **kwargs):
        mock_response = Mock(spec=CreateEmbeddingResponse)
        mock_embedding = Mock(spec=OpenAIEmbedding)
        mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)
        mock_response.data = [mock_embedding]
        return mock_response

    with patch.object(mock_text_embedder, "_embed_text") as mock_embed_text:
        mock_embed_text.return_value = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)
        result = mock_text_embedder._embed_text_batch(["text1", "text2", "text3"])

        # should have called _embed_text for each input
        assert mock_embed_text.call_count == 3
        assert len(result) == 3
        for embedding in result:
            assert isinstance(embedding, np.ndarray)
            assert embedding.shape == (1536,)
            assert embedding.dtype == np.float32
