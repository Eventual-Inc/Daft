from __future__ import annotations

import pytest

pytest.importorskip("openai")

from typing import Any
from unittest.mock import Mock, patch

import numpy as np
from openai.types.create_embedding_response import CreateEmbeddingResponse
from openai.types.embedding import Embedding as OpenAIEmbedding

from daft import DataType
from daft.ai import Provider
from daft.ai.openai.text_embedder import (
    OpenAITextEmbedder,
    OpenAITextEmbedderDescriptor,
    _profiles,
    chunk,
)
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, Options


class MockProvider(Provider):
    def __init__(self, text_embedder: TextEmbedder):
        self.text_embedder = text_embedder

    @property
    def name(self) -> str:
        return "MockProvider"

    def get_text_embedder(self, model: str | None = None, **options: Any) -> TextEmbedderDescriptor:
        return MockDescriptor(self.text_embedder)


class MockDescriptor(TextEmbedderDescriptor):
    def __init__(self, text_embedder: TextEmbedder):
        self.text_embedder = text_embedder

    def get_model(self) -> str:
        return "MockTextEmbedder"

    def get_provider(self) -> str:
        return "MockProvider"

    def get_options(self) -> Options:
        return {}

    def get_dimensions(self) -> EmbeddingDimensions:
        return EmbeddingDimensions(size=101, dtype=DataType.float32())

    def instantiate(self) -> TextEmbedder:
        return self.text_embedder


@pytest.fixture
def mock_client():
    return Mock()


@pytest.fixture
def mock_text_embedder(mock_client):
    return OpenAITextEmbedder(mock_client, "text-embedding-3-small")


def test_valid_model_names():
    """Test that valid model names are accepted."""
    for model_name in _profiles.keys():
        descriptor = OpenAITextEmbedderDescriptor(
            provider_name="openai",
            provider_options={"api_key": "test-key"},
            model_name=model_name,
            model_options={},
        )
        assert descriptor.get_provider() == "openai"
        assert descriptor.get_model() == model_name
        assert descriptor.get_options() == {}
        assert descriptor.get_dimensions() == _profiles[model_name].dimensions


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
    """Test that instantiate creates a proper OpenAITextEmbedder."""
    descriptor = OpenAITextEmbedderDescriptor(
        provider_name="openai",
        provider_options={"api_key": "test-key"},
        model_name="text-embedding-3-small",
        model_options={},
    )

    embedder = descriptor.instantiate()
    assert isinstance(embedder, OpenAITextEmbedder)
    assert embedder._model == "text-embedding-3-small"
    assert embedder._insert_none_on_failure is True


def test_embed_text_single_input(mock_text_embedder, mock_client):
    """Test embedding a single text input."""
    # Mock response
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
    # Mock response
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
    # Create inputs that will exceed the 300k token limit
    # Each character is estimated as 5 tokens, so we need ~60k characters total
    large_text = "x" * 20000  # 100k tokens
    medium_text = "y" * 10000  # 50k tokens
    small_text = "z" * 1000  # 5k tokens

    # Mock responses for each batch
    def mock_create_side_effect(*args, **kwargs):
        input_batch = kwargs.get("input", [])
        mock_response = Mock(spec=CreateEmbeddingResponse)
        mock_embeddings = []
        for _ in input_batch:
            mock_embedding = Mock(spec=OpenAIEmbedding)
            mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)
            mock_embeddings.append(mock_embedding)
        mock_response.data = mock_embeddings
        return mock_response

    mock_client.embeddings.create.side_effect = mock_create_side_effect

    result = mock_text_embedder.embed_text([large_text, medium_text, small_text])

    # Should be called multiple times due to batch splitting
    assert mock_client.embeddings.create.call_count >= 2
    assert len(result) == 3

    # Verify all calls used the correct model and format
    for call in mock_client.embeddings.create.call_args_list:
        assert call[1]["model"] == "text-embedding-3-small"
        assert call[1]["encoding_format"] == "float"


def test_embed_text_large_input_chunking(mock_text_embedder, mock_client):
    """Test that very large inputs are chunked appropriately."""
    # Create input that exceeds the 8192 token limit (1638 characters)
    large_input = "x" * 2000  # Should be chunked

    # Mock responses for different calls
    def mock_create_side_effect(*args, **kwargs):
        input_batch = kwargs.get("input", [])
        mock_response = Mock(spec=CreateEmbeddingResponse)
        mock_embeddings = []

        if len(input_batch) == 0:
            # Empty batch - return empty response
            pass
        else:
            # Chunked batch - return one embedding per chunk
            for i in range(len(input_batch)):
                mock_embedding = Mock(spec=OpenAIEmbedding)
                mock_embedding.embedding = np.array([float(i), 0.2, 0.3] * 512, dtype=np.float32)
                mock_embeddings.append(mock_embedding)

        mock_response.data = mock_embeddings
        print(f"Mock returning {len(mock_embeddings)} embeddings")
        return mock_response

    mock_client.embeddings.create.side_effect = mock_create_side_effect

    result = mock_text_embedder.embed_text([large_input])

    print(f"Result length: {len(result)}")
    print(f"Result type: {type(result)}")
    if len(result) > 0:
        print(f"First result type: {type(result[0])}")
        print(f"First result shape: {result[0].shape if hasattr(result[0], 'shape') else 'no shape'}")
    print(f"Mock call count: {mock_client.embeddings.create.call_count}")
    for i, call in enumerate(mock_client.embeddings.create.call_args_list):
        print(f"Call {i}: input length = {len(call[1]['input'])}")

    assert len(result) == 2  # One embedding per chunk
    for embedding in result:
        assert isinstance(embedding, np.ndarray)
        assert embedding.shape == (1536,)

    # Should be called once with the chunked input
    mock_client.embeddings.create.assert_called_once()
    call_args = mock_client.embeddings.create.call_args
    assert len(call_args[1]["input"]) == 2  # Two chunks


def test_embed_text_mixed_batch_and_chunking(mock_embedder, mock_client):
    """Test complex scenario with both batch splitting and input chunking."""
    small_text = "a" * 1000  # 5k tokens
    large_text = "b" * 2000  # 10k tokens, will be chunked
    huge_text = "c" * 50000  # 250k tokens, will be chunked

    # Mock responses
    def mock_create_side_effect(*args, **kwargs):
        input_batch = kwargs.get("input", [])
        mock_response = Mock(spec=CreateEmbeddingResponse)
        mock_embeddings = []
        for i, _ in enumerate(input_batch):
            mock_embedding = Mock(spec=OpenAIEmbedding)
            mock_embedding.embedding = np.array([float(i), 0.2, 0.3] * 512, dtype=np.float32)
            mock_embeddings.append(mock_embedding)
        mock_response.data = mock_embeddings
        return mock_response

    mock_client.embeddings.create.side_effect = mock_create_side_effect

    result = mock_embedder.embed_text([small_text, large_text, huge_text])

    assert len(result) == 3
    # Should be called multiple times due to complexity
    assert mock_client.embeddings.create.call_count >= 3


def test_embed_text_empty_input(mock_embedder, mock_client):
    """Test embedding with empty input list."""
    result = mock_embedder.embed_text([])
    assert result == []
    mock_client.embeddings.create.assert_not_called()


def test_embed_text_failure_with_insert_none(mock_embedder, mock_client):
    """Test that failures are handled when insert_none_on_failure is True."""
    mock_client.embeddings.create.side_effect = Exception("API Error")

    result = mock_embedder._embed_text("Hello world")

    assert isinstance(result, np.ndarray)
    assert result.shape == (1536,)
    assert np.all(result == 0)  # Should be zero array


def test_embed_text_failure_without_insert_none(mock_client):
    """Test that failures are re-raised when insert_none_on_failure is False."""
    embedder = OpenAITextEmbedder(
        client=mock_client,
        model="text-embedding-3-small",
        insert_none_on_failure=False,
    )
    mock_client.embeddings.create.side_effect = Exception("API Error")

    with pytest.raises(Exception, match="API Error"):
        embedder.embed_text(["Hello world"])


def test_embed_text_batch_method(mock_embedder, mock_client):
    """Test the _embed_text_batch method directly."""
    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embeddings = []
    for i in range(2):
        mock_embedding = Mock(spec=OpenAIEmbedding)
        mock_embedding.embedding = np.array([float(i), 0.2, 0.3] * 512, dtype=np.float32)
        mock_embeddings.append(mock_embedding)
    mock_response.data = mock_embeddings
    mock_client.embeddings.create.return_value = mock_response

    result = mock_embedder._embed_text_batch(["text1", "text2"])

    assert len(result) == 2
    for i, embedding in enumerate(result):
        assert isinstance(embedding, np.ndarray)
        assert embedding.shape == (1536,)
        assert embedding[0] == float(i)


def test_embed_text_single_method(mock_embedder, mock_client):
    """Test the _embed_text method directly."""
    mock_response = Mock(spec=CreateEmbeddingResponse)
    mock_embedding = Mock(spec=OpenAIEmbedding)
    mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)
    mock_response.data = [mock_embedding]
    mock_client.embeddings.create.return_value = mock_response

    result = mock_embedder._embed_text("Hello world")

    assert isinstance(result, np.ndarray)
    assert result.shape == (1536,)
    assert result.dtype == np.float32


def test_different_model_dimensions(mock_client):
    """Test that different models have correct dimensions."""
    # Test text-embedding-3-large which has 3072 dimensions
    embedder = OpenAITextEmbedder(
        client=mock_client,
        model="text-embedding-3-large",
        insert_none_on_failure=True,
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
    assert chunk("", 10) == []
    assert chunk("hello", 10) == ["hello"]
    assert chunk("hello world", 11) == ["hello world"]
    assert chunk("hello world", 5) == ["hello", " worl", "d"]
    assert chunk("abcdefghijklmnop", 4) == ["abcd", "efgh", "ijkl", "mnop"]
    assert chunk("abcdefghijk", 4) == ["abcd", "efgh", "ijk"]


def test_profile_dimensions():
    """Test that all model profiles have correct dimensions."""
    expected_dimensions = {
        "text-embedding-ada-002": 1536,
        "text-embedding-3-small": 1536,
        "text-embedding-3-large": 3072,
    }

    for model_name, expected_size in expected_dimensions.items():
        profile = _profiles[model_name]
        assert profile.dimensions.size == expected_size
        assert profile.dimensions.dtype == DataType.float32()


def test_profile_immutability():
    """Test that profiles are frozen dataclasses."""
    profile = _profiles["text-embedding-3-small"]
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

    with patch("daft.ai.openai.text_embedder.OpenAI") as mock_openai_class:
        mock_client = Mock()
        mock_openai_class.return_value = mock_client

        mock_response = Mock(spec=CreateEmbeddingResponse)
        mock_embedding = Mock(spec=OpenAIEmbedding)
        mock_embedding.embedding = np.array([0.1, 0.2, 0.3] * 512, dtype=np.float32)
        mock_response.data = [mock_embedding]
        mock_client.embeddings.create.return_value = mock_response

        embedder = descriptor.instantiate()
        result = embedder.embed_text(["Hello world"])

        assert len(result) == 1
        assert isinstance(result[0], np.ndarray)
        assert result[0].shape == (1536,)


def test_protocol_compliance():
    """Test that OpenAITextEmbedder implements the TextEmbedder protocol."""
    embedder = OpenAITextEmbedder(
        client=Mock(),
        model="text-embedding-3-small",
    )

    assert isinstance(embedder, TextEmbedder)
    assert hasattr(embedder, "embed_text")
    assert callable(embedder.embed_text)


#
# EXPRESSION TESTING
#


def test_embed_text_function(mock_text_embedder):
    import daft
    from daft.functions.ai import embed_text
    from daft.session import Session

    # create a mock provider which is attached to the session
    mock_provider = MockProvider(text_embedder=mock_text_embedder)

    # use the session for provider resolution
    sess = Session()
    sess.attach(mock_provider, alias="mock_provider")

    # create a scoped daft context to use this provider
    with daft.use_context(sess):
        df = daft.from_pydict({"text": ["abc", "def", "ghi"]})
        df = df.with_column("embedding", embed_text(df["text"], provider="mock_provider"))
        df.show()
