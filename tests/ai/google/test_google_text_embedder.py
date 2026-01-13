from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

# Skip if google-genai is not installed
pytest.importorskip("google.genai")

from daft.ai.google.protocols.text_embedder import (
    _BATCH_SIZE_LIMIT,
    _INPUT_TOKEN_LIMIT,
    GoogleTextEmbedder,
    GoogleTextEmbedderDescriptor,
)
from daft.ai.google.provider import GoogleProvider
from daft.ai.protocols import TextEmbedder


def run_async(coro):
    """Helper to run async functions in sync tests."""
    return asyncio.run(coro)


DEFAULT_MODEL_NAME = "gemini-embedding-001"
DEFAULT_PROVIDER_OPTIONS = {"api_key": "test-key"}


def create_embedder(
    *,
    provider_name: str = "google",
    provider_options: dict[str, Any] | None = None,
    model: str = DEFAULT_MODEL_NAME,
    dimensions: int | None = None,
    model_options: dict[str, Any] | None = None,
) -> GoogleTextEmbedder:
    """Helper to instantiate GoogleTextEmbedder with sensible defaults."""
    opts = dict(provider_options) if provider_options is not None else dict(DEFAULT_PROVIDER_OPTIONS)
    with patch("daft.ai.google.protocols.text_embedder.genai.Client"):
        return GoogleTextEmbedder(
            provider_name=provider_name,
            provider_options=opts,
            model=model,
            embed_options={},
            dimensions=dimensions,
        )


# ===== Provider Tests =====


def test_google_provider_get_text_embedder_default():
    """Test that the provider returns an embedder descriptor with default settings."""
    provider = GoogleProvider(api_key="test-key")
    descriptor = provider.get_text_embedder()

    assert isinstance(descriptor, GoogleTextEmbedderDescriptor)
    assert descriptor.get_provider() == "google"
    assert descriptor.get_model() == "gemini-embedding-001"
    assert descriptor.get_options() == {}
    assert descriptor.get_dimensions().size == 3072


def test_google_provider_get_text_embedder_with_model():
    """Test that the provider accepts custom model names."""
    provider = GoogleProvider(api_key="test-key")
    descriptor = provider.get_text_embedder(model="some-custom-model")

    assert isinstance(descriptor, GoogleTextEmbedderDescriptor)
    assert descriptor.get_model() == "some-custom-model"


def test_google_provider_get_text_embedder_with_dimensions():
    """Test that the provider accepts custom dimensions."""
    provider = GoogleProvider(api_key="test-key")
    descriptor = provider.get_text_embedder(dimensions=128)

    assert descriptor.get_dimensions().size == 128


# ===== Descriptor Tests =====


def test_google_embedder_descriptor_instantiation():
    """Test that descriptor can be instantiated directly."""
    descriptor = GoogleTextEmbedderDescriptor(
        provider_name="google",
        provider_options={"api_key": "test-key"},
        model_name="gemini-embedding-001",
        dimensions=None,
        embed_options={},
    )

    assert descriptor.get_provider() == "google"
    assert descriptor.get_model() == "gemini-embedding-001"
    assert descriptor.get_options() == {}
    assert descriptor.get_dimensions().size == 3072


def test_google_embedder_descriptor_unknown_model_fallback():
    """Test that unknown models fall back to default dimensions (3072)."""
    descriptor = GoogleTextEmbedderDescriptor(
        provider_name="google",
        provider_options={"api_key": "test-key"},
        model_name="some-future-model",
        dimensions=None,
        embed_options={},
    )

    assert descriptor.get_dimensions().size == 3072  # Default fallback


def test_google_embedder_instantiate():
    """Test that descriptor can instantiate a GoogleTextEmbedder."""
    descriptor = GoogleTextEmbedderDescriptor(
        provider_name="google",
        provider_options={"api_key": "test-key"},
        model_name="gemini-embedding-001",
        dimensions=None,
        embed_options={},
    )

    with patch("daft.ai.google.protocols.text_embedder.genai.Client"):
        embedder = descriptor.instantiate()
        assert isinstance(embedder, GoogleTextEmbedder)
        assert isinstance(embedder, TextEmbedder)
        assert embedder.model == "gemini-embedding-001"
        assert embedder.provider_name == "google"


def test_google_embedder_descriptor_is_async():
    """Test that the embedder is marked as async."""
    descriptor = GoogleTextEmbedderDescriptor(
        provider_name="google",
        provider_options={"api_key": "test-key"},
        model_name="gemini-embedding-001",
        dimensions=None,
        embed_options={},
    )

    assert descriptor.is_async() is True


# ===== Embedder Tests =====


def test_google_embedder_basic_embedding():
    """Test basic text embedding."""

    async def _test():
        with patch("daft.ai.google.protocols.text_embedder.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()

            # Mock embeddings response
            # Each embedding object has a 'values' attribute
            emb1 = Mock()
            emb1.values = [0.1, 0.2, 0.3]
            emb2 = Mock()
            emb2.values = [0.4, 0.5, 0.6]

            mock_response.embeddings = [emb1, emb2]
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.embed_content = AsyncMock(return_value=mock_response)

            embedder = GoogleTextEmbedder(
                provider_name="google",
                provider_options={"api_key": "test-key"},
                model="gemini-embedding-001",
                embed_options={},
            )

            text = ["Hello", "World"]
            result = await embedder.embed_text(text)

            import numpy as np

            assert len(result) == 2
            np.testing.assert_allclose(result[0], [0.1, 0.2, 0.3], rtol=1e-5)
            np.testing.assert_allclose(result[1], [0.4, 0.5, 0.6], rtol=1e-5)

            # Verify call arguments
            call_args = mock_client_instance.aio.models.embed_content.call_args
            assert call_args.kwargs["model"] == "gemini-embedding-001"
            assert call_args.kwargs["contents"] == text

    run_async(_test())


def test_google_embedder_custom_dimensions():
    """Test embedding with custom output dimensionality."""

    async def _test():
        with patch("daft.ai.google.protocols.text_embedder.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            emb1 = Mock()
            emb1.values = [0.1] * 128
            mock_response.embeddings = [emb1]
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.embed_content = AsyncMock(return_value=mock_response)

            embedder = GoogleTextEmbedder(
                provider_name="google",
                provider_options={"api_key": "test-key"},
                model="gemini-embedding-001",
                embed_options={},
                dimensions=128,
            )

            result = await embedder.embed_text(["Test"])

            assert len(result) == 1
            assert len(result[0]) == 128

            # Verify config has output_dimensionality
            call_args = mock_client_instance.aio.models.embed_content.call_args
            config = call_args.kwargs["config"]
            assert config.output_dimensionality == 128

    run_async(_test())


def test_google_embedder_error_handling():
    """Test that errors from the API are propagated."""

    async def _test():
        with patch("daft.ai.google.protocols.text_embedder.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_client_instance.aio.models.embed_content = AsyncMock(side_effect=Exception("API Error"))

            embedder = GoogleTextEmbedder(
                provider_name="google",
                provider_options={"api_key": "test-key"},
                model="gemini-embedding-001",
                embed_options={},
            )

            with pytest.raises(ValueError, match="Google GenAI error"):
                await embedder.embed_text(["This will fail"])

    run_async(_test())


def test_embed_text_empty_input():
    """Test embedding with empty input list."""

    async def _test():
        with patch("daft.ai.google.protocols.text_embedder.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_client_instance.aio.models.embed_content = AsyncMock()

            embedder = GoogleTextEmbedder(
                provider_name="google",
                provider_options={"api_key": "test-key"},
                model="gemini-embedding-001",
                embed_options={},
            )

            result = await embedder.embed_text([])
            assert result == []
            mock_client_instance.aio.models.embed_content.assert_not_awaited()

    run_async(_test())


def test_embed_text_with_none_values():
    """Test that None values are handled gracefully (converted to empty strings)."""

    async def _test():
        with patch("daft.ai.google.protocols.text_embedder.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()

            # Mock embeddings response for 3 inputs
            emb1 = Mock()
            emb1.values = [0.1] * 3072
            emb2 = Mock()
            emb2.values = [0.2] * 3072
            emb3 = Mock()
            emb3.values = [0.3] * 3072
            mock_response.embeddings = [emb1, emb2, emb3]
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.embed_content = AsyncMock(return_value=mock_response)

            embedder = GoogleTextEmbedder(
                provider_name="google",
                provider_options={"api_key": "test-key"},
                model="gemini-embedding-001",
                embed_options={},
            )

            # Input with None values
            result = await embedder.embed_text([None, "Hello", None])

            assert len(result) == 3

            # Verify call arguments - None should be replaced with ""
            call_args = mock_client_instance.aio.models.embed_content.call_args
            assert call_args.kwargs["contents"] == ["", "Hello", ""]

    run_async(_test())


def test_protocol_compliance():
    """Test that GoogleTextEmbedder implements the TextEmbedder protocol."""
    with patch("daft.ai.google.protocols.text_embedder.genai.Client"):
        embedder = GoogleTextEmbedder(
            provider_name="google",
            provider_options={"api_key": "test-key"},
            model="gemini-embedding-001",
            embed_options={},
        )
        assert isinstance(embedder, TextEmbedder)
        assert hasattr(embedder, "embed_text")
        assert callable(embedder.embed_text)


def test_embed_text_records_usage_metrics():
    """Ensure that token/usage metrics are recorded if available."""

    async def _test():
        with patch("daft.ai.google.protocols.text_embedder.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            emb1 = Mock()
            emb1.values = [0.1] * 3072
            mock_response.embeddings = [emb1]

            # Mock usage metadata
            mock_usage = Mock()
            mock_usage.prompt_token_count = 10
            mock_usage.total_token_count = 10
            mock_response.usage_metadata = mock_usage

            mock_client_instance.aio.models.embed_content = AsyncMock(return_value=mock_response)

            embedder = GoogleTextEmbedder(
                provider_name="google",
                provider_options={"api_key": "test-key"},
                model="gemini-embedding-001",
                embed_options={},
            )

            with patch("daft.ai.google.protocols.text_embedder.record_text_embedding_metrics") as mock_record:
                await embedder.embed_text(["Hello"])

                # Verify metrics were recorded
                mock_record.assert_called_once_with(
                    model="gemini-embedding-001",
                    provider="google",
                    num_texts=1,
                    input_tokens=10,
                    total_tokens=10,
                )

    run_async(_test())


def test_embed_text_no_metrics_when_usage_metadata_missing():
    """Ensure that metrics are not recorded if usage_metadata is missing."""

    async def _test():
        with patch("daft.ai.google.protocols.text_embedder.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()
            emb1 = Mock()
            emb1.values = [0.1] * 3072
            mock_response.embeddings = [emb1]
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.embed_content = AsyncMock(return_value=mock_response)

            embedder = GoogleTextEmbedder(
                provider_name="google",
                provider_options={"api_key": "test-key"},
                model="gemini-embedding-001",
                embed_options={},
            )

            with patch("daft.ai.google.protocols.text_embedder.record_text_embedding_metrics") as mock_record:
                await embedder.embed_text(["Hello"])

                # Metrics should still record basic request/text volume even without token counts.
                mock_record.assert_called_once_with(
                    model="gemini-embedding-001",
                    provider="google",
                    num_texts=1,
                    input_tokens=None,
                )

    run_async(_test())


# ===== Batching Tests =====


def test_batch_size_limit_constant():
    """Verify the batch size limit is set correctly."""
    assert _BATCH_SIZE_LIMIT == 100


def test_embed_text_batches_large_inputs():
    """Test that large inputs are split into batches of 100."""

    async def _test():
        with patch("daft.ai.google.protocols.text_embedder.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value

            # Create mock responses for each batch
            def create_mock_response(batch_size):
                mock_response = Mock()
                mock_response.embeddings = [Mock(values=[0.1] * 3072) for _ in range(batch_size)]
                mock_response.usage_metadata = None
                return mock_response

            # Will be called twice: once for 100 items, once for 50 items
            mock_client_instance.aio.models.embed_content = AsyncMock(
                side_effect=[
                    create_mock_response(100),
                    create_mock_response(50),
                ]
            )

            embedder = GoogleTextEmbedder(
                provider_name="google",
                provider_options={"api_key": "test-key"},
                model="gemini-embedding-001",
                embed_options={},
            )

            # Create 150 items (should be split into batches of 100 and 50)
            text = [f"text_{i}" for i in range(150)]
            result = await embedder.embed_text(text)

            # Verify we got 150 embeddings
            assert len(result) == 150

            # Verify the API was called twice
            assert mock_client_instance.aio.models.embed_content.call_count == 2

            # Verify first call had 100 items
            first_call = mock_client_instance.aio.models.embed_content.call_args_list[0]
            assert len(first_call.kwargs["contents"]) == 100

            # Verify second call had 50 items
            second_call = mock_client_instance.aio.models.embed_content.call_args_list[1]
            assert len(second_call.kwargs["contents"]) == 50

    run_async(_test())


def test_embed_text_exactly_batch_size():
    """Test that exactly batch_size items result in a single API call."""

    async def _test():
        with patch("daft.ai.google.protocols.text_embedder.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value

            mock_response = Mock()
            mock_response.embeddings = [Mock(values=[0.1] * 3072) for _ in range(100)]
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.embed_content = AsyncMock(return_value=mock_response)

            embedder = GoogleTextEmbedder(
                provider_name="google",
                provider_options={"api_key": "test-key"},
                model="gemini-embedding-001",
                embed_options={},
            )

            # Exactly 100 items
            text = [f"text_{i}" for i in range(100)]
            result = await embedder.embed_text(text)

            assert len(result) == 100
            assert mock_client_instance.aio.models.embed_content.call_count == 1

    run_async(_test())


def test_embed_text_under_batch_size():
    """Test that fewer than batch_size items result in a single API call."""

    async def _test():
        with patch("daft.ai.google.protocols.text_embedder.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value

            mock_response = Mock()
            mock_response.embeddings = [Mock(values=[0.1] * 3072) for _ in range(50)]
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.embed_content = AsyncMock(return_value=mock_response)

            embedder = GoogleTextEmbedder(
                provider_name="google",
                provider_options={"api_key": "test-key"},
                model="gemini-embedding-001",
                embed_options={},
            )

            # 50 items (less than batch size)
            text = [f"text_{i}" for i in range(50)]
            result = await embedder.embed_text(text)

            assert len(result) == 50
            assert mock_client_instance.aio.models.embed_content.call_count == 1

    run_async(_test())


def test_embed_text_long_input_is_chunked_and_normalized():
    """Long single inputs should be chunked, averaged, and L2-normalized to a single embedding."""

    async def _test():
        with patch("daft.ai.google.protocols.text_embedder.genai.Client") as MockClient:
            mock_client_instance = MockClient.return_value
            mock_response = Mock()

            # Simulate 2 chunks -> 2 embeddings returned
            emb1 = Mock()
            emb1.values = [1.0, 0.0, 0.0]
            emb2 = Mock()
            emb2.values = [0.0, 1.0, 0.0]
            mock_response.embeddings = [emb1, emb2]
            mock_response.usage_metadata = None

            mock_client_instance.aio.models.embed_content = AsyncMock(return_value=mock_response)

            embedder = GoogleTextEmbedder(
                provider_name="google",
                provider_options={"api_key": "test-key"},
                model="gemini-embedding-001",
                embed_options={},
            )

            # Force chunking: chars_per_token=3, token_limit=_INPUT_TOKEN_LIMIT
            long_text = "a" * ((_INPUT_TOKEN_LIMIT * 3) + 100)
            result = await embedder.embed_text([long_text])

            import numpy as np

            assert len(result) == 1
            # Should be normalized
            assert np.isclose(np.linalg.norm(result[0]), 1.0)
            # Sanity: should have same dimensionality as chunk vectors
            assert len(result[0]) == 3

            # Called once, with 2 chunks
            call_args = mock_client_instance.aio.models.embed_content.call_args
            assert len(call_args.kwargs["contents"]) == 2

    run_async(_test())
