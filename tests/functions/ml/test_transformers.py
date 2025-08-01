import numpy as np
import pytest

from daft.ml._transformers import _TransformersTextEmbedder
from daft.ml.typing import Embedding


def test_init(self):
    """Test that the embedder initializes correctly with a model name."""
    model_name = "all-MiniLM-L6-v2"
    embedder = _TransformersTextEmbedder(model_name)

    assert embedder._model == model_name
    assert embedder._sentence_transformer is None


def test_context_manager_enter_exit(self):
    """Test that the context manager properly initializes and cleans up the model."""
    model_name = "all-MiniLM-L6-v2"
    embedder = _TransformersTextEmbedder(model_name)

    # Before entering context
    assert embedder._sentence_transformer is None

    # Enter context
    with embedder:
        assert embedder._sentence_transformer is not None
        assert hasattr(embedder._sentence_transformer, "encode")

    # After exiting context
    assert embedder._sentence_transformer is None


def test_embed_text_single_line(self):
    """Test embedding a single line of text."""
    model_name = "all-MiniLM-L6-v2"
    embedder = _TransformersTextEmbedder(model_name)

    test_text = "This is a test sentence for embedding."

    with embedder:
        embedding = embedder.embed_text(test_text)

        # Check that the result is a numpy array
        assert isinstance(embedding, np.ndarray)

        # Check that it's a 1D array (embedding vector)
        assert embedding.ndim == 1

        # Check that the embedding has a reasonable size (all-MiniLM-L6-v2 produces 384-dimensional embeddings)
        assert embedding.shape[0] == 384

        # Check that the values are finite numbers
        assert np.all(np.isfinite(embedding))

        # Check that the embedding is not all zeros
        assert not np.allclose(embedding, 0.0)


def test_embed_text_different_inputs(self):
    """Test embedding different types of text inputs."""
    model_name = "all-MiniLM-L6-v2"
    embedder = _TransformersTextEmbedder(model_name)

    test_cases = [
        "Hello world",
        "A longer sentence with more words to test the embedding functionality.",
        "Short",
        "",  # Empty string
        "Special characters: !@#$%^&*()",
        "Numbers: 12345",
        "Unicode: café, naïve, résumé",
    ]

    with embedder:
        for text in test_cases:
            embedding = embedder.embed_text(text)

            # Basic checks for all embeddings
            assert isinstance(embedding, np.ndarray)
            assert embedding.ndim == 1
            assert embedding.shape[0] == 384
            assert np.all(np.isfinite(embedding))


def test_embed_text_outside_context_error(self):
    """Test that using embed_text outside of context manager raises an error."""
    model_name = "all-MiniLM-L6-v2"
    embedder = _TransformersTextEmbedder(model_name)

    test_text = "This should fail."

    # Should raise RuntimeError when used outside context
    with pytest.raises(RuntimeError, match="Cannot use model outside a context manager"):
        embedder.embed_text(test_text)


def test_multiple_context_managers(self):
    """Test that multiple context manager calls work correctly."""
    model_name = "all-MiniLM-L6-v2"
    embedder = _TransformersTextEmbedder(model_name)

    test_text = "Testing multiple context managers."

    # First context manager
    with embedder:
        embedding1 = embedder.embed_text(test_text)
        assert isinstance(embedding1, np.ndarray)

    # Second context manager (should work fine)
    with embedder:
        embedding2 = embedder.embed_text(test_text)
        assert isinstance(embedding2, np.ndarray)

    # Both embeddings should be valid
    assert embedding1.shape == embedding2.shape
    assert np.all(np.isfinite(embedding1))
    assert np.all(np.isfinite(embedding2))


def test_embedding_consistency(self):
    """Test that the same input produces consistent embeddings."""
    model_name = "all-MiniLM-L6-v2"
    embedder = _TransformersTextEmbedder(model_name)

    test_text = "Consistent embedding test."

    with embedder:
        embedding1 = embedder.embed_text(test_text)
        embedding2 = embedder.embed_text(test_text)

        # Same input should produce same embedding
        np.testing.assert_array_almost_equal(embedding1, embedding2)


def test_embedding_different_inputs_produce_different_embeddings(self):
    """Test that different inputs produce different embeddings."""
    model_name = "all-MiniLM-L6-v2"
    embedder = _TransformersTextEmbedder(model_name)

    text1 = "First sentence for testing."
    text2 = "Second sentence for testing."

    with embedder:
        embedding1 = embedder.embed_text(text1)
        embedding2 = embedder.embed_text(text2)

        # Different inputs should produce different embeddings
        assert not np.allclose(embedding1, embedding2)


def test_embedding_normalization(self):
    """Test that embeddings are properly normalized (if the model does so)."""
    model_name = "all-MiniLM-L6-v2"
    embedder = _TransformersTextEmbedder(model_name)

    test_text = "Testing embedding normalization."

    with embedder:
        embedding = embedder.embed_text(test_text)

        # Check that embedding values are in a reasonable range
        # Sentence transformers typically produce embeddings with values in a reasonable range
        assert np.min(embedding) > -10
        assert np.max(embedding) < 10
