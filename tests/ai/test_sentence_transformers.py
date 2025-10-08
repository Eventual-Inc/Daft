from __future__ import annotations

import pytest

pytest.importorskip("sentence_transformers")
pytest.importorskip("torch")

import torch

from daft.ai.protocols import TextEmbedderDescriptor
from daft.ai.sentence_transformers.provider import SentenceTransformersProvider
from daft.ai.typing import EmbeddingDimensions
from daft.datatype import DataType
from tests.benchmarks.conftest import IS_CI


def test_sentence_transformers_text_embedder_default():
    provider = SentenceTransformersProvider()
    descriptor = provider.get_text_embedder()
    assert isinstance(descriptor, TextEmbedderDescriptor)
    assert descriptor.get_provider() == "sentence_transformers"
    assert descriptor.get_model() == "sentence-transformers/all-MiniLM-L6-v2"
    assert descriptor.get_dimensions() == EmbeddingDimensions(384, dtype=DataType.float32())


@pytest.mark.parametrize(
    "model_name, dimensions, run_model_in_ci",
    [
        ("sentence-transformers/all-MiniLM-L6-v2", 384, True),
        ("sentence-transformers/all-mpnet-base-v2", 768, True),
        ("Qwen/Qwen3-Embedding-8B", 4096, False),  # Too large to run reasonably in CI.
        ("Qwen/Qwen3-Embedding-4B", 2560, False),  # Too large to run reasonably in CI.
        ("Qwen/Qwen3-Embedding-0.6B", 1024, True),
        ("BAAI/bge-base-en-v1.5", 768, True),
    ],
)
def test_sentence_transformers_text_embedder_other(model_name, dimensions, run_model_in_ci):
    mock_options = {"arg1": "val1", "arg2": "val2"}

    provider = SentenceTransformersProvider()
    descriptor = provider.get_text_embedder(model_name, **mock_options)
    assert isinstance(descriptor, TextEmbedderDescriptor)
    assert descriptor.get_provider() == "sentence_transformers"
    assert descriptor.get_model() == model_name
    assert descriptor.get_options() == mock_options

    if not IS_CI or run_model_in_ci:
        embedder = descriptor.instantiate()

        true_dimensions = embedder.model.get_sentence_embedding_dimension()
        assert descriptor.get_dimensions() == EmbeddingDimensions(true_dimensions, dtype=DataType.float32())

        test_texts = ["Hello world", "Bye world"]
        embeddings = embedder.embed_text(test_texts)
        assert len(embeddings) == 2
        assert all(len(emb) == dimensions for emb in embeddings)
    else:
        assert descriptor.get_dimensions() == EmbeddingDimensions(dimensions, dtype=DataType.float32())


def test_sentence_transformers_device_selection():
    """Test that the embedder uses SentenceTransformer's automatic device selection."""
    provider = SentenceTransformersProvider()
    descriptor = provider.get_text_embedder("sentence-transformers/all-MiniLM-L6-v2")
    embedder = descriptor.instantiate()

    if torch.cuda.is_available():
        expected_device = "cuda"
    elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        expected_device = "mps"
    else:
        expected_device = "cpu"

    embedder_device = next(embedder.model.parameters()).device
    assert expected_device in str(embedder_device)
