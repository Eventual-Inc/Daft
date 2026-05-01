from __future__ import annotations

import pytest

pytest.importorskip("sentence_transformers")
pytest.importorskip("torch")

import torch

from daft.ai.transformers.provider import TransformersProvider
from daft.ai.typing import EmbeddingDimensions
from daft.datatype import DataType


def test_sentence_transformers_text_embedder_default():
    provider = TransformersProvider()
    descriptor = provider.get_text_embedder_descriptor()

    assert descriptor["provider"] == "transformers"
    assert descriptor["model"] == "sentence-transformers/all-MiniLM-L6-v2"
    assert descriptor["dimensions"] == EmbeddingDimensions(384, dtype=DataType.float32())


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

    provider = TransformersProvider()
    descriptor = provider.get_text_embedder_descriptor(model_name, **mock_options)

    assert descriptor["provider"] == "transformers"
    assert descriptor["model"] == model_name
    assert descriptor.get("embed_options") == mock_options
    assert descriptor["dimensions"] == EmbeddingDimensions(dimensions, dtype=DataType.float32())


def test_transformers_get_udf_options():
    """Test that Transformers provider returns GPU-aware UDF options."""
    provider = TransformersProvider()
    descriptor = provider.get_text_embedder_descriptor()

    udf_options = provider._get_udf_options(descriptor)
    # GPU detection varies by environment, but batch_size should be default
    assert udf_options.batch_size is None  # no batch_size in default embed_options


def test_transformers_get_udf_options_with_custom_batch_size():
    """Test that custom embed_options flow through to UDF options."""
    provider = TransformersProvider()
    descriptor = provider.get_text_embedder_descriptor(batch_size=16)

    udf_options = provider._get_udf_options(descriptor)
    assert udf_options.batch_size == 16


def test_transformers_embed_text_returns_expression():
    """Test that embed_text() returns a Daft Expression using the sync default."""
    import daft

    provider = TransformersProvider()
    expr = provider.embed_text(daft.col("text"))
    assert isinstance(expr, daft.Expression)


def test_transformers_descriptor_dimension_validation():
    """Test that invalid dimensions are rejected."""
    provider = TransformersProvider()

    with pytest.raises(ValueError, match="Embedding dimensions must be a positive integer"):
        provider.get_text_embedder_descriptor(dimensions=-1)

    with pytest.raises(ValueError, match="Embedding dimensions must be a positive integer"):
        provider.get_text_embedder_descriptor(dimensions=0)


def test_transformers_descriptor_dimension_exceeds_model():
    """Test that dimensions exceeding model size are rejected."""
    provider = TransformersProvider()

    with pytest.raises(ValueError, match="exceeds model output size"):
        provider.get_text_embedder_descriptor(
            model="sentence-transformers/all-MiniLM-L6-v2",
            dimensions=99999,
        )


def test_sentence_transformers_device_selection():
    """Test that the embedder uses SentenceTransformer's automatic device selection."""
    provider = TransformersProvider()
    descriptor = provider.get_text_embedder_descriptor("sentence-transformers/all-MiniLM-L6-v2")
    embedder = provider.get_text_embedder(descriptor)

    if torch.cuda.is_available():
        expected_device = "cuda"
    elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        expected_device = "mps"
    else:
        expected_device = "cpu"

    embedder_device = next(embedder.model.parameters()).device
    assert expected_device in str(embedder_device)
