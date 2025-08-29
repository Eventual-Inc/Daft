from __future__ import annotations

import pytest

pytest.importorskip("sentence_transformers")
pytest.importorskip("torch")

from daft.ai.protocols import TextEmbedderDescriptor
from daft.ai.sentence_transformers import SentenceTransformersProvider
from daft.ai.typing import EmbeddingDimensions
from daft.datatype import DataType


def test_sentence_transformers_text_embedder_default():
    provider = SentenceTransformersProvider()
    descriptor = provider.get_text_embedder()
    assert isinstance(descriptor, TextEmbedderDescriptor)
    assert descriptor.get_provider() == "sentence_transformers"
    assert descriptor.get_model() == "sentence-transformers/all-MiniLM-L6-v2"
    assert descriptor.get_dimensions() == EmbeddingDimensions(384, dtype=DataType.float32())


@pytest.mark.parametrize(
    "model_name, dimensions",
    [
        ("sentence-transformers/all-MiniLM-L6-v2", 384),
        ("sentence-transformers/all-mpnet-base-v2", 768),
        ("Qwen/Qwen3-Embedding-8B", 4096),
        ("Qwen/Qwen3-Embedding-4B", 2560),
        ("Qwen/Qwen3-Embedding-0.6B", 1024),
        ("BAAI/bge-base-en-v1.5", 768),
    ],
)
def test_sentence_transformers_text_embedder_other(model_name, dimensions):
    mock_options = {"arg1": "val1", "arg2": "val2"}

    provider = SentenceTransformersProvider()
    descriptor = provider.get_text_embedder(model_name, **mock_options)
    assert isinstance(descriptor, TextEmbedderDescriptor)
    assert descriptor.get_provider() == "sentence_transformers"
    assert descriptor.get_model() == model_name
    assert descriptor.get_options() == mock_options
    assert descriptor.get_dimensions() == EmbeddingDimensions(dimensions, dtype=DataType.float32())
