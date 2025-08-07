from __future__ import annotations

import pytest

# skip if missing required dependencies
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
    assert descriptor.get_model() == "all-MiniLM-L6-v2"
    assert descriptor.get_dimensions() == EmbeddingDimensions(384, dtype=DataType.float32())


def test_sentence_transformers_text_embedder_other():
    mock_name = "other"
    mock_options = {"arg1": "val1", "arg2": "val2"}

    provider = SentenceTransformersProvider()
    descriptor = provider.get_text_embedder(mock_name, **mock_options)
    assert isinstance(descriptor, TextEmbedderDescriptor)
    assert descriptor.get_provider() == "sentence_transformers"
    assert descriptor.get_model() == mock_name  # currently no validation on the names/identifiers
    assert descriptor.get_options() == mock_options
    assert descriptor.get_dimensions() == EmbeddingDimensions(384, dtype=DataType.float32())
