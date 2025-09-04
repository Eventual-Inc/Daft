from __future__ import annotations

import pytest

pytest.importorskip("openai")

from unittest.mock import patch

import numpy as np
from openai.types.create_embedding_response import CreateEmbeddingResponse
from openai.types.embedding import Embedding as OpenAIEmbedding

from daft.ai.lm_studio.provider import LMStudioProvider
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor


@pytest.mark.parametrize(
    "model, embedding_dim",
    [
        ("text-embedding-qwen3-embedding-0.6b", 1024),
        ("text-embedding-nomic-embed-text-v1.5", 768),
    ],
)
def test_lm_studio_text_embedder(model, embedding_dim):
    text_data = [
        "Alice was beginning to get very tired of sitting by her sister on the bank.",
        "So she was considering in her own mind (as well as she could, for the hot day made her feel very sleepy and stupid),",
        "whether the pleasure of making a daisy-chain would be worth the trouble of getting up and picking the daisies,",
        "when suddenly a White Rabbit with pink eyes ran close by her.",
        "There was nothing so very remarkable in that;",
        "nor did Alice think it so very much out of the way to hear the Rabbit say to itself, 'Oh dear! Oh dear! I shall be late!'",
    ]

    def mock_embedding_response(input_data):
        if isinstance(input_data, list):
            num_texts = len(input_data)
        else:
            num_texts = 1

        embeddings = []
        for i in range(num_texts):
            embedding_values = [0.1] * embedding_dim
            embedding_obj = OpenAIEmbedding(embedding=embedding_values, index=i, object="embedding")
            embeddings.append(embedding_obj)

        response = CreateEmbeddingResponse(
            data=embeddings, model=model, object="list", usage={"prompt_tokens": 0, "total_tokens": 0}
        )
        return response

    with patch("openai.resources.embeddings.Embeddings.create") as mock_embed:
        mock_embed.side_effect = lambda **kwargs: mock_embedding_response(kwargs.get("input"))

        descriptor = LMStudioProvider().get_text_embedder(model=model)
        assert isinstance(descriptor, TextEmbedderDescriptor)
        assert descriptor.get_provider() == "lm_studio"
        assert descriptor.get_model() == model
        assert descriptor.get_dimensions().size == embedding_dim

        embedder = descriptor.instantiate()
        assert isinstance(embedder, TextEmbedder)
        embeddings = embedder.embed_text(text_data)
        assert len(embeddings) == len(text_data)
        assert all(isinstance(embedding, np.ndarray) for embedding in embeddings)
        assert all(len(embedding) == embedding_dim for embedding in embeddings)
