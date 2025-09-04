from __future__ import annotations

import pytest

pytest.importorskip("vllm")

from unittest.mock import Mock, patch

import numpy as np
from vllm.outputs import EmbeddingOutput, EmbeddingRequestOutput

from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.vllm.provider import VLLMProvider


@pytest.mark.parametrize(
    "model, embedding_dim",
    [
        ("sentence-transformers/all-MiniLM-L6-v2", 384),
        ("Qwen/Qwen3-Embedding-0.6B", 1024),
    ],
)
def test_vllm_text_embedder(model, embedding_dim):
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

        embedding_values = [0.1] * embedding_dim
        outputs = EmbeddingOutput(embedding=embedding_values)
        return [
            EmbeddingRequestOutput(request_id=Mock(), outputs=outputs, prompt_token_ids=Mock(), finished=Mock())
        ] * num_texts

    with patch("daft.ai.vllm.protocols.text_embedder.LLM") as MockLLM:
        instance = MockLLM.return_value
        instance.embed.side_effect = lambda input_data, *args, **kwargs: mock_embedding_response(input_data)

        descriptor = VLLMProvider().get_text_embedder(model=model)
        assert isinstance(descriptor, TextEmbedderDescriptor)
        assert descriptor.get_provider() == "vllm"
        assert descriptor.get_model() == model
        assert descriptor.get_dimensions().size == embedding_dim

        embedder = descriptor.instantiate()
        assert isinstance(embedder, TextEmbedder)
        embeddings = embedder.embed_text(text_data)
        assert isinstance(embeddings, np.ndarray)
        assert embeddings.shape == (len(text_data), embedding_dim)
