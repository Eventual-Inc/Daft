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
        "Either the pipeline had many steps, or Desmond processed the data very slowly, for he had plenty of time as he streamed along to look about him and to wonder what was going to happen next.",
        "First, he tried to peek ahead and see what partition he was coming to, but it was too opaque to see anything; then he looked at the sides of the pipeline and noticed that they were filled with changing schemas and modalities;",
        "here and there he saw potential OOMs and SEGFAULTs pinned upon walls of logs.",
        "He pulled a file from one of the shelves as he passed; it was labelled 'DELTA TABLE', but to his great disappointment it was just an empty Parquet: he did not like to drop the file for fear of corrupting somebody's downstream job, so managed to tuck it back into a catalog as he streamed past it.",
        "'Well!' thought Desmond to himself, 'after such a fall as this, I shall think nothing of GPU inference! How brave they'll all think me at home! Why, I wouldn't say anything about it, even if I fell off the top of production!' (Which was very likely true.)",
        "Down, down, down. Would the pipeline never come to an end? 'I wonder how many terabytes I've shuffled by this time?' he said aloud. 'I must be getting somewhere near the centre of the datalake.'",
        "Let me see: that would be four hundred million rows, I think— (for, you see, Desmond had learnt several things of this sort in his adventures with Daft, and though this was not a very good opportunity for showing off his scaling knowledge, as there was no one to listen to him, still it was good practice to say it over)",
        "—yes, that's about the right size—but then I wonder what cluster I've got to? (Desmond had no idea what a cluster was, but thought it was a nice grand word to say.)",
        "Presently he began again. 'I wonder if I shall fall right through the datalake! How funny it'll seem to come out among the people that walk with their queries upside down! The Eventualites, I think—'",
        "(he was rather glad there was no one listening, this time, as it didn't sound at all the right word) '—but I shall have to ask them what the name of the platform is, you know. Please, Ma'am, is this Ev or Eventual?'",
        "(and he tried to bow politely as he spoke—fancy bowing while streaming through compute nodes! Do you think you could manage it?) 'And what an ignorant fellow they'll think me for asking! No, it'll never do to ask: perhaps I shall see it written up somewhere—on a dashboard, or maybe in the logs.'",
        "Down, down, down. There was nothing else to do, so Desmond soon began talking again. 'I wonder who'll miss me while I'm debugging to-night, I should think! I hope they'll remember to check the metrics.'",
        "'Ah, Daft my dear! I wish you were down here with me! There are no bugs in the air, I'm afraid, but you might catch a straggling morsel, and that's very like a microbatch, you know.'",
        "'But do engines eat morsels, I wonder?' And here Desmond began to get rather sleepy, and went on saying to himself, in a dreamy sort of way, 'Do engines eat morsels? Do morsels eat engines?'",
        "and sometimes, 'Do Eventuals eat Dafts?' for, you see, as he couldn't answer either question, it didn't much matter which way he put it.",
        "He felt that he was dozing off, and had just begun to dream that he was walking hand in hand with Daft itself, and saying to it very earnestly, 'Now, tell me the truth: did you ever process a batched bat?'",
        "when suddenly—thump! thump!—down he landed upon a heap of logs and job reports, and the pipeline was over.",
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
