from __future__ import annotations

import numpy as np
import pytest
import torch
from sentence_transformers import SentenceTransformer

import daft
from daft.functions.ai import embed_text


def manually_embed_text(text: list[str]):
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", trust_remote_code=True, backend="torch")
    model.eval()
    for value in text:
        with torch.inference_mode():
            v = model.encode(value, convert_to_numpy=True)
            yield v


@pytest.mark.integration()
def test_embed_text():
    df = daft.from_pydict({"text": ["hello", "world", "goodbye", "universe"]})

    text = df.to_pydict()["text"]
    df = df.with_column(
        "embeddings",
        embed_text(
            daft.col("text"),
            provider="transformers",
            model="sentence-transformers/all-MiniLM-L6-v2",
        ),
    )
    actual = df.to_pydict()["embeddings"]
    expected = list(manually_embed_text(text))

    for a, e in zip(actual, expected):
        assert np.allclose(a, e)
