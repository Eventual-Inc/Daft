from __future__ import annotations

import time

import pytest

import daft
from daft.functions.ai import embed_text


@pytest.mark.skip()
def test_embed_text():
    df = daft.from_pydict({"text": ["Hello, world!"]})
    df = df.with_column("text_embedded", embed_text("text"))
    df.show()


# @pytest.mark.skip()
def test_embed_text_large_corpus_batched():
    """550k rows, processing in batches."""
    start_time = time.time()

    # https://huggingface.co/datasets/stanfordnlp/snli
    df = daft.read_parquet("hf://datasets/stanfordnlp/snli/")
    df = df.with_column("embedding", embed_text("premise"))
    df = df.limit(10_000)
    df = df.select("premise", "embedding")
    df.write_parquet("./out")

    end_time = time.time()
    print(f"test_embed_text_large_corpus took {end_time - start_time:.2f} seconds")
