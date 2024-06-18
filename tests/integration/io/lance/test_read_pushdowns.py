from __future__ import annotations

import pytest

import daft

WORDS_DATASET = "s3://daft-public-data/lance/words-test-dataset"


@pytest.mark.integration()
def test_lance_s3_filter_pushdowns(aws_public_s3_config):
    df = daft.read_lance(WORDS_DATASET, io_config=aws_public_s3_config)
    df = df.where(daft.col("text") == "hello world").collect()
    assert df.count() == 1
