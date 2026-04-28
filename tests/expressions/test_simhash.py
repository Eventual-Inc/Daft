from __future__ import annotations

import pytest

import daft
from daft.datatype import DataType
from daft.expressions import col
from daft.functions import simhash


def test_simhash_identical_strings():
    df = daft.from_pydict({"text": ["hello world", "hello world"]})
    result = df.select(col("text").simhash()).to_pydict()["text"]
    assert result[0] == result[1]


def test_simhash_deterministic():
    df = daft.from_pydict({"text": ["the quick brown fox jumps over the lazy dog"]})
    r1 = df.select(col("text").simhash()).to_pydict()["text"][0]
    r2 = df.select(col("text").simhash()).to_pydict()["text"][0]
    assert r1 == r2


def test_simhash_returns_uint64():
    df = daft.from_pydict({"text": ["hello"]})
    result = df.select(col("text").simhash()).collect()
    assert result.schema()["text"].dtype == DataType.uint64()


def test_simhash_null_propagation():
    df = daft.from_pydict({"text": ["hello", None, "world"]})
    result = df.select(col("text").simhash()).to_pydict()["text"]
    assert result[0] is not None
    assert result[1] is None
    assert result[2] is not None


def test_simhash_empty_string():
    df = daft.from_pydict({"text": [""]})
    result = df.select(col("text").simhash()).to_pydict()["text"]
    assert result[0] == 0


def test_simhash_short_string():
    df = daft.from_pydict({"text": ["ab"]})
    result = df.select(col("text").simhash(ngram_size=3)).to_pydict()["text"]
    assert result[0] == 0


def test_simhash_similar_strings_small_distance():
    df = daft.from_pydict({
        "text": [
            "the quick brown fox jumps over the lazy dog",
            "the quick brown fox jumps over the lazy cat",
        ]
    })
    result = df.select(col("text").simhash()).to_pydict()["text"]
    distance = bin(result[0] ^ result[1]).count("1")
    assert distance < 32


def test_simhash_different_strings_large_distance():
    df = daft.from_pydict({
        "text": [
            "the quick brown fox jumps over the lazy dog",
            "1234567890 abcdefghijklmnopqrstuvwxyz !@#$%",
        ]
    })
    result = df.select(col("text").simhash()).to_pydict()["text"]
    distance = bin(result[0] ^ result[1]).count("1")
    assert distance > 5


@pytest.mark.parametrize("ngram_size", [1, 2, 3, 5])
def test_simhash_different_ngram_sizes(ngram_size):
    df = daft.from_pydict({"text": ["hello world"]})
    result = df.select(col("text").simhash(ngram_size=ngram_size)).to_pydict()["text"]
    assert result[0] is not None
    assert isinstance(result[0], int)


@pytest.mark.parametrize("hash_function", ["murmurhash3", "xxhash", "xxhash32", "xxhash64", "xxhash3_64", "sha1"])
def test_simhash_different_hash_functions(hash_function):
    df = daft.from_pydict({"text": ["hello world"]})
    result = df.select(col("text").simhash(hash_function=hash_function)).to_pydict()["text"]
    assert result[0] is not None
    assert isinstance(result[0], int)


def test_simhash_different_hash_functions_produce_different_results():
    df = daft.from_pydict({"text": ["the quick brown fox jumps over the lazy dog"]})
    r1 = df.select(col("text").simhash(hash_function="murmurhash3")).to_pydict()["text"][0]
    r2 = df.select(col("text").simhash(hash_function="xxhash3_64")).to_pydict()["text"][0]
    assert r1 != r2


def test_simhash_standalone_function():
    df = daft.from_pydict({"text": ["hello world"]})
    result = df.select(simhash(col("text"))).to_pydict()["text"]
    assert result[0] is not None


def test_simhash_multiple_rows():
    texts = [f"document number {i} with some content" for i in range(10)]
    df = daft.from_pydict({"text": texts})
    result = df.select(col("text").simhash()).to_pydict()["text"]
    assert len(result) == 10
    assert all(r is not None for r in result)


def test_simhash_unicode():
    df = daft.from_pydict({"text": ["hello world", "你好世界"]})
    result = df.select(col("text").simhash()).to_pydict()["text"]
    assert result[0] is not None
    assert result[1] is not None
    assert result[0] != result[1]
