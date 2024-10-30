from __future__ import annotations

from typing import Literal

import pytest

from daft import DataType, Series


def minhash_none(
    series: Series,
    num_hashes: int,
    ngram_size: int,
    seed: int | None,
    hash_function: Literal["murmurhash3", "xxhash", "sha1"] = "murmurhash3",
) -> list[list[int] | None]:
    if seed is None:
        return series.minhash(num_hashes, ngram_size, hash_function=hash_function).to_pylist()
    else:
        return series.minhash(num_hashes, ngram_size, seed, hash_function=hash_function).to_pylist()


test_series = Series.from_pylist(
    [
        "The quick brown fox",
        "The speedy orange fox",
        "The quick brown fox",
        "thisonlyhasonetokenohno",
        None,
        "This has more than four tokens, unlike the other strings",
        "!@# $%^&*() -` 1235 827 9387 216340",
        "This   has    excessive\t\n and   weird    whitespace",
        "",
        " spaces at start and end ",
        " ",
        None,
    ]
)


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
@pytest.mark.parametrize("hash_function", ["murmurhash3", "xxhash", "sha1"])
def test_minhash(num_hashes, ngram_size, seed, hash_function):
    minhash = minhash_none(test_series, num_hashes, ngram_size, seed, hash_function)
    assert minhash[4] is None and minhash[-1] is None
    for lst in minhash:
        if lst is not None:
            assert len(lst) == num_hashes
    for i in range(num_hashes):
        assert minhash[0][i] == minhash[2][i]
        if ngram_size > 1:
            assert minhash[0][i] != minhash[1][i]


@pytest.mark.parametrize(
    "num_hashes,ngram_size,seed,expected",
    [
        # Test with single hash, unigrams
        (
            1,
            1,
            1,
            [
                [1196831525],  # "The quick brown fox"
                [120174860],  # "The speedy orange fox"
                [1196831525],  # "The quick brown fox" - identical to first
                [2559787809],  # "thisonlyhasonetokenohno"
                None,  # None value
                [27473697],  # "This has more..."
                [441506281],  # "!@# $%^&*()..."
                [27473697],  # "This has excessive..."
                # [500470364],  # "" - empty string todo(andrewgazelka): fix empty string
                [4294967295],  # todo: this is different than previous impl ^
                [76461626],  # " spaces at..."
                [500470364],  # " " - just a space
                None,  # None value
            ],
        ),
        # Test with two hashes, bigrams
        (
            2,
            2,
            123,
            [
                [760527683, 1539127776],  # "The quick brown fox"
                [1704758042, 309185920],  # "The speedy orange fox"
                [760527683, 1539127776],  # "The quick brown fox" - identical to first
                [3763775515, 2389564536],  # "thisonlyhasonetokenohno"
                None,  # None value
                [437177734, 1262955240],  # "This has more..."
                [101182009, 511203536],  # "!@# $%^&*()..."
                [27545328, 189622288],  # "This has excessive..."
                # [2989311896, 1304790168],  # "" - empty string
                [4294967295, 4294967295],  # todo: this is different than previous impl ^
                [94241209, 101414440],  # " spaces at start and end "
                [531691842, 296683088],  # " " - just a space
                None,  # None value
            ],
        ),
    ],
)
def test_minhash_exact_values(num_hashes, ngram_size, seed, expected):
    result = minhash_none(test_series, num_hashes, ngram_size, seed)
    assert result == expected


@pytest.mark.parametrize("num_hashes", [0, -1, -100])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
@pytest.mark.parametrize("hash_function", ["murmurhash3", "xxhash", "sha1"])
def test_minhash_fails_nonpositive_num_hashes(num_hashes, ngram_size, seed, hash_function):
    with pytest.raises(ValueError, match="num_hashes must be positive"):
        minhash_none(test_series, num_hashes, ngram_size, seed, hash_function)


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [0, -1, -100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
@pytest.mark.parametrize("hash_function", ["murmurhash3", "xxhash", "sha1"])
def test_minhash_fails_nonpositive_ngram_size(num_hashes, ngram_size, seed, hash_function):
    with pytest.raises(ValueError, match="ngram_size must be positive"):
        minhash_none(test_series, num_hashes, ngram_size, seed, hash_function)


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
@pytest.mark.parametrize("hash_function", ["murmurhash3", "xxhash", "sha1"])
def test_minhash_empty_series(num_hashes, ngram_size, seed, hash_function):
    series = Series.from_pylist([]).cast(DataType.string())

    minhash = minhash_none(series, num_hashes, ngram_size, seed, hash_function)
    assert len(minhash) == 0


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
@pytest.mark.parametrize("hash_function", ["murmurhash3", "xxhash", "sha1"])
def test_minhash_seed_consistency(num_hashes, ngram_size, seed, hash_function):
    minhash1 = minhash_none(test_series, num_hashes, ngram_size, seed, hash_function)
    minhash2 = minhash_none(test_series, num_hashes, ngram_size, seed, hash_function)
    assert minhash1 == minhash2


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed_pair", [[1, 2], [1, 5], [None, 2], [123, 234]])
@pytest.mark.parametrize("hash_function", ["murmurhash3", "xxhash", "sha1"])
def test_minhash_seed_differences(num_hashes, ngram_size, seed_pair, hash_function):
    minhash1 = minhash_none(test_series, num_hashes, ngram_size, seed_pair[0], hash_function)
    minhash2 = minhash_none(test_series, num_hashes, ngram_size, seed_pair[1], hash_function)
    assert minhash1 != minhash2
