from __future__ import annotations

from enum import Enum

import pytest

from daft import DataType, Series


class HashFunctionKind(Enum):
    """
    Kind of hash function to use for minhash.
    """

    MurmurHash3 = 0
    XxHash = 1
    Sha1 = 2


def minhash_none(
    series: Series,
    num_hashes: int,
    ngram_size: int,
    seed: int | None,
    hash_function: HashFunctionKind,
) -> list[list[int] | None]:
    if seed is None:
        return series.minhash(num_hashes, ngram_size, hash_function=hash_function.name.lower()).to_pylist()
    else:
        return series.minhash(num_hashes, ngram_size, seed, hash_function=hash_function.name.lower()).to_pylist()


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
@pytest.mark.parametrize(
    "hash_function", [HashFunctionKind.MurmurHash3, HashFunctionKind.XxHash, HashFunctionKind.Sha1]
)
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


@pytest.mark.parametrize("num_hashes", [0, -1, -100])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
@pytest.mark.parametrize(
    "hash_function", [HashFunctionKind.MurmurHash3, HashFunctionKind.XxHash, HashFunctionKind.Sha1]
)
def test_minhash_fails_nonpositive_num_hashes(num_hashes, ngram_size, seed, hash_function):
    with pytest.raises(ValueError, match="num_hashes must be positive"):
        minhash_none(test_series, num_hashes, ngram_size, seed, hash_function)


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [0, -1, -100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
@pytest.mark.parametrize(
    "hash_function", [HashFunctionKind.MurmurHash3, HashFunctionKind.XxHash, HashFunctionKind.Sha1]
)
def test_minhash_fails_nonpositive_ngram_size(num_hashes, ngram_size, seed, hash_function):
    with pytest.raises(ValueError, match="ngram_size must be positive"):
        minhash_none(test_series, num_hashes, ngram_size, seed, hash_function)


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
@pytest.mark.parametrize(
    "hash_function", [HashFunctionKind.MurmurHash3, HashFunctionKind.XxHash, HashFunctionKind.Sha1]
)
def test_minhash_empty_series(num_hashes, ngram_size, seed, hash_function):
    series = Series.from_pylist([]).cast(DataType.string())

    minhash = minhash_none(series, num_hashes, ngram_size, seed, hash_function)
    assert len(minhash) == 0


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
@pytest.mark.parametrize(
    "hash_function", [HashFunctionKind.MurmurHash3, HashFunctionKind.XxHash, HashFunctionKind.Sha1]
)
def test_minhash_seed_consistency(num_hashes, ngram_size, seed, hash_function):
    minhash1 = minhash_none(test_series, num_hashes, ngram_size, seed, hash_function)
    minhash2 = minhash_none(test_series, num_hashes, ngram_size, seed, hash_function)
    assert minhash1 == minhash2


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed_pair", [[1, 2], [1, 5], [None, 2], [123, 234]])
@pytest.mark.parametrize(
    "hash_function", [HashFunctionKind.MurmurHash3, HashFunctionKind.XxHash, HashFunctionKind.Sha1]
)
def test_minhash_seed_differences(num_hashes, ngram_size, seed_pair, hash_function):
    minhash1 = minhash_none(test_series, num_hashes, ngram_size, seed_pair[0], hash_function)
    minhash2 = minhash_none(test_series, num_hashes, ngram_size, seed_pair[1], hash_function)
    assert minhash1 != minhash2
