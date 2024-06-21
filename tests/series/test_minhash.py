import numpy as np
import pytest

from daft import DataType, Series
from daft.exceptions import DaftCoreException


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("hash_seed", [1, -1, 123, None])
def test_minhash(num_hashes, ngram_size, hash_seed):
    series = Series.from_pylist(
        [
            "The quick brown fox",
            "The speedy orange fox",
            "The quick brown fox",
            "thisonlyhasonetokenohno",
            None,
            "This has more than four tokens, unlike the other strings",
            "!@# $%^&*() -` 1235 827 9387 216340",
            None,
        ]
    )
    np_rng = np.random.default_rng(123)
    permutations = np_rng.integers(1, (1 << 32) - 1, num_hashes * 2)

    minhash = series.minhash(num_hashes, ngram_size, permutations.tolist(), hash_seed).to_pylist()
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
@pytest.mark.parametrize("hash_seed", [1, -1, 123, None])
def test_minhash_fails_nonpositive_num_hashes(num_hashes, ngram_size, hash_seed):
    series = Series.from_pylist(
        [
            "The quick brown fox",
            "The speedy orange fox",
            "The quick brown fox",
            "thisonlyhasonetokenohno",
            None,
            "This has more than four tokens, unlike the other strings",
            "!@# $%^&*() -` 1235 827 9387 216340",
            None,
        ]
    )

    with pytest.raises(ValueError, match="num_hashes must be positive"):
        series.minhash(num_hashes, ngram_size, [], hash_seed)


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [0, -1, -100])
@pytest.mark.parametrize("hash_seed", [1, -1, 123, None])
def test_minhash_fails_nonpositive_ngram_size(num_hashes, ngram_size, hash_seed):
    series = Series.from_pylist(
        [
            "The quick brown fox",
            "The speedy orange fox",
            "The quick brown fox",
            "thisonlyhasonetokenohno",
            None,
            "This has more than four tokens, unlike the other strings",
            "!@# $%^&*() -` 1235 827 9387 216340",
            None,
        ]
    )

    with pytest.raises(ValueError, match="ngram_size must be positive"):
        series.minhash(num_hashes, ngram_size, [], hash_seed)


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("hash_seed", [123, -1, None])
@pytest.mark.parametrize("num_perms", [0, 1, 3, 100])
def test_minhash_fails_too_few_permutations(num_hashes, ngram_size, hash_seed, num_perms):
    if num_perms >= 2 * num_hashes:
        return
    series = Series.from_pylist(
        [
            "The quick brown fox",
            "The speedy orange fox",
            "The quick brown fox",
            "thisonlyhasonetokenohno",
            None,
            "This has more than four tokens, unlike the other strings",
            "!@# $%^&*() -` 1235 827 9387 216340",
            None,
        ]
    )
    np_rng = np.random.default_rng(123)
    permutations = np_rng.integers(1, (1 << 32) - 1, num_perms)

    with pytest.raises(DaftCoreException, match="Not enough permutations supplied to minhash"):
        series.minhash(num_hashes, ngram_size, permutations.tolist(), hash_seed)


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("hash_seed", [1, -1, 123, None])
def test_minhash_empty_series(num_hashes, ngram_size, hash_seed):
    series = Series.from_pylist([]).cast(DataType.string())
    np_rng = np.random.default_rng(123)
    permutations = np_rng.integers(1, (1 << 32) - 1, num_hashes * 2)

    minhash = series.minhash(num_hashes, ngram_size, permutations.tolist(), hash_seed).to_pylist()
    assert len(minhash) == 0
