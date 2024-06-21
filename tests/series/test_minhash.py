import pytest

from daft import DataType, Series


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
def test_minhash(num_hashes, ngram_size, seed):
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

    minhash = series.minhash(num_hashes, ngram_size, seed).to_pylist()
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
def test_minhash_fails_nonpositive_num_hashes(num_hashes, ngram_size, seed):
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
        series.minhash(num_hashes, ngram_size, seed)


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [0, -1, -100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
def test_minhash_fails_nonpositive_ngram_size(num_hashes, ngram_size, seed):
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
        series.minhash(num_hashes, ngram_size, seed)


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
def test_minhash_empty_series(num_hashes, ngram_size, seed):
    series = Series.from_pylist([]).cast(DataType.string())

    minhash = series.minhash(num_hashes, ngram_size, seed).to_pylist()
    assert len(minhash) == 0


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
def test_minhash_seed_consistency(num_hashes, ngram_size, seed):
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

    minhash1 = series.minhash(num_hashes, ngram_size, seed).to_pylist()
    minhash2 = series.minhash(num_hashes, ngram_size, seed).to_pylist()
    assert minhash1 == minhash2


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed_pair", [[1, 2], [1, 5], [None, 2], [123, 234]])
def test_minhash_seed_differences(num_hashes, ngram_size, seed_pair):
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

    minhash1 = series.minhash(num_hashes, ngram_size, seed_pair[0]).to_pylist()
    minhash2 = series.minhash(num_hashes, ngram_size, seed_pair[1]).to_pylist()
    assert minhash1 != minhash2
