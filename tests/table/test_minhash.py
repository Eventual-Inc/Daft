import pytest

import daft
from daft import col


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("seed", [1, -1, 123, None])
def test_table_expr_minhash(num_hashes, ngram_size, seed):
    df = daft.from_pydict(
        {
            "data": [
                "The quick brown fox",
                "The speedy orange fox",
                "The quick brown fox",
                "thisonlyhasonetokenohno",
                None,
                "This has more than four tokens, unlike the other strings",
                "!@# $%^&*() -` 1235 827 9387 216340",
                None,
            ]
        }
    )

    res = None
    if seed is None:
        res = df.select(col("data").minhash(num_hashes, ngram_size))
    else:
        res = df.select(col("data").minhash(num_hashes, ngram_size, seed))
    minhash = res.to_pydict()["data"]
    assert minhash[4] is None and minhash[-1] is None
    for lst in minhash:
        if lst is not None:
            assert len(lst) == num_hashes
    for i in range(num_hashes):
        assert minhash[0][i] == minhash[2][i]
        if ngram_size > 1:
            assert minhash[0][i] != minhash[1][i]
