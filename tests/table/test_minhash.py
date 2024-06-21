import numpy as np
import pytest

import daft
from daft import col


@pytest.mark.parametrize("num_hashes", [1, 2, 16, 128])
@pytest.mark.parametrize("ngram_size", [1, 2, 4, 5, 100])
@pytest.mark.parametrize("hash_seed", [1, -1, 123, None])
def test_table_expr_minhash(num_hashes, ngram_size, hash_seed):
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
    np_rng = np.random.default_rng(123)
    permutations = np_rng.integers(1, (1 << 32) - 1, num_hashes * 2)

    res = df.select(col("data").minhash(num_hashes, ngram_size, permutations.tolist(), hash_seed))
    minhash = res.to_pydict()["data"]
    assert minhash[4] is None and minhash[-1] is None
    for lst in minhash:
        if lst is not None:
            assert len(lst) == num_hashes
    for i in range(num_hashes):
        assert minhash[0][i] == minhash[2][i]
        if ngram_size > 1:
            assert minhash[0][i] != minhash[1][i]
