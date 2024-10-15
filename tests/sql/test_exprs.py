import pytest

import daft
from daft import col


def test_nested():
    df = daft.from_pydict(
        {
            "A": [1, 2, 3, 4],
            "B": [1.5, 2.5, 3.5, 4.5],
            "C": [True, True, False, False],
            "D": [None, None, None, None],
        }
    )

    actual = daft.sql("SELECT (A + 1) AS try_this FROM df").collect()
    expected = df.select((daft.col("A") + 1).alias("try_this")).collect()

    assert actual.to_pydict() == expected.to_pydict()

    actual = daft.sql("SELECT *, (A + 1) AS try_this FROM df").collect()
    expected = df.with_column("try_this", df["A"] + 1).collect()

    assert actual.to_pydict() == expected.to_pydict()


def test_hash_exprs():
    df = daft.from_pydict(
        {
            "a": ["foo", "bar", "baz", "qux"],
            "ints": [1, 2, 3, 4],
            "floats": [1.5, 2.5, 3.5, 4.5],
        }
    )

    actual = (
        daft.sql("""
    SELECT
        hash(a) as hash_a,
        hash(a, 0) as hash_a_0,
        hash(a, seed:=0) as hash_a_seed_0,
        minhash(a, num_hashes:=10, ngram_size:= 100, seed:=10) as minhash_a,
        minhash(a, num_hashes:=10, ngram_size:= 100) as minhash_a_no_seed,
    FROM df
    """)
        .collect()
        .to_pydict()
    )

    expected = (
        df.select(
            col("a").hash().alias("hash_a"),
            col("a").hash(0).alias("hash_a_0"),
            col("a").hash(seed=0).alias("hash_a_seed_0"),
            col("a").minhash(num_hashes=10, ngram_size=100, seed=10).alias("minhash_a"),
            col("a").minhash(num_hashes=10, ngram_size=100).alias("minhash_a_no_seed"),
        )
        .collect()
        .to_pydict()
    )

    assert actual == expected

    with pytest.raises(Exception, match="Invalid arguments for minhash"):
        daft.sql("SELECT minhash() as hash_a FROM df").collect()

    with pytest.raises(Exception, match="num_hashes is required"):
        daft.sql("SELECT minhash(a) as hash_a FROM df").collect()
