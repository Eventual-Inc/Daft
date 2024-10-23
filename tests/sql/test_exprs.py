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


def test_count_star():
    df = daft.from_pydict(
        {
            "a": [1, 2, 3, 4],
        }
    )

    actual = daft.sql("SELECT COUNT(*) FROM df").collect()
    expected = df.agg(daft.col("*").count().alias("count")).collect()
    assert actual.to_pydict() == expected.to_pydict()


def test_between():
    df = daft.from_pydict(
        {
            "integers": [0, 1, 2, 3, 5],
        }
    )

    actual = daft.sql("SELECT * FROM df where integers between 1 and 4").collect().to_pydict()

    expected = df.filter(col("integers").between(1, 4)).collect().to_pydict()
    assert actual == expected


def test_is_in():
    df = daft.from_pydict({"idx": [1, 2, 3], "val": ["foo", "bar", "baz"]})
    expected = df.filter(col("val").is_in(["bar", "foo"])).collect().to_pydict()
    actual = daft.sql("select * from df where val in ('bar','foo')").collect().to_pydict()
    assert actual == expected

    # test negated too
    expected = df.filter(~col("val").is_in(["bar", "foo"])).collect().to_pydict()
    actual = daft.sql("select * from df where val not in ('bar','foo')").collect().to_pydict()
    assert actual == expected


def test_is_in_edge_cases():
    df = daft.from_pydict(
        {
            "nums": [1, 2, 3, None, 4, 5],
            "strs": ["a", "b", None, "c", "d", "e"],
        }
    )

    # Test with NULL values in the column
    actual = daft.sql("SELECT * FROM df WHERE strs IN ('a', 'b')").collect().to_pydict()
    expected = df.filter(col("strs").is_in(["a", "b"])).collect().to_pydict()
    assert actual == expected

    # Test with empty IN list
    with pytest.raises(Exception, match="Expected: an expression"):
        daft.sql("SELECT * FROM df WHERE nums IN ()").collect()

    # Test with numbers and NULL in IN list
    actual = daft.sql("SELECT * FROM df WHERE nums IN (1, NULL, 3)").collect().to_pydict()
    expected = df.filter(col("nums").is_in([1, None, 3])).collect().to_pydict()
    assert actual == expected

    # Test with single value
    actual = daft.sql("SELECT * FROM df WHERE nums IN (1)").collect().to_pydict()
    expected = df.filter(col("nums").is_in([1])).collect().to_pydict()

    # Test with mixed types in the IN list
    with pytest.raises(Exception, match="All literals must have the same data type"):
        daft.sql("SELECT * FROM df WHERE nums IN (1, '2', 3.0)").collect().to_pydict()
