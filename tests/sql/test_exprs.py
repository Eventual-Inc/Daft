import datetime

import pytest

import daft
from daft import col, interval
from daft.sql.sql import SQLCatalog


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
        minhash(a, num_hashes:=10, ngram_size:= 100, seed:=10, hash_function:='xxhash') as minhash_a_xxhash,
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
            col("a").minhash(num_hashes=10, ngram_size=100, seed=10, hash_function="xxhash").alias("minhash_a_xxhash"),
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


def test_is_in_exprs():
    df = daft.from_pydict({"x": [1, 2, 3, 5, 9]})
    expected = {"x": [1, 2, 9]}
    catalog = SQLCatalog({"df": df})
    actual = daft.sql("select * from df where x in (0 + 1, 0 + 2, 0 + 9)", catalog).collect().to_pydict()

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
    with pytest.raises(Exception, match="arguments to be of the same type"):
        daft.sql("SELECT * FROM df WHERE nums IN (1, '2', 3.0)").collect().to_pydict()


@pytest.mark.parametrize(
    "date_values, ts_values, expected_intervals",
    [
        (
            ["2022-01-01", "2020-02-29", "2029-05-15"],
            ["2022-01-01 10:00:00", "2020-02-29 23:59:59", "2029-05-15 12:34:56"],
            {
                "date_add_day": [
                    datetime.date(2022, 1, 2),
                    datetime.date(2020, 3, 1),
                    datetime.date(2029, 5, 16),
                ],
                "date_sub_month": [
                    datetime.date(2021, 12, 1),
                    datetime.date(2020, 1, 31),
                    datetime.date(2029, 4, 14),
                ],
                "ts_sub_year": [
                    datetime.datetime(2021, 1, 1, 10),
                    datetime.datetime(2019, 2, 28, 23, 59, 59),
                    datetime.datetime(2028, 5, 15, 12, 34, 56),
                ],
                "ts_add_hour": [
                    datetime.datetime(2022, 1, 1, 11, 0, 0),
                    datetime.datetime(2020, 3, 1, 0, 59, 59),
                    datetime.datetime(2029, 5, 15, 13, 34, 56),
                ],
                "ts_sub_minute": [
                    datetime.datetime(2022, 1, 1, 9, 57, 21),
                    datetime.datetime(2020, 2, 29, 23, 57, 20),
                    datetime.datetime(2029, 5, 15, 12, 32, 17),
                ],
            },
        ),
    ],
)
def test_interval_comparison(date_values, ts_values, expected_intervals):
    # Create DataFrame with date and timestamp columns
    df = daft.from_pydict({"date": date_values, "ts": ts_values}).select(
        col("date").cast(daft.DataType.date()), col("ts").str.to_datetime("%Y-%m-%d %H:%M:%S")
    )
    catalog = SQLCatalog({"test": df})

    expected_df = (
        df.select(
            (col("date") + interval(days=1)).alias("date_add_day"),
            (col("date") - interval(months=1)).alias("date_sub_month"),
            (col("ts") - interval(years=1, days=0)).alias("ts_sub_year"),
            (col("ts") + interval(hours=1)).alias("ts_add_hour"),
            (col("ts") - interval(minutes=1, seconds=99)).alias("ts_sub_minute"),
        )
        .collect()
        .to_pydict()
    )

    actual_sql = (
        daft.sql(
            """
        SELECT
            date + INTERVAL '1' day AS date_add_day,
            date - INTERVAL '1 months' AS date_sub_month,
            ts - INTERVAL '1 year 0 days' AS ts_sub_year,
            ts + INTERVAL '1' hour AS ts_add_hour,
            ts - INTERVAL '1 minutes 99 second' AS ts_sub_minute
        FROM test
        """,
            catalog=catalog,
        )
        .collect()
        .to_pydict()
    )

    assert expected_df == actual_sql == expected_intervals
