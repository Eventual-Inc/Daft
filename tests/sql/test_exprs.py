import datetime

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


def test_interval():
    df = daft.from_pydict(
        {
            "date": ["2021-01-01", "2021-01-02", "2021-01-03"],
            "ts": ["2021-01-01 01:28:40", "2021-01-02 12:12:12", "2011-11-11 11:11:11"],
        }
    ).select(daft.col("date").cast(daft.DataType.date()), daft.col("ts").str.to_datetime("%Y-%m-%d %H:%M:%S"))

    def interval(unit):
        if unit == "year":
            td = datetime.timedelta(days=365)
        else:
            td = datetime.timedelta(**{unit: 1})

        total_microseconds = int(td.total_seconds() * 1_000_000)
        return daft.lit(total_microseconds).cast(daft.DataType.duration("us"))

    actual = (
        daft.sql("""
        SELECT
            date + interval '1' day as date_add_day,
            date + interval '1' week as date_add_week,
            date + interval '1' year as date_add_year,
            date - interval '1' day as date_sub_day,
            date - interval '1' week as date_sub_week,
            date - interval '1' year as date_sub_year,

            ts + interval '1' millisecond as ts_add_millisecond,
            ts + interval '1' second as ts_add_second,
            ts + interval '1' minute as ts_add_minute,
            ts + interval '1' hour as ts_add_hour,
            ts + interval '1' day as ts_add_day ,
            ts + interval '1' week as ts_add_week,
            ts + interval '1' year as ts_add_year,

            ts - interval '1' millisecond as ts_sub_millisecond,
            ts - interval '1' second as ts_sub_second,
            ts - interval '1' minute as ts_sub_minute,
            ts - interval '1' hour as ts_sub_hour,
            ts - interval '1' day as ts_sub_day,
            ts - interval '1' week as ts_sub_week,
            ts - interval '1' year as ts_sub_year,
        FROM df
    """)
        .collect()
        .to_pydict()
    )

    expected = (
        df.select(
            (col("date") + interval("days")).alias("date_add_day"),
            (col("date") + interval("weeks")).alias("date_add_week"),
            (col("date") + interval("year")).alias("date_add_year"),
            (col("date") - interval("days")).alias("date_sub_day"),
            (col("date") - interval("weeks")).alias("date_sub_week"),
            (col("date") - interval("year")).alias("date_sub_year"),
            (col("ts") + interval("milliseconds")).alias("ts_add_millisecond"),
            (col("ts") + interval("seconds")).alias("ts_add_second"),
            (col("ts") + interval("minutes")).alias("ts_add_minute"),
            (col("ts") + interval("hours")).alias("ts_add_hour"),
            (col("ts") + interval("days")).alias("ts_add_day"),
            (col("ts") + interval("weeks")).alias("ts_add_week"),
            (col("ts") + interval("year")).alias("ts_add_year"),
            (col("ts") - interval("milliseconds")).alias("ts_sub_millisecond"),
            (col("ts") - interval("seconds")).alias("ts_sub_second"),
            (col("ts") - interval("minutes")).alias("ts_sub_minute"),
            (col("ts") - interval("hours")).alias("ts_sub_hour"),
            (col("ts") - interval("days")).alias("ts_sub_day"),
            (col("ts") - interval("weeks")).alias("ts_sub_week"),
            (col("ts") - interval("year")).alias("ts_sub_year"),
        )
        .collect()
        .to_pydict()
    )

    assert actual == expected
