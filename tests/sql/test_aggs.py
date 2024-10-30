import daft
from daft import col


def test_aggs_sql():
    df = daft.from_pydict(
        {
            "id": [1, 2, 3, 3, 3, 3, 2, 1, 3, 1],
            "values": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5],
            "floats": [0.01, 0.011, 0.01047, 0.02, 0.019, 0.018, 0.017, 0.016, 0.015, 0.014],
        }
    )
    expected = (
        df.agg(
            [
                col("values").sum().alias("sum"),
                col("values").mean().alias("mean"),
                col("values").min().alias("min"),
                col("values").max().alias("max"),
                col("values").count().alias("count"),
                col("values").stddev().alias("std"),
            ]
        )
        .collect()
        .to_pydict()
    )

    actual = (
        daft.sql("""
    SELECT
        sum(values) as sum,
        mean(values) as mean,
        min(values) as min,
        max(values) as max,
        count(values) as count,
        stddev(values) as std
    FROM df
    """)
        .collect()
        .to_pydict()
    )

    assert actual == expected
