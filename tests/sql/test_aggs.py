import pytest

import daft
from daft import col
from daft.sql import SQLCatalog


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


@pytest.mark.parametrize(
    "agg,cond,expected",
    [
        ("sum(values)", "sum(values) > 10", {"values": [20.5, 29.5]}),
        ("sum(values)", "values > 10", {"values": [20.5, 29.5]}),
        ("sum(values) as sum_v", "sum(values) > 10", {"sum_v": [20.5, 29.5]}),
        ("sum(values) as sum_v", "sum_v > 10", {"sum_v": [20.5, 29.5]}),
        ("count(*) as cnt", "cnt > 2", {"cnt": [3, 5]}),
        ("count(*) as cnt", "count(*) > 2", {"cnt": [3, 5]}),
        ("count(*)", "count(*) > 2", {"count": [3, 5]}),
        ("count(*) as cnt", "sum(values) > 10", {"cnt": [3, 5]}),
        ("sum(values), count(*)", "id > 1", {"values": [10.0, 29.5], "count": [2, 5]}),
    ],
)
def test_having(agg, cond, expected):
    df = daft.from_pydict(
        {
            "id": [1, 2, 3, 3, 3, 3, 2, 1, 3, 1],
            "values": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5],
        }
    )
    catalog = SQLCatalog({"df": df})

    actual = daft.sql(
        f"""
    SELECT
    {agg},
    from df
    group by id
    having {cond}
    """,
        catalog,
    ).to_pydict()

    assert actual == expected


def test_having_non_grouped():
    df = daft.from_pydict(
        {
            "id": [1, 2, 3, 3, 3, 3, 2, 1, 3, 1],
            "values": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5],
            "floats": [0.01, 0.011, 0.01047, 0.02, 0.019, 0.018, 0.017, 0.016, 0.015, 0.014],
        }
    )
    catalog = SQLCatalog({"df": df})

    actual = daft.sql(
        """
    SELECT
    count(*) ,
    from df
    having sum(values) > 40
    """,
        catalog,
    ).to_pydict()

    assert actual == {"count": [10]}
