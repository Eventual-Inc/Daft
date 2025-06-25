from __future__ import annotations

import pytest

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
                col("values").count_distinct().alias("count_distinct"),
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
        count(distinct values) as count_distinct,
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
        ("sum(values)", "sum(values) > 10", {"values": [22.0, 29.5]}),
        ("sum(values)", "values > 10", {"values": [22.0, 29.5]}),
        ("sum(values) as sum_v", "sum(values) > 10", {"sum_v": [22.0, 29.5]}),
        ("sum(values) as sum_v", "sum_v > 10", {"sum_v": [22.0, 29.5]}),
        ("count(*) as cnt", "cnt > 2", {"cnt": [4, 5]}),
        ("count(*) as cnt", "count(*) > 2", {"cnt": [4, 5]}),
        ("count(1) as cnt", "cnt > 2", {"cnt": [4, 5]}),
        ("count(1) as cnt", "count(1) > 2", {"cnt": [4, 5]}),
        ("count(*)", "count(*) > 2", {"count": [4, 5]}),
        ("count(1)", "count(1) > 2", {"count": [4, 5]}),
        ("count(*) as cnt", "sum(values) > 10", {"cnt": [4, 5]}),
        ("count(1) as cnt", "sum(values) > 10", {"cnt": [4, 5]}),
        ("count(true) as cnt", "sum(values) > 10", {"cnt": [4, 5]}),
        ("count(false) as cnt", "sum(values) > 10", {"cnt": [4, 5]}),
        ("count([1]) as cnt", "sum(values) > 10", {"cnt": [4, 5]}),
        ("count(null) as null", "sum(values) > 10", {"null": [0, 0]}),
        # duplicates of the above 4 `count` tests but for count-distinct
        ("count(distinct values) as count_distinct", "count_distinct > 2", {"count_distinct": [3, 5]}),
        ("count(distinct values) as count_distinct", "count(distinct values) > 2", {"count_distinct": [3, 5]}),
        ("count(distinct values)", "count(distinct values) > 2", {"values": [3, 5]}),
        ("count(distinct values) as count_distinct", "sum(values) > 10", {"count_distinct": [3, 5]}),
        ("sum(values), count(*)", "id > 1", {"values": [10.0, 29.5], "count": [2, 5]}),
    ],
)
def test_having(agg, cond, expected):
    df = daft.from_pydict(
        {
            "id": [1, 2, 3, 3, 3, 3, 2, 1, 3, 1, 1],
            "values": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 1.5],
        }
    )
    bindings = {"df": df}

    actual = daft.sql(
        f"""
    SELECT
    {agg},
    from df
    group by id
    having {cond}
    order by id
    """,
        **bindings,
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
    bindings = {"df": df}

    actual = daft.sql(
        """
    SELECT
    count(*) ,
    from df
    having sum(values) > 40
    """,
        **bindings,
    ).to_pydict()

    assert actual == {"count": [10]}


def test_simple_rollup():
    data = {"dept": ["IT", "IT", "HR", "HR"], "year": [2022, 2023, 2022, 2023], "cost": [100, 200, 300, 400]}
    expected = {"dept": ["HR", "IT", None], "year": [None, None, None], "total": [700, 300, 1000]}
    df = daft.from_pydict(data)
    bindings = {"df": df}
    sql = """
    SELECT
        dept,
        year,
        SUM(cost) as total
    FROM df
    GROUP BY ROLLUP(dept, year)
    HAVING year IS NULL
    ORDER BY dept NULLS LAST
    """
    actual = daft.sql(sql, **bindings).to_pydict()
    assert actual == expected


def test_rollup_null_handling():
    data = {"dept": ["IT", "IT", "HR", "HR"], "year": [2022, 2023, 2022, 2023], "cost": [100, 200, 300, 400]}
    expected = {"dept": ["HR", "IT", None], "year": [None, None, None], "total": [700, 300, 1000]}
    df = daft.from_pydict(data)
    bindings = {"df": df}

    sql = """
    SELECT dept, year, SUM(cost) as total
    FROM df
    GROUP BY ROLLUP(dept, year)
    HAVING year IS NULL
    ORDER BY dept NULLS LAST
    """
    actual = daft.sql(sql, **bindings).to_pydict()
    assert actual == expected


def test_rollup_multiple_aggs():
    data = {
        "region": ["East", "East", "West", "West"],
        "product": ["A", "B", "A", "B"],
        "qty": [10, 20, 30, 40],
        "price": [5, 8, 6, 9],
    }
    expected = {"region": ["East", "West", None], "total_qty": [30, 70, 100], "avg_price": [6.5, 7.5, 7.0]}

    df = daft.from_pydict(data)
    bindings = {"df": df}

    sql = """
    SELECT
        region,
        SUM(qty) as total_qty,
        AVG(price) as avg_price
    FROM df
    GROUP BY ROLLUP(region, product)
    HAVING product IS NULL
    ORDER BY region NULLS LAST
    """
    actual = daft.sql(sql, **bindings).to_pydict()
    assert actual == expected


def test_groupby_rollup():
    data = {
        "region": ["EMEA", "EMEA", "APAC", "APAC", "AMER", "AMER"],
        "product": ["HW", "SW", "HW", "SW", "HW", "SW"],
        "channel": ["Direct", "Partner", "Direct", "Partner", "Direct", "Partner"],
        "sales": [100, 200, 300, 400, 500, 600],
        "costs": [50, 80, 120, 160, 200, 240],
    }
    expected = {
        "region": ["AMER", "All Regions"],
        "product": ["All Products", "All Products"],
        "channel": ["All Channels", "All Channels"],
        "total_sales": [1100, 2100],
        "total_costs": [440, 850],
        "profit": [660, 1250],
        "roi": [2.5, 2.4705882352941178],
    }
    df = daft.from_pydict(data)
    bindings = {"sales": df}

    sql = """
    SELECT
        COALESCE(region, 'All Regions') as region,
        COALESCE(product, 'All Products') as product,
        COALESCE(channel, 'All Channels') as channel,
        SUM(sales) as total_sales,
        SUM(costs) as total_costs,
        SUM(sales - costs) as profit,
        SUM(sales) / SUM(costs) as roi
    FROM df
    GROUP BY ROLLUP(region, product, channel)
    HAVING SUM(sales) > 1000
    ORDER BY region, product, channel;
    """
    actual = daft.sql(sql, **bindings).to_pydict()
    assert actual == expected


@pytest.mark.parametrize(
    "query",
    [
        "SELECT capitalize(strings) as s, count(*) from data group by s",
        "SELECT capitalize(strings) as s, count(*) from data group by capitalize(strings)",
        "SELECT strings, count(*) from data group by capitalize(strings)",
        "SELECT strings as s, count(*) from data group by capitalize(strings)",
    ],
)
def test_various_groupby_positions(query):
    data = daft.from_pydict({"strings": ["foo", "bar"]})
    res = daft.sql(query, **{"data": data})
    if "s" in res.column_names:
        res = res.sort("s").to_pydict()
        assert res == {"s": ["Bar", "Foo"], "count": [1, 1]}
    else:
        res = res.sort("strings").to_pydict()
        assert res == {"strings": ["Bar", "Foo"], "count": [1, 1]}


def test_group_by_with_lit_selection():
    df = daft.from_pydict({"ids": [1, 1, 2, 2, 3, 4], "values": [10, 20, 30, 40, 50, 60]})

    res = daft.sql("select 0 from df group by ids", df=df)
    expected = {"literal": [0, 0, 0, 0]}
    assert res.to_pydict() == expected
