from __future__ import annotations

import pytest

import daft
from tests.utils import sort_pydict


def test_joins_using():
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    df2 = daft.from_pydict({"idx": [1, 2], "score": [0.1, 0.2]})

    df_sql = daft.sql("select * from df1 join df2 using (idx)")
    actual = df_sql.collect().to_pydict()

    expected = df1.join(df2, on="idx").collect().to_pydict()

    assert sort_pydict(actual, "idx") == sort_pydict(expected, "idx")


def test_joins_with_alias():
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    df2 = daft.from_pydict({"idx": [1, 2], "score": [0.1, 0.2]})

    bindings = {"df1": df1, "df2": df2}

    df_sql = daft.sql("select * from df1 as foo join df2 as bar on foo.idx=bar.idx where bar.score>0.1", **bindings)

    actual = df_sql.collect().to_pydict()

    expected = {"idx": [2], "val": [20], "bar.idx": [2], "score": [0.2]}

    assert actual == expected


def test_joins_with_spaceship():
    df1 = daft.from_pydict({"idx": [1, 2, None], "val": [10, 20, 30]})
    df2 = daft.from_pydict({"idx": [1, 2, None], "score": [0.1, 0.2, None]})

    bindings = {"df1": df1, "df2": df2}
    df_sql = daft.sql("select idx, val, score from df1 join df2 on (df1.idx<=>df2.idx)", **bindings)

    actual = df_sql.collect().to_pydict()

    expected = {"idx": [1, 2, None], "val": [10, 20, 30], "score": [0.1, 0.2, None]}

    assert sort_pydict(actual, "idx", ascending=True) == expected


def test_joins_with_wildcard_expansion():
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    df2 = daft.from_pydict({"idx": [3], "score": [0.1]})
    df3 = daft.from_pydict({"idx": [1], "score": [0.1], "a": [1], "b": [2], "c": [3]})

    bindings = {"df1": df1, "df2": df2, "df3": df3}

    df_sql = (
        daft.sql(
            """
        select df3.*
        from df1
        left join df2 on (df1.idx=df2.idx)
        left join df3 on (df1.idx=df3.idx)
        """,
            **bindings,
        )
        .collect()
        .to_pydict()
    )

    expected = {"idx": [1, None], "score": [0.1, None], "a": [1, None], "b": [2, None], "c": [3, None]}

    assert sort_pydict(df_sql, "idx") == expected
    # make sure it works with exclusion patterns too

    df_sql = (
        daft.sql("""
        select df3.* EXCLUDE (a,b,c)
        from df1
        left join df2 on (df1.idx=df2.idx)
        left join df3 on (df1.idx=df3.idx)
        """)
        .collect()
        .to_pydict()
    )

    expected = {"idx": [1, None], "score": [0.1, None]}

    assert sort_pydict(df_sql, "idx") == expected


def test_joins_with_duplicate_columns():
    table1 = daft.from_pydict({"id": [1, 2, 3, 4], "value": ["a", "b", "c", "d"]})

    table2 = daft.from_pydict({"id": [2, 3, 4, 5], "value": ["b", "c", "d", "e"]})

    bindings = {"table1": table1, "table2": table2}

    actual = daft.sql(
        """
        SELECT *
        FROM table1 t1
        LEFT JOIN table2 t2 on t2.id = t1.id
        ORDER BY t1.id;
        """,
        **bindings,
    ).collect()

    expected = {
        "id": [1, 2, 3, 4],
        "value": ["a", "b", "c", "d"],
        "t2.id": [None, 2, 3, 4],
        "t2.value": [None, "b", "c", "d"],
    }

    assert sort_pydict(actual.to_pydict(), "id", ascending=True) == expected


@pytest.mark.parametrize(
    "join_condition",
    [
        "x = y",
        "x = b.y",
        "y = x",
        "y = a.x",
        "a.x = y",
        "a.x = b.y",
        "b.y = x",
        "b.y = a.x",
    ],
)
@pytest.mark.parametrize("selection", ["*", "a.*, b.y, b.score", "a.x, a.val, b.*", "a.x, a.val, b.y, b.score"])
def test_join_qualifiers(join_condition, selection):
    a = daft.from_pydict({"x": [1, None], "val": [10, 20]})
    b = daft.from_pydict({"y": [1, None], "score": [0.1, 0.2]})

    bindings = {"a": a, "b": b}

    df_sql = daft.sql(f"select {selection} from a join b on {join_condition}", **bindings).to_pydict()

    expected = {"x": [1], "val": [10], "y": [1], "score": [0.1]}

    assert df_sql == expected


@pytest.mark.parametrize(
    "join_condition",
    [
        "x = y",
        "x = b1.y",
        "y = x",
        "y = a1.x",
        "a1.x = y",
        "a1.x = b1.y",
        "b1.y = x",
        "b1.y = a1.x",
    ],
)
@pytest.mark.parametrize(
    "selection", ["*", "a1.*, b1.y, b1.score", "a1.x, a1.val, b1.*", "a1.x, a1.val, b1.y, b1.score"]
)
def test_join_qualifiers_with_alias(join_condition, selection):
    a = daft.from_pydict({"x": [1, None], "val": [10, 20]})
    b = daft.from_pydict({"y": [1, None], "score": [0.1, 0.2]})

    bindings = {"a": a, "b": b}

    df_sql = daft.sql(f"select {selection} from a as a1 join b as b1 on {join_condition}", **bindings).to_pydict()

    expected = {"x": [1], "val": [10], "y": [1], "score": [0.1]}

    assert df_sql == expected


@pytest.mark.parametrize("join_syntax", [",", "CROSS JOIN"])
def test_cross_join(join_syntax):
    x = daft.from_pydict(
        {
            "A": [1, 3, 5],
            "B": ["a", "b", "c"],
        },
    )

    y = daft.from_pydict(
        {
            "C": [2, 4, 6, 8],
            "D": ["d", "e", "f", "g"],
        },
    )

    bindings = {"x": x, "y": y}

    df = daft.sql(f"select * from x {join_syntax} y order by A, C", **bindings)

    assert df.to_pydict() == {
        "A": [1, 1, 1, 1, 3, 3, 3, 3, 5, 5, 5, 5],
        "B": ["a", "a", "a", "a", "b", "b", "b", "b", "c", "c", "c", "c"],
        "C": [2, 4, 6, 8, 2, 4, 6, 8, 2, 4, 6, 8],
        "D": ["d", "e", "f", "g", "d", "e", "f", "g", "d", "e", "f", "g"],
    }


@pytest.mark.parametrize(
    "selection, right_id, right_value",
    [("*", "b.id", "b.value"), ("a.*, b.id AS right_id, b.value AS right_value", "right_id", "right_value")],
    ids=["star", "qualified"],
)
def test_cross_join_with_aliases_and_duplicate_columns(selection, right_id, right_value):
    left = daft.from_pydict({"id": [1, None], "value": ["a", "b"]})
    right = daft.from_pydict({"id": [1, 2], "value": ["c", "d"]})

    actual = daft.sql(
        f"""
        SELECT {selection}
        FROM left_table AS a CROSS JOIN right_table AS b
        ORDER BY a.id NULLS LAST, b.id
        """,
        left_table=left,
        right_table=right,
    ).to_pydict()

    assert actual == {
        "id": [1, 1, None, None],
        "value": ["a", "a", "b", "b"],
        right_id: [1, 2, 1, 2],
        right_value: ["c", "d", "c", "d"],
    }


@pytest.mark.parametrize(
    "from_clause, expected",
    [
        (
            "a CROSS JOIN b CROSS JOIN c",
            {"x": [1, 1, 1, 1, 2, 2, 2, 2], "y": [1, 1, 2, 2, 1, 1, 2, 2], "z": [1, 2, 1, 2, 1, 2, 1, 2]},
        ),
        (
            "a CROSS JOIN b JOIN c ON b.y = c.z",
            {"x": [1, 1, 2, 2], "y": [1, 2, 1, 2], "z": [1, 2, 1, 2]},
        ),
        (
            "a JOIN b ON a.x = b.y CROSS JOIN c",
            {"x": [1, 1, 2, 2], "y": [1, 1, 2, 2], "z": [1, 2, 1, 2]},
        ),
    ],
    ids=["cross-cross", "cross-then-join", "join-then-cross"],
)
def test_cross_join_chained(from_clause, expected):
    a = daft.from_pydict({"x": [1, 2]})
    b = daft.from_pydict({"y": [1, 2]})
    c = daft.from_pydict({"z": [1, 2]})

    actual = daft.sql(f"SELECT a.x, b.y, c.z FROM {from_clause} ORDER BY a.x, b.y, c.z", a=a, b=b, c=c)

    assert actual.to_pydict() == expected


def test_cross_join_with_aggregate_cte_and_filter():
    customer = daft.from_pydict({"c_acctbal": [-10.0, 0.0, 10.0, 30.0, None]})

    actual = daft.sql(
        """
        WITH t AS (SELECT AVG(c_acctbal) AS b FROM customer)
        SELECT COUNT(*) AS n
        FROM customer CROSS JOIN t
        WHERE c_acctbal > t.b
        """,
        customer=customer,
    ).to_pydict()

    assert actual == {"n": [2]}


def test_join_requires_constraint():
    a = daft.from_pydict({"id": [1]})
    b = daft.from_pydict({"id": [1]})

    with pytest.raises(Exception, match="JOIN without ON/USING not supported"):
        daft.sql("SELECT * FROM a JOIN b", a=a, b=b)
