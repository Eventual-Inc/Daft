import pytest

import daft
from daft.sql import SQLCatalog


def test_joins_using():
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    df2 = daft.from_pydict({"idx": [1, 2], "score": [0.1, 0.2]})

    df_sql = daft.sql("select * from df1 join df2 using (idx)")
    actual = df_sql.collect().to_pydict()

    expected = df1.join(df2, on="idx").collect().to_pydict()

    assert actual == expected


def test_joins_with_alias():
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    df2 = daft.from_pydict({"idx": [1, 2], "score": [0.1, 0.2]})

    catalog = SQLCatalog({"df1": df1, "df2": df2})

    df_sql = daft.sql("select * from df1 as foo join df2 as bar on foo.idx=bar.idx where bar.score>0.1", catalog)

    actual = df_sql.collect().to_pydict()

    expected = {"idx": [2], "val": [20], "bar.idx": [2], "score": [0.2]}

    assert actual == expected


def test_joins_with_spaceship():
    df1 = daft.from_pydict({"idx": [1, 2, None], "val": [10, 20, 30]})
    df2 = daft.from_pydict({"idx": [1, 2, None], "score": [0.1, 0.2, None]})

    catalog = SQLCatalog({"df1": df1, "df2": df2})
    df_sql = daft.sql("select idx, val, score from df1 join df2 on (df1.idx<=>df2.idx)", catalog=catalog)

    actual = df_sql.collect().to_pydict()

    expected = {"idx": [1, 2, None], "val": [10, 20, 30], "score": [0.1, 0.2, None]}

    assert actual == expected


def test_joins_with_wildcard_expansion():
    df1 = daft.from_pydict({"idx": [1, 2], "val": [10, 20]})
    df2 = daft.from_pydict({"idx": [3], "score": [0.1]})
    df3 = daft.from_pydict({"idx": [1], "score": [0.1], "a": [1], "b": [2], "c": [3]})

    catalog = SQLCatalog({"df1": df1, "df2": df2, "df3": df3})

    df_sql = (
        daft.sql(
            """
        select df3.*
        from df1
        left join df2 on (df1.idx=df2.idx)
        left join df3 on (df1.idx=df3.idx)
        """,
            catalog,
        )
        .collect()
        .to_pydict()
    )

    expected = {"idx": [1, None], "score": [0.1, None], "a": [1, None], "b": [2, None], "c": [3, None]}

    assert df_sql == expected
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

    assert df_sql == expected


def test_joins_with_duplicate_columns():
    table1 = daft.from_pydict({"id": [1, 2, 3, 4], "value": ["a", "b", "c", "d"]})

    table2 = daft.from_pydict({"id": [2, 3, 4, 5], "value": ["b", "c", "d", "e"]})

    catalog = SQLCatalog({"table1": table1, "table2": table2})

    actual = daft.sql(
        """
        SELECT *
        FROM table1 t1
        LEFT JOIN table2 t2 on t2.id = t1.id;
        """,
        catalog,
    ).collect()

    expected = {
        "id": [1, 2, 3, 4],
        "value": ["a", "b", "c", "d"],
        "t2.id": [None, 2, 3, 4],
        "t2.value": [None, "b", "c", "d"],
    }

    assert actual.to_pydict() == expected


@pytest.mark.parametrize("join_condition", ["idx=idax", "idax=idx"])
def test_joins_without_compound_ident(join_condition):
    df1 = daft.from_pydict({"idx": [1, None], "val": [10, 20]})
    df2 = daft.from_pydict({"idax": [1, None], "score": [0.1, 0.2]})

    catalog = SQLCatalog({"df1": df1, "df2": df2})

    df_sql = daft.sql(f"select * from df1 join df2 on {join_condition}", catalog).to_pydict()

    expected = {"idx": [1], "val": [10], "idax": [1], "score": [0.1]}

    assert df_sql == expected
