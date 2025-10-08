from __future__ import annotations

import pytest

import daft


@pytest.fixture()
def df():
    return daft.from_pydict(
        {
            "text": ["g1", "g1", "g2", "g3", "g3", "g1"],
            "n": [1, 2, 3, 3, 4, 100],
        }
    )


def test_orderby_basic(df):
    df = daft.sql("""
        SELECT * from df order by n
    """)

    assert df.collect().to_pydict() == {
        "text": ["g1", "g1", "g2", "g3", "g3", "g1"],
        "n": [1, 2, 3, 3, 4, 100],
    }


def test_orderby_compound(df):
    df = daft.sql("""
        SELECT * from df order by n, text
    """)

    assert df.collect().to_pydict() == {
        "text": ["g1", "g1", "g2", "g3", "g3", "g1"],
        "n": [1, 2, 3, 3, 4, 100],
    }


def test_orderby_desc(df):
    df = daft.sql("""
        SELECT n from df order by n desc
    """)

    assert df.collect().to_pydict() == {
        "n": [100, 4, 3, 3, 2, 1],
    }


def test_orderby_groupby(df):
    df = daft.sql("""
        SELECT
            text,
            count(*) as count_star
        from df
        group by text
        order by count_star DESC
    """)

    assert df.collect().to_pydict() == {
        "text": ["g1", "g3", "g2"],
        "count_star": [3, 2, 1],
    }


def test_orderby_groupby_expr(df):
    df = daft.sql("""
SELECT
    text,
    count(*) as count_star
from df
group by text
order by count(*) DESC
    """)

    assert df.collect().to_pydict() == {"text": ["g1", "g3", "g2"], "count_star": [3, 2, 1]}


def test_groupby_orderby_non_final_expr(df):
    df = daft.sql("""
        SELECT
            text,
            count(*) as count_star
        from df
        group by text
        order by sum(n) ASC
    """)

    assert df.collect().to_pydict() == {
        "text": ["g2", "g3", "g1"],
        "count_star": [1, 2, 3],
    }


def test_groupby_orderby_count_star(df):
    df = daft.sql("""
        SELECT
            text,
            sum(n) as n
        from df
        group by text
        order by count(*) ASC
    """)

    assert df.collect().to_pydict() == {"text": ["g2", "g3", "g1"], "n": [3, 7, 103]}
