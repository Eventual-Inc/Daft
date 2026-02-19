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


def test_orderby_positional_basic(df):
    df = daft.sql("""
        SELECT text, n from df order by 2
    """)

    assert df.collect().to_pydict() == {
        "text": ["g1", "g1", "g2", "g3", "g3", "g1"],
        "n": [1, 2, 3, 3, 4, 100],
    }


def test_orderby_positional_desc(df):
    df = daft.sql("""
        SELECT n from df order by 1 desc
    """)

    assert df.collect().to_pydict() == {
        "n": [100, 4, 3, 3, 2, 1],
    }


def test_orderby_positional_out_of_range(df):
    with pytest.raises(Exception):
        df = daft.sql("""
            SELECT text, n from df order by 3
        """)
        df.collect()


def test_orderby_positional_zero(df):
    with pytest.raises(Exception):
        df = daft.sql("""
            SELECT text, n from df order by 0
        """)
        df.collect()
