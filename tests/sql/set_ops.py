import pytest

import daft
from daft import col
from daft.sql import SQLCatalog


def test_simple_intersect(make_df):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"bar": [2, 3, 4]})
    expected = df1.intersect(df2).to_pydict()
    actual = daft.sql("select * from df1 intersect select * from df2").to_pydict()

    assert actual == expected


def test_intersect_with_duplicate(make_df):
    df1 = make_df({"foo": [1, 2, 2, 3]})
    df2 = make_df({"bar": [2, 3, 3]})
    expected = df1.intersect(df2).to_pydict()
    actual = daft.sql("select * from df1 intersect select * from df2").to_pydict()
    assert actual == expected


def test_self_intersect(make_df):
    df = make_df({"foo": [1, 2, 3]})
    expected = df.intersect(df).sort(by="foo")
    actual = daft.sql("select * from df intersect select * from df").sort(by="foo")
    assert actual.to_pydict() == expected.to_pydict()


def test_intersect_empty(make_df):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"bar": []}).select(col("bar").cast(daft.DataType.int64()))
    expected = df1.intersect(df2).to_pydict()
    actual = daft.sql("select * from df1 intersect select * from df2").to_pydict()
    assert actual == expected


def test_intersect_with_nulls(make_df):
    df1 = make_df({"foo": [1, 2, None]})
    df1_without_mull = make_df({"foo": [1, 2]})
    df2 = make_df({"bar": [2, 3, None]})
    df2_without_null = make_df({"bar": [2, 3]})

    expected = df1.intersect(df2)
    actual = daft.sql("select * from df1 intersect select * from df2")
    assert actual.to_pydict() == expected.to_pydict()

    expected = df1_without_mull.intersect(df2)
    actual = daft.sql("select * from df1_without_mull intersect select * from df2")
    assert actual.to_pydict() == expected.to_pydict()

    expected = df1.intersect(df2_without_null)
    actual = daft.sql("select * from df1 intersect select * from df2_without_null")
    assert actual.to_pydict() == expected.to_pydict()


def test_union_simple(make_df):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"foo": [2, 3, 4]})
    expected = df1.concat(df2).distinct().sort(by="foo").to_pydict()
    actual = daft.sql(
        "with unioned as (select * from df1 union select * from df2) select * from unioned order by foo"
    ).to_pydict()

    assert actual == expected


def test_union_all_simple(make_df):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"foo": [2, 3, 4]})
    expected = df1.concat(df2).to_pydict()
    actual = daft.sql("select * from df1 union all select * from df2").to_pydict()

    assert actual == expected


def test_union_with_nulls(make_df):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"foo": [None]})

    expected = {"foo": [1, 2, 3, None]}
    catalog = SQLCatalog({"df1": df1, "df2": df2})
    actual = daft.sql("select * from df1 union select * from df2", catalog).to_pydict()
    assert actual == expected


def test_union_with_implicit_type_coercion(make_df):
    df1 = make_df({"foo": [1, 2, 3, 0, 1, 1]})
    df2 = make_df({"foo": ["hello", "1"]})
    catalog = SQLCatalog({"df1": df1, "df2": df2})
    expected = {"foo": ["0", "1", "2", "3", "hello"]}
    actual = daft.sql("select * from df1 union select * from df2", catalog).sort(by="foo").to_pydict()

    expected = {"foo": ["0", "1", "1", "1", "1", "2", "3", "hello"]}
    actual = daft.sql("select * from df1 union all select * from df2", catalog).sort(by="foo").to_pydict()
    assert actual == expected


def test_union_with_failed_implicit_type_coercion(make_df):
    df1 = make_df({"foo": [[1, 2, 3, 0, 1, 1], [None]]})
    df2 = make_df({"foo": ["hello", "1"]})
    catalog = SQLCatalog({"df1": df1, "df2": df2})

    with pytest.raises(Exception, match=r"unable to find a common supertype for union"):
        daft.sql("select * from df1 union all select * from df2", catalog).sort(by="foo").to_pydict()
