from __future__ import annotations

import daft


def test_sql_default_column_name_agg() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})

    res = daft.sql("select sum(a) from df", df=df).to_pydict()

    assert res == {"sum(a)": [6]}


def test_sql_default_column_name_count() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})

    res = daft.sql("select count(*) from df", df=df).to_pydict()

    assert res == {"count(*)": [3]}

    res = daft.sql("select count(1) from df", df=df).to_pydict()

    assert res == {"count(1)": [3]}

    res = daft.sql("select count(a) from df", df=df).to_pydict()

    assert res == {"count(a)": [3]}


def test_sql_default_column_name_non_agg_functions() -> None:
    df = daft.from_pydict({"a": ["AbC", None]})

    res = daft.sql("select upper(a) from df", df=df).to_pydict()
    assert res == {"upper(a)": ["ABC", None]}

    res = daft.sql("select coalesce(a, 'x') from df", df=df).to_pydict()
    assert res == {'coalesce(a, "x")': ["AbC", "x"]}

    # String concatenation operator `||` is not supported; use concat().
    res = daft.sql("select concat(upper(a), 'x') from df", df=df).to_pydict()
    assert res == {'concat(upper(a), "x")': ["ABCx", None]}


def test_sql_default_column_name_binary_op_is_parenthesized() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})

    res = daft.sql("select a + 1 from df", df=df).to_pydict()

    assert res == {"(a + 1)": [2, 3, 4]}


def test_sql_default_column_name_string_literal_is_quoted() -> None:
    df = daft.from_pydict({"a": [1]})

    res = daft.sql("select 'a' from df", df=df).to_pydict()

    assert res == {'"a"': ["a"]}


def test_sql_explicit_alias_overrides_default_name() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})

    res = daft.sql("select sum(a) as s from df", df=df).to_pydict()

    assert res == {"s": [6]}

    res = daft.sql("select sum(a) as S from df", df=df).to_pydict()

    assert res == {"S": [6]}
