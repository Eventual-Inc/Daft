from __future__ import annotations

import re

import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.functions import format, uuid


def test_uuid_column_generation(make_df) -> None:
    data = {"a": list(range(200))}
    df = make_df(data).with_column("uuid", uuid()).collect()

    assert len(df) == 200
    assert df.schema()["uuid"].dtype == DataType.string()

    values = df.to_pydict()["uuid"]
    assert len(set(values)) == 200
    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    assert all(isinstance(v, str) and uuid_re.match(v) is not None for v in values)


def test_uuid_empty_table(make_df) -> None:
    data = {"a": []}
    df = make_df(data).with_column("uuid", uuid()).collect()
    assert len(df) == 0
    assert df.schema()["uuid"].dtype == DataType.string()
    assert df.to_pydict()["uuid"] == []


def test_uuid_with_multiple_columns(make_df) -> None:
    data = {"a": list(range(200))}
    df = make_df(data).with_column("u1", uuid()).with_column("u2", uuid()).collect()

    assert len(df) == 200
    assert df.schema()["u1"].dtype == DataType.string()
    assert df.schema()["u2"].dtype == DataType.string()

    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    values1 = df.to_pydict()["u1"]
    values2 = df.to_pydict()["u2"]
    assert len(set(values1)) == 200
    assert len(set(values2)) == 200
    assert any(v1 != v2 for v1, v2 in zip(values1, values2))
    assert all(isinstance(v, str) and uuid_re.match(v) is not None for v in values1)
    assert all(isinstance(v, str) and uuid_re.match(v) is not None for v in values2)


def test_uuid_with_nested_expression(make_df) -> None:
    data = {"a": list(range(50))}
    df = make_df(data).with_columns({"u": uuid(), "u_fmt": format("{}", uuid())}).collect()

    assert len(df) == 50
    assert df.schema()["u"].dtype == DataType.string()
    assert df.schema()["u_fmt"].dtype == DataType.string()

    pydict = df.to_pydict()
    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    assert all(isinstance(v, str) and uuid_re.match(v) is not None for v in pydict["u"])
    assert all(isinstance(v, str) and uuid_re.match(v) is not None for v in pydict["u_fmt"])
    assert any(u != uf for u, uf in zip(pydict["u"], pydict["u_fmt"]))


def test_uuid_in_select_allowed(make_df) -> None:
    data = {"a": list(range(50))}
    df = make_df(data).select("a", u1=uuid(), u2=uuid()).collect()

    assert len(df) == 50
    assert df.schema()["u1"].dtype == DataType.string()
    assert df.schema()["u2"].dtype == DataType.string()

    pydict = df.to_pydict()
    assert len(set(pydict["u1"])) == 50
    assert len(set(pydict["u2"])) == 50
    assert any(v1 != v2 for v1, v2 in zip(pydict["u1"], pydict["u2"]))


def test_uuid_in_filter_raises(make_df) -> None:
    data = {"a": [1, 2, 3]}
    with pytest.raises(Exception, match=r"uuid\(\) is only allowed in projections"):
        make_df(data).filter(uuid() == "x").collect()

    df = make_df(data).with_column("u", uuid()).filter(col("u") == "x").collect()
    assert df.schema()["u"].dtype == DataType.string()


def test_uuid_in_aggregation_raises(make_df) -> None:
    data = {"key": ["a", "b", "a"], "value": [1, 2, 3]}
    with pytest.raises(Exception, match=r"uuid\(\) is only allowed in projections"):
        make_df(data).groupby("key").agg(uuid().min().alias("u")).collect()

    df = make_df(data).with_column("u", uuid()).groupby("key").agg(col("u").min()).collect()
    assert df.schema()["u"].dtype == DataType.string()


def test_uuid_in_join_condition_raises(make_df) -> None:
    left_data = {"key": ["a", "b", "c"], "value": [1, 2, 3]}
    right_data = {"key": ["b", "c", "d"], "other": [4, 5, 6]}

    with pytest.raises(Exception, match=r"uuid\(\) is only allowed in projections"):
        make_df(left_data).join(make_df(right_data), on=uuid() == uuid(), how="inner").collect()

    df = (
        make_df(left_data)
        .with_column("u", uuid())
        .join(make_df(right_data).with_column("u", uuid()), on="u", how="inner")
        .collect()
    )
    assert "u" in df.column_names
    assert "key" in df.column_names
    assert "value" in df.column_names
    assert "other" in df.column_names
    assert any(name in df.column_names for name in ("key_right", "right.key", "right_key"))


def test_uuid_chained_with_column_calls_are_distinct(make_df) -> None:
    data = {"foo": [1, 2, 3]}
    df = make_df(data).with_column("uuid", uuid()).with_column("u2", uuid()).with_column("u3", uuid()).collect()

    assert df.schema()["uuid"].dtype == DataType.string()
    assert df.schema()["u2"].dtype == DataType.string()
    assert df.schema()["u3"].dtype == DataType.string()

    pydict = df.to_pydict()
    assert any(v2 != v3 for v2, v3 in zip(pydict["u2"], pydict["u3"]))
