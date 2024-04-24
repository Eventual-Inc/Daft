from __future__ import annotations


def test_with_columns(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    expanded_df = df.with_columns({"foo": df["sepal_width"], "bar": df["sepal_width"] + df["petal_length"]})
    data = expanded_df.to_pydict()
    assert expanded_df.column_names == df.column_names + ["foo", "bar"]
    assert data["foo"] == data["sepal_width"]
    assert data["bar"] == [sw + pl for sw, pl in zip(data["sepal_width"], data["petal_length"])]


def test_with_columns_empty(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    expanded_df = df.with_columns({})
    assert expanded_df.column_names == df.column_names
    assert expanded_df.to_pydict() == df.to_pydict()
