from __future__ import annotations


def test_with_columns(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    expanded_df = df.with_columns({"foo": df["sepal_width"], "bar": df["sepal_width"] + df["petal_length"]})
    data = expanded_df.to_pydict()
    assert expanded_df.column_names == df.column_names + ["foo", "bar"]
    assert data["foo"] == data["sepal_width"]
    assert data["bar"] == [sw + pl for sw, pl in zip(data["sepal_width"], data["petal_length"])]


def test_with_columns_same_name(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    expanded_df = df.with_columns(
        {"sepal_length": df["sepal_length"] + df["sepal_width"], "petal_length": df["petal_length"] + df["petal_width"]}
    )
    data = expanded_df.to_pydict()
    assert expanded_df.column_names == df.column_names
    expected_sepal_length = [
        valid_data[i]["sepal_length"] + valid_data[i]["sepal_width"] for i in range(len(valid_data))
    ]
    expected_petal_length = [
        valid_data[i]["petal_length"] + valid_data[i]["petal_width"] for i in range(len(valid_data))
    ]
    assert data["sepal_length"] == expected_sepal_length
    assert data["petal_length"] == expected_petal_length


def test_with_columns_empty(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    expanded_df = df.with_columns({})
    assert expanded_df.column_names == df.column_names
    assert expanded_df.to_pydict() == df.to_pydict()
