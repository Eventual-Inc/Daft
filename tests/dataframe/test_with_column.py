from __future__ import annotations


def test_with_column(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    expanded_df = df.with_column("bar", df["sepal_width"] + df["petal_length"])
    data = expanded_df.to_pydict()
    assert expanded_df.column_names == df.column_names + ["bar"]
    assert data["bar"] == [sw + pl for sw, pl in zip(data["sepal_width"], data["petal_length"])]


def test_with_column_same_name(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    expanded_df = df.with_column("sepal_width", df["sepal_width"] + df["petal_length"])
    data = expanded_df.to_pydict()
    assert expanded_df.column_names == df.column_names
    expected = [valid_data[i]["sepal_width"] + valid_data[i]["petal_length"] for i in range(len(valid_data))]
    assert data["sepal_width"] == expected


def test_stacked_with_columns(make_df, valid_data: list[dict[str, float]]):
    df = make_df(valid_data)
    df = df.select(df["sepal_length"])
    df = df.with_column("sepal_length_2", df["sepal_length"])
    df = df.with_column("sepal_length_3", df["sepal_length_2"])
    pdf = df.to_pandas()
    assert len(pdf.columns) == 3
    assert pdf.columns.to_list() == ["sepal_length", "sepal_length_2", "sepal_length_3"]
