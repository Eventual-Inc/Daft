from __future__ import annotations

import re

import pytest

import daft
from daft import DataType, Series, col


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_map_groups(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "group": [1, 1, 2],
            "a": [1, 3, 3],
            "b": [5, 6, 7],
        },
        repartition=repartition_nparts,
    )

    @daft.udf(return_dtype=daft.DataType.list(daft.DataType.float64()))
    def udf(a, b):
        a, b = a.to_pylist(), b.to_pylist()
        res = []
        for i in range(len(a)):
            res.append(a[i] / sum(a) + b[i])
        res.sort()
        return [res]

    daft_df = daft_df.groupby("group").map_groups(udf(daft_df["a"], daft_df["b"])).sort("group", desc=False)
    expected = {
        "group": [1, 2],
        "a": [[5.25, 6.75], [8.0]],
    }

    daft_cols = daft_df.to_pydict()

    assert daft_cols == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_map_groups_batch(make_df, repartition_nparts, with_morsel_size):
    df = make_df(
        {
            "group": ["a", "a", "b"],
            "a": [1, 3, 3],
            "b": [5, 6, 7],
        },
        repartition=repartition_nparts,
    )

    @daft.func.batch(return_dtype=DataType.list(DataType.float64()))
    def udf(a: Series, b: Series) -> Series:
        a_list = a.to_pylist()
        b_list = b.to_pylist()
        if not a_list:
            return Series.from_pylist([])
        res: list[float] = []
        denom = sum(a_list)
        for i in range(len(a_list)):
            res.append(a_list[i] / denom + b_list[i])
        res.sort()
        # One list per group
        return Series.from_pylist([res])

    out = df.groupby("group").map_groups(udf(col("a"), col("b"))).sort("group")

    expected = {
        "group": ["a", "b"],
        "a": [[5.25, 6.75], [8.0]],
    }
    assert out.to_pydict() == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 3])
@pytest.mark.parametrize("output_when_empty", [[], [1], [1, 2]])
def test_map_groups_more_than_one_output_row(make_df, repartition_nparts, output_when_empty, with_morsel_size):
    daft_df = make_df(
        {
            "group": [1, 2],
            "a": [1, 3],
        },
        repartition=repartition_nparts,
    )

    @daft.udf(return_dtype=daft.DataType.int64())
    def udf(a):
        a = a.to_pylist()
        if len(a) == 0:
            return output_when_empty
        return [a[0]] * 3

    daft_df = daft_df.groupby("group").map_groups(udf(daft_df["a"])).sort("group", desc=False)
    expected = {"group": [1, 1, 1, 2, 2, 2], "a": [1, 1, 1, 3, 3, 3]}

    daft_cols = daft_df.to_pydict()

    assert daft_cols == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 3])
@pytest.mark.parametrize("output_when_empty", [[], [1], [1, 2]])
def test_map_groups_batch_multiple_output_rows(make_df, repartition_nparts, output_when_empty, with_morsel_size):
    df = make_df(
        {
            "group": [1, 2],
            "a": [1, 3],
        },
        repartition=repartition_nparts,
    )

    @daft.func.batch(return_dtype=DataType.int64())
    def udf(a: Series) -> Series:
        values = a.to_pylist()
        if len(values) == 0:
            return Series.from_pylist(output_when_empty)
        return Series.from_pylist([values[0]] * 3)

    out = df.groupby("group").map_groups(udf(col("a"))).sort("group")

    expected = {"group": [1, 1, 1, 2, 2, 2], "a": [1, 1, 1, 3, 3, 3]}
    assert out.to_pydict() == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_map_groups_batch_single_group(make_df, repartition_nparts, with_morsel_size):
    df = make_df(
        {
            "group": [1, 1, 1],
            "a": [1, 2, 3],
        },
        repartition=repartition_nparts,
    )

    @daft.func.batch(return_dtype=DataType.int64())
    def udf(a: Series) -> Series:
        values = a.to_pylist()
        if not values:
            return Series.from_pylist([])
        return Series.from_pylist([sum(x**2 for x in values)])

    out = df.groupby("group").map_groups(udf(col("a")))

    expected = {"group": [1], "a": [14]}
    assert out.to_pydict() == expected


@pytest.mark.parametrize("repartition_nparts", [1, 5, 11])
def test_map_groups_batch_double_group_by(make_df, repartition_nparts, with_morsel_size):
    df = make_df(
        {
            "group_1": [1, 1, 1, 1, 1, 2, 2, 2, 2, 2],
            "group_2": [1, 1, 1, 2, 2, 1, 1, 1, 2, 2],
            "a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        },
        repartition=repartition_nparts,
    )

    @daft.func.batch(return_dtype=DataType.int64())
    def udf(a: Series) -> Series:
        values = a.to_pylist()
        if not values:
            return Series.from_pylist([])
        return Series.from_pylist([sum(x**2 for x in values)])

    out = df.groupby("group_1", "group_2").map_groups(udf(col("a")))

    expected = {
        "group_1": [2, 2, 1, 1],
        "group_2": [1, 2, 1, 2],
        "a": [149, 181, 14, 41],
    }

    rows = set()
    for i in range(4):
        rows.add((expected["group_1"][i], expected["group_2"][i], expected["a"][i]))

    out_dict = out.to_pydict()
    for i in range(4):
        assert (out_dict["group_1"][i], out_dict["group_2"][i], out_dict["a"][i]) in rows


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_map_groups_batch_compound_input(make_df, repartition_nparts, with_morsel_size):
    df = make_df(
        {
            "group": [1, 1, 2, 2],
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8],
        },
        repartition=repartition_nparts,
    )

    @daft.func.batch(return_dtype=DataType.int64())
    def udf(data: Series) -> Series:
        values = data.to_pylist()
        if not values:
            return Series.from_pylist([])
        return Series.from_pylist([sum(x**2 for x in values)])

    out = df.groupby("group").map_groups(udf((col("a").alias("c") * col("b")).alias("c"))).sort("group", desc=True)

    expected = {"group": [2, 1], "c": [1465, 169]}
    assert out.to_pydict() == expected


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_map_groups_batch_with_alias(make_df, repartition_nparts, with_morsel_size):
    df = make_df(
        {
            "group": [1, 1, 2],
            "a": [1, 3, 3],
            "b": [5, 6, 7],
        },
        repartition=repartition_nparts,
    )

    @daft.func.batch(return_dtype=DataType.list(DataType.float64()))
    def udf(a: Series, b: Series) -> Series:
        a_list = a.to_pylist()
        b_list = b.to_pylist()
        if not a_list:
            return Series.from_pylist([])
        res: list[float] = []
        denom = sum(a_list)
        for i in range(len(a_list)):
            res.append(a_list[i] / denom + b_list[i])
        res.sort()
        return Series.from_pylist([res])

    out = df.groupby(col("group").alias("group_alias")).map_groups(udf(col("a"), col("b"))).sort("group_alias")

    expected = {
        "group_alias": [1, 2],
        "a": [[5.25, 6.75], [8.0]],
    }
    assert out.to_pydict() == expected


def test_cls_batch_map_groups():
    df = daft.from_pydict({"group": [1, 1, 2], "a": [1, 2, 3], "b": [4, 5, 6]})

    @daft.cls
    class BatchAdder:
        def __init__(self, offset: int):
            self.offset = offset

        @daft.method.batch(return_dtype=DataType.int64())
        def add(self, a: Series, b: Series) -> Series:
            import pyarrow.compute as pc

            a_arrow = a.to_arrow()
            b_arrow = b.to_arrow()
            result = pc.add(a_arrow, b_arrow)
            result = pc.add(result, self.offset)
            return Series.from_arrow(result)

    adder = BatchAdder(10)
    out = df.groupby("group").map_groups(adder.add(col("a"), col("b"))).sort("group")

    expected = {"group": [1, 1, 2], "a": [15, 17, 19]}
    assert out.to_pydict() == expected


def test_map_groups_rowwise_func_error():
    df = daft.from_pydict({"group": ["a", "a", "b"], "value": [1, 2, 3]})

    @daft.func  # row-wise by default
    def plus_one(x: int) -> int:
        return x + 1

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Row-wise Python UDFs (daft.func or @daft.method) are not supported in aggregations; "
            "use daft.func.batch or @daft.method.batch for group-wise UDFs, or apply the row-wise "
            "UDF in a projection before aggregation."
        ),
    ):
        df.groupby("group").map_groups(plus_one(col("value")))


def test_map_groups_rowwise_cls_method_error():
    df = daft.from_pydict({"group": [1, 1, 2], "value": [1, 2, 3]})

    @daft.cls
    class Adder:
        def __init__(self, offset: int):
            self.offset = offset

        @daft.method  # row-wise method
        def add(self, x: int) -> int:
            return x + self.offset

    adder = Adder(10)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Row-wise Python UDFs (daft.func or @daft.method) are not supported in aggregations; "
            "use daft.func.batch or @daft.method.batch for group-wise UDFs, or apply the row-wise "
            "UDF in a projection before aggregation."
        ),
    ):
        df.groupby("group").map_groups(adder.add(col("value")))
