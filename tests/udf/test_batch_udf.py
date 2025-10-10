from __future__ import annotations

import daft
from daft import DataType, Series, col


def test_batch_udf():
    @daft.func.batch(return_dtype=DataType.int64())
    def my_sum(a: Series, b: Series) -> Series:
        import pyarrow.compute as pc

        a_arrow = a.to_arrow()
        b_arrow = b.to_arrow()
        result = pc.add(a_arrow, b_arrow)
        return Series.from_arrow(result)

    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    actual = df.select(my_sum(col("x"), col("y"))).to_pydict()

    expected = {"x": [5, 7, 9]}

    assert actual == expected


def test_batch_udf_with_literal():
    @daft.func.batch(return_dtype=DataType.int64())
    def my_sum_with_scalar(a: Series, b: int) -> Series:
        import pyarrow.compute as pc

        a_arrow = a.to_arrow()
        result = pc.add(a_arrow, b)
        return Series.from_arrow(result)

    df = daft.from_pydict({"x": [1, 2, 3]})
    actual = df.select(my_sum_with_scalar(col("x"), 10)).to_pydict()

    expected = {"x": [11, 12, 13]}

    assert actual == expected


def test_batch_udf_literal_eval():
    @daft.func.batch(return_dtype=DataType.int64())
    def my_sum_with_scalar(a: Series, b: int) -> Series:
        import pyarrow.compute as pc

        a_arrow = a.to_arrow()
        result = pc.add(a_arrow, b)
        return Series.from_arrow(result)

    a = Series.from_pylist([1, 2, 3])
    result = my_sum_with_scalar(a, 3)
    assert result.to_pylist() == [4, 5, 6]


def test_batch_udf_unnest():
    @daft.func.batch(
        return_dtype=DataType.struct(
            {"doubled": DataType.int64(), "tripled": DataType.int64(), "name": DataType.string()}
        ),
        unnest=True,
    )
    def create_records(a: Series) -> Series:
        doubled = [x * 2 for x in a.to_pylist()]
        tripled = [x * 3 for x in a.to_pylist()]
        names = [f"val_{x}" for x in a.to_pylist()]
        data = [{"doubled": d, "tripled": t, "name": n} for d, t, n in zip(doubled, tripled, names)]
        return Series.from_pylist(data)

    df = daft.from_pydict({"value": [1, 2, 3]})
    result = df.select(create_records(col("value"))).to_pydict()

    expected = {"doubled": [2, 4, 6], "tripled": [3, 6, 9], "name": ["val_1", "val_2", "val_3"]}
    assert result == expected


def test_batch_udf_with_batch_size():
    # Test that batch_size parameter is accepted
    @daft.func.batch(return_dtype=DataType.int64(), batch_size=3)
    def my_sum(a: Series, b: Series) -> Series:
        import pyarrow.compute as pc

        assert len(a) <= 3

        a_arrow = a.to_arrow()
        b_arrow = b.to_arrow()
        result = pc.add(a_arrow, b_arrow)
        return Series.from_arrow(result)

    df = daft.from_pydict({"x": [1, 2, 3, 4], "y": [5, 6, 7, 8]})
    actual = df.select(my_sum(col("x"), col("y"))).to_pydict()

    expected = {"x": [6, 8, 10, 12]}

    assert actual == expected
