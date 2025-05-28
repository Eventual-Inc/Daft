import daft
from daft.scalar_udf import scalar_udf


def test_fallibility():
    @scalar_udf(return_dtype=daft.DataType.int64())
    def add_one(x: int) -> int:
        if x % 2 == 0:
            raise ValueError("oopsie")
        return x + 1

    df = daft.range(5)
    df = df.with_column("y", add_one(daft.col("id")))

    pydict = df.to_pydict()
    assert pydict == {"id": [None, 1, None, 3, None], "y": [None, 2, None, 4, None]}


def test_fallibility_sequential():
    @scalar_udf(return_dtype=daft.DataType.int64())
    def add_one(x: int) -> int:
        # this is interesting behaviour, do we want to capture failed assertions as errors?
        # assert isinstance(x, int) or x is None
        if x % 2 == 0:
            raise ValueError("oopsie")
        return x + 1

    df = daft.range(5)
    df = df.with_column("y", add_one(daft.col("id")))
    df = df.with_column("y2", add_one(daft.col("y")))

    pydict = df.to_pydict()
    print(pydict)
    assert pydict["id"] == [None, None, None, None, None]
    assert pydict["y"] == [None, None, None, None, None]
    assert pydict["y2"] == [None, None, None, None, None]


# def test_fallibility_split():
#     @scalar_udf(return_dtype=daft.DataType.int64())
#     def add_one(x: int) -> int:
#         # this is interesting behaviour, do we want to capture failed assertions as errors?
#         # assert isinstance(x, int) or x is None
#         if x % 2 == 0:
#             raise ValueError("oopsie")
#         return x + 1

#     df = daft.range(5)
#     df = df.with_column("y", add_one(daft.col("id")))
#     ok_df, err_df = df.split()
