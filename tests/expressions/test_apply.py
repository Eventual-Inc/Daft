# def test_apply_lambda
# def test_apply_module_func
# def test_apply_inline_func
# def test_apply_lambda_pyobj

from __future__ import annotations

import dataclasses

import pytest

import daft
from daft import DataType


def add_1(x):
    return x + 1


def test_apply_module_func():
    df = daft.from_pydict({"a": [1, 2, 3]})
    df = df.with_column("a_plus_1", df["a"].apply(add_1, return_dtype=DataType.int32()))
    assert df.to_pydict() == {"a": [1, 2, 3], "a_plus_1": [2, 3, 4]}


@pytest.mark.parametrize(
    "return_dtype, expected_dtype",
    [(DataType.int32(), DataType.int32()), (int, DataType.int64()), ("INT", DataType.int32())],
)
def test_apply_lambda(return_dtype, expected_dtype):
    df = daft.from_pydict({"a": [1, 2, 3]})
    df = df.with_column("a_plus_1", df["a"].apply(lambda x: x + 1, return_dtype=return_dtype))
    assert df.to_pydict() == {"a": [1, 2, 3], "a_plus_1": [2, 3, 4]}
    assert df.schema()["a_plus_1"].dtype == expected_dtype


@pytest.mark.parametrize("values", [[1, None, 3], [None, None]])
@pytest.mark.parametrize("null_result", [None, 0])
def test_apply_nulls(values, null_result):
    df = daft.from_pydict({"a": values})
    df = df.with_column(
        "a_plus_1",
        df["a"].apply(lambda x: null_result if x is None else x + 1, return_dtype=DataType.int32()),
    )
    assert df.to_pydict() == {
        "a": values,
        "a_plus_1": [null_result if x is None else x + 1 for x in values],
    }
    assert df.schema()["a_plus_1"].dtype == DataType.int32()


def test_apply_empty():
    df = daft.from_pydict({"a": [1]})
    df = df.where(df["a"] != 1)
    df = df.with_column("a_plus_1", df["a"].apply(add_1, return_dtype=DataType.int32()))
    assert df.to_pydict() == {"a": [], "a_plus_1": []}
    assert df.schema()["a_plus_1"].dtype == DataType.int32()


def test_apply_inline_func():
    def inline_add_1(x):
        return x + 1

    df = daft.from_pydict({"a": [1, 2, 3]})
    df = df.with_column("a_plus_1", df["a"].apply(inline_add_1, return_dtype=DataType.int32()))
    assert df.to_pydict() == {"a": [1, 2, 3], "a_plus_1": [2, 3, 4]}


@dataclasses.dataclass
class MyObj:
    x: int


def test_apply_obj():
    df = daft.from_pydict({"obj": [MyObj(x=0), MyObj(x=0), MyObj(x=0)]})

    def inline_mutate_obj(obj):
        obj.x = 1
        return obj

    df = df.with_column("mut_obj", df["obj"].apply(inline_mutate_obj, return_dtype=DataType.python()))
    result = df.to_pydict()
    for mut_obj in result["mut_obj"]:
        assert mut_obj.x == 1
