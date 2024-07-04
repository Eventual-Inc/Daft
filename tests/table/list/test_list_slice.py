import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.table import MicroPartition


def test_list_slice():
    table = MicroPartition.from_pydict(
        {
            "col1": [["a"], ["ab", "a"], [None, "a", "", "b", "c"], None, ["a", ""]],
            "col2": [
                [[1], [2]],
                [[3, 3], [4], [5, 5]],
                [],
                [[], []],
                None,
            ],
            "start": [-1, 1, 0, 2, -2],
            "length": [1, 1, 0, 2, 3],
        }
    )

    result = table.eval_expression_list(
        [
            col("col1").list.slice(0, 1).alias("col1"),
            col("col1").list.slice(col("start"), 1).alias("col1-start"),
            col("col1").list.slice(col("start"), col("length")).alias("col1-start-length"),
            col("col1").list.slice(1, col("length")).alias("col1-length"),
            col("col1").list.slice(20, 25).alias("col1-invalid-start"),
            col("col2").list.slice(0, 1).alias("col2"),
            col("col2").list.slice(col("start"), 1).alias("col2-start"),
            col("col2").list.slice(col("start"), col("length")).alias("col2-start-length"),
            col("col2").list.slice(0, col("length")).alias("col2-length"),
            col("col2").list.slice(20, 25).alias("col2-invalid-start"),
        ]
    )

    assert result.to_pydict() == {
        "col1": [["a"], ["ab"], [None], None, ["a"]],
        "col1-start": [["a"], ["a"], [None], None, ["a"]],
        "col1-start-length": [["a"], ["a"], [], None, ["a", ""]],
        "col1-length": [[], ["a"], [], None, [""]],
        "col1-invalid-start": [[], [], [], None, []],
        "col2": [[[1]], [[3, 3]], [], [[]], None],
        "col2-start": [[[2]], [[4]], [], [], None],
        "col2-start-length": [[[2]], [[4]], [], [], None],
        "col2-length": [[[1]], [[3, 3]], [], [[], []], None],
        "col2-invalid-start": [[], [], [], [], None],
    }


def test_fixed_size_list_slice():
    table = MicroPartition.from_pydict(
        {
            "col1": [["a", "b"], ["aa", "bb"], None, [None, "bbbb"], ["aaaaa", None]],
            "col2": [
                [[1], [2]],
                [[11, 111], [22, 222]],
                None,
                [None, [3333]],
                [[], []],
            ],
            "start": [-1, 1, 0, 2, -2],
            "length": [1, 1, 0, 2, 3],
        }
    )

    dtype1 = DataType.fixed_size_list(DataType.string(), 2)
    dtype2 = DataType.fixed_size_list(DataType.list(DataType.int32()), 2)

    table = table.eval_expression_list(
        [
            col("col1").cast(dtype1),
            col("col2").cast(dtype2),
            col("start"),
            col("length"),
        ]
    )

    result = table.eval_expression_list(
        [
            col("col1").list.slice(0, 1).alias("col1"),
            col("col1").list.slice(col("start"), 1).alias("col1-start"),
            col("col1").list.slice(col("start"), col("length")).alias("col1-start-length"),
            col("col1").list.slice(1, col("length")).alias("col1-length"),
            col("col1").list.slice(20, 25).alias("col1-invalid-start"),
            col("col2").list.slice(0, 1).alias("col2"),
            col("col2").list.slice(col("start"), 1).alias("col2-start"),
            col("col2").list.slice(col("start"), col("length")).alias("col2-start-length"),
            col("col2").list.slice(0, col("length")).alias("col2-length"),
            col("col2").list.slice(20, 25).alias("col2-invalid-start"),
        ]
    )

    assert result.to_pydict() == {
        "col1": [["a"], ["aa"], None, [None], ["aaaaa"]],
        "col1-start": [["b"], ["bb"], None, [], ["aaaaa"]],
        "col1-start-length": [["b"], ["bb"], None, [], ["aaaaa", None]],
        "col1-length": [["b"], ["bb"], None, ["bbbb"], [None]],
        "col1-invalid-start": [[], [], None, [], []],
        "col2": [[[1]], [[11, 111]], None, [None], [[]]],
        "col2-start": [[[2]], [[22, 222]], None, [], [[]]],
        "col2-start-length": [[[2]], [[22, 222]], None, [], [[], []]],
        "col2-length": [[[1]], [[11, 111]], None, [None, [3333]], [[], []]],
        "col2-invalid-start": [[], [], None, [], []],
    }


def test_list_slice_invalid_parameters():
    table = MicroPartition.from_pydict(
        {
            "col": [["a", "b", "c"], ["aa", "bb", "cc"], None, [None, "bbbb"], ["aaaaa", None]],
            "start": [0, -1, 1, 3, -4],
            "length": [1, 2, 3, -1, 0],
        }
    )
    with pytest.raises(ValueError):
        table.eval_expression_list([col("col").list.slice(0, -1)])
    with pytest.raises(ValueError):
        table.eval_expression_list([col("col").list.slice(0, -100)])
    with pytest.raises(ValueError, match="Expected start index to be integer"):
        table.eval_expression_list([col("col").list.slice(1.0, 0)])
    with pytest.raises(ValueError, match="Expected length to be integer"):
        table.eval_expression_list([col("col").list.slice(0, 1.0)])
    with pytest.raises(TypeError, match="missing 1 required positional argument: 'length'"):
        table.eval_expression_list([col("col").list.slice(0)])
    with pytest.raises(TypeError, match="missing 2 required positional arguments"):
        table.eval_expression_list([col("col").list.slice()])
    with pytest.raises(TypeError, match="takes 3 positional arguments but 4 were given"):
        table.eval_expression_list([col("col").list.slice(0, 0, 0)])


def test_list_slice_non_list_type():
    table = MicroPartition.from_pydict(
        {
            "mapcol": [{"a": 1}, {"b": 1}, {"c": 1}],
            "stringcol": ["a", "b", "c"],
            "intcol": [1, 2, 3],
        },
    )

    with pytest.raises(ValueError):
        table.eval_expression_list([col("map").list.slice(0, 2)])
    with pytest.raises(ValueError):
        table.eval_expression_list([col("stringcol").list.slice(0, 2)])
    with pytest.raises(ValueError):
        table.eval_expression_list([col("intcol").list.slice(0, 2)])
