import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.table import MicroPartition


def test_list_slice_empty_series():
    table = MicroPartition.from_pydict(
        {
            "col": pa.array([], type=pa.list_(pa.int64())),
            "start": pa.array([], type=pa.int64()),
            "end": pa.array([], type=pa.int64()),
        }
    )

    result = table.eval_expression_list(
        [
            col("col").list.slice(0, 1).alias("col"),
            col("col").list.slice(col("start"), 1).alias("col-start"),
            col("col").list.slice(col("start"), col("end")).alias("col-start-end"),
            col("col").list.slice(0, col("end")).alias("col-end"),
        ]
    )

    assert result.to_pydict() == {
        "col": [],
        "col-start": [],
        "col-start-end": [],
        "col-end": [],
    }


def test_list_slice():
    table = MicroPartition.from_pydict(
        {
            # Test list of an atomic type.
            "col1": [["a"], ["ab", "a"], [None, "a", "", "b", "c"], None, ["a", ""]],
            # Test lists of a nested type.
            "col2": [
                [[1]],
                [[3, 3], [4], [5, 5]],
                [],
                [[], []],
                None,
            ],
            "start": [-1, 1, 0, 2, -2],
            "end": [1, 2, 0, 4, 3],
            "edge_start": [-1, -2, -5, 0, -2],
            "edge_end": [-1, -2, -1, -2, -1],
        }
    )

    result = table.eval_expression_list(
        [
            col("col1").list.slice(0, 1).alias("col1"),
            col("col1").list.slice(col("start"), 1).alias("col1-start"),
            col("col1").list.slice(col("start"), col("end")).alias("col1-start-end"),
            col("col1").list.slice(1, col("end")).alias("col1-end"),
            col("col1").list.slice(20, 25).alias("col1-invalid-start"),
            col("col2").list.slice(0, 1).alias("col2"),
            col("col2").list.slice(col("start"), 1).alias("col2-start"),
            col("col2").list.slice(col("start"), col("end")).alias("col2-start-end"),
            col("col2").list.slice(0, col("end")).alias("col2-end"),
            col("col2").list.slice(20, 25).alias("col2-invalid-start"),
            # Test edge cases.
            col("col1").list.slice(-10, -20).alias("col1-edge1"),
            col("col1").list.slice(-20, -10).alias("col1-edge2"),
            col("col1").list.slice(-20, 10).alias("col1-edge3"),
            col("col1").list.slice(-20, -1).alias("col1-edge4"),
            col("col1").list.slice(col("edge_start"), col("edge_end")).alias("col1-edge5"),
            col("col1").list.slice(10, 1).alias("col1-edge6"),
            col("col1").list.slice(1, -1).alias("col1-edge7"),
        ]
    )

    assert result.to_pydict() == {
        "col1": [["a"], ["ab"], [None], None, ["a"]],
        "col1-start": [["a"], [], [None], None, ["a"]],
        "col1-start-end": [["a"], ["a"], [], None, ["a", ""]],
        "col1-end": [[], ["a"], [], None, [""]],
        "col1-invalid-start": [[], [], [], None, []],
        "col2": [[[1]], [[3, 3]], [], [[]], None],
        "col2-start": [[[1]], [], [], [], None],
        "col2-start-end": [[[1]], [[4]], [], [], None],
        "col2-end": [[[1]], [[3, 3], [4]], [], [[], []], None],
        "col2-invalid-start": [[], [], [], [], None],
        "col1-edge1": [[], [], [], None, []],
        "col1-edge2": [[], [], [], None, []],
        "col1-edge3": [["a"], ["ab", "a"], [None, "a", "", "b", "c"], None, ["a", ""]],
        "col1-edge4": [[], ["ab"], [None, "a", "", "b"], None, ["a"]],
        "col1-edge5": [[], [], [None, "a", "", "b"], None, ["a"]],
        "col1-edge6": [[], [], [], None, []],
        "col1-edge7": [[], [], ["a", "", "b"], None, []],
    }


def test_fixed_size_list_slice():
    table = MicroPartition.from_pydict(
        {
            # Test list of an atomic type.
            "col1": [["a", "b"], ["aa", "bb"], None, [None, "bbbb"], ["aaaaa", None]],
            # Test lists of a nested type.
            "col2": [
                [[1], [2]],
                [[11, 111], [22, 222]],
                None,
                [None, [3333]],
                [[], []],
            ],
            "start": [-1, 1, 0, 2, -2],
            "end": [1, 1, 0, 2, 3],
            "edge_start": [-1, -2, -5, 0, -2],
            "edge_end": [-1, -2, -1, -2, -1],
        }
    )

    dtype1 = DataType.fixed_size_list(DataType.string(), 2)
    dtype2 = DataType.fixed_size_list(DataType.list(DataType.int32()), 2)

    table = table.eval_expression_list(
        [
            col("col1").cast(dtype1),
            col("col2").cast(dtype2),
            col("start"),
            col("end"),
            col("edge_start"),
            col("edge_end"),
        ]
    )

    result = table.eval_expression_list(
        [
            col("col1").list.slice(0, 1).alias("col1"),
            col("col1").list.slice(col("start"), 1).alias("col1-start"),
            col("col1").list.slice(col("start"), col("end")).alias("col1-start-end"),
            col("col1").list.slice(1, col("end")).alias("col1-end"),
            col("col1").list.slice(20, 25).alias("col1-invalid-start"),
            col("col2").list.slice(0, 1).alias("col2"),
            col("col2").list.slice(col("start"), 2).alias("col2-start"),
            col("col2").list.slice(col("start"), col("end")).alias("col2-start-end"),
            col("col2").list.slice(0, col("end")).alias("col2-end"),
            col("col2").list.slice(20, 25).alias("col2-invalid-start"),
            # Test edge cases.
            col("col1").list.slice(-10, -20).alias("col1-edge1"),
            col("col1").list.slice(-20, -10).alias("col1-edge2"),
            col("col1").list.slice(-20, 10).alias("col1-edge3"),
            col("col1").list.slice(-20, -1).alias("col1-edge4"),
            col("col1").list.slice(col("edge_start"), col("edge_end")).alias("col1-edge5"),
            col("col1").list.slice(10, 1).alias("col1-edge6"),
            col("col1").list.slice(0, -1).alias("col1-edge7"),
        ]
    )

    assert result.to_pydict() == {
        "col1": [["a"], ["aa"], None, [None], ["aaaaa"]],
        "col1-start": [[], [], None, [], ["aaaaa"]],
        "col1-start-end": [[], [], None, [], ["aaaaa", None]],
        "col1-end": [[], [], None, ["bbbb"], [None]],
        "col1-invalid-start": [[], [], None, [], []],
        "col2": [[[1]], [[11, 111]], None, [None], [[]]],
        "col2-start": [[[2]], [[22, 222]], None, [], [[], []]],
        "col2-start-end": [[], [], None, [], [[], []]],
        "col2-end": [[[1]], [[11, 111]], None, [None, [3333]], [[], []]],
        "col2-invalid-start": [[], [], None, [], []],
        "col1-edge1": [[], [], None, [], []],
        "col1-edge2": [[], [], None, [], []],
        "col1-edge3": [["a", "b"], ["aa", "bb"], None, [None, "bbbb"], ["aaaaa", None]],
        "col1-edge4": [["a"], ["aa"], None, [None], ["aaaaa"]],
        "col1-edge5": [[], [], None, [], ["aaaaa"]],
        "col1-edge6": [[], [], None, [], []],
        "col1-edge7": [["a"], ["aa"], None, [None], ["aaaaa"]],
    }


def test_list_slice_invalid_parameters():
    table = MicroPartition.from_pydict(
        {
            "col": [["a", "b", "c"], ["aa", "bb", "cc"], None, [None, "bbbb"], ["aaaaa", None]],
            "start": [0, -1, 1, 3, -4],
            "end": [1, 2, 3, -1, 0],
        }
    )
    with pytest.raises(ValueError, match="Expected start index to be integer"):
        table.eval_expression_list([col("col").list.slice(1.0, 0)])
    with pytest.raises(ValueError, match="Expected end index to be integer"):
        table.eval_expression_list([col("col").list.slice(0, 1.0)])
    with pytest.raises(TypeError, match="missing 1 required positional argument: 'end'"):
        table.eval_expression_list([col("col").list.slice(0)])
    with pytest.raises(TypeError, match="missing 2 required positional arguments"):
        table.eval_expression_list([col("col").list.slice()])
    with pytest.raises(TypeError, match="takes 3 positional arguments but 4 were given"):
        table.eval_expression_list([col("col").list.slice(0, 0, 0)])


def test_list_slice_non_list_type():
    table = MicroPartition.from_pydict(
        {
            "structcol": [{"a": 1}, {"b": 1}, {"c": 1}],
            "stringcol": ["a", "b", "c"],
            "intcol": [1, 2, 3],
        },
    )

    with pytest.raises(ValueError):
        table.eval_expression_list([col("structcol").list.slice(0, 2)])
    with pytest.raises(ValueError):
        table.eval_expression_list([col("stringcol").list.slice(0, 2)])
    with pytest.raises(ValueError):
        table.eval_expression_list([col("intcol").list.slice(0, 2)])
