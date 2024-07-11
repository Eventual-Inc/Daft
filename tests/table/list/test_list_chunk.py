import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.table import MicroPartition


def test_list_chunk_empty_series():
    table = MicroPartition.from_pydict(
        {
            "col": pa.array([], type=pa.list_(pa.int64())),
            "fixed_col": pa.array([], type=pa.list_(pa.int64(), 2)),
        }
    )

    result = table.eval_expression_list(
        [
            col("col").list.chunk(1).alias("col1"),
            col("col").list.chunk(2).alias("col2"),
            col("col").list.chunk(1000).alias("col3"),
            col("fixed_col").list.chunk(1).alias("fixed_col1"),
            col("fixed_col").list.chunk(2).alias("fixed_col2"),
            col("fixed_col").list.chunk(1000).alias("fixed_col3"),
        ]
    )

    assert result.to_pydict() == {
        "col1": [],
        "col2": [],
        "col3": [],
        "fixed_col1": [],
        "fixed_col2": [],
        "fixed_col3": [],
    }


def test_list_chunk():
    table = MicroPartition.from_pydict(
        {
            # Test list of an atomic type.
            "col1": [[1, 2, None, 4, 5], None, [7, 8, 9, None, 11, 12]],
            # Test lists of nested types.
            "col2": [
                [[1, 1], [2, 2], [None, None], [4, 4], [5, 5]],
                [[7, 7], [8, 8], [9, 9], [None, None], [11, 11], [12, 12]],
                None,
            ],
            "col3": [
                [[[1, 1]], None, [[None, None]], [[4, 4]], [[5, 5]]],
                [[[7, 7]], [[8, 8]], [None], [[None, None]], None, [[12, 12]]],
                None,
            ],
        }
    )

    result = table.eval_expression_list(
        [
            col("col1").list.chunk(1).alias("col1-1"),
            col("col1").list.chunk(2).alias("col1-2"),
            col("col1").list.chunk(3).alias("col1-3"),
            col("col1").list.chunk(10).alias("col1-10"),  # Test chunk size > list size
            col("col2").list.chunk(2).alias("col2-2"),
            col("col3").list.chunk(2).alias("col3-2"),
        ]
    )

    assert result.to_pydict() == {
        "col1-1": [[[1], [2], [None], [4], [5]], None, [[7], [8], [9], [None], [11], [12]]],
        "col1-2": [[[1, 2], [None, 4]], None, [[7, 8], [9, None], [11, 12]]],
        "col1-3": [[[1, 2, None]], None, [[7, 8, 9], [None, 11, 12]]],
        "col1-10": [[], None, []],
        "col2-2": [
            [[[1, 1], [2, 2]], [[None, None], [4, 4]]],
            [[[7, 7], [8, 8]], [[9, 9], [None, None]], [[11, 11], [12, 12]]],
            None,
        ],
        "col3-2": [
            [[[[1, 1]], None], [[[None, None]], [[4, 4]]]],
            [[[[7, 7]], [[8, 8]]], [[None], [[None, None]]], [None, [[12, 12]]]],
            None,
        ],
    }


def test_fixed_size_list_chunk():
    table = MicroPartition.from_pydict(
        {
            # Test list of atomic types.
            "col1": [["aa", "bb", None, "dd"], ["ee", None, "gg", "hh"], ["ii", "jj", "kk", "ll"]],
            # Test list with invalid elements.
            "col2": [["aa", "bb", "cc", "dd"], None, ["ee", "ff", "gg", "hh"]],
            # Test list of a nested type.
            "col3": [
                [[1], [2]],
                [[None, 111], [22, 222]],
                None,
            ],
            # Test deeper nested types.
            "col4": [
                [[[1]], [[None]]],
                [[[None], [111]], None],
                None,
            ],
        }
    )

    dtype1 = DataType.fixed_size_list(DataType.string(), 4)
    dtype2 = DataType.fixed_size_list(DataType.list(DataType.int32()), 2)
    dtype3 = DataType.fixed_size_list(DataType.list(DataType.list(DataType.int32())), 2)

    table = table.eval_expression_list(
        [
            col("col1").cast(dtype1),
            col("col2").cast(dtype1),
            col("col3").cast(dtype2),
            col("col4").cast(dtype3),
        ]
    )

    result = table.eval_expression_list(
        [
            col("col1").list.chunk(2).alias("col1-2"),
            col("col1").list.chunk(3).alias("col1-3"),
            col("col2").list.chunk(2).alias("col2-2"),
            col("col2").list.chunk(3).alias("col2-3"),
            col("col3").list.chunk(2).alias("col3-2"),
            col("col3").list.chunk(3).alias("col3-3"),  # Test chunk size > list size
            col("col3").list.chunk(4).alias("col3-4"),  # Test chunk size > list size
            col("col4").list.chunk(2).alias("col4-2"),
        ]
    )

    assert result.to_pydict() == {
        "col1-2": [[["aa", "bb"], [None, "dd"]], [["ee", None], ["gg", "hh"]], [["ii", "jj"], ["kk", "ll"]]],
        "col1-3": [[["aa", "bb", None]], [["ee", None, "gg"]], [["ii", "jj", "kk"]]],
        "col2-2": [[["aa", "bb"], ["cc", "dd"]], None, [["ee", "ff"], ["gg", "hh"]]],
        "col2-3": [[["aa", "bb", "cc"]], None, [["ee", "ff", "gg"]]],
        "col3-2": [[[[1], [2]]], [[[None, 111], [22, 222]]], None],
        "col3-3": [[], [], None],
        "col3-4": [[], [], None],
        "col4-2": [[[[[1]], [[None]]]], [[[[None], [111]], None]], None],
    }


def test_list_chunk_invalid_parameters():
    table = MicroPartition.from_pydict(
        {
            "col": [["a", "b", "c"], ["aa", "bb", "cc"], None, [None, "bbbb"], ["aaaaa", None]],
            "size": [1, 2, 3, 2, 2],
        }
    )
    with pytest.raises(ValueError, match="Invalid value for `size`"):
        table.eval_expression_list([col("col").list.chunk(col("size"))])
    with pytest.raises(ValueError, match="Invalid value for `size`"):
        table.eval_expression_list([col("col").list.chunk(0)])
    with pytest.raises(ValueError, match="Invalid value for `size`"):
        table.eval_expression_list([col("col").list.chunk(-1)])
    with pytest.raises(ValueError, match="Invalid value for `size`"):
        table.eval_expression_list([col("col").list.chunk(1.0)])


def test_list_chunk_non_list_type():
    table = MicroPartition.from_pydict(
        {
            "structcol": [{"a": 1}, {"b": 1}, {"c": 1}],
            "stringcol": ["a", "b", "c"],
            "intcol": [1, 2, 3],
        },
    )

    with pytest.raises(ValueError):
        table.eval_expression_list([col("structcol").list.chunk(2)])
    with pytest.raises(ValueError):
        table.eval_expression_list([col("stringcol").list.chunk(2)])
    with pytest.raises(ValueError):
        table.eval_expression_list([col("intcol").list.chunk(2)])
