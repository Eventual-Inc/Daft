from __future__ import annotations

from daft.datatype import DataType
from daft.expressions import col
from daft.table import MicroPartition


def test_list_unique():
    table = MicroPartition.from_pydict(
        {
            "a": [[1, 2, 2, 3], [4, 4, 6, 2], [3, 3], [11, 6, 6], None, [], [1, None, 3, 3], None],
        }
    ).eval_expression_list([col("a").cast(DataType.list(DataType.int64()))])

    res = table.eval_expression_list([col("a").list.unique().alias("unique")])
    result = res.to_pydict()["unique"]
    expected = [[1, 2, 3], [4, 6, 2], [3], [11, 6], None, [], [1, 3], None]

    # Check that nulls are preserved
    assert [x is None for x in result] == [x is None for x in expected]

    # For non-null lists, check that elements are distinct and sets match
    for r, e in zip(result, expected):
        if r is not None:
            assert len(r) == len(set(r)), "Elements should be distinct"
            assert set(r) == set(e), "Sets should match"


def test_list_unique_fixed_size():
    table = MicroPartition.from_pydict(
        {
            "a": [[1, 2], [2, 2], [3, 3], [11, 11], None, [2, None], None],
        }
    ).eval_expression_list([col("a").cast(DataType.fixed_size_list(DataType.int64(), 2))])

    res = table.eval_expression_list([col("a").list.unique().alias("unique")])
    result = res.to_pydict()["unique"]
    expected = [[1, 2], [2], [3], [11], None, [2], None]

    # Check that nulls are preserved
    assert [x is None for x in result] == [x is None for x in expected]

    # For non-null lists, check that elements are distinct and sets match
    for r, e in zip(result, expected):
        if r is not None:
            assert len(r) == len(set(r)), "Elements should be distinct"
            assert set(r) == set(e), "Sets should match"
