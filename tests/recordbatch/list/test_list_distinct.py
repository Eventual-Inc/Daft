from __future__ import annotations

from daft.datatype import DataType
from daft.expressions import col
from daft.recordbatch import MicroPartition


def compare_lists(result, expected):
    # Check that nulls are preserved at list level.
    assert [x is None for x in result] == [x is None for x in expected]
    # For non-null lists, check that elements are distinct and sets match.
    for i, (r, e) in enumerate(zip(result, expected)):
        if r is not None:
            assert len(r) == len(set(r)), "Elements should be distinct"
            assert set(r) == set(e), "Sets should match"
            # Check order preservation - first occurrence of each element should be in same order.
            r_order = {x: i for i, x in enumerate(r) if x is not None}
            e_order = {x: i for i, x in enumerate(e) if x is not None}
            assert r_order == e_order, "Order of first occurrence should be preserved"


def test_list_distinct():
    table = MicroPartition.from_pydict(
        {
            "a": [
                [1, 2, 2, 3],
                [None, None, None],
                [4, 4, 6, 2],
                [3, 3],
                [11, 6, 6],
                None,
                [],
                [1, None, 3, 3],
                None,
                [None, None],
            ],
        }
    ).eval_expression_list([col("a").cast(DataType.list(DataType.int64()))])
    expected = [[1, 2, 3], [], [4, 6, 2], [3], [11, 6], None, [], [1, 3], None, []]

    res = table.eval_expression_list([col("a").list.distinct().alias("distinct")])
    result = res.to_pydict()["distinct"]
    compare_lists(result, expected)

    # Test unique alias.
    res = table.eval_expression_list([col("a").list.unique().alias("unique")])
    result = res.to_pydict()["unique"]
    compare_lists(result, expected)


def test_list_distinct_fixed_size():
    data = [[1, 2], [2, 2], [3, 3], [11, 11], None, [None, 2], [None, None], [4, None], None]
    table = MicroPartition.from_pydict(
        {
            "a": data,
        }
    ).eval_expression_list([col("a").cast(DataType.fixed_size_list(DataType.int64(), 2))])
    expected = [[1, 2], [2], [3], [11], None, [2], [], [4], None]

    res = table.eval_expression_list([col("a").list.distinct().alias("distinct")])
    result = res.to_pydict()["distinct"]
    compare_lists(result, expected)

    # Test unique alias.
    res = table.eval_expression_list([col("a").list.unique().alias("unique")])
    result = res.to_pydict()["unique"]
    compare_lists(result, expected)


def test_list_distinct_list():
    data = [[1, 2], [2, 2], [3, 3], [11, 11], None, [None, 2], [4, None], None]
    table = MicroPartition.from_pydict(
        {
            "a": data,
        }
    ).eval_expression_list([col("a").cast(DataType.list(DataType.int64()))])
    expected = [[1, 2], [2], [3], [11], None, [2], [4], None]

    res = table.eval_expression_list([col("a").list.distinct().alias("distinct")])
    result = res.to_pydict()["distinct"]
    compare_lists(result, expected)

    # Test unique alias.
    res = table.eval_expression_list([col("a").list.unique().alias("unique")])
    result = res.to_pydict()["unique"]
    compare_lists(result, expected)
