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

    # Test with default include_nulls=False
    res = table.eval_expression_list([col("a").list.unique().alias("unique")])
    result = res.to_pydict()["unique"]
    expected = [[1, 2, 3], [4, 6, 2], [3], [11, 6], None, [], [1, 3], None]

    # Check that nulls are preserved at list level
    assert [x is None for x in result] == [x is None for x in expected]

    # For non-null lists, check that elements are distinct and sets match
    for r, e in zip(result, expected):
        if r is not None:
            assert len(r) == len(set(r)), "Elements should be distinct"
            assert set(r) == set(e), "Sets should match"
            # Check order preservation - first occurrence of each element should be in same order
            r_order = {x: i for i, x in enumerate(r) if x is not None}
            e_order = {x: i for i, x in enumerate(e) if x is not None}
            assert r_order == e_order, "Order of first occurrence should be preserved"

    # Test with include_nulls=True
    res = table.eval_expression_list([col("a").list.unique(include_nulls=True).alias("unique")])
    result = res.to_pydict()["unique"]
    expected = [[1, 2, 3], [4, 6, 2], [3], [11, 6], None, [], [1, None, 3], None]

    # Check that nulls are preserved at list level
    assert [x is None for x in result] == [x is None for x in expected]

    # For non-null lists, check that elements are distinct and sets match
    for r, e in zip(result, expected):
        if r is not None:
            assert len(r) == len(set(x for x in r)), "Elements should be distinct"
            assert set(r) == set(e), "Sets should match"
            # Check order preservation - first occurrence of each element should be in same order
            r_order = {x: i for i, x in enumerate(r)}
            e_order = {x: i for i, x in enumerate(e)}
            assert r_order == e_order, "Order of first occurrence should be preserved"


def test_list_unique_fixed_size():
    table = MicroPartition.from_pydict(
        {
            "a": [[1, 2], [2, 2], [3, 3], [11, 11], None, [2, None], None],
        }
    ).eval_expression_list([col("a").cast(DataType.fixed_size_list(DataType.int64(), 2))])

    # Test with default include_nulls=False
    res = table.eval_expression_list([col("a").list.unique().alias("unique")])
    result = res.to_pydict()["unique"]
    expected = [[1, 2], [2], [3], [11], None, [2], None]

    # Check that nulls are preserved at list level
    assert [x is None for x in result] == [x is None for x in expected]

    # For non-null lists, check that elements are distinct and sets match
    for r, e in zip(result, expected):
        if r is not None:
            assert len(r) == len(set(r)), "Elements should be distinct"
            assert set(r) == set(e), "Sets should match"
            # Check order preservation - first occurrence of each element should be in same order
            r_order = {x: i for i, x in enumerate(r) if x is not None}
            e_order = {x: i for i, x in enumerate(e) if x is not None}
            assert r_order == e_order, "Order of first occurrence should be preserved"

    # Test with include_nulls=True
    res = table.eval_expression_list([col("a").list.unique(include_nulls=True).alias("unique")])
    result = res.to_pydict()["unique"]
    expected = [[1, 2], [2], [3], [11], None, [2, None], None]

    # Check that nulls are preserved at list level
    assert [x is None for x in result] == [x is None for x in expected]

    # For non-null lists, check that elements are distinct and sets match
    for r, e in zip(result, expected):
        if r is not None:
            assert len(r) == len(set(x for x in r)), "Elements should be distinct"
            assert set(r) == set(e), "Sets should match"
            # Check order preservation - first occurrence of each element should be in same order
            r_order = {x: i for i, x in enumerate(r)}
            e_order = {x: i for i, x in enumerate(e)}
            assert r_order == e_order, "Order of first occurrence should be preserved"
