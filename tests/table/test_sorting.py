from __future__ import annotations

import itertools

import numpy as np
import pyarrow as pa
import pytest

from daft import col
from daft.logical.schema import Schema
from daft.series import Series
from daft.table import MicroPartition
from tests.table import daft_numeric_types, daft_string_types


@pytest.mark.parametrize(
    "mp",
    [
        MicroPartition.from_pydict({"a": pa.array([], type=pa.int64())}),  # 1 empty table
        MicroPartition.empty(Schema.from_pyarrow_schema(pa.schema({"a": pa.int64()}))),  # No tables
    ],
)
def test_micropartitions_sort_empty(mp) -> None:
    sorted_table = mp.sort([col("a")])
    assert len(mp) == len(sorted_table) == 0
    assert mp.schema() == sorted_table.schema()


@pytest.mark.parametrize(
    "mp",
    [
        MicroPartition.from_pydict({"a": [1, 3, 2, 4]}),  # 1 table
        MicroPartition.concat(
            [
                MicroPartition.from_pydict({"a": np.array([]).astype(np.int64)}),
                MicroPartition.from_pydict({"a": [1]}),
                MicroPartition.from_pydict({"a": [3, 2, 4]}),
            ]
        ),  # 3 tables
    ],
)
def test_micropartitions_sort(mp) -> None:
    sorted_table = mp.sort([col("a")])
    assert len(mp) == len(sorted_table) == 4
    assert mp.schema() == sorted_table.schema()
    assert sorted_table.to_pydict()["a"] == sorted(mp.to_pydict()["a"])


@pytest.mark.parametrize(
    "sort_dtype, value_dtype, first_col",
    itertools.product(daft_numeric_types + daft_string_types, daft_numeric_types + daft_string_types, [False, True]),
)
def test_table_single_col_sorting(sort_dtype, value_dtype, first_col) -> None:
    pa_table = pa.Table.from_pydict({"a": [None, 4, 2, 1, 5], "b": [0, 1, 2, 3, None]})

    argsort_order = Series.from_pylist([3, 2, 1, 4, 0])

    daft_table = MicroPartition.from_arrow(pa_table)

    if first_col:
        daft_table = daft_table.eval_expression_list([col("a").cast(sort_dtype), col("b").cast(value_dtype)])
    else:
        daft_table = daft_table.eval_expression_list([col("b").cast(value_dtype), col("a").cast(sort_dtype)])

    assert len(daft_table) == 5
    if first_col:
        assert daft_table.column_names() == ["a", "b"]
    else:
        assert daft_table.column_names() == ["b", "a"]

    sorted_table = daft_table.sort([col("a")])

    assert len(sorted_table) == 5

    if first_col:
        assert sorted_table.column_names() == ["a", "b"]
    else:
        assert sorted_table.column_names() == ["b", "a"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()

    assert daft_table.argsort([col("a")]).to_pylist() == argsort_order.to_pylist()

    # Descending

    sorted_table = daft_table.sort([col("a")], descending=True)

    assert len(sorted_table) == 5
    if first_col:
        assert sorted_table.column_names() == ["a", "b"]
    else:
        assert sorted_table.column_names() == ["b", "a"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()[::-1]
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()[::-1]

    assert daft_table.argsort([col("a")], descending=True).to_pylist() == argsort_order.to_pylist()[::-1]


@pytest.mark.parametrize(
    "sort_dtype, value_dtype, data",
    itertools.product(
        daft_numeric_types + daft_string_types,
        daft_numeric_types + daft_string_types,
        [
            ([None, 4, 2, 1, 5], [0, 1, 2, 3, None], False, False, [3, 2, 1, 4, 0]),
            ([None, 4, 2, 1, 5], [0, 1, 2, 3, None], False, True, [3, 2, 1, 4, 0]),
            ([1, 1, 1, 1, 1], [None, 3, 1, 2, 0], False, False, [4, 2, 3, 1, 0]),
            ([1, 1, 1, 1, 1], [None, 3, 1, 2, 0], True, False, [4, 2, 3, 1, 0]),
            ([None, None, None, None, None], [None, 3, 1, 2, 0], False, False, [4, 2, 3, 1, 0]),
            ([None, None, None, None, None], [None, 3, 1, 2, 0], True, False, [4, 2, 3, 1, 0]),
            ([None, 4, 2, 1, 5], [None, None, None, None, None], False, False, [3, 2, 1, 4, 0]),
            ([None, 4, 2, 1, 5], [None, None, None, None, None], False, True, [3, 2, 1, 4, 0]),
        ],
    ),
)
def test_table_multiple_col_sorting(sort_dtype, value_dtype, data) -> None:
    a, b, a_desc, b_desc, expected = data
    pa_table = pa.Table.from_pydict({"a": a, "b": b})

    argsort_order = Series.from_pylist(expected)

    daft_table = MicroPartition.from_arrow(pa_table)

    daft_table = daft_table.eval_expression_list([col("a").cast(sort_dtype), col("b").cast(value_dtype)])

    assert len(daft_table) == 5
    assert daft_table.column_names() == ["a", "b"]

    sorted_table = daft_table.sort([col("a"), col("b")], descending=[a_desc, b_desc])

    assert len(sorted_table) == 5

    assert sorted_table.column_names() == ["a", "b"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()

    assert (
        daft_table.argsort([col("a"), col("b")], descending=[a_desc, b_desc]).to_pylist() == argsort_order.to_pylist()
    )

    # Descending

    sorted_table = daft_table.sort([col("a"), col("b")], descending=[not a_desc, not b_desc])

    assert len(sorted_table) == 5

    assert sorted_table.column_names() == ["a", "b"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()[::-1]
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()[::-1]

    assert (
        daft_table.argsort([col("a"), col("b")], descending=[not a_desc, not b_desc]).to_pylist()
        == argsort_order.to_pylist()[::-1]
    )


@pytest.mark.parametrize(
    "data",
    [
        ([None, 4, 2, 1, 5], [0, 1, 2, 3, None], False, False, [3, 2, 1, 4, 0]),
        ([None, 4, 2, 1, 5], [0, 1, 2, 3, None], False, True, [3, 2, 1, 4, 0]),
        ([1, 1, 1, 1, 1], [None, 3, 1, 2, 0], False, False, [4, 2, 3, 1, 0]),
        ([1, 1, 1, 1, 1], [None, 3, 1, 2, 0], True, False, [4, 2, 3, 1, 0]),
        ([None, None, None, None, None], [None, 3, 1, 2, 0], False, False, [4, 2, 3, 1, 0]),
        ([None, None, None, None, None], [None, 3, 1, 2, 0], True, False, [4, 2, 3, 1, 0]),
        ([None, 4, 2, 1, 5], [None, None, None, None, None], False, False, [3, 2, 1, 4, 0]),
        ([None, 4, 2, 1, 5], [None, None, None, None, None], False, True, [3, 2, 1, 4, 0]),
    ],
)
def test_table_multiple_col_sorting_binary(data) -> None:
    a, b, a_desc, b_desc, expected = data
    a = pa.array([x.to_bytes(1, "little") if x is not None else None for x in a], type=pa.binary())
    b = pa.array([x.to_bytes(1, "little") if x is not None else None for x in b], type=pa.binary())

    pa_table = pa.Table.from_pydict({"a": a, "b": b})

    argsort_order = Series.from_pylist(expected)

    daft_table = MicroPartition.from_arrow(pa_table)
    assert len(daft_table) == 5
    assert daft_table.column_names() == ["a", "b"]

    sorted_table = daft_table.sort([col("a"), col("b")], descending=[a_desc, b_desc])

    assert len(sorted_table) == 5

    assert sorted_table.column_names() == ["a", "b"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()

    assert (
        daft_table.argsort([col("a"), col("b")], descending=[a_desc, b_desc]).to_pylist() == argsort_order.to_pylist()
    )

    # Descending

    sorted_table = daft_table.sort([col("a"), col("b")], descending=[not a_desc, not b_desc])

    assert len(sorted_table) == 5

    assert sorted_table.column_names() == ["a", "b"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()[::-1]
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()[::-1]

    assert (
        daft_table.argsort([col("a"), col("b")], descending=[not a_desc, not b_desc]).to_pylist()
        == argsort_order.to_pylist()[::-1]
    )


@pytest.mark.parametrize(
    "second_dtype, data",
    itertools.product(
        daft_numeric_types + daft_string_types,
        [
            ([None, True, False, True, False], [0, 1, 2, 3, None], False, False, [2, 4, 1, 3, 0]),
            ([None, True, False, True, False], [0, 1, 2, 3, None], True, False, [0, 1, 3, 2, 4]),
            ([True, True, True, True, True], [None, 3, 1, 2, 0], False, False, [4, 2, 3, 1, 0]),
            ([True, True, True, True, True], [None, 3, 1, 2, 0], True, False, [4, 2, 3, 1, 0]),
            ([None, None, None, None, None], [None, 3, 1, 2, 0], False, False, [4, 2, 3, 1, 0]),
            ([None, None, None, None, None], [None, 3, 1, 2, 0], True, False, [4, 2, 3, 1, 0]),
        ],
    ),
)
def test_table_boolean_multiple_col_sorting(second_dtype, data) -> None:
    a, b, a_desc, b_desc, expected = data
    pa_table = pa.Table.from_pydict({"a": a, "b": b})
    argsort_order = Series.from_pylist(expected)

    daft_table = MicroPartition.from_arrow(pa_table)

    daft_table = daft_table.eval_expression_list([col("a"), col("b").cast(second_dtype)])

    assert len(daft_table) == 5
    assert daft_table.column_names() == ["a", "b"]

    sorted_table = daft_table.sort([col("a"), col("b")], descending=[a_desc, b_desc])

    assert len(sorted_table) == 5

    assert sorted_table.column_names() == ["a", "b"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()

    assert (
        daft_table.argsort([col("a"), col("b")], descending=[a_desc, b_desc]).to_pylist() == argsort_order.to_pylist()
    )

    # Descending

    sorted_table = daft_table.sort([col("a"), col("b")], descending=[not a_desc, not b_desc])

    assert len(sorted_table) == 5

    assert sorted_table.column_names() == ["a", "b"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()[::-1]
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()[::-1]

    assert (
        daft_table.argsort([col("a"), col("b")], descending=[not a_desc, not b_desc]).to_pylist()
        == argsort_order.to_pylist()[::-1]
    )


def test_table_sample() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    source_pairs = {(1, 5), (2, 6), (3, 7), (4, 8)}

    daft_table = MicroPartition.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    # subsample
    sampled = daft_table.sample(size=3)
    assert len(sampled) == 3
    assert sampled.column_names() == ["a", "b"]
    assert all(
        pair in source_pairs for pair in zip(sampled.get_column("a").to_pylist(), sampled.get_column("b").to_pylist())
    )

    # oversample
    sampled = daft_table.sample(size=4)
    assert len(sampled) == 4
    assert sampled.column_names() == ["a", "b"]
    assert all(
        pair in source_pairs for pair in zip(sampled.get_column("a").to_pylist(), sampled.get_column("b").to_pylist())
    )

    # negative sample
    with pytest.raises(ValueError, match="negative size"):
        daft_table.sample(size=-1)

    # fraction > 1.0
    with pytest.raises(ValueError, match="fraction greater than 1.0"):
        daft_table.sample(fraction=1.1)

    # fraction < 0.0
    with pytest.raises(ValueError, match="negative fraction"):
        daft_table.sample(fraction=-0.1)

    # size and fraction
    with pytest.raises(ValueError, match="Must specify either `fraction` or `size`"):
        daft_table.sample(size=1, fraction=0.5)

    # no arguments
    with pytest.raises(ValueError, match="Must specify either `fraction` or `size`"):
        daft_table.sample()


@pytest.mark.parametrize("size, k", itertools.product([0, 1, 10, 33, 100, 101], [0, 1, 2, 3, 100, 101, 200]))
def test_table_quantiles(size, k) -> None:
    first = np.arange(size)

    second = 2 * first

    daft_table = MicroPartition.from_pydict({"a": first, "b": second})
    assert len(daft_table) == size
    assert daft_table.column_names() == ["a", "b"]

    # sub
    quantiles = daft_table.quantiles(k)

    if size > 0:
        assert len(quantiles) == max(k - 1, 0)
    else:
        assert len(quantiles) == 0

    assert quantiles.column_names() == ["a", "b"]
    ind = quantiles.get_column("a").to_pylist()

    if k > 0:
        assert np.all(np.diff(ind) >= 0)
        expected_delta = size / k
        assert np.all(np.abs(np.diff(ind) - expected_delta) <= 1)
    else:
        assert len(ind) == 0


def test_table_quantiles_bad_input() -> None:
    # negative sample

    first = np.arange(10)

    second = 2 * first

    pa_table = pa.Table.from_pydict({"a": first, "b": second})

    daft_table = MicroPartition.from_arrow(pa_table)

    with pytest.raises(ValueError, match="negative number"):
        daft_table.quantiles(-1)


def test_string_table_sorting():
    daft_table = MicroPartition.from_pydict(
        {
            "firstname": [
                "bob",
                "alice",
                "eve",
                None,
                None,
                "bob",
                "alice",
            ],
            "lastname": ["a", "a", "a", "bond", None, None, "a"],
        }
    )
    sorted_table = daft_table.sort([col("firstname"), col("lastname")])
    assert sorted_table.to_pydict() == {
        "firstname": ["alice", "alice", "bob", "bob", "eve", None, None],
        "lastname": ["a", "a", "a", None, "a", "bond", None],
    }
