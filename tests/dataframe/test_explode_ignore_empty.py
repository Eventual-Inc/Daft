from __future__ import annotations

import daft


def test_explode_ignore_empty():
    data = {"id": [1, 2, 3, 4], "values": [[1, 2], [], None, [3]]}
    df = daft.from_pydict(data)

    # Default behavior (ignore_empty=False) - keep empty/null as None
    exploded_default = df.explode(daft.col("values")).to_pydict()
    assert exploded_default["id"] == [1, 1, 2, 3, 4]
    assert exploded_default["values"] == [1, 2, None, None, 3]

    # New behavior (ignore_empty=True) - drop empty/null
    exploded_drop = df.explode(daft.col("values"), ignore_empty=True).to_pydict()
    assert exploded_drop["id"] == [1, 1, 4]
    assert exploded_drop["values"] == [1, 2, 3]


def test_explode_multi_ignore_empty():
    data = {"id": [1, 2, 3], "v1": [[1, 2], [], [3]], "v2": [["a", "b"], [], ["c"]]}
    df = daft.from_pydict(data)

    # ignore_empty=True
    exploded = df.explode(daft.col("v1"), daft.col("v2"), ignore_empty=True).to_pydict()
    assert exploded["id"] == [1, 1, 3]
    assert exploded["v1"] == [1, 2, 3]
    assert exploded["v2"] == ["a", "b", "c"]

    # ignore_empty=False
    exploded = df.explode(daft.col("v1"), daft.col("v2"), ignore_empty=False).to_pydict()
    assert exploded["id"] == [1, 1, 2, 3]
    assert exploded["v1"] == [1, 2, None, 3]
    assert exploded["v2"] == ["a", "b", None, "c"]


def test_explode_expression_ignore_empty():
    df = daft.from_pydict({"a": [[1, 2], [], None]})

    # Using Expression.explode
    exploded = df.select(daft.col("a").explode(ignore_empty=True)).to_pydict()
    assert exploded["a"] == [1, 2]

    exploded = df.select(daft.col("a").explode(ignore_empty=False)).to_pydict()
    assert exploded["a"] == [1, 2, None, None]


def test_explode_function_ignore_empty():
    from daft.functions import explode

    df = daft.from_pydict({"a": [[1, 2], [], None]})

    # Using daft.functions.explode
    exploded = df.select(explode(daft.col("a"), ignore_empty=True)).to_pydict()
    assert exploded["a"] == [1, 2]

    exploded = df.select(explode(daft.col("a"), ignore_empty=False)).to_pydict()
    assert exploded["a"] == [1, 2, None, None]


# Edge case tests


def test_explode_all_empty_lists():
    """Test explode when all rows are empty lists."""
    df = daft.from_pydict({"id": [1, 2, 3], "values": [[], [], []]})

    # ignore_empty=False: each empty list produces a null row
    exploded = df.explode(daft.col("values"), ignore_empty=False).to_pydict()
    assert exploded["id"] == [1, 2, 3]
    assert exploded["values"] == [None, None, None]

    # ignore_empty=True: all rows are dropped
    exploded = df.explode(daft.col("values"), ignore_empty=True).to_pydict()
    assert exploded["id"] == []
    assert exploded["values"] == []


def test_explode_all_nulls():
    """Test explode when all rows are None."""
    import pyarrow as pa

    # Need to explicitly specify list type, otherwise all-null column is inferred as Null type
    arr = pa.array([None, None, None], type=pa.list_(pa.int64()))
    df = daft.from_arrow(pa.table({"id": [1, 2, 3], "values": arr}))

    # ignore_empty=False: each null produces a null row
    exploded = df.explode(daft.col("values"), ignore_empty=False).to_pydict()
    assert exploded["id"] == [1, 2, 3]
    assert exploded["values"] == [None, None, None]

    # ignore_empty=True: all rows are dropped
    exploded = df.explode(daft.col("values"), ignore_empty=True).to_pydict()
    assert exploded["id"] == []
    assert exploded["values"] == []


def test_explode_mixed_empty_and_null():
    """Test explode with mixed empty lists and nulls."""
    df = daft.from_pydict({"id": [1, 2, 3, 4], "values": [[], None, [], None]})

    # ignore_empty=False
    exploded = df.explode(daft.col("values"), ignore_empty=False).to_pydict()
    assert exploded["id"] == [1, 2, 3, 4]
    assert exploded["values"] == [None, None, None, None]

    # ignore_empty=True
    exploded = df.explode(daft.col("values"), ignore_empty=True).to_pydict()
    assert exploded["id"] == []
    assert exploded["values"] == []


def test_explode_no_empty_or_null():
    """Test explode when there are no empty lists or nulls - both modes should be identical."""
    df = daft.from_pydict({"id": [1, 2, 3], "values": [[1], [2, 3], [4, 5, 6]]})

    exploded_false = df.explode(daft.col("values"), ignore_empty=False).to_pydict()
    exploded_true = df.explode(daft.col("values"), ignore_empty=True).to_pydict()

    assert exploded_false == exploded_true
    assert exploded_false["id"] == [1, 2, 2, 3, 3, 3]
    assert exploded_false["values"] == [1, 2, 3, 4, 5, 6]


def test_explode_fixed_size_list_ignore_empty():
    """Test explode with FixedSizeList type."""
    import pyarrow as pa

    # Create FixedSizeList with size 2
    arr = pa.array([[1, 2], [3, 4], None], type=pa.list_(pa.int64(), 2))
    df = daft.from_arrow(pa.table({"id": [1, 2, 3], "values": arr}))

    # ignore_empty=False: null produces a null row
    exploded = df.explode(daft.col("values"), ignore_empty=False).to_pydict()
    assert exploded["id"] == [1, 1, 2, 2, 3]
    assert exploded["values"] == [1, 2, 3, 4, None]

    # ignore_empty=True: null row is dropped
    exploded = df.explode(daft.col("values"), ignore_empty=True).to_pydict()
    assert exploded["id"] == [1, 1, 2, 2]
    assert exploded["values"] == [1, 2, 3, 4]


def test_explode_nested_list_ignore_empty():
    """Test explode with nested lists (list of lists)."""
    df = daft.from_pydict({"id": [1, 2, 3], "values": [[[1, 2], [3]], [], [[4]]]})

    # ignore_empty=False
    exploded = df.explode(daft.col("values"), ignore_empty=False).to_pydict()
    assert exploded["id"] == [1, 1, 2, 3]
    assert exploded["values"] == [[1, 2], [3], None, [4]]

    # ignore_empty=True
    exploded = df.explode(daft.col("values"), ignore_empty=True).to_pydict()
    assert exploded["id"] == [1, 1, 3]
    assert exploded["values"] == [[1, 2], [3], [4]]


def test_explode_single_row_empty():
    """Test explode with a single row that is empty."""
    df = daft.from_pydict({"id": [1], "values": [[]]})

    # ignore_empty=False
    exploded = df.explode(daft.col("values"), ignore_empty=False).to_pydict()
    assert exploded["id"] == [1]
    assert exploded["values"] == [None]

    # ignore_empty=True
    exploded = df.explode(daft.col("values"), ignore_empty=True).to_pydict()
    assert exploded["id"] == []
    assert exploded["values"] == []
