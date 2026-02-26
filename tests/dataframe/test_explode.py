from __future__ import annotations

import pyarrow as pa
import pytest

from daft.expressions import col


@pytest.mark.parametrize(
    "data",
    [
        pa.array([[1, 2], [3, 4], None, []], type=pa.list_(pa.int64())),
        pa.array([[1, 2], [3, 4], None, []], type=pa.large_list(pa.int64())),
    ],
)
def test_explode(make_df, data):
    df = make_df({"nested": data, "sidecar": ["a", "b", "c", "d"]})
    df = df.explode(col("nested"))
    assert df.to_pydict() == {
        "nested": [1, 2, 3, 4, None, None],
        "sidecar": ["a", "a", "b", "b", "c", "d"],
    }


@pytest.mark.parametrize(
    "data",
    [
        pa.array([[1, 2], [3, 4], None, []], type=pa.list_(pa.int64())),
        pa.array([[1, 2], [3, 4], None, []], type=pa.large_list(pa.int64())),
    ],
)
def test_explode_multiple_cols(make_df, data):
    df = make_df({"nested": data, "nested2": data, "sidecar": ["a", "b", "c", "d"]})
    df = df.explode(col("nested"), col("nested2"))
    assert df.to_pydict() == {
        "nested": [1, 2, 3, 4, None, None],
        "nested2": [1, 2, 3, 4, None, None],
        "sidecar": ["a", "a", "b", "b", "c", "d"],
    }


@pytest.mark.parametrize(
    "data_type,sample_data,sidecar,expected_sidecar",
    [
        # List of maps with timestamp values - 4 rows -> 5 rows
        (
            pa.list_(pa.map_(pa.string(), pa.timestamp("us"))),
            [
                [
                    [
                        ("time1", 1717000000000),
                        ("time2", 1717000000001),
                    ]
                ],
                [
                    [
                        ("time1", 1717000000002),
                        ("time2", 1717000000003),
                    ],
                    [
                        ("time3", 1717000000004),
                    ],
                ],
                None,
                [],
            ],
            # initial sidecar
            ["a", "b", "c", "d"],
            # expected sidecar
            ["a", "b", "b", "c", "d"],
        ),
        # List of maps with integer values - 3 rows -> 4 rows
        (
            pa.list_(pa.map_(pa.string(), pa.int64())),
            [
                [
                    [
                        ("score", 95),
                        ("rating", 5),
                    ]
                ],
                [
                    [
                        ("score", 87),
                        ("rating", 4),
                    ],
                    [
                        ("views", 1000),
                    ],
                ],
                [],
            ],
            # initial sidecar
            ["x", "y", "z"],
            # expected sidecar
            ["x", "y", "y", "z"],
        ),
        # List of structs with nested map values - 1 row -> 5 rows
        (
            pa.list_(
                pa.struct(
                    [
                        (
                            "typeMap",
                            pa.map_(pa.string(), pa.map_(pa.string(), pa.list_(pa.string()))),
                        ),
                    ]
                )
            ),
            [
                [
                    {
                        "typeMap": [("genre", [("rock", ["alternative", "indie"])])],
                    },
                    {
                        "typeMap": [("genre", [("pop", ["dance", "electronic"])])],
                    },
                    {
                        "typeMap": [("mood", [("happy", ["upbeat", "energetic"])])],
                    },
                    {
                        "typeMap": [("style", [("classical", ["baroque", "romantic"])])],
                    },
                    {
                        "typeMap": [("era", [("modern", ["contemporary", "experimental"])])],
                    },
                ]
            ],
            # initial sidecar
            ["p"],
            # expected sidecar
            ["p", "p", "p", "p", "p"],
        ),
    ],
)
def test_explode_list_of_maps(make_df, data_type, sample_data, sidecar, expected_sidecar):
    """Test exploding lists of maps with various value types."""
    field = pa.field("data", data_type)
    array = pa.array(sample_data, type=field.type)

    df = make_df({"data": array, "sidecar": sidecar})
    df = df.explode(col("data"))

    result = df.to_pydict()
    # Check that we have the expected number of rows after exploding
    assert len(result["sidecar"]) == len(expected_sidecar)
    assert result["sidecar"] == expected_sidecar


def test_explode_bad_col_type(make_df):
    df = make_df({"a": [1, 2, 3]})
    with pytest.raises(ValueError, match="Input must be a list"):
        df = df.explode(col("a"))


def test_explode_with_index_column(make_df):
    df = make_df({"a": [[1, 2], [3, 4, 3]]})
    result = df.explode("a", index_column="idx").to_pydict()
    assert result == {
        "a": [1, 2, 3, 4, 3],
        "idx": [0, 1, 0, 1, 2],
    }


@pytest.mark.parametrize(
    "data",
    [
        pa.array([[1, 2], [3, 4], None, []], type=pa.list_(pa.int64())),
        pa.array([[1, 2], [3, 4], None, []], type=pa.large_list(pa.int64())),
    ],
)
def test_explode_with_index_column_null_and_empty(make_df, data):
    df = make_df({"nested": data, "sidecar": ["a", "b", "c", "d"]})
    result = df.explode(col("nested"), index_column="idx").to_pydict()
    assert result == {
        "nested": [1, 2, 3, 4, None, None],
        "sidecar": ["a", "a", "b", "b", "c", "d"],
        "idx": [0, 1, 0, 1, None, None],
    }


def test_explode_with_index_column_multiple_cols(make_df):
    data = pa.array([[1, 2], [3, 4]], type=pa.list_(pa.int64()))
    df = make_df({"nested": data, "nested2": data, "sidecar": ["a", "b"]})
    result = df.explode(col("nested"), col("nested2"), index_column="idx").to_pydict()
    assert result == {
        "nested": [1, 2, 3, 4],
        "nested2": [1, 2, 3, 4],
        "sidecar": ["a", "a", "b", "b"],
        "idx": [0, 1, 0, 1],
    }


# Tests for ignore_empty_and_null parameter


def test_explode_ignore_empty_and_null(make_df):
    """Test basic ignore_empty_and_null functionality."""
    import daft

    data = {"id": [1, 2, 3, 4], "values": [[1, 2], [], None, [3]]}
    df = daft.from_pydict(data)

    # Default behavior (ignore_empty_and_null=False) - keep empty/null as None
    exploded_default = df.explode(daft.col("values")).to_pydict()
    assert exploded_default["id"] == [1, 1, 2, 3, 4]
    assert exploded_default["values"] == [1, 2, None, None, 3]

    # New behavior (ignore_empty_and_null=True) - drop empty/null
    exploded_drop = df.explode(daft.col("values"), ignore_empty_and_null=True).to_pydict()
    assert exploded_drop["id"] == [1, 1, 4]
    assert exploded_drop["values"] == [1, 2, 3]


def test_explode_multi_ignore_empty_and_null(make_df):
    """Test ignore_empty_and_null with multiple columns."""
    import daft

    data = {"id": [1, 2, 3], "v1": [[1, 2], [], [3]], "v2": [["a", "b"], [], ["c"]]}
    df = daft.from_pydict(data)

    # ignore_empty_and_null=True
    exploded = df.explode(daft.col("v1"), daft.col("v2"), ignore_empty_and_null=True).to_pydict()
    assert exploded["id"] == [1, 1, 3]
    assert exploded["v1"] == [1, 2, 3]
    assert exploded["v2"] == ["a", "b", "c"]

    # ignore_empty_and_null=False
    exploded = df.explode(daft.col("v1"), daft.col("v2"), ignore_empty_and_null=False).to_pydict()
    assert exploded["id"] == [1, 1, 2, 3]
    assert exploded["v1"] == [1, 2, None, 3]
    assert exploded["v2"] == ["a", "b", None, "c"]


def test_explode_expression_ignore_empty_and_null(make_df):
    """Test ignore_empty_and_null via Expression.explode."""
    import daft

    df = daft.from_pydict({"a": [[1, 2], [], None]})

    # Using Expression.explode
    exploded = df.select(daft.col("a").explode(ignore_empty_and_null=True)).to_pydict()
    assert exploded["a"] == [1, 2]

    exploded = df.select(daft.col("a").explode(ignore_empty_and_null=False)).to_pydict()
    assert exploded["a"] == [1, 2, None, None]


def test_explode_function_ignore_empty_and_null(make_df):
    """Test ignore_empty_and_null via daft.functions.explode."""
    import daft
    from daft.functions import explode

    df = daft.from_pydict({"a": [[1, 2], [], None]})

    # Using daft.functions.explode
    exploded = df.select(explode(daft.col("a"), ignore_empty_and_null=True)).to_pydict()
    assert exploded["a"] == [1, 2]

    exploded = df.select(explode(daft.col("a"), ignore_empty_and_null=False)).to_pydict()
    assert exploded["a"] == [1, 2, None, None]


def test_explode_all_empty_lists(make_df):
    """Test explode when all rows are empty lists."""
    import daft

    df = daft.from_pydict({"id": [1, 2, 3], "values": [[], [], []]})

    # ignore_empty_and_null=False: each empty list produces a null row
    exploded = df.explode(daft.col("values"), ignore_empty_and_null=False).to_pydict()
    assert exploded["id"] == [1, 2, 3]
    assert exploded["values"] == [None, None, None]

    # ignore_empty_and_null=True: all rows are dropped
    exploded = df.explode(daft.col("values"), ignore_empty_and_null=True).to_pydict()
    assert exploded["id"] == []
    assert exploded["values"] == []


def test_explode_all_nulls(make_df):
    """Test explode when all rows are None."""
    import daft

    # Need to explicitly specify list type, otherwise all-null column is inferred as Null type
    arr = pa.array([None, None, None], type=pa.list_(pa.int64()))
    df = daft.from_arrow(pa.table({"id": [1, 2, 3], "values": arr}))

    # ignore_empty_and_null=False: each null produces a null row
    exploded = df.explode(daft.col("values"), ignore_empty_and_null=False).to_pydict()
    assert exploded["id"] == [1, 2, 3]
    assert exploded["values"] == [None, None, None]

    # ignore_empty_and_null=True: all rows are dropped
    exploded = df.explode(daft.col("values"), ignore_empty_and_null=True).to_pydict()
    assert exploded["id"] == []
    assert exploded["values"] == []


def test_explode_fixed_size_list_ignore_empty_and_null(make_df):
    """Test explode with FixedSizeList type."""
    import daft

    # Create FixedSizeList with size 2
    arr = pa.array([[1, 2], [3, 4], None], type=pa.list_(pa.int64(), 2))
    df = daft.from_arrow(pa.table({"id": [1, 2, 3], "values": arr}))

    # ignore_empty_and_null=False: null produces a null row
    exploded = df.explode(daft.col("values"), ignore_empty_and_null=False).to_pydict()
    assert exploded["id"] == [1, 1, 2, 2, 3]
    assert exploded["values"] == [1, 2, 3, 4, None]

    # ignore_empty_and_null=True: null row is dropped
    exploded = df.explode(daft.col("values"), ignore_empty_and_null=True).to_pydict()
    assert exploded["id"] == [1, 1, 2, 2]
    assert exploded["values"] == [1, 2, 3, 4]


def test_explode_nested_list_ignore_empty_and_null(make_df):
    """Test explode with nested lists (list of lists)."""
    import daft

    df = daft.from_pydict({"id": [1, 2, 3], "values": [[[1, 2], [3]], [], [[4]]]})

    # ignore_empty_and_null=False
    exploded = df.explode(daft.col("values"), ignore_empty_and_null=False).to_pydict()
    assert exploded["id"] == [1, 1, 2, 3]
    assert exploded["values"] == [[1, 2], [3], None, [4]]

    # ignore_empty_and_null=True
    exploded = df.explode(daft.col("values"), ignore_empty_and_null=True).to_pydict()
    assert exploded["id"] == [1, 1, 3]
    assert exploded["values"] == [[1, 2], [3], [4]]
