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
