import pandas as pd
import pytest

import daft

COLUMN_NAME = "values"
CONTAINS_COLUMN_NAME = "contains"
OUTPUT_COLUMN_NAME = "output"


def assert_equal(df: daft.DataFrame, expected):
    pd.testing.assert_series_equal(
        pd.Series(df.to_pydict()[OUTPUT_COLUMN_NAME]),
        pd.Series(expected),
        check_exact=True,
    )


# These test contain a single, literal value for the contains expression.
# This value is checked for against each list inside the "values" column.
LITERAL_CONTAINS_TESTS = [
    [
        [
            [1, 2, 3, 4, 5],
            [2, 3, 4, 5, 6],
            [3, 4, 5, 6, 7],
        ],
        daft.lit(2).cast(daft.DataType.int64()),
        [True, True, False],
    ],
    [
        [
            [None] * 10,
        ]
        * 10,
        None,
        [False] * 10,
    ],
    [
        [
            [None, 1],
        ],
        daft.lit(None).cast(daft.DataType.int64()),
        [False],
    ],
]


@pytest.mark.parametrize("test_cases", LITERAL_CONTAINS_TESTS)
def test_list_contains_on_literals(
    test_cases,
):
    table, contains, expected = test_cases
    df = daft.from_pydict({COLUMN_NAME: table})

    if isinstance(contains, daft.Expression):
        contains_expr = contains
    else:
        contains_expr = daft.lit(contains)

    df = df.with_column(OUTPUT_COLUMN_NAME, daft.col(COLUMN_NAME).list.contains(contains_expr)).collect()
    assert_equal(df, expected)


# These test have the "contains" value be a series of the same length as the "values" column.
# Here, a row-by-row evaluation is done.
SERIES_CONTAINS_TESTS = [
    [
        [
            [1, 2, 3, 4, 5],
            [2, 3, 4, 5, 6],
            [3, 4, 5, 6, 7],
        ],
        [1, 2, 3],
        [True, True, True],
        None,
    ],
    [
        [
            [1, 2, 3, 4, 5],
            [2, 3, 4, 5, 6],
            [3, 4, 5, 6, 7],
        ],
        [6, 7, 8],
        [False, False, False],
        None,
    ],
    [
        [
            [None] * 10,
        ]
        * 10,
        [None] * 10,
        [False] * 10,
        None,
    ],
    [
        [
            [None, 1],
        ],
        [None],
        [False],
        daft.DataType.int64(),
    ],
]


@pytest.mark.parametrize("test_cases", SERIES_CONTAINS_TESTS)
def test_list_contains_on_series(
    test_cases,
):
    table, contains, expected, cast = test_cases
    df = daft.from_pydict({COLUMN_NAME: table, CONTAINS_COLUMN_NAME: contains})
    contains_expr = daft.col(CONTAINS_COLUMN_NAME).cast(cast) if cast else daft.col(CONTAINS_COLUMN_NAME)
    df = df.with_column(OUTPUT_COLUMN_NAME, daft.col(COLUMN_NAME).list.contains(contains_expr)).collect()
    assert_equal(df, expected)
