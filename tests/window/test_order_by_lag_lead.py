from __future__ import annotations

import pytest

from daft import Window, col
from tests.conftest import get_tests_daft_runner_name
from tests.window.test_partition_only import assert_equal_ignoring_order


@pytest.mark.skip(reason="Row number function not yet implemented")
@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_row_number_function(make_df):
    """Test row_number function with partition by and order by clauses.

    Note: This test is for a row_number function we plan to implement.
    Currently serves as a specification for the expected behavior.
    """
    # Create a DataFrame with category and sales data, including duplicates
    df = make_df(
        {
            "category": ["A", "A", "A", "B", "B", "B", "B", "C", "C"],
            "sales": [100, 200, 50, 500, 100, 100, 300, 250, 150],
        }
    )

    # Define a window specification with partition by and order by
    window_spec = Window.partition_by("category").order_by("sales", ascending=False)

    # Apply the row_number function over the window
    # Note: The row_number() function is not yet implemented, this is a specification
    result = df.select(
        col("category"), col("sales"), col("sales").row_number().over(window_spec).alias("row_num")
    ).collect()

    # Expected results:
    # Row number assigns unique sequential integers regardless of ties
    # - Category A: 200(1), 100(2), 50(3)
    # - Category B: 500(1), 300(2), 100(3), 100(4)  <- note the sequential numbers even for duplicates
    # - Category C: 250(1), 150(2)
    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "B", "C", "C"],
        "sales": [100, 200, 50, 500, 100, 100, 300, 250, 150],
        "row_num": [2, 1, 3, 1, 3, 4, 2, 1, 2],
    }

    assert_equal_ignoring_order(result.to_pydict(), expected)


@pytest.mark.skip(reason="Lead/lag functions not yet implemented")
@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_lead_lag_functions(make_df):
    """Test lead and lag functions with partition by and order by clauses.

    Note: These are future functions we plan to implement.
    Currently serves as a specification for the expected behavior.
    """
    # Create a DataFrame with category and date-ordered sales data
    df = make_df(
        {
            "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
            "date": [
                "2023-01-01",
                "2023-01-02",
                "2023-01-03",
                "2023-01-01",
                "2023-01-02",
                "2023-01-03",
                "2023-01-01",
                "2023-01-02",
            ],
            "sales": [100, 120, 90, 200, 210, 190, 300, 250],
        }
    )

    # Define a window specification with partition by and order by
    window_spec = Window.partition_by("category").order_by("date")

    # Apply the lead and lag functions over the window
    # Note: The lead() and lag() functions are not yet implemented, this is a specification
    result = df.select(
        col("category"),
        col("date"),
        col("sales"),
        col("sales").lag(1).over(window_spec).alias("prev_day_sales"),
        col("sales").lead(1).over(window_spec).alias("next_day_sales"),
    ).collect()

    # Expected results:
    # - Category A: (null, 100, 120), (100, 120, 90), (120, 90, null)
    # - Category B: (null, 200, 210), (200, 210, 190), (210, 190, null)
    # - Category C: (null, 300, 250), (300, 250, null)
    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "date": [
            "2023-01-01",
            "2023-01-02",
            "2023-01-03",
            "2023-01-01",
            "2023-01-02",
            "2023-01-03",
            "2023-01-01",
            "2023-01-02",
        ],
        "sales": [100, 120, 90, 200, 210, 190, 300, 250],
        "prev_day_sales": [None, 100, 120, None, 200, 210, None, 300],
        "next_day_sales": [120, 90, None, 210, 190, None, 250, None],
    }

    assert_equal_ignoring_order(result.to_pydict(), expected)
