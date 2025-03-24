from __future__ import annotations

import pytest

from daft import Window, col
from tests.conftest import get_tests_daft_runner_name
from tests.window.test_partition_only import assert_equal_ignoring_order


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_rank_function(make_df):
    """Test rank function with partition by and order by clauses.

    Note: This test is for a rank function we plan to implement.
    Currently serves as a specification for the expected behavior.
    """
    # Create a DataFrame with category and sales data
    df = make_df(
        {"category": ["A", "A", "A", "B", "B", "B", "C", "C"], "sales": [100, 200, 50, 500, 100, 300, 250, 150]}
    )

    # Define a window specification with partition by and order by
    window_spec = Window.partition_by("category").order_by("sales", ascending=False)

    # Apply the rank function over the window
    # Note: The rank() function is not yet implemented, this is a specification
    result = df.select(
        col("category"), col("sales"), col("sales").rank().over(window_spec).alias("rank_sales")
    ).collect()

    # Expected results:
    # - Category A: 200(rank 1), 100(rank 2), 50(rank 3)
    # - Category B: 500(rank 1), 300(rank 2), 100(rank 3)
    # - Category C: 250(rank 1), 150(rank 2)
    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "sales": [100, 200, 50, 500, 100, 300, 250, 150],
        "rank_sales": [2, 1, 3, 1, 3, 2, 1, 2],
    }

    assert_equal_ignoring_order(result.to_pydict(), expected)


@pytest.mark.skip(reason="Dense rank function not yet implemented")
@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner")
def test_dense_rank_function(make_df):
    """Test dense_rank function with partition by and order by clauses.

    Note: This test is for a dense_rank function we plan to implement.
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

    # Apply the dense_rank function over the window
    # Note: The dense_rank() function is not yet implemented, this is a specification
    result = df.select(
        col("category"), col("sales"), col("sales").dense_rank().over(window_spec).alias("dense_rank_sales")
    ).collect()

    # Expected results:
    # Dense rank doesn't skip ranks for ties
    # - Category A: 200(rank 1), 100(rank 2), 50(rank 3)
    # - Category B: 500(rank 1), 300(rank 2), 100(rank 3), 100(rank 3)
    # - Category C: 250(rank 1), 150(rank 2)
    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "B", "C", "C"],
        "sales": [100, 200, 50, 500, 100, 100, 300, 250, 150],
        "dense_rank_sales": [2, 1, 3, 1, 3, 3, 2, 1, 2],
    }

    assert_equal_ignoring_order(result.to_pydict(), expected)
