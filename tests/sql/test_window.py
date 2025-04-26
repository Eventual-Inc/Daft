import random

import pytest

import daft
from daft import Window, col
from daft.functions import dense_rank, rank, row_number
from daft.sql.sql import SQLCatalog
from tests.conftest import assert_df_equals, get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="Window tests only run on native runner"
)


def test_row_number_window_function():
    """Test SQL ROW_NUMBER() window function over partitions with ordering."""
    random.seed(42)

    data = {"category": [], "value": []}

    for category in ["A", "B", "C"]:
        values = random.sample(range(1, 1001), 100)

        for value in values:
            data["category"].append(category)
            data["value"].append(value)

    df = daft.from_pydict(data)

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            value,
            ROW_NUMBER() OVER(
                PARTITION BY category
                ORDER BY value
            ) AS row_num
        FROM test_data
        ORDER BY category, value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("value")

    daft_result = df.with_column("row_num", row_number().over(window_spec)).sort(["category", "value"]).collect()

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "value"])


def test_rank_window_function():
    """Test SQL RANK() window function over partitions with ordering."""
    df = daft.from_pydict(
        {
            "category": ["A", "A", "A", "B", "B", "B", "C", "C", "C"],
            "value": [1, 1, 2, 2, 2, 3, 3, 4, 4],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            value,
            RANK() OVER(
                PARTITION BY category
                ORDER BY value
            ) AS rank_val
        FROM test_data
        ORDER BY category, value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("value")

    daft_result = df.with_column("rank_val", rank().over(window_spec)).sort(["category", "value"]).collect()

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "value"])


def test_dense_rank_window_function():
    """Test SQL DENSE_RANK() window function over partitions with ordering."""
    df = daft.from_pydict(
        {
            "category": ["A", "A", "A", "B", "B", "B", "C", "C", "C"],
            "value": [1, 1, 2, 2, 2, 3, 3, 4, 4],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            value,
            DENSE_RANK() OVER(
                PARTITION BY category
                ORDER BY value
            ) AS dense_rank_val
        FROM test_data
        ORDER BY category, value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("value")

    daft_result = df.with_column("dense_rank_val", dense_rank().over(window_spec)).sort(["category", "value"]).collect()

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "value"])


def test_lag_with_one_arg():
    """Test SQL LAG() with one argument."""
    df = daft.from_pydict(
        {
            "category": ["A", "A", "A", "B", "B", "B"],
            "value": [1, 2, 3, 4, 5, 6],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            value,
            LAG(value) OVER(
                PARTITION BY category
                ORDER BY value
            ) AS lagged
        FROM test_data
        ORDER BY category, value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("value")

    daft_result = df.with_column("lagged", col("value").lag(1).over(window_spec)).sort(["category", "value"]).collect()

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "value"])


def test_lag_with_two_args():
    """Test SQL LAG() with two arguments (value, offset)."""
    df = daft.from_pydict(
        {
            "category": ["A", "A", "A", "B", "B", "B"],
            "value": [1, 2, 3, 4, 5, 6],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            value,
            LAG(value, 2) OVER(
                PARTITION BY category
                ORDER BY value
            ) AS lagged
        FROM test_data
        ORDER BY category, value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("value")

    daft_result = df.with_column("lagged", col("value").lag(2).over(window_spec)).sort(["category", "value"]).collect()

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "value"])


def test_lag_with_three_args():
    """Test SQL LAG() with three arguments (value, offset, default)."""
    df = daft.from_pydict(
        {
            "category": ["A", "A", "A", "B", "B", "B"],
            "value": [1, 2, 3, 4, 5, 6],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            value,
            LAG(value, 2, 999) OVER(
                PARTITION BY category
                ORDER BY value
            ) AS lagged
        FROM test_data
        ORDER BY category, value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("value")

    daft_result = (
        df.with_column("lagged", col("value").lag(2, 999).over(window_spec)).sort(["category", "value"]).collect()
    )

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "value"])


def test_lead_with_one_arg():
    """Test SQL LEAD() with one argument."""
    df = daft.from_pydict(
        {
            "category": ["A", "A", "A", "B", "B", "B"],
            "value": [1, 2, 3, 4, 5, 6],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            value,
            LEAD(value) OVER(
                PARTITION BY category
                ORDER BY value
            ) AS lead_val
        FROM test_data
        ORDER BY category, value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("value")

    daft_result = (
        df.with_column("lead_val", col("value").lead(1).over(window_spec)).sort(["category", "value"]).collect()
    )

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "value"])


def test_lead_with_two_args():
    """Test SQL LEAD() with two arguments (value, offset)."""
    df = daft.from_pydict(
        {
            "category": ["A", "A", "A", "B", "B", "B"],
            "value": [1, 2, 3, 4, 5, 6],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            value,
            LEAD(value, 2) OVER(
                PARTITION BY category
                ORDER BY value
            ) AS lead_val
        FROM test_data
        ORDER BY category, value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("value")

    daft_result = (
        df.with_column("lead_val", col("value").lead(2).over(window_spec)).sort(["category", "value"]).collect()
    )

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "value"])


def test_lead_with_three_args():
    """Test SQL LEAD() with three arguments (value, offset, default)."""
    df = daft.from_pydict(
        {
            "category": ["A", "A", "A", "B", "B", "B"],
            "value": [1, 2, 3, 4, 5, 6],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            value,
            LEAD(value, 2, 999) OVER(
                PARTITION BY category
                ORDER BY value
            ) AS lead_val
        FROM test_data
        ORDER BY category, value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("value")

    daft_result = (
        df.with_column("lead_val", col("value").lead(2, 999).over(window_spec)).sort(["category", "value"]).collect()
    )

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "value"])


def test_multiple_window_functions():
    """Test using multiple window functions together."""
    df = daft.from_pydict(
        {
            "category": ["A", "A", "A", "B", "B", "B"],
            "value": [1, 2, 3, 4, 5, 6],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            value,
            ROW_NUMBER() OVER(PARTITION BY category ORDER BY value) AS row_num,
            RANK() OVER(PARTITION BY category ORDER BY value) AS rank_val,
            DENSE_RANK() OVER(PARTITION BY category ORDER BY value) AS dense_rank_val,
            LAG(value) OVER(PARTITION BY category ORDER BY value) AS lag_val,
            LEAD(value) OVER(PARTITION BY category ORDER BY value) AS lead_val
        FROM test_data
        ORDER BY category, value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("value")

    daft_result = (
        df.with_column("row_num", row_number().over(window_spec))
        .with_column("rank_val", rank().over(window_spec))
        .with_column("dense_rank_val", dense_rank().over(window_spec))
        .with_column("lag_val", col("value").lag(1).over(window_spec))
        .with_column("lead_val", col("value").lead(1).over(window_spec))
        .sort(["category", "value"])
        .collect()
    )

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "value"])


@pytest.mark.skip(reason="Window function implementation on main does not support ROWS BETWEEN yet")
def test_row_number_and_running_sum_window_functions():
    """Test SQL ROW_NUMBER() and SUM() window functions over partitions with ordering."""
    random.seed(42)

    data = {"category": [], "value": []}

    for category in ["A", "B", "C"]:
        values = random.sample(range(1, 1001), 100)

        for value in values:
            data["category"].append(category)
            data["value"].append(value)

    df = daft.from_pydict(data)

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            value,
            ROW_NUMBER() OVER(
                PARTITION BY category
                ORDER BY value
            ) AS row_num,
            SUM(value) OVER(
                PARTITION BY category
                ORDER BY value
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS running_sum
        FROM test_data
        ORDER BY category, value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("value")
    running_window = (
        Window().partition_by("category").order_by("value").rows_between(Window.unbounded_preceding, Window.current_row)
    )

    daft_result = (
        df.with_column("row_num", row_number().over(window_spec))
        .with_column("running_sum", col("value").sum().over(running_window))
        .sort(["category", "value"])
        .collect()
    )

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "value"])
