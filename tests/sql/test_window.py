import random

import pytest

import daft
from daft import Window, col
from daft.functions import row_number
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
