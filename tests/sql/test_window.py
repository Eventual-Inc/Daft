from __future__ import annotations

import datetime
import random

import pandas as pd
import pytest

import daft
from daft import Window, col
from daft.expressions import interval
from daft.functions import dense_rank, rank, row_number
from daft.sql.sql import SQLCatalog
from tests.conftest import assert_df_equals, get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    # and get_context().daft_execution_config.use_experimental_distributed_engine is False,
    reason="requires Native Runner to be in use",
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


def test_range_window_sql():
    """Test SQL window functions with range frame type."""
    random.seed(50)

    data = {"category": [], "ts": [], "value": []}
    possible_timestamps = random.sample(range(1000), 300)
    possible_timestamps.sort()

    for category in ["A", "B"]:
        timestamps = possible_timestamps.copy()
        values = [random.randint(1, 100) for _ in range(len(timestamps))]

        for ts, value in zip(timestamps, values):
            data["category"].append(category)
            data["ts"].append(ts)
            data["value"].append(value)

    df = daft.from_pydict(data)

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            ts,
            value,
            SUM(value) OVER(
                PARTITION BY category
                ORDER BY ts
                RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING
            ) AS range_sum,
            AVG(value) OVER(
                PARTITION BY category
                ORDER BY ts
                RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING
            ) AS range_avg,
            MIN(value) OVER(
                PARTITION BY category
                ORDER BY ts
                RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING
            ) AS range_min,
            MAX(value) OVER(
                PARTITION BY category
                ORDER BY ts
                RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING
            ) AS range_max
        FROM test_data
        ORDER BY category, ts
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("ts", desc=False).range_between(-2, 2)

    daft_result = (
        df.with_column("range_sum", col("value").sum().over(window_spec))
        .with_column("range_avg", col("value").mean().over(window_spec))
        .with_column("range_min", col("value").min().over(window_spec))
        .with_column("range_max", col("value").max().over(window_spec))
        .sort(["category", "ts"])
        .collect()
    )

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "ts"], check_dtype=False)


def test_range_window_desc_sql():
    """Test SQL window functions with range frame type and descending order."""
    random.seed(53)

    data = {"category": [], "ts": [], "value": []}
    possible_timestamps = random.sample(range(1000), 300)
    possible_timestamps.sort()

    for category in ["A", "B"]:
        timestamps = possible_timestamps.copy()
        values = [random.randint(1, 100) for _ in range(len(timestamps))]

        for ts, value in zip(timestamps, values):
            data["category"].append(category)
            data["ts"].append(ts)
            data["value"].append(value)

    df = daft.from_pydict(data)

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            ts,
            value,
            SUM(value) OVER(
                PARTITION BY category
                ORDER BY ts DESC
                RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING
            ) AS range_sum,
            AVG(value) OVER(
                PARTITION BY category
                ORDER BY ts DESC
                RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING
            ) AS range_avg,
            MIN(value) OVER(
                PARTITION BY category
                ORDER BY ts DESC
                RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING
            ) AS range_min,
            MAX(value) OVER(
                PARTITION BY category
                ORDER BY ts DESC
                RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING
            ) AS range_max
        FROM test_data
        ORDER BY category, ts
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("ts", desc=True).range_between(-2, 2)

    daft_result = (
        df.with_column("range_sum", col("value").sum().over(window_spec))
        .with_column("range_avg", col("value").mean().over(window_spec))
        .with_column("range_min", col("value").min().over(window_spec))
        .with_column("range_max", col("value").max().over(window_spec))
        .sort(["category", "ts"])
        .collect()
    )

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "ts"], check_dtype=False)


def test_range_window_with_dates():
    """Test SQL window functions with range frame type and date/timestamp data."""
    random.seed(54)

    base_date = datetime.datetime(2023, 1, 1)
    data = {"category": [], "date": [], "value": []}

    for category in ["A", "B"]:
        dates = sorted([random.randint(0, 1000) for _ in range(700)])
        values = random.sample(range(1, 1000), 700)
        dates = [base_date + datetime.timedelta(days=date) for date in dates]

        for date, value in zip(dates, values):
            data["category"].append(category)
            data["date"].append(date)
            data["value"].append(value)

    pdf = pd.DataFrame(data)
    df = daft.from_pandas(pdf)

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            category,
            date,
            value,
            SUM(value) OVER(
                PARTITION BY category
                ORDER BY date
                RANGE BETWEEN INTERVAL '3' DAY PRECEDING AND INTERVAL '3' DAY FOLLOWING
            ) AS date_range_sum,
            AVG(value) OVER(
                PARTITION BY category
                ORDER BY date
                RANGE BETWEEN INTERVAL '3' DAY PRECEDING AND INTERVAL '3' DAY FOLLOWING
            ) AS date_range_avg
        FROM test_data
        ORDER BY category, date
        """,
        catalog=catalog,
    ).collect()

    window_spec = Window().partition_by("category").order_by("date").range_between(interval(days=-3), interval(days=3))

    daft_result = (
        df.with_column("date_range_sum", col("value").sum().over(window_spec))
        .with_column("date_range_avg", col("value").mean().over(window_spec))
        .sort(["category", "date"])
        .collect()
    )

    assert_df_equals(
        sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["category", "date", "value"], check_dtype=False
    )


def test_order_by_only_row_number():
    """Test SQL ROW_NUMBER() window function with only ORDER BY (no PARTITION BY)."""
    df = daft.from_pydict(
        {
            "value": [100, 200, 50, 500, 125, 300, 250, 150],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            value,
            ROW_NUMBER() OVER(
                ORDER BY value
            ) AS row_num
        FROM test_data
        ORDER BY value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().order_by("value")

    daft_result = df.with_column("row_num", row_number().over(window_spec)).sort(["value"]).collect()

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["value"])


def test_order_by_only_rank():
    """Test SQL RANK() window function with only ORDER BY (no PARTITION BY)."""
    df = daft.from_pydict(
        {
            "value": [1, 1, 2, 2, 2, 3, 3, 4, 4],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            value,
            RANK() OVER(
                ORDER BY value
            ) AS rank_val
        FROM test_data
        ORDER BY value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().order_by("value")

    daft_result = df.with_column("rank_val", rank().over(window_spec)).sort(["value"]).collect()

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["value"])


def test_order_by_only_dense_rank():
    """Test SQL DENSE_RANK() window function with only ORDER BY (no PARTITION BY)."""
    df = daft.from_pydict(
        {
            "value": [1, 1, 2, 2, 2, 3, 3, 4, 4],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            value,
            DENSE_RANK() OVER(
                ORDER BY value
            ) AS dense_rank_val
        FROM test_data
        ORDER BY value
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().order_by("value")

    daft_result = df.with_column("dense_rank_val", dense_rank().over(window_spec)).sort(["value"]).collect()

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["value"])


def test_order_by_only_desc():
    """Test window functions with ORDER BY DESC."""
    df = daft.from_pydict(
        {
            "value": [1, 2, 3, 4, 5],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            value,
            ROW_NUMBER() OVER(ORDER BY value DESC) AS row_num,
            RANK() OVER(ORDER BY value DESC) AS rank_val,
            DENSE_RANK() OVER(ORDER BY value DESC) AS dense_rank_val
        FROM test_data
        ORDER BY value DESC
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().order_by("value", desc=True)

    daft_result = (
        df.with_column("row_num", row_number().over(window_spec))
        .with_column("rank_val", rank().over(window_spec))
        .with_column("dense_rank_val", dense_rank().over(window_spec))
        .sort(["value"], desc=True)
        .collect()
    )

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["value"])


@pytest.mark.parametrize(
    "desc,nulls_first",
    [
        (False, False),
        (False, True),
        (True, False),
        (True, True),
    ],
)
def test_order_by_nulls(desc, nulls_first):
    """Test window functions with ORDER BY NULLS FIRST and NULLS LAST."""
    ids = list(range(1, 7))
    values = [10, None, 20, None, 30, 30]
    data = {
        "id": ids,
        "value": values,
    }
    df = daft.from_pydict(data)
    catalog = SQLCatalog({"test_data": df})
    desc_str = "DESC" if desc else "ASC"
    nulls_first_str = "NULLS FIRST" if nulls_first else "NULLS LAST"
    sql_result = daft.sql(
        f"""
        SELECT
            id,
            value,
            ROW_NUMBER() OVER(ORDER BY value {desc_str} {nulls_first_str}) AS row_num,
            RANK() OVER(ORDER BY value {desc_str} {nulls_first_str}) AS rank_val,
            DENSE_RANK() OVER(ORDER BY value {desc_str} {nulls_first_str}) AS dense_rank_val
        FROM test_data
        ORDER BY id
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().order_by("value", desc=desc, nulls_first=nulls_first)

    daft_result = (
        df.with_column("row_num", row_number().over(window_spec))
        .with_column("rank_val", rank().over(window_spec))
        .with_column("dense_rank_val", dense_rank().over(window_spec))
        .sort(["id"], desc=True)
        .collect()
    )

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["id"])


def test_order_by_only_multiple_columns():
    """Test window functions with ORDER BY multiple columns."""
    df = daft.from_pydict(
        {
            "x": [1, 1, 1, 2, 2, 2],
            "y": [1, 2, 3, 1, 2, 3],
        }
    )

    catalog = SQLCatalog({"test_data": df})

    sql_result = daft.sql(
        """
        SELECT
            x,
            y,
            ROW_NUMBER() OVER(ORDER BY x, y) AS row_num,
            RANK() OVER(ORDER BY x, y) AS rank_val,
            DENSE_RANK() OVER(ORDER BY x, y) AS dense_rank_val
        FROM test_data
        ORDER BY x, y
    """,
        catalog=catalog,
    ).collect()

    window_spec = Window().order_by(["x", "y"])

    daft_result = (
        df.with_column("row_num", row_number().over(window_spec))
        .with_column("rank_val", rank().over(window_spec))
        .with_column("dense_rank_val", dense_rank().over(window_spec))
        .sort(["x", "y"])
        .collect()
    )

    assert_df_equals(sql_result.to_pandas(), daft_result.to_pandas(), sort_key=["x", "y"])
