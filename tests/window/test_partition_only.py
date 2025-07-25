from __future__ import annotations

import random

import pandas as pd
import pytest

from daft import Window, col
from daft.context import get_context
from tests.conftest import assert_df_equals, get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray"
    and get_context().daft_execution_config.use_experimental_distributed_engine is False,
    reason="requires Native Runner or Flotilla to be in use",
)


def test_single_partition_sum(make_df):
    """Stage: PARTITION BY-Only Window Aggregations.

    Test sum over a single partition column.
    """
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        # sum("value").over(window).alias("sum"), # TODO: Support .over() directly on expressions
        col("value").sum().over(window).alias("sum"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "sum": [22, 22, 22, 29, 29, 29, 21, 21],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()))


def test_single_partition_min(make_df):
    """Test min over a single partition column."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").min().over(window).alias("min"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "min": [5, 5, 5, 7, 7, 7, 6, 6],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()))


def test_single_partition_max(make_df):
    """Test max over a single partition column."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").max().over(window).alias("max"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "max": [9, 9, 9, 12, 12, 12, 15, 15],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()))


def test_single_partition_mean(make_df):
    """Test mean over a single partition column."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").mean().over(window).alias("mean"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "mean": [
            7.333333333333333,
            7.333333333333333,
            7.333333333333333,
            9.666666666666666,
            9.666666666666666,
            9.666666666666666,
            10.5,
            10.5,
        ],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()))


def test_single_partition_count(make_df):
    """Test count over a single partition column."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").count().over(window).alias("count"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "count": [3, 3, 3, 3, 3, 3, 2, 2],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()), check_dtype=False)


def test_multiple_partition_columns(make_df):
    """Test sum over multiple partition columns."""
    df = make_df(
        {
            "category": ["B", "A", "C", "A", "B", "C", "A", "B"],
            "group": [1, 1, 2, 1, 2, 2, 2, 1],
            "value": [10, 5, 15, 8, 12, 6, 9, 7],
        }
    )

    window = Window().partition_by(["category", "group"])
    result = df.select(
        col("category"),
        col("group"),
        col("value"),
        col("value").sum().over(window).alias("sum"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "group": [1, 1, 2, 1, 2, 1, 2, 2],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "sum": [13, 13, 9, 17, 12, 17, 21, 21],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()))


def test_similar_partition_keys(make_df):
    """Test partition keys with similar string/number combinations that might cause hash collisions."""
    df = make_df(
        {
            "key1": ["A1", "B2", "C3", "D4", "E5", "A2", "B1", "C4", "D3", "E2"],
            "key2": ["X1", "Y2", "Z3", "X2", "Y3", "Z1", "X3", "Y1", "Z2", "X4"],
            "value": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )

    window = Window().partition_by(["key1", "key2"])
    result = df.select(
        col("key1"),
        col("key2"),
        col("value"),
        col("value").sum().over(window).alias("sum"),
    ).collect()

    expected_dict = result.to_pydict()

    for i in range(len(expected_dict["key1"])):
        assert (
            expected_dict["sum"][i] == expected_dict["value"][i]
        ), f"Hash collision detected for keys {expected_dict['key1'][i]}/{expected_dict['key2'][i]}"


def test_null_partition_values(make_df):
    """Test window functions with null values in partition columns."""
    df = make_df(
        {
            "category": ["A", "A", None, "B", "B", None, "C", None],
            "value": [5, 8, 9, 10, 12, 7, 15, 6],
        }
    )

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").sum().over(window).alias("sum"),
    ).collect()

    result_dict = result.to_pydict()

    for category in set(result_dict["category"]):
        if category is None:
            null_indices = [i for i, cat in enumerate(result_dict["category"]) if cat is None]
            null_values = [result_dict["value"][i] for i in null_indices]
            expected_sum = sum(null_values)
            actual_sums = [result_dict["sum"][i] for i in null_indices]
            assert all(
                sum == expected_sum for sum in actual_sums
            ), f"Incorrect sum for null category: {actual_sums} != {expected_sum}"
        else:
            cat_indices = [i for i, cat in enumerate(result_dict["category"]) if cat == category]
            cat_values = [result_dict["value"][i] for i in cat_indices]
            expected_sum = sum(cat_values)
            actual_sums = [result_dict["sum"][i] for i in cat_indices]
            assert all(
                sum == expected_sum for sum in actual_sums
            ), f"Incorrect sum for category {category}: {actual_sums} != {expected_sum}"


def test_multiple_window_functions(make_df):
    """Test multiple window functions in the same query."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").sum().over(window).alias("sum"),
        col("value").min().over(window).alias("min"),
        col("value").max().over(window).alias("max"),
        col("value").mean().over(window).alias("mean"),
        col("value").count().over(window).alias("count"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 10, 12, 7, 15, 6],
        "sum": [22, 22, 22, 29, 29, 29, 21, 21],
        "min": [5, 5, 5, 7, 7, 7, 6, 6],
        "max": [9, 9, 9, 12, 12, 12, 15, 15],
        "mean": [
            7.333333333333333,
            7.333333333333333,
            7.333333333333333,
            9.666666666666666,
            9.666666666666666,
            9.666666666666666,
            10.5,
            10.5,
        ],
        "count": [3, 3, 3, 3, 3, 3, 2, 2],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()), check_dtype=False)


def test_many_partitions(make_df):
    """Test window functions with a large number of partitions."""
    num_partitions = 100
    categories = [f"category_{i}" for i in range(num_partitions)]
    values = list(range(num_partitions))

    df = make_df({"category": categories, "value": values})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").sum().over(window).alias("sum"),
    ).collect()

    result_dict = result.to_pydict()

    for i in range(len(result_dict["category"])):
        assert (
            result_dict["sum"][i] == result_dict["value"][i]
        ), f"Expected sum equal to value for single-row partition {result_dict['category'][i]}"


def test_window_mean_minus_value(make_df):
    """Test arithmetic with window mean and value."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        (col("value").mean().over(window) - col("value")).alias("mean_minus_value"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 7, 10, 12, 6, 15],
        "mean_minus_value": [
            2.333333333333333,
            -0.666666666666667,
            -1.666666666666667,
            2.666666666666666,
            -0.3333333333333339,
            -2.333333333333334,
            4.5,
            -4.5,
        ],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()))


def test_window_value_over_sum(make_df):
    """Test division with value and window sum."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        (col("value") / col("value").sum().over(window)).alias("value_over_sum"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 7, 10, 12, 6, 15],
        "value_over_sum": [
            0.22727272727272727,
            0.36363636363636365,
            0.4090909090909091,
            0.2413793103448276,
            0.3448275862068966,
            0.41379310344827586,
            0.2857142857142857,
            0.7142857142857143,
        ],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()))


def test_factor_window_expression(make_df):
    """Test factor window expression."""
    df = make_df({"category": ["B", "A", "C", "A", "B", "C", "A", "B"], "value": [10, 5, 15, 8, 12, 6, 9, 7]})

    window = Window().partition_by("category")
    result = df.select(
        col("category"),
        col("value"),
        col("value").sum().over(window).alias("sum"),
        (col("value") / col("value").sum().over(window)).alias("value_over_sum"),
        col("value").max().over(window).alias("max"),
        (col("value") - col("value").max().over(window)).alias("value_minus_max"),
        col("value").min().over(window).alias("min"),
        (col("value") - col("value").min().over(window)).alias("value_minus_min"),
        (col("value").max().over(window) - col("value").min().over(window)).alias("max_minus_min"),
    ).collect()

    expected = {
        "category": ["A", "A", "A", "B", "B", "B", "C", "C"],
        "value": [5, 8, 9, 7, 10, 12, 6, 15],
        "sum": [22, 22, 22, 29, 29, 29, 21, 21],
        "value_over_sum": [
            0.22727272727272727,
            0.36363636363636365,
            0.4090909090909091,
            0.2413793103448276,
            0.3448275862068966,
            0.41379310344827586,
            0.2857142857142857,
            0.7142857142857143,
        ],
        "max": [9, 9, 9, 12, 12, 12, 15, 15],
        "value_minus_max": [-4, -1, 0, -5, -2, 0, -9, 0],
        "min": [5, 5, 5, 7, 7, 7, 6, 6],
        "value_minus_min": [0, 3, 4, 0, 3, 5, 0, 9],
        "max_minus_min": [4, 4, 4, 5, 5, 5, 9, 9],
    }

    assert_df_equals(result.to_pandas(), pd.DataFrame(expected), sort_key=list(expected.keys()))


def test_partition_by_with_expressions(make_df):
    """Test window functions with expressions in partition_by clause."""
    df = make_df({"num": [1, 2, 3, 4, 5, 6, 7, 8], "value": [10, 20, 30, 40, 50, 60, 70, 80]})

    df = df.with_column("parity", col("num") % 2)

    parity_expr = col("parity")
    window = Window().partition_by(parity_expr)

    result = df.select(
        col("num"),
        col("value"),
        col("parity"),
        col("value").sum().over(window).alias("sum"),
    ).collect()

    result_dict = result.to_pydict()

    odd_values = [result_dict["value"][i] for i, p in enumerate(result_dict["parity"]) if p == 1]
    even_values = [result_dict["value"][i] for i, p in enumerate(result_dict["parity"]) if p == 0]

    odd_sum = sum(odd_values)
    even_sum = sum(even_values)

    for i, parity in enumerate(result_dict["parity"]):
        expected_sum = odd_sum if parity == 1 else even_sum
        assert result_dict["sum"][i] == expected_sum, f"Row {i} has incorrect sum for parity {parity}"


def test_multiple_window_partitions(make_df):
    """Test multiple window functions with different partition keys using random numbers.

    Creates a dataset with 18 rows (2 rows each for A1, A2, A3, B1, B2, B3, C1, C2, C3)
    and verifies sums across different partition keys (A/B/C and 1/2/3).
    """
    random.seed(42)

    data = []
    for letter in ["A", "B", "C"]:
        for num in ["1", "2", "3"]:
            for _ in range(3):
                data.append({"letter": letter, "num": num, "value": random.randint(1, 100)})

    df = make_df(data)

    letter_window = Window().partition_by("letter")
    num_window = Window().partition_by("num")
    combined_window = Window().partition_by(["letter", "num"])

    result = df.select(
        col("letter"),
        col("num"),
        col("value"),
        col("value").sum().over(letter_window).alias("letter_sum"),
        col("value").sum().over(num_window).alias("num_sum"),
        col("value").sum().over(combined_window).alias("combined_sum"),
    ).collect()

    result_dict = result.to_pydict()

    for letter in ["A", "B", "C"]:
        letter_indices = [i for i, ltr in enumerate(result_dict["letter"]) if ltr == letter]
        letter_values = [result_dict["value"][i] for i in letter_indices]
        expected_letter_sum = sum(letter_values)
        actual_letter_sums = [result_dict["letter_sum"][i] for i in letter_indices]
        assert all(
            sum == expected_letter_sum for sum in actual_letter_sums
        ), f"Incorrect sum for letter {letter}: {actual_letter_sums} != {expected_letter_sum}"

    for num in ["1", "2", "3"]:
        num_indices = [i for i, n in enumerate(result_dict["num"]) if n == num]
        num_values = [result_dict["value"][i] for i in num_indices]
        expected_num_sum = sum(num_values)
        actual_num_sums = [result_dict["num_sum"][i] for i in num_indices]
        assert all(
            sum == expected_num_sum for sum in actual_num_sums
        ), f"Incorrect sum for number {num}: {actual_num_sums} != {expected_num_sum}"

    for letter in ["A", "B", "C"]:
        for num in ["1", "2", "3"]:
            combined_indices = [
                i
                for i, (ltr, n) in enumerate(zip(result_dict["letter"], result_dict["num"]))
                if ltr == letter and n == num
            ]
            combined_values = [result_dict["value"][i] for i in combined_indices]
            expected_combined_sum = sum(combined_values)
            actual_combined_sums = [result_dict["combined_sum"][i] for i in combined_indices]
            assert all(
                sum == expected_combined_sum for sum in actual_combined_sums
            ), f"Incorrect sum for combination {letter}{num}: {actual_combined_sums} != {expected_combined_sum}"


def test_multi_window_agg_functions(make_df):
    """Test multiple window aggregation functions with different partition keys.

    Tests window functions with two different partition specifications:
    1. Partitioning by both category and group
    2. Partitioning by just category

    Using sum(), mean(), min(), and max() aggregations.
    """
    data = [
        {"category": "A", "group": 1, "value": 10},
        {"category": "A", "group": 1, "value": 20},
        {"category": "A", "group": 2, "value": 30},
        {"category": "A", "group": 2, "value": 40},
        {"category": "B", "group": 1, "value": 50},
        {"category": "B", "group": 1, "value": 60},
        {"category": "B", "group": 2, "value": 70},
        {"category": "B", "group": 2, "value": 80},
    ]

    df = make_df(data)

    multi_partition_window = Window().partition_by(["category", "group"])
    single_partition_window = Window().partition_by("category")

    result = df.select(
        col("category"),
        col("group"),
        col("value"),
        col("value").sum().over(multi_partition_window).alias("sum_multi"),
        col("value").mean().over(multi_partition_window).alias("avg_multi"),
        col("value").min().over(single_partition_window).alias("min_single"),
        col("value").max().over(single_partition_window).alias("max_single"),
    ).collect()

    result_dict = result.to_pydict()

    for category in ["A", "B"]:
        for group in [1, 2]:
            indices = [
                i
                for i, (cat, grp) in enumerate(zip(result_dict["category"], result_dict["group"]))
                if cat == category and grp == group
            ]

            values = [result_dict["value"][i] for i in indices]

            expected_sum = sum(values)
            expected_avg = sum(values) / len(values)

            for idx in indices:
                assert (
                    result_dict["sum_multi"][idx] == expected_sum
                ), f"Incorrect sum for {category}/{group}: {result_dict['sum_multi'][idx]} != {expected_sum}"
                assert (
                    abs(result_dict["avg_multi"][idx] - expected_avg) < 1e-10
                ), f"Incorrect avg for {category}/{group}: {result_dict['avg_multi'][idx]} != {expected_avg}"

    for category in ["A", "B"]:
        indices = [i for i, cat in enumerate(result_dict["category"]) if cat == category]

        values = [result_dict["value"][i] for i in indices]

        expected_min = min(values)
        expected_max = max(values)

        for idx in indices:
            assert (
                result_dict["min_single"][idx] == expected_min
            ), f"Incorrect min for {category}: {result_dict['min_single'][idx]} != {expected_min}"
            assert (
                result_dict["max_single"][idx] == expected_max
            ), f"Incorrect max for {category}: {result_dict['max_single'][idx]} != {expected_max}"


def test_without_source_columns_and_with_duplicate_window_functions(make_df):
    data = {
        "product": [],
        "category": [],
        "store": [],
        "revenue": [],
    }

    for store in [f"Store_{i}" for i in range(1, 3)]:
        idx = 1
        for category in ["Electronics", "Clothing", "Books"]:
            for _ in range(1, 3):
                data["product"].append(f"Product_{idx}")
                data["category"].append(category)
                data["store"].append(store)
                data["revenue"].append(random.randint(10, 100))
                idx += 1

    df = make_df(data)

    window_store = Window().partition_by("store")
    window_product = Window().partition_by("product")
    window_store_category = Window().partition_by(["store", "category"])

    result = df.select(
        col("revenue").sum().over(window_store).alias("store revenue sum"),
        (col("revenue") / col("revenue").sum().over(window_store)).alias("store revenue share"),
        col("revenue").sum().over(window_product).alias("product revenue sum"),
        (col("revenue") / col("revenue").sum().over(window_product)).alias("product revenue share"),
        col("revenue").sum().over(window_store_category).alias("store category revenue sum"),
        (col("revenue") - col("revenue").max().over(window_store_category)).alias("store category max revenue diff"),
    ).collect()

    pdf = pd.DataFrame(data)
    store_revenue_sum = pdf.groupby("store")["revenue"].transform("sum")
    store_revenue_share = pdf["revenue"] / store_revenue_sum
    product_revenue_sum = pdf.groupby("product")["revenue"].transform("sum")
    product_revenue_share = pdf["revenue"] / product_revenue_sum
    store_category_revenue_sum = pdf.groupby(["store", "category"])["revenue"].transform("sum")
    store_category_max_revenue = pdf.groupby(["store", "category"])["revenue"].transform("max")
    store_category_max_revenue_diff = pdf["revenue"] - store_category_max_revenue
    expected = pd.DataFrame(
        {
            "store revenue sum": store_revenue_sum,
            "store revenue share": store_revenue_share,
            "product revenue sum": product_revenue_sum,
            "product revenue share": product_revenue_share,
            "store category revenue sum": store_category_revenue_sum,
            "store category max revenue diff": store_category_max_revenue_diff,
        }
    )

    assert_df_equals(
        result.to_pandas(),
        expected,
        sort_key=[
            "store revenue sum",
            "product revenue sum",
            "store category revenue sum",
            "store category max revenue diff",
        ],
    )
