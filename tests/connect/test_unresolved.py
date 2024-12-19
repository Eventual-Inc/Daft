import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


@pytest.fixture
def sample_df(spark_session):
    """Create a sample DataFrame with various test cases."""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("value_a", IntegerType(), True),
        StructField("value_b", IntegerType(), True),
        StructField("text_a", StringType(), True),
        StructField("text_b", StringType(), True),
    ])

    data = [
        (1, 10, 20, "apple", "banana"),
        (2, 20, 20, "apple", "apple"),
        (3, 30, 20, "cherry", "banana"),
        (4, None, 20, None, "banana"),
        (5, 50, None, "date", None),
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.mark.parametrize("operator,func_name,expected_results", [
    ("==", "equals", [
        (1, False),  # 10 == 20
        (2, True),   # 20 == 20
        (3, False),  # 30 == 20
        (4, None),   # NULL == 20
        (5, None),   # 50 == NULL
    ]),
    ("!=", "not_equals", [
        (1, True),   # 10 != 20
        (2, False),  # 20 != 20
        (3, True),   # 30 != 20
        (4, None),   # NULL != 20
        (5, None),   # 50 != NULL
    ]),
    (">", "greater_than", [
        (1, False),  # 10 > 20
        (2, False),  # 20 > 20
        (3, True),   # 30 > 20
        (4, None),   # NULL > 20
        (5, None),   # 50 > NULL
    ]),
    (">=", "greater_than_equals", [
        (1, False),  # 10 >= 20
        (2, True),   # 20 >= 20
        (3, True),   # 30 >= 20
        (4, None),   # NULL >= 20
        (5, None),   # 50 >= NULL
    ]),
    ("<", "less_than", [
        (1, True),   # 10 < 20
        (2, False),  # 20 < 20
        (3, False),  # 30 < 20
        (4, None),   # NULL < 20
        (5, None),   # 50 < NULL
    ]),
    ("<=", "less_than_equals", [
        (1, True),   # 10 <= 20
        (2, True),   # 20 <= 20
        (3, False),  # 30 <= 20
        (4, None),   # NULL <= 20
        (5, None),   # 50 <= NULL
    ]),
])
def test_numeric_comparisons(sample_df, operator, func_name, expected_results):
    """
    Test various numeric comparison operations with NULL handling.
    Tests both direct column comparisons and literal value comparisons.
    """
    # Test column to column comparison
    result_df = sample_df.withColumn(
        f"result_{func_name}_col",
        eval(f"F.col('value_a') {operator} F.col('value_b')")
    ).orderBy("id")

    # Test column to literal comparison
    result_df = result_df.withColumn(
        f"result_{func_name}_lit",
        eval(f"F.col('value_a') {operator} F.lit(20)")
    )

    # Collect results and compare row by row
    actual_results_col = [(row['id'], row[f'result_{func_name}_col'])
                          for row in result_df.collect()]
    actual_results_lit = [(row['id'], row[f'result_{func_name}_lit'])
                          for row in result_df.collect()]

    # Compare column results
    for expected, actual in zip(expected_results, actual_results_col):
        assert expected == actual, (
            f"Column comparison {operator} failed for id {expected[0]}. "
            f"Expected {expected[1]}, got {actual[1]}"
        )

    # Compare literal results (same expected results since we're comparing with 20)
    for expected, actual in zip(expected_results, actual_results_lit):
        assert expected == actual, (
            f"Literal comparison {operator} failed for id {expected[0]}. "
            f"Expected {expected[1]}, got {actual[1]}"
        )


@pytest.mark.parametrize("operator,func_name,expected_results", [
    ("==", "equals", [
        (1, False),  # apple == banana
        (2, True),   # apple == apple
        (3, False),  # cherry == banana
        (4, None),   # NULL == banana
        (5, None),   # date == NULL
    ]),
    ("!=", "not_equals", [
        (1, True),   # apple != banana
        (2, False),  # apple != apple
        (3, True),   # cherry != banana
        (4, None),   # NULL != banana
        (5, None),   # date != NULL
    ]),
])
def test_string_comparisons(sample_df, operator, func_name, expected_results):
    """
    Test string comparison operations with NULL handling.
    Tests both direct column comparisons and literal value comparisons.
    """
    # Test column to column comparison
    result_df = sample_df.withColumn(
        f"result_{func_name}_col",
        eval(f"F.col('text_a') {operator} F.col('text_b')")
    ).orderBy("id")

    # Test column to literal comparison
    result_df = result_df.withColumn(
        f"result_{func_name}_lit",
        eval(f"F.col('text_a') {operator} F.lit('banana')")
    )

    # Collect results and compare row by row
    actual_results_col = [(row['id'], row[f'result_{func_name}_col'])
                          for row in result_df.collect()]
    actual_results_lit = [(row['id'], row[f'result_{func_name}_lit'])
                          for row in result_df.collect()]

    # Compare column results
    for expected, actual in zip(expected_results, actual_results_col):
        assert expected == actual, (
            f"Column comparison {operator} failed for id {expected[0]}. "
            f"Expected {expected[1]}, got {actual[1]}"
        )

    # Compare literal results
    for expected, actual in zip(expected_results, actual_results_lit):
        assert expected == actual, (
            f"Literal comparison {operator} failed for id {expected[0]}. "
            f"Expected {expected[1]}, got {actual[1]}"
        )


@pytest.mark.skip(reason="We believe null-safe equals are not yet implemented")
def test_null_safe_equals(sample_df):
    """
    Test null-safe equality comparison using the <=> operator.
    This operator treats NULL = NULL as TRUE.
    """
    result_df = sample_df.withColumn(
        "result_null_safe_equals",
        F.col("value_a").eqNullSafe(F.col("value_b"))
    ).orderBy("id")

    actual_results = [(row['id'], row['result_null_safe_equals'])
                      for row in result_df.collect()]

    expected_results = [
        (1, False),  # 10 <=> 20
        (2, True),   # 20 <=> 20
        (3, False),  # 30 <=> 20
        (4, False),  # NULL <=> 20
        (5, False),  # 50 <=> NULL
    ]

    for expected, actual in zip(expected_results, actual_results):
        assert expected == actual, (
            f"Null-safe equals failed for id {expected[0]}. "
            f"Expected {expected[1]}, got {actual[1]}"
        )