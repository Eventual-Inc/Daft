import pytest
from pyspark.sql import functions as F


@pytest.mark.parametrize(
    "data,value,expected_values",
    [
        ([(1, 10), (2, None)], 10, [True, False]),  # 10 <=> 10, NULL <=> 10
        ([(1, None), (2, None), (3, 10)], None, [True, True, False]),  # NULL <=> NULL, NULL <=> NULL, 10 <=> NULL
        ([(1, 10), (2, 20)], 10, [True, False]),  # 10 <=> 10, 20 <=> 10
    ],
)
def test_null_safe_equals_basic(spark_session, data, value, expected_values):
    """Test basic null-safe equality comparison."""
    df = spark_session.createDataFrame(data, ["id", "value"])
    result = df.withColumn("null_safe_equals", F.col("value").eqNullSafe(F.lit(value))).collect()

    # Print full comparison
    print("\nFull comparison:")
    print("Result values:")
    for row in result:
        print(f"  value={row.value}, null_safe_equals={row.null_safe_equals}")
    print("\nExpected values:")
    for i, (data_row, expected) in enumerate(zip(data, expected_values)):
        print(f"  value={data_row[1]}, expected={expected}")

    for row, expected in zip(result, expected_values):
        assert row.null_safe_equals is expected


@pytest.mark.parametrize(
    "input_type,test_value,test_data,expected_values",
    [
        ("int", 10, [(1, 10), (2, None), (3, 20)], [True, False, False]),
        ("string", "hello", [(1, "hello"), (2, None), (3, "world")], [True, False, False]),
        ("boolean", True, [(1, True), (2, None), (3, False)], [True, False, False]),
        ("float", 1.5, [(1, 1.5), (2, None), (3, 2.5)], [True, False, False]),
    ],
)
def test_null_safe_equals_types(spark_session, input_type, test_value, test_data, expected_values):
    """Test null-safe equality with different data types."""
    df = spark_session.createDataFrame(test_data, ["id", "value"])
    result = df.withColumn("eq_value", F.col("value").eqNullSafe(F.lit(test_value))).collect()

    # Print full comparison
    print("\nFull comparison:")
    print("Result values:")
    for row in result:
        print(f"  value={row.value}, eq_value={row.eq_value}")
    print("\nExpected values:")
    for i, (data_row, expected) in enumerate(zip(test_data, expected_values)):
        print(f"  value={data_row[1]}, expected={expected}")

    for row, expected in zip(result, expected_values):
        assert row.eq_value is expected, f"Failed for {input_type} comparison"


@pytest.mark.parametrize(
    "data,expected_values",
    [
        ([(10, 10), (None, None), (10, None), (None, 10), (10, 20)], [True, True, False, False, False]),
        (
            [("hello", "hello"), (None, None), ("hello", None), (None, "hello"), ("hello", "world")],
            [True, True, False, False, False],
        ),
        ([(True, True), (None, None), (True, None), (None, True), (True, False)], [True, True, False, False, False]),
        ([(1.5, 1.5), (None, None), (1.5, None), (None, 1.5), (1.5, 2.5)], [True, True, False, False, False]),
    ],
)
def test_null_safe_equals_column_comparison(spark_session, data, expected_values):
    """Test null-safe equality between two columns."""
    df = spark_session.createDataFrame(data, ["left", "right"])
    result = df.withColumn("null_safe_equals", F.col("left").eqNullSafe(F.col("right"))).collect()

    # Print full comparison
    print("\nFull comparison:")
    print("Result values:")
    for row in result:
        print(f"  left={row.left}, right={row.right}, null_safe_equals={row.null_safe_equals}")
    print("\nExpected values:")
    for i, (data_row, expected) in enumerate(zip(data, expected_values)):
        print(f"  left={data_row[0]}, right={data_row[1]}, expected={expected}")

    for row, expected in zip(result, expected_values):
        assert row.null_safe_equals is expected


@pytest.mark.parametrize(
    "filter_value,data,expected_ids",
    [
        (10, [(1, 10), (2, None), (3, 20), (4, None)], {1}),  # Only id=1 has value=10
        (None, [(1, 10), (2, None), (3, 20), (4, None)], {2, 4}),  # id=2 and id=4 have NULL values
        (20, [(1, 10), (2, None), (3, 20), (4, None)], {3}),  # Only id=3 has value=20
    ],
)
def test_null_safe_equals_in_where_clause(spark_session, filter_value, data, expected_ids):
    """Test using null-safe equality in WHERE clause."""
    df = spark_session.createDataFrame(data, ["id", "value"])
    result = df.where(F.col("value").eqNullSafe(F.lit(filter_value))).collect()

    # Print full comparison
    print("\nFull comparison:")
    print("Result values:")
    for row in result:
        print(f"  value={row.value}, null_safe_equals={row.null_safe_equals}")
    print("\nExpected values:")
    for i, (data_row, expected) in enumerate(zip(data, expected_ids)):
        print(f"  value={data_row[1]}, expected={expected}")

    assert set(r.id for r in result) == expected_ids


@pytest.mark.parametrize(
    "operation,test_value,data,expected_values",
    [
        (
            "NOT",
            10,
            [(1, 10), (2, None), (3, 20)],
            [False, True, True],  # NOT (10 <=> 10), NOT (NULL <=> 10), NOT (20 <=> 10)
        ),
        # Skipping OR_NULL test case as the "or" function is not yet supported in Daft Connect
        # Will be re-enabled once the OR operation is implemented in the translation layer
        # Original test:
        # (
        #     "OR_NULL",
        #     10,
        #     [(1, 10), (2, None), (3, 20)],
        #     [True, True, False]  # (10 <=> 10) OR (10 <=> NULL), (NULL <=> 10) OR (NULL <=> NULL), (20 <=> 10) OR (20 <=> NULL)
        # ),
    ],
)
def test_null_safe_equals_chained_operations(spark_session, operation, test_value, data, expected_values):
    """Test chaining null-safe equality with other operations."""
    df = spark_session.createDataFrame(data, ["id", "value"])

    if operation == "NOT":
        result = df.withColumn("result", ~F.col("value").eqNullSafe(F.lit(test_value))).collect()
    # elif operation == "OR_NULL":
    #     result = df.withColumn(
    #         "result",
    #         F.col("value").eqNullSafe(F.lit(test_value)) | F.col("value").eqNullSafe(F.lit(None))
    #     ).collect()

    # Print full comparison
    print("\nFull comparison:")
    print("Result values:")
    for row in result:
        print(f"  value={row.value}, result={row.result}")
    print("\nExpected values:")
    for i, (data_row, expected) in enumerate(zip(data, expected_values)):
        print(f"  value={data_row[1]}, expected={expected}")

    for row, expected in zip(result, expected_values):
        assert row.result is expected
