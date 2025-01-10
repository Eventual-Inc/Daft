from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.table import MicroPartition


@pytest.mark.parametrize(
    "data,value,expected_values",
    [
        ([(1, 10), (2, None)], 10, [True, False]),  # 10 <=> 10, NULL <=> 10
        ([(1, None), (2, None), (3, 10)], None, [True, True, False]),  # NULL <=> NULL, NULL <=> NULL, 10 <=> NULL
        ([(1, 10), (2, 20)], 10, [True, False]),  # 10 <=> 10, 20 <=> 10
    ],
)
def test_null_safe_equals_basic(data, value, expected_values):
    """Test basic null-safe equality comparison."""
    # Create a table with the test data
    table = MicroPartition.from_pydict(
        {
            "id": [x[0] for x in data],
            "value": [x[1] for x in data],
        }
    )

    # Apply the null-safe equals operation
    result = table.eval_expression_list([col("value").eq_null_safe(lit(value))])
    result_values = result.get_column("value").to_pylist()

    # Print full comparison for debugging
    print("\nFull comparison:")
    print("Result values:")
    for val, res in zip([x[1] for x in data], result_values):
        print(f"  value={val}, eq_value={res}")
    print("\nExpected values:")
    for val, exp in zip([x[1] for x in data], expected_values):
        print(f"  value={val}, expected={exp}")

    # Verify results
    assert result_values == expected_values


@pytest.mark.parametrize(
    "type_name,test_value,test_data,expected_values",
    [
        ("int", 10, [(1, 10), (2, None), (3, 20)], [True, False, False]),
        ("string", "hello", [(1, "hello"), (2, None), (3, "world")], [True, False, False]),
        ("boolean", True, [(1, True), (2, None), (3, False)], [True, False, False]),
        ("float", 1.5, [(1, 1.5), (2, None), (3, 2.5)], [True, False, False]),
        ("binary", b"hello", [(1, b"hello"), (2, None), (3, b"world")], [True, False, False]),
    ],
)
def test_null_safe_equals_types(type_name, test_value, test_data, expected_values):
    """Test null-safe equality with different data types."""
    # Create a table with the test data
    table = MicroPartition.from_pydict(
        {
            "id": [x[0] for x in test_data],
            "value": [x[1] for x in test_data],
        }
    )

    # Apply the null-safe equals operation
    result = table.eval_expression_list([col("value").eq_null_safe(lit(test_value))])
    result_values = result.get_column("value").to_pylist()

    # Print full comparison for debugging
    print("\nFull comparison:")
    print("Result values:")
    for val, res in zip([x[1] for x in test_data], result_values):
        print(f"  value={val}, eq_value={res}")
    print("\nExpected values:")
    for val, exp in zip([x[1] for x in test_data], expected_values):
        print(f"  value={val}, expected={exp}")

    # Verify results
    assert result_values == expected_values, f"Failed for {type_name} comparison"


@pytest.mark.parametrize(
    "type_name,test_value,test_data,expected_values",
    [
        ("int", 10, [(1, 10), (2, None), (3, 20)], [True, False, False]),
        ("string", "hello", [(1, "hello"), (2, None), (3, "world")], [True, False, False]),
        ("boolean", True, [(1, True), (2, None), (3, False)], [True, False, False]),
        ("float", 1.5, [(1, 1.5), (2, None), (3, 2.5)], [True, False, False]),
        ("binary", b"hello", [(1, b"hello"), (2, None), (3, b"world")], [True, False, False]),
        ("null", None, [(1, 10), (2, None), (3, 20)], [False, True, False]),
    ],
)
def test_null_safe_equals_scalar_both_directions(type_name, test_value, test_data, expected_values):
    """Test null-safe equality with scalars on both LHS and RHS of the comparison."""
    # Create a table with the test data
    table = MicroPartition.from_pydict(
        {
            "id": [x[0] for x in test_data],
            "value": [x[1] for x in test_data],
        }
    )

    # Test scalar on RHS: col <=> lit(scalar)
    result = table.eval_expression_list([col("value").eq_null_safe(lit(test_value))])
    result_values = result.get_column("value").to_pylist()
    assert result_values == expected_values, f"Failed for {type_name} with scalar on RHS"

    # LHS literals are not yet supported (in regular equality nor null safe)
    # Test scalar on LHS: lit(scalar) <=> col
    # result = table.eval_expression_list([lit(test_value) == col("value")])
    # result = table.eval_expression_list([lit(test_value).eq_null_safe(col("value"))])
    # result_values = result.get_column("literal").to_pylist()  # When literal is on LHS, the column is named "literal"
    # assert result_values == expected_values, f"Failed for {type_name} with scalar on LHS"


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
        (
            [(b"hello", b"hello"), (None, None), (b"hello", None), (None, b"hello"), (b"hello", b"world")],
            [True, True, False, False, False],
        ),
    ],
)
def test_null_safe_equals_column_comparison(data, expected_values):
    """Test null-safe equality between two columns."""
    # Create a table with the test data
    table = MicroPartition.from_pydict(
        {
            "left": [x[0] for x in data],
            "right": [x[1] for x in data],
        }
    )

    # Apply the null-safe equals operation
    result = table.eval_expression_list([col("left").eq_null_safe(col("right"))])
    result_values = result.get_column("left").to_pylist()

    # Print full comparison for debugging
    print("\nFull comparison:")
    print("Result values:")
    for left, right, res in zip([x[0] for x in data], [x[1] for x in data], result_values):
        print(f"  left={left}, right={right}, eq_value={res}")
    print("\nExpected values:")
    for left, right, exp in zip([x[0] for x in data], [x[1] for x in data], expected_values):
        print(f"  left={left}, right={right}, expected={exp}")

    # Verify results
    assert result_values == expected_values


@pytest.mark.parametrize(
    "filter_value,data,expected_ids",
    [
        (10, [(1, 10), (2, None), (3, 20), (4, None)], {1}),  # Only id=1 has value=10
        (None, [(1, 10), (2, None), (3, 20), (4, None)], {2, 4}),  # id=2 and id=4 have NULL values
        (20, [(1, 10), (2, None), (3, 20), (4, None)], {3}),  # Only id=3 has value=20
    ],
)
def test_null_safe_equals_in_filter(filter_value, data, expected_ids):
    """Test using null-safe equality in filter."""
    # Create a table with the test data
    table = MicroPartition.from_pydict(
        {
            "id": [x[0] for x in data],
            "value": [x[1] for x in data],
        }
    )

    # Apply the filter with null-safe equals
    result = table.filter([col("value").eq_null_safe(lit(filter_value))])
    result_ids = set(result.get_column("id").to_pylist())

    # Print full comparison for debugging
    print("\nFull comparison:")
    print("Result IDs:", result_ids)
    print("Expected IDs:", expected_ids)

    # Verify results
    assert result_ids == expected_ids


@pytest.mark.parametrize(
    "operation,test_value,data,expected_values",
    [
        (
            "NOT",
            10,
            [(1, 10), (2, None), (3, 20)],
            [False, True, True],  # NOT (10 <=> 10), NOT (NULL <=> 10), NOT (20 <=> 10)
        ),
    ],
)
def test_null_safe_equals_chained_operations(operation, test_value, data, expected_values):
    """Test chaining null-safe equality with other operations."""
    # Create a table with the test data
    table = MicroPartition.from_pydict(
        {
            "id": [x[0] for x in data],
            "value": [x[1] for x in data],
        }
    )

    # Apply the operation
    if operation == "NOT":
        result = table.eval_expression_list([~col("value").eq_null_safe(lit(test_value))])

    result_values = result.get_column("value").to_pylist()

    # Print full comparison for debugging
    print("\nFull comparison:")
    print("Result values:")
    for val, res in zip([x[1] for x in data], result_values):
        print(f"  value={val}, result={res}")
    print("\nExpected values:")
    for val, exp in zip([x[1] for x in data], expected_values):
        print(f"  value={val}, expected={exp}")

    # Verify results
    assert result_values == expected_values


def test_null_safe_equals_fixed_size_binary():
    """Test null-safe equality specifically for fixed-size binary arrays."""
    import pyarrow as pa

    # Create arrays with fixed size binary data
    l_arrow = pa.array([b"11111", b"22222", b"33333", None, b"12345", None], type=pa.binary(5))
    r_arrow = pa.array([b"11111", b"33333", b"11111", b"12345", None, None], type=pa.binary(5))

    # Create table with these arrays
    table = MicroPartition.from_pydict(
        {
            "left": l_arrow.to_pylist(),
            "right": r_arrow.to_pylist(),
        }
    )

    # Test column to column comparison
    result = table.eval_expression_list([col("left").eq_null_safe(col("right"))])
    result_values = result.get_column("left").to_pylist()
    assert result_values == [True, False, False, False, False, True]  # True for equal values and both-null cases

    # Test column to scalar comparisons
    test_cases = [
        (b"11111", [True, False, False, False, False, False]),  # Matches first value
        (b"22222", [False, True, False, False, False, False]),  # Matches second value
        (None, [False, False, False, True, False, True]),  # True for all null values
    ]

    for test_value, expected in test_cases:
        result = table.eval_expression_list([col("left").eq_null_safe(lit(test_value))])
        result_values = result.get_column("left").to_pylist()
        assert result_values == expected, f"Failed for test value: {test_value}"

        # Test the reverse comparison as well
        result = table.eval_expression_list([col("right").eq_null_safe(lit(test_value))])
        result_values = result.get_column("right").to_pylist()
        expected_reverse = [
            True if test_value == b"11111" else False,  # First value is "11111"
            False,  # Second value is "33333"
            True if test_value == b"11111" else False,  # Third value is "11111"
            True if test_value == b"12345" else False,  # Fourth value is "12345"
            True if test_value is None else False,  # Fifth value is None
            True if test_value is None else False,  # Sixth value is None
        ]
        assert result_values == expected_reverse, f"Failed for reverse test value: {test_value}"
