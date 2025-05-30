from __future__ import annotations

import pyarrow as pa
import pytest

from daft.daft import series_lit
from daft.expressions import col, lit
from daft.expressions.expressions import Expression
from daft.recordbatch import MicroPartition


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
    result_values = result.get_column_by_name("value").to_pylist()

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
        ("fixed_size_binary", b"aaa", [(1, b"aaa"), (2, None), (3, b"bbb")], [True, False, False]),
        ("null", None, [(1, 10), (2, None), (3, 20)], [False, True, False]),
    ],
)
def test_null_safe_equals_types(type_name, test_value, test_data, expected_values):
    """Test null-safe equality with different data types."""
    # Create a table with the test data
    if type_name == "fixed_size_binary":
        # Convert to PyArrow array for fixed size binary
        value_array = pa.array([x[1] for x in test_data], type=pa.binary(3))
        table = MicroPartition.from_pydict(
            {
                "id": [x[0] for x in test_data],
                "value": value_array.to_pylist(),
            }
        )
    else:
        table = MicroPartition.from_pydict(
            {
                "id": [x[0] for x in test_data],
                "value": [x[1] for x in test_data],
            }
        )

    # Apply the null-safe equals operation
    result = table.eval_expression_list([col("value").eq_null_safe(lit(test_value))])
    result_values = result.get_column_by_name("value").to_pylist()

    # Verify results
    assert result_values == expected_values, f"Failed for {type_name} comparison"


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
        (
            [(b"aaa", b"aaa"), (None, None), (b"aaa", None), (None, b"aaa"), (b"aaa", b"bbb")],
            [True, True, False, False, False],
        ),
    ],
)
def test_null_safe_equals_column_comparison(data, expected_values):
    """Test null-safe equality between two columns."""
    # Check if this is a fixed-size binary test case
    is_fixed_binary = isinstance(data[0][0], bytes) and len(data[0][0]) == 3

    # Create a table with the test data
    if is_fixed_binary:
        # Convert to PyArrow arrays for fixed size binary
        left_array = pa.array([x[0] for x in data], type=pa.binary(3))
        right_array = pa.array([x[1] for x in data], type=pa.binary(3))
        table = MicroPartition.from_pydict(
            {
                "left": left_array.to_pylist(),
                "right": right_array.to_pylist(),
            }
        )
    else:
        table = MicroPartition.from_pydict(
            {
                "left": [x[0] for x in data],
                "right": [x[1] for x in data],
            }
        )

    # Apply the null-safe equals operation
    result = table.eval_expression_list([col("left").eq_null_safe(col("right"))])
    result_values = result.get_column_by_name("left").to_pylist()

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
    result_ids = set(result.get_column_by_name("id").to_pylist())

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

    result_values = result.get_column_by_name("value").to_pylist()

    # Verify results
    assert result_values == expected_values


def test_null_safe_equals_fixed_size_binary():
    """Test null-safe equality specifically for fixed-size binary arrays."""
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
    result_values = result.get_column_by_name("left").to_pylist()
    assert result_values == [True, False, False, False, False, True]  # True for equal values and both-null cases

    # Test column to scalar comparisons
    test_cases = [
        (b"11111", [True, False, False, False, False, False]),  # Matches first value
        (b"22222", [False, True, False, False, False, False]),  # Matches second value
        (None, [False, False, False, True, False, True]),  # True for all null values
    ]

    for test_value, expected in test_cases:
        result = table.eval_expression_list([col("left").eq_null_safe(lit(test_value))])
        result_values = result.get_column_by_name("left").to_pylist()
        assert result_values == expected, f"Failed for test value: {test_value}"

        # Test the reverse comparison as well
        result = table.eval_expression_list([col("right").eq_null_safe(lit(test_value))])
        result_values = result.get_column_by_name("right").to_pylist()
        expected_reverse = [
            True if test_value == b"11111" else False,  # First value is "11111"
            False,  # Second value is "33333"
            True if test_value == b"11111" else False,  # Third value is "11111"
            True if test_value == b"12345" else False,  # Fourth value is "12345"
            True if test_value is None else False,  # Fifth value is None
            True if test_value is None else False,  # Sixth value is None
        ]
        assert result_values == expected_reverse, f"Failed for reverse test value: {test_value}"


@pytest.mark.parametrize(
    "type_name,left_data,right_data,expected_values",
    [
        ("int", [1, 2, 3], [1, 2, 4], [True, True, False]),
        ("float", [1.0, 2.0, 3.0], [1.0, 2.0, 4.0], [True, True, False]),
        ("boolean", [True, False, True], [True, False, False], [True, True, False]),
        ("string", ["a", "b", "c"], ["a", "b", "d"], [True, True, False]),
        ("binary", [b"a", b"b", b"c"], [b"a", b"b", b"d"], [True, True, False]),
        ("fixed_size_binary", [b"aaa", b"bbb", b"ccc"], [b"aaa", b"bbb", b"ddd"], [True, True, False]),
    ],
)
def test_no_nulls_all_types(type_name, left_data, right_data, expected_values):
    """Test null-safe equality with no nulls in either array for all data types."""
    if type_name == "fixed_size_binary":
        # Convert to PyArrow arrays for fixed size binary
        left_array = pa.array(left_data, type=pa.binary(3))
        right_array = pa.array(right_data, type=pa.binary(3))
        table = MicroPartition.from_pydict(
            {
                "left": left_array.to_pylist(),
                "right": right_array.to_pylist(),
            }
        )
    else:
        table = MicroPartition.from_pydict(
            {
                "left": left_data,
                "right": right_data,
            }
        )

    result = table.eval_expression_list([col("left").eq_null_safe(col("right"))])
    result_values = result.get_column_by_name("left").to_pylist()

    assert result_values == expected_values, f"Failed for {type_name} comparison"


@pytest.mark.parametrize(
    "type_name,left_data,right_data,expected_values",
    [
        ("int", [1, 2, 3], [1, None, 3], [True, False, True]),
        ("float", [1.0, 2.0, 3.0], [1.0, None, 3.0], [True, False, True]),
        ("boolean", [True, False, True], [True, None, True], [True, False, True]),
        ("string", ["a", "b", "c"], ["a", None, "c"], [True, False, True]),
        ("binary", [b"a", b"b", b"c"], [b"a", None, b"c"], [True, False, True]),
        ("fixed_size_binary", [b"aaa", b"bbb", b"ccc"], [b"aaa", None, b"ccc"], [True, False, True]),
    ],
)
def test_right_nulls_all_types(type_name, left_data, right_data, expected_values):
    """Test null-safe equality where left array has no nulls and right array has some nulls."""
    if type_name == "fixed_size_binary":
        # Convert to PyArrow arrays for fixed size binary
        left_array = pa.array(left_data, type=pa.binary(3))
        right_array = pa.array(right_data, type=pa.binary(3))
        table = MicroPartition.from_pydict(
            {
                "left": left_array.to_pylist(),
                "right": right_array.to_pylist(),
            }
        )
    else:
        table = MicroPartition.from_pydict(
            {
                "left": left_data,
                "right": right_data,
            }
        )

    result = table.eval_expression_list([col("left").eq_null_safe(col("right"))])
    result_values = result.get_column_by_name("left").to_pylist()

    assert result_values == expected_values, f"Failed for {type_name} comparison"


@pytest.mark.parametrize(
    "type_name,left_data,right_data,expected_values",
    [
        ("int", [1, None, 3], [1, 2, 3], [True, False, True]),
        ("float", [1.0, None, 3.0], [1.0, 2.0, 3.0], [True, False, True]),
        ("boolean", [True, None, True], [True, False, True], [True, False, True]),
        ("string", ["a", None, "c"], ["a", "b", "c"], [True, False, True]),
        ("binary", [b"a", None, b"c"], [b"a", b"b", b"c"], [True, False, True]),
        ("fixed_size_binary", [b"aaa", None, b"ccc"], [b"aaa", b"bbb", b"ccc"], [True, False, True]),
    ],
)
def test_left_nulls_all_types(type_name, left_data, right_data, expected_values):
    """Test null-safe equality where left array has some nulls and right array has no nulls."""
    if type_name == "fixed_size_binary":
        # Convert to PyArrow arrays for fixed size binary
        left_array = pa.array(left_data, type=pa.binary(3))
        right_array = pa.array(right_data, type=pa.binary(3))
        table = MicroPartition.from_pydict(
            {
                "left": left_array.to_pylist(),
                "right": right_array.to_pylist(),
            }
        )
    else:
        table = MicroPartition.from_pydict(
            {
                "left": left_data,
                "right": right_data,
            }
        )

    result = table.eval_expression_list([col("left").eq_null_safe(col("right"))])
    result_values = result.get_column_by_name("left").to_pylist()

    assert result_values == expected_values, f"Failed for {type_name} comparison"


@pytest.mark.parametrize(
    "type_name,left_data,right_data",
    [
        ("int", [1, 2, 3], [1, 2]),
        ("float", [1.0, 2.0, 3.0], [1.0, 2.0]),
        ("boolean", [True, False, True], [True, False]),
        ("string", ["a", "b", "c"], ["a", "b"]),
        ("binary", [b"a", b"b", b"c"], [b"a", b"b"]),
        ("fixed_size_binary", [b"aaa", b"bbb", b"ccc"], [b"aaa", b"bbb"]),
    ],
)
def test_length_mismatch_all_types(type_name, left_data, right_data):
    """Test that length mismatches raise appropriate error for all data types."""
    # Create two separate tables
    left_table = MicroPartition.from_pydict({"value": left_data})
    right_table = MicroPartition.from_pydict({"value": right_data})

    with pytest.raises(ValueError) as exc_info:
        result = left_table.eval_expression_list(
            [
                col("value").eq_null_safe(
                    Expression._from_pyexpr(series_lit(right_table.get_column_by_name("value")._series))
                )
            ]
        )
        # Force evaluation by accessing the result
        result.get_column_by_name("value").to_pylist()

    # Verify error message format
    error_msg = str(exc_info.value)
    assert "trying to compare different length arrays" in error_msg


@pytest.mark.parametrize(
    "type_name,left_data,right_data,expected_values",
    [
        ("int", [None, None, 1], [None, None, 2], [True, True, False]),
        ("float", [None, None, 1.0], [None, None, 2.0], [True, True, False]),
        ("boolean", [None, None, True], [None, None, False], [True, True, False]),
        ("string", [None, None, "a"], [None, None, "b"], [True, True, False]),
        ("binary", [None, None, b"a"], [None, None, b"b"], [True, True, False]),
        ("fixed_size_binary", [None, None, b"aaa"], [None, None, b"bbb"], [True, True, False]),
    ],
)
def test_null_equals_null_all_types(type_name, left_data, right_data, expected_values):
    """Test that NULL <=> NULL returns True for all data types."""
    if type_name == "fixed_size_binary":
        # Convert to PyArrow arrays for fixed size binary
        left_array = pa.array(left_data, type=pa.binary(3))
        right_array = pa.array(right_data, type=pa.binary(3))
        table = MicroPartition.from_pydict(
            {
                "left": left_array.to_pylist(),
                "right": right_array.to_pylist(),
            }
        )
    else:
        table = MicroPartition.from_pydict(
            {
                "left": left_data,
                "right": right_data,
            }
        )

    result = table.eval_expression_list([col("left").eq_null_safe(col("right"))])
    result_values = result.get_column_by_name("left").to_pylist()

    assert result_values == expected_values, f"Failed for {type_name} comparison"
