"""Testing utilities for Daft DataFrames and Schemas.

This module provides assertion functions for comparing Daft DataFrames, Schemas,
and Series in tests. These utilities provide detailed error messages when
comparisons fail, making it easier to debug test failures.

Example:
    >>> import daft
    >>> from daft.testing import assert_frame_equal, assert_schema_equal
    >>>
    >>> df1 = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    >>> df2 = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    >>> assert_frame_equal(df1, df2)  # Passes silently
    >>>
    >>> # Compare with a dict
    >>> assert_frame_equal(df1, {"a": [1, 2, 3], "b": ["x", "y", "z"]})
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.schema import Schema
    from daft.series import Series

__all__ = [
    "assert_frame_equal",
    "assert_schema_equal",
    "assert_series_equal",
]


def assert_schema_equal(
    actual: Schema,
    expected: Schema,
    *,
    check_column_order: bool = True,
) -> None:
    """Assert that two Schemas are equal.

    Args:
        actual: The actual Schema to check.
        expected: The expected Schema to compare against.
        check_column_order: Whether column order matters. If False, schemas with
            the same columns in different orders are considered equal. Default True.

    Raises:
        TypeError: If actual or expected is not a Schema.
        AssertionError: If the Schemas are not equal, with detailed diff information.

    Example:
        >>> import daft
        >>> from daft.testing import assert_schema_equal
        >>>
        >>> schema1 = daft.from_pydict({"a": [1], "b": ["x"]}).schema()
        >>> schema2 = daft.from_pydict({"a": [2], "b": ["y"]}).schema()
        >>> assert_schema_equal(schema1, schema2)  # Passes - same column names and types
    """
    from daft.schema import Schema

    if not isinstance(actual, Schema):
        raise TypeError(f"actual must be a Schema, got {type(actual).__name__}")
    if not isinstance(expected, Schema):
        raise TypeError(f"expected must be a Schema, got {type(expected).__name__}")

    actual_fields = {f.name: f for f in actual}
    expected_fields = {f.name: f for f in expected}

    # Check column names
    actual_names = set(actual_fields.keys())
    expected_names = set(expected_fields.keys())

    missing = expected_names - actual_names
    extra = actual_names - expected_names

    if missing or extra:
        msg = "Schema mismatch:\n"
        if missing:
            msg += f"  Missing columns: {sorted(missing)}\n"
        if extra:
            msg += f"  Extra columns: {sorted(extra)}\n"
        raise AssertionError(msg)

    # Check column order
    if check_column_order:
        actual_order = [f.name for f in actual]
        expected_order = [f.name for f in expected]
        if actual_order != expected_order:
            raise AssertionError(f"Column order mismatch:\n  Actual:   {actual_order}\n  Expected: {expected_order}")

    # Check data types
    type_mismatches = []
    for name in expected_fields:
        actual_field = actual_fields[name]
        expected_field = expected_fields[name]
        if actual_field.dtype != expected_field.dtype:
            type_mismatches.append(f"  {name}: {actual_field.dtype} != {expected_field.dtype}")

    if type_mismatches:
        raise AssertionError("Data type mismatch:\n" + "\n".join(type_mismatches))


def assert_series_equal(
    actual: Series,
    expected: Series,
    *,
    check_dtype: bool = True,
    check_names: bool = True,
    rtol: float = 1e-5,
    atol: float = 1e-8,
) -> None:
    """Assert that two Series are equal.

    Args:
        actual: The actual Series to check.
        expected: The expected Series to compare against.
        check_dtype: Whether to check data types strictly. Default True.
        check_names: Whether to check Series names. Default True.
        rtol: Relative tolerance for floating point comparison. Default 1e-5.
        atol: Absolute tolerance for floating point comparison. Default 1e-8.

    Raises:
        TypeError: If actual or expected is not a Series.
        AssertionError: If the Series are not equal, with detailed diff information.

    Example:
        >>> import daft
        >>> from daft.testing import assert_series_equal
        >>>
        >>> s1 = daft.from_pydict({"a": [1, 2, 3]}).to_pandas()["a"]
        >>> s2 = daft.from_pydict({"a": [1, 2, 3]}).to_pandas()["a"]
        >>> # Note: This function compares Daft Series, not pandas Series
    """
    import pandas as pd

    from daft.series import Series

    if not isinstance(actual, Series):
        raise TypeError(f"actual must be a Series, got {type(actual).__name__}")
    if not isinstance(expected, Series):
        raise TypeError(f"expected must be a Series, got {type(expected).__name__}")

    # Check names
    if check_names and actual.name() != expected.name():
        raise AssertionError(f"Series name mismatch: actual={actual.name()!r}, expected={expected.name()!r}")

    # Check length
    if len(actual) != len(expected):
        raise AssertionError(f"Series length mismatch: actual has {len(actual)} elements, expected has {len(expected)}")

    # Check dtype
    if check_dtype and actual.datatype() != expected.datatype():
        raise AssertionError(f"Series dtype mismatch: actual={actual.datatype()}, expected={expected.datatype()}")

    # Convert to pandas for detailed comparison
    actual_pd = actual.to_arrow().to_pandas()
    expected_pd = expected.to_arrow().to_pandas()

    try:
        pd.testing.assert_series_equal(
            actual_pd,
            expected_pd,
            check_dtype=check_dtype,
            check_names=False,  # We already checked names above
            rtol=rtol,
            atol=atol,
        )
    except AssertionError as e:
        raise AssertionError(f"Series values mismatch:\n{e}") from None


def assert_frame_equal(
    actual: DataFrame,
    expected: DataFrame | Mapping[str, Any],
    *,
    check_schema: bool = True,
    check_column_order: bool = True,
    check_row_order: bool = False,
    check_dtype: bool = True,
    rtol: float = 1e-5,
    atol: float = 1e-8,
) -> None:
    """Assert that two DataFrames are equal.

    Args:
        actual: The actual DataFrame to check.
        expected: The expected DataFrame or a dict to compare against.
            If a dict is provided, it will be converted to a DataFrame using
            `daft.from_pydict()`.
        check_schema: Whether to check schema equality first. Default True.
        check_column_order: Whether column order matters. Default True.
        check_row_order: Whether row order matters. If False, rows are sorted
            before comparison. Default False.
        check_dtype: Whether to check data types strictly. Default True.
        rtol: Relative tolerance for floating point comparison. Default 1e-5.
        atol: Absolute tolerance for floating point comparison. Default 1e-8.

    Raises:
        TypeError: If actual is not a DataFrame or expected is not a DataFrame/dict.
        AssertionError: If the DataFrames are not equal, with detailed diff information.

    Example:
        >>> import daft
        >>> from daft.testing import assert_frame_equal
        >>>
        >>> df1 = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        >>> df2 = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        >>> assert_frame_equal(df1, df2)  # Passes silently
        >>>
        >>> # Compare with a dict
        >>> assert_frame_equal(df1, {"a": [1, 2, 3], "b": ["x", "y", "z"]})
        >>>
        >>> # Ignore row order
        >>> df3 = daft.from_pydict({"a": [3, 1, 2]})
        >>> df4 = daft.from_pydict({"a": [1, 2, 3]})
        >>> assert_frame_equal(df3, df4, check_row_order=False)  # Passes
    """
    import pandas as pd

    import daft
    from daft.dataframe import DataFrame

    if not isinstance(actual, DataFrame):
        raise TypeError(f"actual must be a DataFrame, got {type(actual).__name__}")

    # Convert expected to DataFrame if it's a dict
    if isinstance(expected, Mapping):
        expected = daft.from_pydict(dict(expected))
    elif not isinstance(expected, DataFrame):
        raise TypeError(f"expected must be a DataFrame or dict, got {type(expected).__name__}")

    # Collect both DataFrames
    actual_collected = actual.collect()
    expected_collected = expected.collect()

    # Check schema first
    if check_schema:
        try:
            assert_schema_equal(
                actual_collected.schema(),
                expected_collected.schema(),
                check_column_order=check_column_order,
            )
        except AssertionError as e:
            raise AssertionError(f"Schema mismatch:\n{e}") from None

    # Check row count
    actual_len = len(actual_collected)
    expected_len = len(expected_collected)
    if actual_len != expected_len:
        raise AssertionError(f"Row count mismatch: actual has {actual_len} rows, expected has {expected_len} rows")

    # If empty, we're done
    if actual_len == 0:
        return

    # Convert to pandas for detailed comparison
    actual_pd = actual_collected.to_pandas()
    expected_pd = expected_collected.to_pandas()

    # Reorder columns if not checking column order
    if not check_column_order:
        common_cols = sorted(actual_pd.columns)
        actual_pd = actual_pd[common_cols]
        expected_pd = expected_pd[common_cols]

    # Sort if row order doesn't matter
    if not check_row_order:
        sort_cols = list(actual_pd.columns)
        # Handle columns that might not be sortable (e.g., lists, dicts)
        try:
            actual_pd = actual_pd.sort_values(by=sort_cols, ignore_index=True)
            expected_pd = expected_pd.sort_values(by=sort_cols, ignore_index=True)
        except TypeError:
            # If sorting fails due to unhashable types, try sorting by string representation
            actual_pd = actual_pd.iloc[actual_pd.astype(str).apply(tuple, axis=1).argsort()].reset_index(drop=True)
            expected_pd = expected_pd.iloc[expected_pd.astype(str).apply(tuple, axis=1).argsort()].reset_index(
                drop=True
            )

    # Compare column by column
    for col in expected_pd.columns:
        try:
            pd.testing.assert_series_equal(
                actual_pd[col],
                expected_pd[col],
                check_dtype=check_dtype,
                check_names=False,
                rtol=rtol,
                atol=atol,
            )
        except AssertionError as e:
            raise AssertionError(f"Column '{col}' mismatch:\n{e}") from None
