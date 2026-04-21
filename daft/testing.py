from __future__ import annotations

"""
Testing utilities for Daft DataFrames and Schemas.

These helpers make it easier to write unit tests that verify DataFrame
contents and structure without boilerplate.
"""

from typing import TYPE_CHECKING, Optional, Sequence

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.schema import Schema


def assert_schema_equal(
    actual: "Schema",
    expected: "Schema",
    check_field_order: bool = True,
) -> None:
    """Assert that two Daft Schemas are equal.

    Args:
        actual: The schema to check.
        expected: The expected schema.
        check_field_order: If True (default), field order must match exactly.
            If False, only field names and types must match (order-insensitive).

    Raises:
        AssertionError: If the schemas differ, with a descriptive message.

    Example:
        >>> import daft
        >>> df = daft.from_pydict({"a": [1, 2], "b": ["x", "y"]})
        >>> assert_schema_equal(df.schema(), daft.Schema._from_field_name_and_types([("a", daft.DataType.int64()), ("b", daft.DataType.string())]))
    """
    actual_fields = {field.name: field.dtype for field in actual}
    expected_fields = {field.name: field.dtype for field in expected}

    actual_names = [field.name for field in actual]
    expected_names = [field.name for field in expected]

    if check_field_order:
        if actual_names != expected_names:
            raise AssertionError(
                f"Schema field order mismatch.\n"
                f"  Actual fields:   {actual_names}\n"
                f"  Expected fields: {expected_names}"
            )
    else:
        missing = set(expected_fields) - set(actual_fields)
        extra = set(actual_fields) - set(expected_fields)
        if missing or extra:
            raise AssertionError(
                f"Schema field name mismatch.\n"
                f"  Missing fields: {sorted(missing)}\n"
                f"  Extra fields:   {sorted(extra)}"
            )

    type_mismatches = []
    for name in (actual_names if check_field_order else sorted(actual_fields)):
        if name in expected_fields and actual_fields[name] != expected_fields[name]:
            type_mismatches.append(
                f"  Field '{name}': actual={actual_fields[name]}, expected={expected_fields[name]}"
            )

    if type_mismatches:
        raise AssertionError(
            "Schema field type mismatch:\n" + "\n".join(type_mismatches)
        )


def assert_frame_equal(
    actual: "DataFrame",
    expected: "DataFrame",
    check_column_order: bool = True,
    check_row_order: bool = False,
    check_dtype: bool = True,
    sort_by: Optional[Sequence[str]] = None,
) -> None:
    """Assert that two Daft DataFrames are equal.

    Collects both DataFrames and compares their contents.  By default rows
    can appear in any order (``check_row_order=False``) which is appropriate
    for most distributed-query tests where output order is non-deterministic.

    Args:
        actual: The DataFrame to check.
        expected: The expected DataFrame.
        check_column_order: If True (default), column order must match.
        check_row_order: If True, rows must be in the same order.
            Defaults to False; rows are sorted before comparison.
        check_dtype: If True (default), column data types must match exactly.
        sort_by: Column name(s) to sort by when ``check_row_order=False``.
            Defaults to sorting by all columns.

    Raises:
        AssertionError: If the DataFrames differ, with a descriptive message.

    Example:
        >>> import daft
        >>> df = daft.from_pydict({"a": [3, 1, 2], "b": ["z", "x", "y"]})
        >>> expected = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        >>> assert_frame_equal(df, expected)  # passes â row order ignored by default
    """
    # --- schema check ---
    actual_schema = actual.schema()
    expected_schema = expected.schema()

    actual_col_names = [field.name for field in actual_schema]
    expected_col_names = [field.name for field in expected_schema]

    if check_column_order:
        if actual_col_names != expected_col_names:
            raise AssertionError(
                f"Column order mismatch.\n"
                f"  Actual columns:   {actual_col_names}\n"
                f"  Expected columns: {expected_col_names}"
            )
    else:
        if sorted(actual_col_names) != sorted(expected_col_names):
            missing = sorted(set(expected_col_names) - set(actual_col_names))
            extra = sorted(set(actual_col_names) - set(expected_col_names))
            raise AssertionError(
                f"Column name mismatch.\n"
                f"  Missing columns: {missing}\n"
                f"  Extra columns:   {extra}"
            )
        # Reorder actual to match expected column order
        actual = actual.select(*expected_col_names)

    if check_dtype:
        type_mismatches = []
        for field_a, field_e in zip(actual.schema(), expected.schema()):
            if field_a.dtype != field_e.dtype:
                type_mismatches.append(
                    f"  Column '{field_a.name}': actual={field_a.dtype}, expected={field_e.dtype}"
                )
        if type_mismatches:
            raise AssertionError(
                "Column dtype mismatch:\n" + "\n".join(type_mismatches)
            )

    # --- collect and compare data ---
    actual_collected = actual.collect()
    expected_collected = expected.collect()

    actual_len = len(actual_collected)
    expected_len = len(expected_collected)

    if actual_len != expected_len:
        raise AssertionError(
            f"Row count mismatch: actual={actual_len}, expected={expected_len}"
        )

    actual_pydict = actual_collected.to_pydict()
    expected_pydict = expected_collected.to_pydict()

    col_names = expected_col_names if not check_column_order else actual_col_names

    if not check_row_order:
        if sort_by:
            invalid = [c for c in sort_by if c not in col_names]
            if invalid:
                raise AssertionError(
                    f"sort_by references column(s) not present in the DataFrames: {invalid}.\n"
                    f"  Available columns: {col_names}"
                )
            sort_cols = list(sort_by)
        else:
            sort_cols = col_names
        # Build sort keys as tuples
        def _row_key(pydict: dict, idx: int) -> tuple:
            return tuple(
                (pydict[c][idx] is None, pydict[c][idx])
                for c in sort_cols
                if c in pydict
            )

        actual_order = sorted(range(actual_len), key=lambda i: _row_key(actual_pydict, i))
        expected_order = sorted(range(expected_len), key=lambda i: _row_key(expected_pydict, i))
    else:
        actual_order = list(range(actual_len))
        expected_order = list(range(expected_len))

    mismatches = []
    for rank, (ai, ei) in enumerate(zip(actual_order, expected_order)):
        for col in col_names:
            av = actual_pydict[col][ai]
            ev = expected_pydict[col][ei]
            if av != ev:
                mismatches.append(
                    f"  Row {rank}, column '{col}': actual={av!r}, expected={ev!r}"
                )
        if len(mismatches) >= 10:
            mismatches.append("  ... (truncated after 10 mismatches)")
            break

    if mismatches:
        raise AssertionError(
            f"DataFrame content mismatch ({len(mismatches)} difference(s)):\n"
            + "\n".join(mismatches)
        )
