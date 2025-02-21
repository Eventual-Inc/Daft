from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.functions import monotonically_increasing_id
from tests.conftest import get_tests_daft_runner_name


def test_monotonic_id_with_complex_projections(make_df) -> None:
    """Test monotonically_increasing_id with complex projections to verify optimization rule."""
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}

    # Create DataFrame with multiple column operations
    df1 = (
        make_df(data)
        .with_columns(
            {"id": monotonically_increasing_id(), "a_plus_b": col("a") + col("b"), "b_squared": col("b") * col("b")}
        )
        .collect()
    )

    # Verify schema
    assert set(df1.column_names) == {"id", "a", "b", "a_plus_b", "b_squared"}
    assert df1.schema()["id"].dtype == DataType.uint64()

    # Create same DataFrame using old method
    df2 = (
        make_df(data)
        ._add_monotonically_increasing_id()
        .with_columns({"a_plus_b": col("a") + col("b"), "b_squared": col("b") * col("b")})
        .collect()
    )

    # Verify both paths produce identical results
    assert df1.to_pydict() == df2.to_pydict()


def test_monotonic_id_with_filter_operations(make_df) -> None:
    """Test monotonically_increasing_id with filter operations to verify optimization rule."""
    data = {"a": list(range(10))}

    # Create DataFrame with filter before and after
    df1 = (
        make_df(data)
        .filter(col("a") > 3)  # Filter before
        .with_column("id", monotonically_increasing_id())
        .filter(col("id") < 5)  # Filter after
        .collect()
    )

    # Create same DataFrame using old method
    df2 = (
        make_df(data)
        .filter(col("a") > 3)  # Filter before
        ._add_monotonically_increasing_id()
        .filter(col("id") < 5)  # Filter after
        .collect()
    )

    # Verify both paths produce identical results
    assert df1.to_pydict() == df2.to_pydict()


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "native",
    reason="Native runner does not support repartitioning",
)
def test_monotonic_id_with_mixed_operations(make_df) -> None:
    """Test monotonically_increasing_id with a mix of operations to verify optimization rule."""
    data = {"key": ["a", "b", "a", "b", "c"], "value": [1, 2, 3, 4, 5]}

    # Create DataFrame with mixed operations
    df1 = (
        make_df(data)
        .into_partitions(2)
        .with_column("id", monotonically_increasing_id())
        .groupby("key")
        .agg(col("value").sum().alias("sum"), col("id").min().alias("first_id"))
        .sort("key")
        .collect()
    )

    # Create same DataFrame using old method
    df2 = (
        make_df(data)
        .into_partitions(2)
        ._add_monotonically_increasing_id()
        .groupby("key")
        .agg(col("value").sum().alias("sum"), col("id").min().alias("first_id"))
        .sort("key")
        .collect()
    )

    # Verify both paths produce identical results
    assert df1.to_pydict() == df2.to_pydict()


def test_monotonic_id_chained_operations(make_df) -> None:
    """Test multiple monotonically_increasing_id operations in a chain."""
    data = {"a": [1, 2, 3]}

    # Create DataFrame with multiple ID columns
    df1 = (
        make_df(data)
        .with_column("id1", monotonically_increasing_id())
        .with_column("id2", monotonically_increasing_id())
        .collect()
    )

    # Create same DataFrame using old method
    df2 = make_df(data)._add_monotonically_increasing_id("id1")._add_monotonically_increasing_id("id2").collect()

    # Verify both paths produce identical results
    assert df1.to_pydict() == df2.to_pydict()


def test_monotonic_id_with_join(make_df) -> None:
    """Test monotonically_increasing_id with join operations."""
    left_data = {"key": ["a", "b", "c"], "value": [1, 2, 3]}
    right_data = {"key": ["b", "c", "d"], "other": [4, 5, 6]}

    # Create DataFrames with ID before join
    df1 = (
        make_df(left_data)
        .with_column("id", monotonically_increasing_id())
        .join(make_df(right_data).with_column("id", monotonically_increasing_id()), on="key", how="outer")
        .sort("key")
        .collect()
    )

    # Create same DataFrame using old method
    df2 = (
        make_df(left_data)
        ._add_monotonically_increasing_id()
        .join(make_df(right_data)._add_monotonically_increasing_id(), on="key", how="outer")
        .sort("key")
        .collect()
    )

    # Verify both paths produce identical results
    assert df1.to_pydict() == df2.to_pydict()
