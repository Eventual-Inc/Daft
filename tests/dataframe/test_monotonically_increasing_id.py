from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.functions import monotonically_increasing_id
from daft.io._generator import read_generator
from daft.recordbatch.recordbatch import RecordBatch
from tests.conftest import get_tests_daft_runner_name
from tests.utils import sort_pydict


def test_monotonically_increasing_id_single_partition(make_df) -> None:
    data = {"a": [1, 2, 3, 4, 5]}
    df = make_df(data)._add_monotonically_increasing_id().collect()

    assert len(df) == 5
    assert set(df.column_names) == {"id", "a"}
    assert df.schema()["id"].dtype == DataType.uint64()
    assert df.to_pydict() == {"id": [0, 1, 2, 3, 4], "a": [1, 2, 3, 4, 5]}

    # Test new function matches old behavior
    df2 = make_df(data).with_column("id", monotonically_increasing_id()).collect()
    assert df.to_pydict() == df2.to_pydict()


def test_monotonically_increasing_id_empty_table(make_df) -> None:
    data = {"a": []}
    df = make_df(data)._add_monotonically_increasing_id().collect()

    assert len(df) == 0
    assert set(df.column_names) == {"id", "a"}
    assert df.schema()["id"].dtype == DataType.uint64()
    assert df.to_pydict() == {"id": [], "a": []}

    # Test new function matches old behavior
    df2 = make_df(data).with_column("id", monotonically_increasing_id()).collect()
    assert df.to_pydict() == df2.to_pydict()


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "native",
    reason="Native runner does not support repartitioning",
)
@pytest.mark.parametrize("repartition_nparts", [1, 2, 20, 50, 100])
def test_monotonically_increasing_id_multiple_partitions_with_into_partition(make_df, repartition_nparts) -> None:
    ITEMS = [i for i in range(100)]

    data = {"a": ITEMS}
    df = make_df(data).into_partitions(repartition_nparts)._add_monotonically_increasing_id().collect()

    assert len(df) == 100
    assert set(df.column_names) == {"id", "a"}
    assert df.schema()["id"].dtype == DataType.uint64()

    # Test new function matches old behavior
    df2 = make_df(data).into_partitions(repartition_nparts).with_column("id", monotonically_increasing_id()).collect()
    assert df.to_pydict() == df2.to_pydict()

    # we can predict the ids because into_partitions evenly distributes without shuffling the data,
    # and the chosen repartition_nparts is a multiple of the number of items, so each partition will have the same number of items
    items_per_partition = len(ITEMS) // repartition_nparts
    ids = []
    for index, _ in enumerate(ITEMS):
        partition_num = index // items_per_partition
        counter = index % items_per_partition
        ids.append(partition_num << 36 | counter)

    assert df.to_pydict() == {"id": ids, "a": ITEMS}


def test_monotonically_increasing_id_from_generator() -> None:
    ITEMS = list(range(10))
    table = RecordBatch.from_pydict({"a": ITEMS})

    num_tables = 3
    num_generators = 3

    def generator():
        for _ in range(num_tables):
            yield table

    def generators():
        for _ in range(num_generators):
            yield generator

    df = read_generator(generators(), schema=table.schema())._add_monotonically_increasing_id().collect()

    assert len(df) == 90
    assert set(df.column_names) == {"id", "a"}
    assert df.schema()["id"].dtype == DataType.uint64()

    # Test new function matches old behavior
    df2 = read_generator(generators(), schema=table.schema()).with_column("id", monotonically_increasing_id()).collect()
    assert df.to_pydict() == df2.to_pydict()

    if get_tests_daft_runner_name() == "native":
        # On the native runner, there are no partitions, so the ids are just the row numbers.
        assert df.to_pydict() == {"id": list(range(90)), "a": ITEMS * 9}
    else:
        # On the ray / py runner, the ids are generated based on the partition number and the row number within the partition.
        # The partition number is put in the upper 28 bits and the row number is put in the lower 36 bits.
        # There are num_generators partitions, and each partition has num_tables * len(ITEMS) rows.
        ids = [(p << 36) | c for p in range(num_generators) for c in range(num_tables * len(ITEMS))]
        assert df.to_pydict() == {"id": ids, "a": ITEMS * 9}


@pytest.mark.parametrize("repartition_nparts", [1, 2, 20, 50, 100])
def test_monotonically_increasing_id_multiple_partitions_with_repartition(make_df, repartition_nparts) -> None:
    ITEMS = [i for i in range(100)]

    data = {"a": ITEMS}
    df = make_df(data, repartition=repartition_nparts)._add_monotonically_increasing_id().collect()

    assert len(df) == 100
    assert set(df.column_names) == {"id", "a"}
    assert df.schema()["id"].dtype == DataType.uint64()

    py_dict = df.to_pydict()
    assert set(py_dict["a"]) == set(ITEMS)

    # cannot predict the ids because repartition shuffles the data, so we just check that they are unique
    assert len(set(py_dict["id"])) == 100

    # Test new function matches old behavior
    df2 = make_df(data, repartition=repartition_nparts).with_column("id", monotonically_increasing_id()).collect()

    assert len(df2) == 100
    assert set(df2.column_names) == {"id", "a"}
    assert df2.schema()["id"].dtype == DataType.uint64()

    py_dict2 = df2.to_pydict()
    assert set(py_dict2["a"]) == set(ITEMS)
    assert len(set(py_dict2["id"])) == 100


def test_monotonically_increasing_id_custom_col_name(make_df) -> None:
    data = {"a": [1, 2, 3, 4, 5]}
    df = make_df(data)._add_monotonically_increasing_id("custom_id").collect()

    assert len(df) == 5
    assert set(df.column_names) == {"custom_id", "a"}
    assert df.schema()["custom_id"].dtype == DataType.uint64()
    assert df.to_pydict() == {"custom_id": [0, 1, 2, 3, 4], "a": [1, 2, 3, 4, 5]}

    # Test new function matches old behavior with custom column name
    df2 = make_df(data).with_column("custom_id", monotonically_increasing_id()).collect()
    assert df.to_pydict() == df2.to_pydict()


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


def test_monotonic_id_with_complex_expressions(make_df) -> None:
    """Test monotonically_increasing_id with complex expressions to verify optimization rule handles nested expressions."""
    data = {"a": [1, 2, 3]}

    # Create DataFrame with monotonically_increasing_id in complex expressions
    df1 = (
        make_df(data)
        .with_columns(
            {
                "id_plus_one": monotonically_increasing_id() + 1,
                "id_times_a": monotonically_increasing_id() * col("a"),
                "id_conditional": (monotonically_increasing_id() > 1).if_else(col("a") * 2, col("a")),
            }
        )
        .collect()
    )

    # Verify schema
    assert set(df1.column_names) == {"a", "id_plus_one", "id_times_a", "id_conditional"}

    # Create same DataFrame using old method with manual calculations
    df2 = (
        make_df(data)
        ._add_monotonically_increasing_id()
        .with_columns(
            {
                "id_plus_one": col("id") + 1,
                "id_times_a": col("id") * col("a"),
                "id_conditional": (col("id") > 1).if_else(col("a") * 2, col("a")),
            }
        )
        .select("a", "id_plus_one", "id_times_a", "id_conditional")  # Pass column names as separate arguments
        .collect()
    )

    # Verify both paths produce identical results
    assert df1.to_pydict() == df2.to_pydict()


def test_monotonic_id_multiple_calls_same_expression(make_df) -> None:
    """Test using multiple monotonically_increasing_id calls in the same expression."""
    data = {"a": [1, 2, 3]}

    # Create DataFrame with multiple monotonically_increasing_id calls in same expression
    df1 = (
        make_df(data)
        .with_columns(
            {
                "id_sum": monotonically_increasing_id() + monotonically_increasing_id(),
                "id_product": monotonically_increasing_id() * monotonically_increasing_id(),
                "id_compare": monotonically_increasing_id() > monotonically_increasing_id(),
            }
        )
        .collect()
    )

    # Create same DataFrame using old method
    df2 = (
        make_df(data)
        ._add_monotonically_increasing_id("id1")
        ._add_monotonically_increasing_id("id2")
        .with_columns(
            {
                "id_sum": col("id1") + col("id2"),
                "id_product": col("id1") * col("id2"),
                "id_compare": col("id1") > col("id2"),
            }
        )
        .select("a", "id_sum", "id_product", "id_compare")
        .collect()
    )

    # Verify both paths produce identical results
    assert df1.to_pydict() == df2.to_pydict()


def test_monotonic_id_deeply_nested(make_df) -> None:
    """Test deeply nested expressions containing monotonically_increasing_id."""
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}

    # Create DataFrame with deeply nested expressions
    df1 = (
        make_df(data)
        .with_columns(
            {
                "nested1": (monotonically_increasing_id() + col("a")) * (monotonically_increasing_id() + col("b")),
                "nested2": ((monotonically_increasing_id() > 0) & (col("a") > 2)).if_else(
                    monotonically_increasing_id() * 2, monotonically_increasing_id() + 1
                ),
            }
        )
        .collect()
    )

    # Create same DataFrame using old method
    df2 = (
        make_df(data)
        ._add_monotonically_increasing_id("id1")
        ._add_monotonically_increasing_id("id2")
        ._add_monotonically_increasing_id("id3")
        .with_columns(
            {
                "nested1": (col("id1") + col("a")) * (col("id2") + col("b")),
                "nested2": ((col("id1") > 0) & (col("a") > 2)).if_else(col("id2") * 2, col("id3") + 1),
            }
        )
        .select("a", "b", "nested1", "nested2")
        .collect()
    )

    # Verify both paths produce identical results
    assert df1.to_pydict() == df2.to_pydict()


def test_monotonic_id_complex_conditionals(make_df) -> None:
    """Test complex conditional expressions with monotonically_increasing_id."""
    data = {"value": [1, 2, 3, 4, 5]}

    # Create DataFrame with complex conditional logic
    df1 = (
        make_df(data)
        .with_columns(
            {
                "case1": (monotonically_increasing_id() % 2 == 0).if_else(col("value") * 2, col("value")),
                "case2": (
                    (monotonically_increasing_id() < 2).if_else(
                        col("value"), (monotonically_increasing_id() < 4).if_else(col("value") * 2, col("value") * 3)
                    )
                ),
            }
        )
        .collect()
    )

    # Create same DataFrame using old method
    df2 = (
        make_df(data)
        ._add_monotonically_increasing_id()
        .with_columns(
            {
                "case1": (col("id") % 2 == 0).if_else(col("value") * 2, col("value")),
                "case2": (
                    (col("id") < 2).if_else(col("value"), (col("id") < 4).if_else(col("value") * 2, col("value") * 3))
                ),
            }
        )
        .select("value", "case1", "case2")
        .collect()
    )

    # Verify both paths produce identical results
    assert df1.to_pydict() == df2.to_pydict()


def test_monotonic_id_in_filter(make_df) -> None:
    """Test using monotonically_increasing_id in filter conditions."""
    data = {"a": [1, 2, 3, 4, 5]}

    # Use _add_monotonically_increasing_id
    df1 = make_df(data)._add_monotonically_increasing_id().filter(col("id") > 2).collect()

    # Filter using monotonically_increasing_id directly in filter should raise an error
    with pytest.raises(Exception, match="monotonically_increasing_id\\(\\) is only allowed in projections"):
        make_df(data).filter(monotonically_increasing_id() > 2).collect()

    # This approach works - add column first, then filter
    df2 = make_df(data).with_column("id", monotonically_increasing_id()).filter(col("id") > 2).collect()

    assert df1.to_pydict() == df2.to_pydict()


def test_monotonic_id_in_aggregation(make_df) -> None:
    """Test using monotonically_increasing_id in aggregation expressions."""
    data = {"key": ["a", "b", "a", "b", "c"], "value": [1, 2, 3, 4, 5]}

    # Use _add_monotonically_increasing_id
    df1 = (
        make_df(data)
        ._add_monotonically_increasing_id()
        .groupby("key")
        .agg((col("id") + col("value")).sum().alias("sum_id_plus_value"), col("id").max().alias("max_id"))
        .sort("key")
        .collect()
    )

    # Using monotonically_increasing_id directly in aggregation should raise an error
    with pytest.raises(Exception, match="monotonically_increasing_id\\(\\) is only allowed in projections"):
        (
            make_df(data)
            .groupby("key")
            .agg(
                (monotonically_increasing_id() + col("value")).sum().alias("sum_id_plus_value"),
                monotonically_increasing_id().max().alias("max_id"),
            )
            .sort("key")
            .collect()
        )

    # This approach works - add column first, then aggregate
    df2 = (
        make_df(data)
        .with_column("id", monotonically_increasing_id())
        .groupby("key")
        .agg((col("id") + col("value")).sum().alias("sum_id_plus_value"), col("id").max().alias("max_id"))
        .sort("key")
        .collect()
    )

    assert df1.to_pydict() == df2.to_pydict()


def test_monotonic_id_in_join_condition(make_df) -> None:
    """Test using monotonically_increasing_id in join conditions."""
    left_data = {"key": ["a", "b", "c"], "value": [1, 2, 3]}
    right_data = {"key": ["b", "c", "d"], "other": [4, 5, 6]}

    # Use _add_monotonically_increasing_id
    df1 = (
        make_df(left_data)
        ._add_monotonically_increasing_id()
        .join(make_df(right_data)._add_monotonically_increasing_id(), on="id", how="inner")
        .collect()
    )

    # Using monotonically_increasing_id directly in join condition should raise an error
    with pytest.raises(Exception, match="monotonically_increasing_id\\(\\) is only allowed in projections"):
        (
            make_df(left_data)
            .join(make_df(right_data), on=monotonically_increasing_id() == monotonically_increasing_id(), how="inner")
            .collect()
        )

    # This approach works - add column first, then join
    df2 = (
        make_df(left_data)
        .with_column("id", monotonically_increasing_id())
        .join(make_df(right_data).with_column("id", monotonically_increasing_id()), on="id", how="inner")
        .collect()
    )

    assert sort_pydict(df1.to_pydict(), "id") == sort_pydict(df2.to_pydict(), "id")


def test_monotonically_increasing_id_with_cast(make_df) -> None:
    """Test that casting monotonically_increasing_id directly works correctly.

    This tests addresses a bug where calling .cast() directly on monotonically_increasing_id()
    would result in a PanicException with 'called `Option::unwrap()` on a `None` value'.
    """
    data = {"foo": list(range(10))}

    df = make_df(data).with_column("id", monotonically_increasing_id().cast(DataType.string())).collect()

    assert len(df) == 10
    assert df.schema()["id"].dtype == DataType.string()

    df2 = (
        make_df(data)
        .with_column("id", monotonically_increasing_id())
        .with_column("id_str", col("id").cast(DataType.string()))
        .collect()
    )

    assert df.to_pydict()["id"] == df2.to_pydict()["id_str"]
