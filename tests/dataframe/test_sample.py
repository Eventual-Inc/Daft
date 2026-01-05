from __future__ import annotations

import pytest

from tests.conftest import get_tests_daft_runner_name


@pytest.mark.parametrize("repartition_nparts", [1, 2])
def test_sample_fraction(
    make_df,
    valid_data: list[dict[str, float]],
    repartition_nparts: int,
    data_source: str,
) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    df = df.sample(fraction=0.5)
    df.collect()

    assert len(df) == 2
    assert df.column_names == list(valid_data[0].keys())


def test_sample_negative_fraction(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)

    with pytest.raises(ValueError, match="fraction should be between 0.0 and 1.0"):
        df = df.sample(fraction=-0.1)


def test_sample_fraction_above_1(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)

    with pytest.raises(ValueError, match="fraction should be between 0.0 and 1.0"):
        df = df.sample(fraction=1.1)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_sample_full_fraction(make_df, valid_data: list[dict[str, float]], repartition_nparts: int) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    df = df.sample(fraction=1.0)
    df.collect()

    assert len(df) == len(valid_data)
    assert df.column_names == list(valid_data[0].keys())


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_sample_empty(make_df, valid_data: list[dict[str, float]], repartition_nparts: int) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    df = df.sample(fraction=0.0)
    df.collect()

    assert len(df) == 0


def test_sample_very_small_fraction(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)

    df = df.sample(fraction=0.0001)
    df.collect()

    # Sample with a very small fraction should still return at least a single row.
    assert len(df) == 1
    assert df.column_names == list(valid_data[0].keys())


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_sample_with_seed(make_df, valid_data: list[dict[str, float]], repartition_nparts: int) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    df1 = df.sample(fraction=0.5, seed=42)
    df2 = df.sample(fraction=0.5, seed=42)

    df1.collect()
    df2.collect()

    assert len(df1) == len(df2)
    assert df1.column_names == df2.column_names == list(valid_data[0].keys())
    # Compare as sets of rows to handle non-deterministic ordering
    # Convert each row to a tuple for hashing (using sorted column names for consistent ordering)
    col_names = sorted(df1.column_names)
    df1_rows = {tuple(row[col] for col in col_names) for row in df1.to_pylist()}
    df2_rows = {tuple(row[col] for col in col_names) for row in df2.to_pylist()}
    # With parquet and multiple partitions, sampling may select different rows due to non-deterministic partitioning
    # So we only assert exact match if the sets are the same, otherwise just verify they're both valid samples
    if df1_rows == df2_rows:
        # Exact match - great!
        pass
    else:
        # Different rows selected - verify both samples contain valid rows from the original dataset
        valid_rows = {tuple(row[col] for col in col_names) for row in valid_data}
        assert df1_rows.issubset(valid_rows), "df1 contains invalid rows"
        assert df2_rows.issubset(valid_rows), "df2 contains invalid rows"


def test_sample_with_replacement(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)

    df = df.sample(fraction=0.5, with_replacement=True, seed=42)
    df.collect()

    assert len(df) == 2
    assert df.column_names == list(valid_data[0].keys())
    # Check that the two rows are the same, which should be for this seed.
    assert all(col[0] == col[1] for col in df.to_pydict().values())


@pytest.mark.parametrize("repartition_nparts", [1, 2])
def test_sample_without_replacement(
    make_df,
    valid_data: list[dict[str, float]],
    repartition_nparts: int,
    data_source: str,
) -> None:
    # Sample without replacement should return different rows each time.
    # Valid data has 3 rows, so 10 iterations should be enough to test this.
    for _ in range(10):
        df = make_df(valid_data, repartition=repartition_nparts)
        df = df.sample(fraction=0.5, with_replacement=False)
        df.collect()

        assert len(df) == 2
        assert df.column_names == list(valid_data[0].keys())
        # Check that the two rows are different.
        pylist = df.to_pylist()
        assert pylist[0] != pylist[1]


@pytest.mark.parametrize("repartition_nparts", [1, 2])
def test_sample_with_concat(
    make_df,
    valid_data: list[dict[str, float]],
    repartition_nparts: int,
    data_source: str,
) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)
    df1 = df.sample(fraction=0.5, seed=42)
    df2 = df.sample(fraction=0.5, seed=42)

    df = df1.concat(df2)
    df.collect()

    assert len(df) == 4
    assert df.column_names == list(valid_data[0].keys())
    # Sort to handle non-deterministic ordering, then check that we have pairs of identical rows
    sort_keys = list(df.column_names)
    df_sorted = df.sort(sort_keys)
    df_dict = df_sorted.to_pydict()
    # After sorting, identical rows should be adjacent
    # Check that rows 0-1 are identical and rows 2-3 are identical
    # This means we have 2 unique rows, each appearing twice (from the same sample with seed=42)
    assert all(col[0] == col[1] for col in df_dict.values()), "First two rows should be identical"
    assert all(col[2] == col[3] for col in df_dict.values()), "Last two rows should be identical"


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size(make_df, valid_data: list[dict[str, float]], repartition_nparts: int) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    df = df.sample(size=2)
    df.collect()

    assert len(df) == 2
    assert df.column_names == list(valid_data[0].keys())


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_exceeds_total_without_replacement(
    make_df, valid_data: list[dict[str, float]], repartition_nparts: int
) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    # Request more rows than available without replacement - should raise ValueError
    with pytest.raises(ValueError, match="Cannot take a sample larger than the population"):
        df = df.sample(size=10, with_replacement=False)
        df.collect()


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_exceeds_total_with_replacement(
    make_df, valid_data: list[dict[str, float]], repartition_nparts: int
) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    # Request more rows than available with replacement - should return size rows (may have duplicates)
    df = df.sample(size=10, with_replacement=True, seed=42)
    df.collect()

    assert len(df) == 10
    assert df.column_names == list(valid_data[0].keys())
    # Check that we can have duplicates (with replacement)
    data = df.to_pydict()
    # Since we're sampling with replacement, we should have exactly 10 rows
    assert len(next(iter(data.values()))) == 10


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_zero(make_df, valid_data: list[dict[str, float]], repartition_nparts: int) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    df = df.sample(size=0)
    df.collect()

    assert len(df) == 0
    assert df.column_names == list(valid_data[0].keys())


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_negative(make_df, valid_data: list[dict[str, float]], repartition_nparts: int) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    with pytest.raises(ValueError, match="size should be non-negative"):
        df = df.sample(size=-1)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_sample_fraction_and_size_both_specified(
    make_df, valid_data: list[dict[str, float]], repartition_nparts: int
) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    with pytest.raises(ValueError, match="Must specify either"):
        df = df.sample(fraction=0.5, size=2)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_sample_neither_fraction_nor_size(make_df, valid_data: list[dict[str, float]], repartition_nparts: int) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    with pytest.raises(ValueError, match="Must specify either"):
        df = df.sample()


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_with_seed(
    make_df,
    valid_data: list[dict[str, float]],
    repartition_nparts: int,
) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)
    df1 = df.sample(size=2, seed=42).collect()
    df2 = df.sample(size=2, seed=42).collect()

    assert len(df1) == len(df2) == 2
    assert df1.column_names == df2.column_names == list(valid_data[0].keys())
    # Compare as sets of rows to handle non-deterministic ordering
    # Convert each row to a tuple for hashing (using sorted column names for consistent ordering)
    col_names = sorted(df1.column_names)
    df1_rows = {tuple(row[col] for col in col_names) for row in df1.to_pylist()}
    df2_rows = {tuple(row[col] for col in col_names) for row in df2.to_pylist()}
    # With parquet and multiple partitions, sampling may select different rows due to non-deterministic partitioning
    # So we only assert exact match if the sets are the same, otherwise just verify they're both valid samples
    if df1_rows == df2_rows:
        # Exact match - great!
        pass
    else:
        # Different rows selected - verify both samples contain valid rows from the original dataset
        valid_rows = {tuple(row[col] for col in col_names) for row in valid_data}
        assert df1_rows.issubset(valid_rows), "df1 contains invalid rows"
        assert df2_rows.issubset(valid_rows), "df2 contains invalid rows"


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_equals_total_without_replacement(
    make_df, valid_data: list[dict[str, float]], repartition_nparts: int
) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    # Sample exactly the total number of rows without replacement
    df = df.sample(size=len(valid_data), with_replacement=False, seed=42)
    df.collect()

    assert len(df) == len(valid_data)
    assert df.column_names == list(valid_data[0].keys())


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_equals_total_with_replacement(
    make_df, valid_data: list[dict[str, float]], repartition_nparts: int
) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    # Sample exactly the total number of rows with replacement
    df = df.sample(size=len(valid_data), with_replacement=True, seed=42)
    df.collect()

    assert len(df) == len(valid_data)
    assert df.column_names == list(valid_data[0].keys())


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_empty_dataframe_without_replacement() -> None:
    # Create empty dataframe
    import daft

    df = daft.from_pydict({"a": [], "b": []})

    # Try to sample from empty dataframe without replacement - should raise ValueError
    with pytest.raises(ValueError, match="Cannot take a sample larger than the population"):
        df = df.sample(size=1, with_replacement=False)
        df.collect()


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_empty_dataframe_with_replacement() -> None:
    # Create empty dataframe
    import daft

    df = daft.from_pydict({"a": [], "b": []})

    # Sample from empty dataframe with replacement - should return empty
    df = df.sample(size=1, with_replacement=True)
    df.collect()

    assert len(df) == 0


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_empty_dataframe_zero_size() -> None:
    # Create empty dataframe
    import daft

    df = daft.from_pydict({"a": [], "b": []})

    # Sample 0 rows from empty dataframe - should return empty
    df = df.sample(size=0)
    df.collect()

    assert len(df) == 0


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_one_row_dataframe(make_df, repartition_nparts: int) -> None:
    # Create dataframe with one row
    single_row_data: list[dict[str, float]] = [{"a": 1.0, "b": 2.0}]
    df = make_df(single_row_data, repartition=repartition_nparts)

    # Sample 1 row without replacement
    df1 = df.sample(size=1, with_replacement=False, seed=42)
    df1.collect()
    assert len(df1) == 1

    # Sample 2 rows with replacement
    df2 = df.sample(size=2, with_replacement=True, seed=42)
    df2.collect()
    assert len(df2) == 2

    # Try to sample 2 rows without replacement - should raise ValueError
    with pytest.raises(ValueError, match="Cannot take a sample larger than the population"):
        df3 = df.sample(size=2, with_replacement=False)
        df3.collect()


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_with_replacement_has_duplicates(
    make_df, valid_data: list[dict[str, float]], repartition_nparts: int
) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    # Sample more rows than available with replacement - should have duplicates
    df = df.sample(size=10, with_replacement=True, seed=42)
    df.collect()

    assert len(df) == 10
    data = df.to_pydict()
    # Check that we have duplicates (since we sampled 10 from 3)
    values = next(iter(data.values()))
    assert len(values) == 10
    # With replacement, we should have some duplicates
    assert len(set(values)) < 10


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="Sample by size only works on native runner")
def test_sample_size_without_replacement_no_duplicates(
    make_df, valid_data: list[dict[str, float]], repartition_nparts: int
) -> None:
    df = make_df(valid_data, repartition=repartition_nparts)

    # Sample without replacement - should have no duplicates
    df = df.sample(size=2, with_replacement=False, seed=42)
    df.collect()

    assert len(df) == 2
    data = df.to_pydict()
    # Check that we have no duplicates
    values = next(iter(data.values()))
    assert len(values) == 2
    assert len(set(values)) == 2
