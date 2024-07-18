from __future__ import annotations

import pytest


def test_sample_fraction(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)

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


def test_sample_full_fraction(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)

    df = df.sample(fraction=1.0)
    df.collect()

    assert len(df) == len(valid_data)
    assert df.column_names == list(valid_data[0].keys())


def test_sample_empty(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)

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


def test_sample_with_seed(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)

    df1 = df.sample(fraction=0.5, seed=42)
    df2 = df.sample(fraction=0.5, seed=42)

    df1.collect()
    df2.collect()

    assert len(df1) == len(df2)
    assert df1.column_names == df2.column_names == list(valid_data[0].keys())
    assert df1.to_pydict() == df2.to_pydict()


def test_sample_with_replacement(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)

    df = df.sample(fraction=0.5, with_replacement=True, seed=42)
    df.collect()

    assert len(df) == 2
    assert df.column_names == list(valid_data[0].keys())
    # Check that the two rows are the same, which should be for this seed.
    assert all(col[0] == col[1] for col in df.to_pydict().values())


def test_sample_with_concat(make_df, valid_data: list[dict[str, float]]) -> None:
    df1 = make_df(valid_data)
    df2 = make_df(valid_data)

    df1 = df1.sample(fraction=0.5, seed=42)
    df2 = df2.sample(fraction=0.5, seed=42)

    df = df1.concat(df2)
    df.collect()

    assert len(df) == 4
    assert df.column_names == list(valid_data[0].keys())
    # Check that the two rows are the same, which should be for this seed.
    assert all(col[:2] == col[2:] for col in df.to_pydict().values())
