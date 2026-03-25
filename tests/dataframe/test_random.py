from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.functions import random_int


def test_random_int_column_generation(make_df) -> None:
    df = make_df({"a": list(range(200))}).with_column("r", random_int(low=10, high=20, seed=0)).collect()

    assert len(df) == 200
    assert df.schema()["r"].dtype == DataType.int64()
    assert all(10 <= value <= 20 for value in df.to_pydict()["r"])


def test_random_unseeded_calls_are_distinct(make_df) -> None:
    df = (
        make_df({"a": list(range(512))})
        .select(
            random_int(low=0, high=1_000_000).alias("r1"),
            random_int(low=0, high=1_000_000).alias("r2"),
        )
        .collect()
    )

    values = df.to_pydict()
    assert any(left != right for left, right in zip(values["r1"], values["r2"]))


def test_random_seeded_calls_are_stable_within_test_run(make_df) -> None:
    base = make_df({"a": list(range(100))})

    first = base.select(
        random_int(low=5, high=25, seed=7).alias("ri"),
    ).collect()
    second = base.select(
        random_int(low=5, high=25, seed=7).alias("ri"),
    ).collect()

    assert first.to_pydict() == second.to_pydict()


def test_random_int_invalid_inputs_raise(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="lower bound (`low`) must be strictly less than the upper bound (`high`)"):
        df.with_column("r", random_int(low=10, high=10)).collect()

    with pytest.raises(ValueError, match="`high` to be a literal"):
        df.with_column("r", random_int(low=1, high=col("a"))).collect()


def test_shuffle_reproducible_with_seed(make_df) -> None:
    df = make_df({"a": list(range(40))})
    first = df.shuffle(seed=99).to_pydict()["a"]
    second = df.shuffle(seed=99).to_pydict()["a"]
    assert first == second
    assert set(first) == set(range(40))
    # Note: Theoretically could be equal, but insanely unlikely (1/40!)
    assert first != list(range(40))


def test_shuffle_changes_row_order(make_df) -> None:
    df = make_df({"a": list(range(30))})
    original = df.to_pydict()["a"]
    shuffled = df.shuffle(seed=7).to_pydict()["a"]
    assert shuffled != original
