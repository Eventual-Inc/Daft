from __future__ import annotations

import pytest

import daft
from daft import col
from daft.dataframe import DataFrame

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_df() -> DataFrame:
    return daft.from_pydict({"device_id": [1, 2, 3], "t": [10, 20, 30], "val": [1.0, 2.0, 3.0]})


# ---------------------------------------------------------------------------
# Basic API — returns a DataFrame, data is unchanged
# ---------------------------------------------------------------------------


def test_string_col():
    df = make_df().assume_clustered_by("device_id")
    assert isinstance(df, DataFrame)


def test_expression_col():
    df = make_df().assume_clustered_by(col("device_id"))
    assert isinstance(df, DataFrame)


def test_multi_col_list():
    df = make_df().assume_clustered_by(["device_id", "t"])
    assert isinstance(df, DataFrame)


def test_multi_col_data_unchanged():
    original = make_df()
    annotated = original.assume_clustered_by(["device_id", "t"])
    assert original.collect().to_pydict() == annotated.collect().to_pydict()


def test_multi_col_mixed_str_and_expr():
    # str and Expression in the same list both resolve correctly
    df = make_df().assume_clustered_by(["device_id", col("t")])
    assert isinstance(df, DataFrame)


def test_multi_col_rejects_if_any_missing():
    # all columns must exist — one bad column should fail even if others are valid
    with pytest.raises(Exception, match="not found in schema"):
        make_df().assume_clustered_by(["device_id", "nonexistent", "t"])


def test_multi_col_rejects_if_any_non_column_expr():
    # one bad expr in the list should reject the whole call
    with pytest.raises(Exception, match="only accepts column references"):
        make_df().assume_clustered_by([col("device_id"), col("t") + 1])


def test_data_unchanged():
    original = make_df()
    annotated = original.assume_clustered_by("device_id")
    assert original.collect().to_pydict() == annotated.collect().to_pydict()


# ---------------------------------------------------------------------------
# Chaining — two calls compose correctly (each patches its own field,
# neither clobbers the other, guard still passes on the second call)
# ---------------------------------------------------------------------------


def test_chaining_does_not_raise():
    # Second call still sees Source(InMemory) at root — should not hit the
    # "must be called before any operations" branch.
    df = make_df().assume_clustered_by("device_id").assume_clustered_by("t")
    assert isinstance(df, DataFrame)


def test_chaining_data_unchanged():
    original = make_df()
    annotated = original.assume_clustered_by("device_id").assume_clustered_by("t")
    assert original.collect().to_pydict() == annotated.collect().to_pydict()


# ---------------------------------------------------------------------------
# Guard: non-column expressions are rejected
# ---------------------------------------------------------------------------


def test_rejects_binary_expr():
    with pytest.raises(Exception, match="only accepts column references"):
        make_df().assume_clustered_by(col("device_id") + 1)


def test_rejects_literal():
    with pytest.raises(Exception, match="only accepts column references"):
        make_df().assume_clustered_by(daft.lit(1))


# ---------------------------------------------------------------------------
# Guard: column must exist in schema
# ---------------------------------------------------------------------------


def test_rejects_missing_column():
    with pytest.raises(Exception, match="not found in schema"):
        make_df().assume_clustered_by("nonexistent")


def test_rejects_one_missing_in_multi():
    with pytest.raises(Exception, match="not found in schema"):
        make_df().assume_clustered_by(["device_id", "nonexistent"])


# ---------------------------------------------------------------------------
# Guard: must be called before any operations
# ---------------------------------------------------------------------------


def test_rejects_after_filter():
    with pytest.raises(Exception, match="must be called before any operations"):
        make_df().filter(col("val") > 1.0).assume_clustered_by("device_id")


def test_rejects_after_select():
    with pytest.raises(Exception, match="must be called before any operations"):
        make_df().select("device_id", "t").assume_clustered_by("device_id")


def test_rejects_after_sort():
    with pytest.raises(Exception, match="must be called before any operations"):
        make_df().sort("t").assume_clustered_by("device_id")


# ---------------------------------------------------------------------------
# Guard: file-backed sources are rejected
# ---------------------------------------------------------------------------


def test_rejects_glob_scan(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"x": [1, 2, 3]})
    pq.write_table(table, tmp_path / "data.parquet")

    with pytest.raises(Exception, match="file-backed sources"):
        daft.read_parquet(str(tmp_path / "data.parquet")).assume_clustered_by("x")
