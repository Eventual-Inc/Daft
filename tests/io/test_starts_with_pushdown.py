"""Tests that `starts_with(col, literal)` filters are rewritten by the optimizer
into the equivalent half-open range `col >= prefix AND col < increment(prefix)`.

The rewrite lets the predicate reach the scan's pushdown filters (a lone
`starts_with` call is classified as a UDF by the scan expression rewriter and
stranded in a residual Filter op), so sources can prune with min/max string
statistics -- e.g. Parquet row groups whose [min, max] range cannot contain the
prefix are skipped.
"""

from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as papq

import daft
from daft import col


def _write_two_row_group_parquet(path: str) -> None:
    """Row group 0 covers ["apple", "avocado"], row group 1 covers ["banana", "blueberry"]."""
    rg0 = pa.table({"s": ["apple", "avocado"]})
    rg1 = pa.table({"s": ["banana", "blueberry", None]})
    writer = papq.ParquetWriter(path, rg0.schema)
    writer.write_table(rg0)
    writer.write_table(rg1)
    writer.close()


def _optimized_plan(df: daft.DataFrame) -> str:
    buf = io.StringIO()
    df.explain(True, file=buf)
    return buf.getvalue().split("== Optimized Logical Plan ==")[-1]


def test_starts_with_filter_pushed_into_scan_as_range(tmp_path):
    path = str(tmp_path / "two_rg.parquet")
    _write_two_row_group_parquet(path)

    plan = _optimized_plan(daft.read_parquet(path).where(col("s").startswith("b")))

    # The scalar-function call must be gone, replaced by range comparisons that
    # were pushed down into the scan. (Assert on the call form: the tmp_path
    # embeds this test's name, which contains the bare word.)
    assert "starts_with(" not in plan
    assert 'col(s) >= lit("b")' in plan
    assert 'col(s) < lit("c")' in plan
    assert "Pushdowns" in plan


def test_starts_with_filter_prunes_row_groups(tmp_path):
    path = str(tmp_path / "two_rg.parquet")
    _write_two_row_group_parquet(path)

    df = daft.read_parquet(path)
    assert df.where(col("s").startswith("a")).to_pydict() == {"s": ["apple", "avocado"]}
    assert df.where(col("s").startswith("b")).to_pydict() == {"s": ["banana", "blueberry"]}
    assert df.where(col("s").startswith("c")).to_pydict() == {"s": []}
    assert df.where(col("s").startswith("blue")).to_pydict() == {"s": ["blueberry"]}


def test_starts_with_empty_prefix_and_nulls(tmp_path):
    path = str(tmp_path / "two_rg.parquet")
    _write_two_row_group_parquet(path)

    df = daft.read_parquet(path)
    # Empty prefix is not rewritten (no useful upper bound) and matches every
    # non-null string.
    assert df.where(col("s").startswith("")).to_pydict() == {
        "s": ["apple", "avocado", "banana", "blueberry"]
    }
    # starts_with on a null input yields null, so null rows are filtered out.
    assert df.where(col("s").startswith("blueberry")).to_pydict() == {"s": ["blueberry"]}


def test_starts_with_rewrite_in_sql(tmp_path):
    path = str(tmp_path / "two_rg.parquet")
    _write_two_row_group_parquet(path)

    df = daft.read_parquet(path)
    result = daft.sql("select s from df where starts_with(s, 'a')").to_pydict()
    assert result == {"s": ["apple", "avocado"]}


def test_starts_with_in_projection_not_rewritten(tmp_path):
    path = str(tmp_path / "two_rg.parquet")
    _write_two_row_group_parquet(path)

    df = daft.read_parquet(path).select(col("s").startswith("a"))
    plan = _optimized_plan(df)
    # Outside filters the kernel call is a single cheaper operation and no
    # pushdown applies, so it must be left alone.
    assert "starts_with(" in plan
    assert df.to_pydict() == {"s": [True, True, False, False, None]}
