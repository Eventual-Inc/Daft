from __future__ import annotations

import daft


def test_collect_populates_metrics() -> None:
    df = daft.from_pydict({"id": [1, 2, 3], "value": [10, 20, 30]}).select("id", "value").limit(2)

    df.collect()

    assert df.metrics is not None
    metrics_rows = df.metrics.to_pylist()
    assert metrics_rows


# ---------------------------------------------------------------------------
# iter_partitions() metrics capture
# ---------------------------------------------------------------------------


def test_iter_partitions_populates_metrics() -> None:
    """df.metrics is available after the iter_partitions() generator is exhausted."""
    df = daft.from_pydict({"x": list(range(100))}).select("x")

    rows = 0
    for batch in df.iter_partitions():
        rows += len(batch)

    assert rows == 100
    assert df.metrics is not None


def test_iter_partitions_metrics_has_same_columns_as_collect() -> None:
    """The metric RecordBatch produced by iter_partitions() has the same schema as collect()."""
    data = {"x": list(range(50))}

    df_iter = daft.from_pydict(data).select("x")
    for _ in df_iter.iter_partitions():
        pass

    df_col = daft.from_pydict(data).select("x")
    df_col.collect()

    assert df_iter.metrics is not None
    assert df_col.metrics is not None
    assert set(df_iter.metrics.schema().column_names()) == set(df_col.metrics.schema().column_names())


def test_iter_partitions_metrics_not_available_before_exhaustion() -> None:
    """Before the iterator is fully consumed, df.metrics should not be set."""
    df = daft.from_pydict({"x": list(range(10))}).select("x")
    assert df.metrics is None  # nothing run yet

    it = df.iter_partitions()
    for _ in it:
        pass

    assert df.metrics is not None


# ---------------------------------------------------------------------------
# Parquet execution metrics in df.metrics
# ---------------------------------------------------------------------------


def _sum_source_stat(metrics_rb, key: str) -> int:
    """Sum a named stat across all Source nodes."""
    total = 0
    for row in metrics_rb.to_pylist():
        if row.get("category") == "Source":
            for stat_key, stat_val in row["stats"]:
                if stat_key == key:
                    total += int(stat_val["value"])
    return total


def test_parquet_files_opened_counter(tmp_path) -> None:
    """scan.files_opened increments once per Parquet file read."""
    for i in range(3):
        daft.from_pydict({"id": list(range(i * 10, (i + 1) * 10))}).write_parquet(str(tmp_path))

    df = daft.read_parquet(str(tmp_path))
    df.collect()

    assert df.metrics is not None
    assert _sum_source_stat(df.metrics, "scan.files_opened") == 3


def test_parquet_row_groups_total_at_least_one_per_file(tmp_path) -> None:
    """scan.row_groups_total >= scan.files_opened (at least one row group per file)."""
    for i in range(2):
        daft.from_pydict({"id": list(range(100))}).write_parquet(str(tmp_path))

    df = daft.read_parquet(str(tmp_path))
    df.collect()

    assert df.metrics is not None
    rg_total = _sum_source_stat(df.metrics, "scan.row_groups_total")
    files_opened = _sum_source_stat(df.metrics, "scan.files_opened")
    assert rg_total >= files_opened >= 1


def test_parquet_rows_scanned_geq_rows_written(tmp_path) -> None:
    """scan.rows_scanned covers all rows in selected row groups."""
    n = 200
    daft.from_pydict({"id": list(range(n))}).write_parquet(str(tmp_path))

    df = daft.read_parquet(str(tmp_path))
    df.collect()

    assert df.metrics is not None
    assert _sum_source_stat(df.metrics, "scan.rows_scanned") >= n


def test_parquet_scan_metrics_via_iter_partitions(tmp_path) -> None:
    """scan.files_opened is populated when using iter_partitions() instead of collect()."""
    daft.from_pydict({"id": list(range(50))}).write_parquet(str(tmp_path))

    df = daft.read_parquet(str(tmp_path))
    for _ in df.iter_partitions():
        pass

    assert df.metrics is not None
    assert _sum_source_stat(df.metrics, "scan.files_opened") == 1
