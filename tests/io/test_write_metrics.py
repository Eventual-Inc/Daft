from __future__ import annotations

import os
import tempfile

import pytest

import daft


@pytest.fixture(autouse=True)
def _reset_execution_config():
    yield daft.set_execution_config()


def _build_df(n=10000):
    return daft.from_pydict(
        {
            "id": list(range(10000)),
            "name": [f"item_{i}" for i in range(10000)],
            "value": [float(i) * 1.1 for i in range(10000)],
        }
    )


def _assert_write_metrics(result, expected_rows):
    # Find the Write node and verify rows.written/bytes.written are actually emitted.
    # Prior versions only asserted the value when the stat happened to be present, which
    # silently passed when Flotilla dropped the stat entirely (DF-1933).
    write_nodes = [node for node in result.metrics.to_pylist() if node["type"] == "Write"]
    assert len(write_nodes) >= 1, (
        f"Expected at least one Write node in metrics, got nodes: "
        f"{[(n['name'], n['type']) for n in result.metrics.to_pylist()]}"
    )

    for write_node in write_nodes:
        stats = dict(write_node["stats"])
        assert "rows.written" in stats, (
            f"Write node {write_node['name']!r} is missing rows.written stat; "
            f"got stats: {list(stats.keys())}"
        )
        assert stats["rows.written"]["value"] == expected_rows, (
            f"Write node {write_node['name']!r} rows.written = "
            f"{stats['rows.written']['value']}, expected {expected_rows}"
        )
        assert "bytes.written" in stats, (
            f"Write node {write_node['name']!r} is missing bytes.written stat; "
            f"got stats: {list(stats.keys())}"
        )
        assert stats["bytes.written"]["value"] > 0, (
            f"Write node {write_node['name']!r} bytes.written = "
            f"{stats['bytes.written']['value']}, expected > 0"
        )


@pytest.mark.parametrize("parquet_target_row_group_size", [32 * 1024 * 1024, 64 * 1024 * 1024, 1 * 1024])
def test_write_metrics_emitted_parquet(parquet_target_row_group_size):
    # The first two target row group sizes will flush everything in close and
    # the write metrics are incorrectly emitted. Issue #6518
    daft.set_execution_config(parquet_target_row_group_size=parquet_target_row_group_size)

    df = _build_df(n=10000)

    with tempfile.TemporaryDirectory() as directory_name:
        result = df.write_parquet(os.path.join(directory_name, "out"))
        _assert_write_metrics(result, expected_rows=10000)


def test_write_metrics_emitted_csv():
    df = _build_df()

    with tempfile.TemporaryDirectory() as directory_name:
        result = df.write_csv(os.path.join(directory_name, "out"))
        _assert_write_metrics(result, expected_rows=10000)
