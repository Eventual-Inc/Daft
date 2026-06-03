from __future__ import annotations

import io

import pytest

import daft


def test_collect_populates_metrics() -> None:
    df = daft.from_pydict({"id": [1, 2, 3], "value": [10, 20, 30]}).select("id", "value").limit(2)

    df.collect()

    assert df.metrics is not None
    metrics_rows = df.metrics.to_pylist()
    assert metrics_rows


def test_profile_requires_materialized_dataframe() -> None:
    df = daft.from_pydict({"id": [1, 2, 3]}).select("id")

    with pytest.raises(ValueError, match="Profile is not available until the DataFrame has been materialized"):
        df.profile()


def test_profile_prints_query_summary() -> None:
    df = daft.from_pydict({"id": [1, 2, 3], "value": [10, 20, 30]}).select("id", "value").limit(2).collect()

    output = io.StringIO()
    df.profile(top_n=1, file=output)
    text = output.getvalue()

    assert "== Query Profile ==" in text
    assert "Query ID:" in text
    assert "Total operator time:" in text
    assert "Rows read:" in text
    assert "Bytes scanned:" in text
    assert "Slowest operators:" in text
    assert "1. " in text


def test_profile_validates_top_n() -> None:
    df = daft.from_pydict({"id": [1, 2, 3]})

    with pytest.raises(ValueError, match="top_n must be at least 1"):
        df.profile(top_n=0)
