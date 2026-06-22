from __future__ import annotations

import daft


def test_collect_populates_metrics() -> None:
    df = daft.from_pydict({"id": [1, 2, 3], "value": [10, 20, 30]}).select("id", "value").limit(2)

    df.collect()

    assert df.metrics is not None
    metrics_rows = df.metrics.to_pylist()
    assert metrics_rows
