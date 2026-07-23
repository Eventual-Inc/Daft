from __future__ import annotations

import io

import pytest

import daft
from daft.execution.metadata import _metric_duration_us, _profile_warnings, _ProfileNode


def test_collect_populates_metrics() -> None:
    df = daft.from_pydict({"id": [1, 2, 3], "value": [10, 20, 30]}).select("id", "value").limit(2)

    df.collect()

    assert df.metrics is not None
    metrics_rows = df.metrics.to_pylist()
    assert metrics_rows


def test_profile_requires_fully_materialized_dataframe() -> None:
    df = daft.from_pydict({"id": [1, 2, 3]}).select("id")

    with pytest.raises(ValueError, match=r"fully materialized with collect\(\)"):
        df.profile()

    df.show()
    with pytest.raises(ValueError, match=r"fully materialized with collect\(\)"):
        df.profile()


def test_profile_prints_complete_query_summary() -> None:
    df = daft.from_pydict({"id": [1, 2, 3], "value": [10, 20, 30]}).select("id", "value").limit(2).collect()

    output = io.StringIO()
    df.profile(top_n=1, file=output)
    text = output.getvalue()

    assert "== Query Profile ==" in text
    assert "Query ID:" in text
    assert "Wall time:" in text
    assert "Operator CPU time:" in text
    assert "Rows read: 3" in text
    assert "Bytes scanned:" in text
    assert "Peak sampled process RSS:" in text
    assert "Slowest operators:" in text
    assert "Warnings:" in text
    assert "1. " in text
    assert "% CPU" in text
    assert "rows in/out:" in text


@pytest.mark.parametrize("top_n", [0, -1])
def test_profile_validates_top_n(top_n: object) -> None:
    df = daft.from_pydict({"id": [1]}).collect()
    with pytest.raises(ValueError, match="top_n must be a positive integer"):
        df.profile(top_n=top_n)  # type: ignore[arg-type]


@pytest.mark.parametrize("top_n", [1.5, True])
def test_profile_rejects_non_integer_top_n(top_n: object) -> None:
    df = daft.from_pydict({"id": [1]}).collect()
    with pytest.raises(TypeError, match="integer|wrong input type"):
        df.profile(top_n=top_n)  # type: ignore[arg-type]


def test_profile_duration_uses_first_builtin_metric() -> None:
    stats = [
        ("duration", {"value": 5, "unit": "ms"}),
        ("duration", {"value": 999, "unit": None}),
    ]
    assert _metric_duration_us(stats) == 5_000


def test_profile_missing_duration_is_unavailable() -> None:
    assert _metric_duration_us([]) is None


def test_profile_rejects_unknown_duration_unit() -> None:
    with pytest.raises(ValueError, match="Unknown duration unit"):
        _metric_duration_us([("duration", {"value": 1, "unit": "ns"})])


def test_profile_warning_thresholds() -> None:
    nodes = [
        _ProfileNode(
            1,
            "shuffle",
            "Repartition",
            "BlockingSink",
            80,
            100_000,
            100_000,
            None,
            512 * 1024**2,
            512 * 1024**2,
        ),
        _ProfileNode(2, "other", "Project", "Intermediate", 20, 100_000, 100_000, None, 1, 1),
    ]
    telemetry = {
        "spilled_bytes": 1,
        "partition_stats": {1: {"count": 4, "total_rows": 100_000, "max_rows": 100_000}},
    }

    warnings = _profile_warnings(nodes, 100, telemetry)

    assert any("Most operator CPU time" in warning for warning in warnings)
    assert any("Large shuffle" in warning for warning in warnings)
    assert any("spilled" in warning for warning in warnings)
    assert any("partition skew" in warning for warning in warnings)


def test_profile_warnings_remain_absent_below_thresholds() -> None:
    nodes = [
        _ProfileNode(
            1,
            "shuffle",
            "Repartition",
            "BlockingSink",
            79,
            99_999,
            99_999,
            None,
            (1024**3 - 1) // 2,
            (1024**3 - 1) // 2,
        ),
        _ProfileNode(2, "other", "Project", "Intermediate", 21, 99_999, 99_999, None, 1, 1),
    ]
    telemetry = {
        "spilled_bytes": 0,
        "partition_stats": {1: {"count": 4, "total_rows": 99_999, "max_rows": 99_999}},
    }

    assert _profile_warnings(nodes, 100, telemetry) == []


def test_profile_warns_when_requested_pushdown_is_missing_at_boundary() -> None:
    node = _ProfileNode(
        1,
        "scan",
        "ScanTask",
        "Source",
        1,
        None,
        1,
        256 * 1024**2,
        None,
        None,
    )
    telemetry = {
        "scan_pushdowns": {
            1: {
                "filter_requested": True,
                "filter_applied": False,
                "projection_requested": True,
                "projection_applied": False,
            }
        }
    }

    warnings = _profile_warnings([node], 1, telemetry)

    assert any("Filter pushdown" in warning for warning in warnings)
    assert any("Projection pushdown" in warning for warning in warnings)
