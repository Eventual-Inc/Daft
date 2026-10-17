from __future__ import annotations

import io
from types import SimpleNamespace
from typing import Any

import pytest

import daft
from daft.execution.metadata import (
    ExecutionMetadata,
    _infer_requested_pushdowns,
    _metric_duration_us,
    _profile_warnings,
    _ProfileNode,
)


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
    assert "Wall time: N/A" not in text
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


def test_profile_duration_without_unit_is_unavailable() -> None:
    assert _metric_duration_us([("duration", {"value": 1, "unit": None})]) is None


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
        "shuffle_write_bytes": 1024**3,
        "partition_stats": {1: {"count": 4, "total_rows": 100_000, "max_rows": 100_000}},
    }

    warnings = _profile_warnings(nodes, 100, telemetry)

    assert any("Most operator CPU time" in warning for warning in warnings)
    assert any("Large shuffle" in warning for warning in warnings)
    assert any("Flight shuffle" in warning for warning in warnings)
    assert any("logical data" in warning for warning in warnings)
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
        "shuffle_write_bytes": 1024**3 - 1,
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


@pytest.mark.parametrize(
    ("bytes_read", "applied", "expects_warning"),
    [
        (256 * 1024**2, True, False),
        (256 * 1024**2 - 1, False, False),
    ],
)
def test_profile_pushdown_warning_requires_large_unapplied_scan(
    bytes_read: int, applied: bool, expects_warning: bool
) -> None:
    node = _ProfileNode(1, "scan", "ScanTask", "Source", 1, None, 1, bytes_read, None, None)
    telemetry = {
        "scan_pushdowns": {
            1: {
                "filter_requested": True,
                "filter_applied": applied,
                "projection_requested": True,
                "projection_applied": applied,
            }
        }
    }

    warnings = _profile_warnings([node], 1, telemetry)

    assert any("pushdown" in warning for warning in warnings) is expects_warning


@pytest.mark.parametrize(
    ("operator", "request_key"), [("Project", "projection_requested"), ("Filter", "filter_requested")]
)
def test_profile_infers_pushdown_only_for_direct_source_children(operator: str, request_key: str) -> None:
    direct_source = {"id": 2, "name": "scan", "type": "ScanTask", "category": "Source", "children": []}
    nested_source = {"id": 4, "name": "scan", "type": "ScanTask", "category": "Source", "children": []}
    plan = {
        "id": 1,
        "name": operator.lower(),
        "type": operator,
        "category": "Intermediate",
        "children": [
            direct_source,
            {
                "id": 3,
                "name": "aggregate",
                "type": "Aggregate",
                "category": "BlockingSink",
                "children": [nested_source],
            },
        ],
    }
    pushdowns = {
        2: {request_key.replace("requested", "applied"): False},
        4: {request_key.replace("requested", "applied"): False},
    }

    _infer_requested_pushdowns(plan, pushdowns)

    assert pushdowns[2][request_key] is True
    assert request_key not in pushdowns[4]


def test_profile_infers_nested_direct_pairs_without_crossing_operators() -> None:
    plan = {
        "id": 1,
        "name": "aggregate",
        "type": "Aggregate",
        "category": "BlockingSink",
        "children": [
            {
                "id": 2,
                "name": "project",
                "type": "Project",
                "category": "Intermediate",
                "children": [{"id": 3, "name": "scan", "type": "GlobScan", "category": "Source", "children": []}],
            }
        ],
    }
    pushdowns: dict[int, dict[str, Any]] = {}

    _infer_requested_pushdowns(plan, pushdowns)

    assert pushdowns == {3: {"projection_requested": True}}


def test_profile_mixes_timed_and_untimed_nodes() -> None:
    rows = [
        {
            "id": 1,
            "name": "untimed",
            "type": "Project",
            "category": "Intermediate",
            "stats": [("duration", {"value": 10, "unit": None})],
        },
        {
            "id": 2,
            "name": "timed",
            "type": "Filter",
            "category": "Intermediate",
            "stats": [("duration", {"value": 2, "unit": "ms"})],
        },
    ]
    metadata = ExecutionMetadata.__new__(ExecutionMetadata)
    metadata._py = SimpleNamespace(profile_telemetry={})  # type: ignore[assignment]
    metadata._query_id = "query"
    metadata._query_plan = "{}"
    metadata._wall_time_us = 2_000
    metadata.to_recordbatch = lambda: SimpleNamespace(to_pylist=lambda: rows)  # type: ignore[method-assign]

    profile = metadata._format_profile()

    assert "timed (Filter) - 2.00 ms" in profile
    assert "untimed (Project) - N/A" in profile
