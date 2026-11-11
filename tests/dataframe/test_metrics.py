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


@pytest.mark.parametrize("unit,expected", [("ns", 0.001), ("us", 1), ("µs", 1), ("ms", 1000), ("s", 1000000)])
def test_profile_duration_units(unit, expected):
    from daft.execution.metadata import _metric_duration_us

    assert _metric_duration_us({"duration": {"value": 1, "unit": unit}}) == expected


@pytest.mark.parametrize("unit", [None, "minutes"])
def test_profile_rejects_invalid_duration_units(unit):
    from daft.execution.metadata import _metric_duration_us

    with pytest.raises(ValueError):
        _metric_duration_us({"duration": {"value": 1, "unit": unit}})


def test_profile_preserves_builtin_metrics():
    from daft.execution.metadata import _metric_duration_us, _stats_to_dict

    stats = _stats_to_dict(
        [
            ("duration", {"value": 2000, "unit": "us"}),
            ("duration", {"value": 99, "unit": None}),
        ]
    )
    assert _metric_duration_us(stats) == 2000


def test_show_does_not_enable_profile():
    df = daft.from_pydict({"id": [1, 2, 3]}).select("id")
    df.show()
    with pytest.raises(ValueError, match="materialized"):
        df.profile()


@pytest.mark.parametrize("show_details", [False, True])
def test_profile_formats_metrics(monkeypatch, show_details):
    from daft.execution.metadata import ExecutionMetadata

    class Batch:
        def to_pylist(self):
            return [
                {
                    "name": "Scan",
                    "type": "Scan",
                    "category": "Source",
                    "stats": [
                        ("duration", {"value": 2, "unit": "ms"}),
                        ("duration", {"value": 99, "unit": None}),
                        ("rows.out", {"value": 3, "unit": None}),
                        ("bytes.read", {"value": 1024, "unit": "bytes"}),
                    ],
                },
                {
                    "name": "Filter",
                    "type": "Filter",
                    "category": "Intermediate",
                    "stats": [
                        ("duration", {"value": 1, "unit": "ms"}),
                    ],
                },
            ]

    meta = ExecutionMetadata.__new__(ExecutionMetadata)
    meta._query_id = "test"
    monkeypatch.setattr(ExecutionMetadata, "to_recordbatch", lambda self: Batch())
    text = meta.format_profile(top_n=1, show_details=show_details)
    assert "Total operator time: 3.00 ms" in text
    assert "Rows read: 3" in text
    assert "Bytes scanned: 1.00 KiB" in text
    assert "1. Scan (Scan) - 2.00 ms" in text
    assert "2. Filter" not in text
    assert ("rows out: 3" in text) is show_details


def test_profile_empty_metrics(monkeypatch):
    from daft.execution.metadata import ExecutionMetadata

    meta = ExecutionMetadata.__new__(ExecutionMetadata)
    meta._query_id = "empty"
    monkeypatch.setattr(ExecutionMetadata, "_profile_nodes", lambda self: [])
    text = meta.format_profile()
    assert text.count("No execution metrics were recorded.") == 1
    assert "No operator metrics were recorded." not in text


def test_profile_rejects_binary_stream():
    df = daft.from_pydict({"id": [1]}).select("id").collect()
    with pytest.raises(TypeError):
        df.profile(file=io.BytesIO())


def test_profile_join_counters(monkeypatch):
    from daft.execution.metadata import ExecutionMetadata

    class Batch:
        def to_pylist(self):
            counters = {
                "build rows inserted": 2,
                "probe rows in": 3,
                "probe rows out": 4,
                "bytes.join.build_inserted": 1024,
                "bytes.join.probe_in": 2048,
                "bytes.join.probe_out": 4096,
            }
            return [
                {
                    "name": "Join",
                    "type": "HashJoin",
                    "category": "Blocking",
                    "stats": [(name, {"value": value, "unit": None}) for name, value in counters.items()],
                }
            ]

    meta = ExecutionMetadata.__new__(ExecutionMetadata)
    meta._query_id = "join"
    monkeypatch.setattr(ExecutionMetadata, "to_recordbatch", lambda self: Batch())
    text = meta.format_profile(show_details=True)
    assert "rows in: 5, rows out: 4" in text
    assert "bytes in/out: 3.00 KiB / 4.00 KiB" in text


def test_profile_write_counters(monkeypatch):
    from daft.execution.metadata import ExecutionMetadata

    class Batch:
        def to_pylist(self):
            return [
                {
                    "name": "Write",
                    "type": "Write",
                    "category": "Sink",
                    "stats": [
                        ("rows.written", {"value": 3, "unit": None}),
                        ("bytes.written", {"value": 2048, "unit": "bytes"}),
                    ],
                }
            ]

    meta = ExecutionMetadata.__new__(ExecutionMetadata)
    meta._query_id = "write"
    monkeypatch.setattr(ExecutionMetadata, "to_recordbatch", lambda self: Batch())
    text = meta.format_profile(show_details=True)
    assert "rows written: 3" in text
    assert "bytes written: 2.00 KiB" in text
    assert "rows written:" not in meta.format_profile(show_details=False)
