from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.trace_to_speedscope import SPEEDSCOPE_SCHEMA, TraceConversionError, convert_file, convert_records, main


def span_event(
    timestamp: str,
    message: str,
    name: str,
    ancestors: list[str] | None = None,
    *,
    include_current_span: bool = False,
) -> dict:
    spans = [*ancestors] if ancestors is not None else []
    if include_current_span:
        spans.append(name)
    return {
        "timestamp": timestamp,
        "fields": {"message": message},
        "span": {"name": name},
        "spans": [{"name": ancestor} for ancestor in spans],
    }


@pytest.mark.parametrize("include_current_span", [False, True])
def test_convert_nested_span_events(include_current_span: bool):
    records = [
        {"timestamp": "2026-01-01T00:00:00Z", "fields": {"message": "ordinary log"}},
        span_event(
            "2026-01-01T00:00:01.000000Z",
            "new",
            "VideoFile::frames",
            include_current_span=include_current_span,
        ),
        span_event(
            "2026-01-01T00:00:01.000250Z",
            "new",
            "VideoFile::read",
            ["VideoFile::frames"],
            include_current_span=include_current_span,
        ),
        span_event(
            "2026-01-01T00:00:01.000750Z",
            "close",
            "VideoFile::read",
            ["VideoFile::frames"],
            include_current_span=include_current_span,
        ),
        span_event(
            "2026-01-01T00:00:01.001000Z",
            "close",
            "VideoFile::frames",
            include_current_span=include_current_span,
        ),
    ]

    output = convert_records(records, "video trace")

    assert output["$schema"] == SPEEDSCOPE_SCHEMA
    assert output["shared"]["frames"] == [{"name": "VideoFile::frames"}, {"name": "VideoFile::read"}]
    profile = output["profiles"][0]
    assert profile["name"] == "video trace"
    assert profile["unit"] == "microseconds"
    assert profile["startValue"] == 0
    assert profile["endValue"] == 1000
    assert profile["events"] == [
        {"type": "O", "at": 0, "frame": 0},
        {"type": "O", "at": 250, "frame": 1},
        {"type": "C", "at": 750, "frame": 1},
        {"type": "C", "at": 1000, "frame": 0},
    ]


def test_rejects_non_nested_span_events():
    records = [
        span_event("2026-01-01T00:00:00Z", "new", "outer"),
        span_event("2026-01-01T00:00:00.001000Z", "new", "concurrent"),
    ]

    with pytest.raises(TraceConversionError, match="may contain concurrent spans"):
        convert_records(records, "invalid")


def test_rejects_unclosed_spans():
    records = [span_event("2026-01-01T00:00:00Z", "new", "File::read")]

    with pytest.raises(TraceConversionError, match="unclosed spans"):
        convert_records(records, "invalid")


def test_convert_file_and_cli_default_output(tmp_path: Path, capsys: pytest.CaptureFixture[str]):
    source = tmp_path / "file-read.trace.jsonl"
    source.write_text(
        "\n".join(
            json.dumps(record)
            for record in [
                span_event("2026-01-01T00:00:00Z", "new", "File::read"),
                span_event("2026-01-01T00:00:00.001000Z", "close", "File::read"),
            ]
        )
    )

    destination = tmp_path / "explicit.speedscope.json"
    convert_file(source, destination, "explicit")
    assert json.loads(destination.read_text())["name"] == "explicit"

    assert main([str(source), "--name", "CLI profile"]) == 0
    default_output = source.with_suffix(".speedscope.json")
    assert capsys.readouterr().out.strip() == str(default_output)
    assert json.loads(default_output.read_text())["name"] == "CLI profile"
