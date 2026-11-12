#!/usr/bin/env python3
"""Convert Daft JSON tracing span events into a SpeedScope evented profile.

Capture a trace with Daft's JSON formatter, then run this utility:

    DAFT_TRACE=info DAFT_TRACE_FORMAT=json python workload.py 2> trace.jsonl
    python tools/trace_to_speedscope.py trace.jsonl

The output can be opened at https://www.speedscope.app. The converter ignores
ordinary JSON log records, but requires span ``new`` and ``close`` events to be
properly nested. A trace containing interleaved concurrent spans cannot be
converted because Daft's JSON formatter does not currently include span IDs.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

SPEEDSCOPE_SCHEMA = "https://www.speedscope.app/file-format-schema.json"


class TraceConversionError(ValueError):
    """Raised when tracing records cannot form a valid SpeedScope profile."""


def _parse_timestamp(value: Any, event_number: int) -> datetime:
    if not isinstance(value, str):
        raise TraceConversionError(f"Span event {event_number} has no string timestamp")

    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise TraceConversionError(f"Span event {event_number} has invalid timestamp {value!r}") from error

    if timestamp.tzinfo is None:
        raise TraceConversionError(f"Span event {event_number} has a timestamp without a UTC offset")
    return timestamp


def _extract_span_event(record: dict[str, Any]) -> tuple[str, str, list[str]] | None:
    fields = record.get("fields")
    span = record.get("span")
    if not isinstance(fields, dict) or not isinstance(span, dict):
        return None

    message = fields.get("message")
    name = span.get("name")
    if message not in {"new", "close"} or not isinstance(name, str):
        return None

    raw_ancestors = record.get("spans", [])
    if not isinstance(raw_ancestors, list):
        raise TraceConversionError(f"Span {name!r} has a non-list ancestor field")

    ancestors: list[str] = []
    for ancestor in raw_ancestors:
        if not isinstance(ancestor, dict) or not isinstance(ancestor.get("name"), str):
            raise TraceConversionError(f"Span {name!r} has an invalid ancestor entry")
        ancestors.append(ancestor["name"])

    return message, name, ancestors


def load_json_lines(path: Path) -> list[dict[str, Any]]:
    """Load JSON objects from a newline-delimited trace file."""
    records: list[dict[str, Any]] = []
    with path.open() as trace_file:
        for line_number, line in enumerate(trace_file, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise TraceConversionError(f"{path}:{line_number}: invalid JSON: {error.msg}") from error
            if not isinstance(record, dict):
                raise TraceConversionError(f"{path}:{line_number}: expected a JSON object")
            records.append(record)
    return records


def convert_records(records: list[dict[str, Any]], profile_name: str) -> dict[str, Any]:
    """Convert Daft tracing records into a SpeedScope file-format object."""
    frame_names: list[str] = []
    frame_indexes: dict[str, int] = {}
    events: list[dict[str, int | float | str]] = []
    stack: list[str] = []
    start: datetime | None = None
    previous_timestamp: datetime | None = None
    span_event_number = 0

    for record in records:
        extracted = _extract_span_event(record)
        if extracted is None:
            continue

        span_event_number += 1
        message, name, ancestors = extracted
        timestamp = _parse_timestamp(record.get("timestamp"), span_event_number)

        if previous_timestamp is not None and timestamp < previous_timestamp:
            raise TraceConversionError(f"Span event {span_event_number} is earlier than the preceding event")
        previous_timestamp = timestamp
        if start is None:
            start = timestamp

        if message == "new":
            if ancestors != stack and ancestors == [*stack, name]:
                ancestors.pop()
            if ancestors != stack:
                raise TraceConversionError(
                    f"Span event {span_event_number} opens {name!r} beneath {ancestors!r}, "
                    f"but the active stack is {stack!r}. The trace may contain concurrent spans."
                )
            stack.append(name)
            event_type = "O"
        else:
            expected_ancestors = stack[:-1]
            if ancestors != expected_ancestors and ancestors == [*expected_ancestors, name]:
                ancestors.pop()
            if not stack or stack[-1] != name or ancestors != stack[:-1]:
                raise TraceConversionError(
                    f"Span event {span_event_number} closes {name!r} beneath {ancestors!r}, "
                    f"but the active stack is {stack!r}. The trace may contain concurrent spans."
                )
            stack.pop()
            event_type = "C"

        if name not in frame_indexes:
            frame_indexes[name] = len(frame_names)
            frame_names.append(name)

        events.append(
            {
                "type": event_type,
                "at": (timestamp - start).total_seconds() * 1_000_000,
                "frame": frame_indexes[name],
            }
        )

    if not events:
        raise TraceConversionError("Trace contains no span lifecycle events")
    if stack:
        raise TraceConversionError(f"Trace ended with unclosed spans: {stack!r}")

    end_value = float(events[-1]["at"])
    return {
        "$schema": SPEEDSCOPE_SCHEMA,
        "name": profile_name,
        "activeProfileIndex": 0,
        "exporter": "Daft JSON tracing to SpeedScope",
        "shared": {"frames": [{"name": name} for name in frame_names]},
        "profiles": [
            {
                "type": "evented",
                "name": profile_name,
                "unit": "microseconds",
                "startValue": 0,
                "endValue": end_value,
                "events": events,
            }
        ],
    }


def convert_file(source: Path, destination: Path, profile_name: str) -> None:
    """Convert a Daft JSONL trace file and write a SpeedScope JSON file."""
    profile = convert_records(load_json_lines(source), profile_name)
    destination.write_text(json.dumps(profile, indent=2) + "\n")


def _default_output_path(source: Path) -> Path:
    return source.with_suffix(".speedscope.json")


def main(argv: list[str] | None = None) -> int:
    """Convert TRACE.jsonl to TRACE.speedscope.json from the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path, help="Daft newline-delimited JSON trace")
    parser.add_argument("-o", "--output", type=Path, help="Output path (default: TRACE.speedscope.json)")
    parser.add_argument("--name", help="Profile name shown in SpeedScope")
    args = parser.parse_args(argv)

    output = args.output or _default_output_path(args.trace)
    profile_name = args.name or args.trace.stem
    try:
        convert_file(args.trace, output, profile_name)
    except (OSError, TraceConversionError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
