from __future__ import annotations

import json
import os
import time
import warnings
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, TypeAlias, TypedDict

from daft.daft import PyExecutionStats as _PyExecutionStats
from daft.recordbatch.recordbatch import RecordBatch


class Metric(TypedDict):
    value: int | float
    unit: str | None


Metrics: TypeAlias = list[tuple[str, Metric]]
PlanNode: TypeAlias = dict[str, Any]

_LARGE_SHUFFLE_BYTES = 1024**3
_PUSHDOWN_SCAN_BYTES = 256 * 1024**2
_SKEW_MIN_PARTITIONS = 4
_SKEW_MIN_ROWS = 100_000
_SKEW_RATIO = 4.0
_DOMINANT_OPERATOR_RATIO = 0.8


@dataclass(frozen=True)
class _ProfileNode:
    node_id: int
    name: str
    node_type: str
    category: str
    duration_us: float | None
    rows_in: float | None
    rows_out: float | None
    bytes_read: float | None
    bytes_in: float | None
    bytes_out: float | None


def _format_duration(duration_us: float | None) -> str:
    if duration_us is None:
        return "N/A"
    if duration_us < 1_000:
        return f"{duration_us:.0f} us"
    if duration_us < 1_000_000:
        return f"{duration_us / 1_000:.2f} ms"
    return f"{duration_us / 1_000_000:.2f} s"


def _format_count(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{int(value):,}"


def _format_bytes(value: float | None) -> str:
    if value is None:
        return "N/A"
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    current = float(value)
    unit = units[0]
    for unit in units:
        if abs(current) < 1024 or unit == units[-1]:
            break
        current /= 1024
    return f"{int(current)} {unit}" if unit == "B" else f"{current:.2f} {unit}"


def _first_metric(stats: Metrics, name: str) -> Metric | None:
    """Return the built-in metric emitted before same-named UDF counters."""
    return next((metric for metric_name, metric in stats if metric_name == name), None)


def _metric_value(stats: Metrics, name: str) -> float | None:
    metric = _first_metric(stats, name)
    return None if metric is None else float(metric["value"])


def _metric_duration_us(stats: Metrics) -> float | None:
    metric = _first_metric(stats, "duration")
    if metric is None:
        return None
    value = float(metric["value"])
    unit = metric["unit"]
    if unit in ("us", "µs"):
        return value
    if unit == "ms":
        return value * 1_000
    if unit == "s":
        return value * 1_000_000
    raise ValueError(f"Unknown duration unit: {unit!r}")


def _profile_warnings(
    nodes: list[_ProfileNode],
    total_operator_us: float,
    telemetry: dict[str, Any],
) -> list[str]:
    """Return deterministic warnings derived from a completed query profile."""
    warnings_out: list[str] = []
    if not nodes:
        return ["No execution metrics were recorded."]
    timed_nodes = [node for node in nodes if node.duration_us is not None]
    if total_operator_us <= 0 or not timed_nodes:
        warnings_out.append("No operator duration metrics were recorded.")
    else:
        slowest = max(timed_nodes, key=lambda node: node.duration_us or 0)
        slowest_duration = slowest.duration_us
        if slowest_duration is not None and slowest_duration / total_operator_us >= _DOMINANT_OPERATOR_RATIO:
            warnings_out.append(f"Most operator CPU time was spent in {slowest.name}.")

    shuffle_types = {"Gather", "IntoPartitions", "RandomShuffle", "Repartition"}
    for node in nodes:
        shuffle_bytes = (node.bytes_in or 0) + (node.bytes_out or 0)
        if node.node_type in shuffle_types and shuffle_bytes >= _LARGE_SHUFFLE_BYTES:
            warnings_out.append(f"Large shuffle in {node.name}: {_format_bytes(shuffle_bytes)} of logical I/O.")

    spilled_bytes = telemetry.get("spilled_bytes")
    if spilled_bytes:
        warnings_out.append(f"Flight shuffle spilled {_format_bytes(spilled_bytes)} to disk.")

    partition_stats = telemetry.get("partition_stats", {})
    for node in nodes:
        stats = partition_stats.get(str(node.node_id), partition_stats.get(node.node_id))
        if not stats:
            continue
        count = int(stats.get("count", 0))
        total_rows = int(stats.get("total_rows", 0))
        max_rows = int(stats.get("max_rows", 0))
        mean = total_rows / count if count else 0
        if count >= _SKEW_MIN_PARTITIONS and total_rows >= _SKEW_MIN_ROWS and mean and max_rows >= _SKEW_RATIO * mean:
            warnings_out.append(
                f"Heavy partition skew in {node.name}: largest partition has {_format_count(max_rows)} rows "
                f"({max_rows / mean:.1f}x the mean)."
            )

    pushdowns = telemetry.get("scan_pushdowns", {})
    for node in nodes:
        if (node.bytes_read or 0) < _PUSHDOWN_SCAN_BYTES:
            continue
        state = pushdowns.get(str(node.node_id), pushdowns.get(node.node_id))
        if not state:
            continue
        if state.get("filter_requested") and not state.get("filter_applied"):
            warnings_out.append(f"Filter pushdown was not applied for {node.name}.")
        if state.get("projection_requested") and not state.get("projection_applied"):
            warnings_out.append(f"Projection pushdown was not applied for {node.name}.")
    return warnings_out


def _traverse_plan(plan: PlanNode, metrics: dict[int, Metrics]) -> tuple[list[str], dict[int, int]]:
    """Convert query_plan tree (dict with id, name, optional type, category, children) to mermaid nodes and edges.

    Returns (node_lines, edges) for bottom-up: edges are (source, target).
    """
    nodes_out: list[str] = []
    edges_out: dict[int, int] = {}

    def visit(node: PlanNode) -> int:
        node_id = int(node["id"])
        node_name = str(node["name"]).replace('"', "'")
        node_type = str(node["type"])

        if node_type in node_name:
            title = node_name
        else:
            title = f"{node_name} ({node_type})"
        parts = [title]

        if "approx_stats" in node:
            parts.append(f"Stats = {node['approx_stats']}")
        for name, entry in metrics[node_id]:
            name = name.capitalize()
            value = entry["value"]
            unit = entry["unit"]
            if unit is None:
                parts.append(f"{name} = {value}")
            elif unit == "us" or unit == "µs":
                parts.append(f"{name} = {timedelta(microseconds=value).total_seconds()}s")
            else:
                parts.append(f"{name} = {value} {unit}")

        display = "\n".join(parts)
        nodes_out.append(f'{node_id}["`{display}`"]')

        children = node.get("children")
        if isinstance(children, list):
            for child in children:
                child_id = visit(child)
                edges_out[child_id] = node_id

        return node_id

    _ = visit(plan)
    return nodes_out, edges_out


class ExecutionMetadata:
    _py: _PyExecutionStats
    _query_id: str
    _query_plan: str
    _wall_time_us: float | None

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating ExecutionMetadata via __init__")

    @staticmethod
    def _from_runner_output(
        py_stats: _PyExecutionStats,
        query_id: str,
        query_plan: str,
        wall_time_us: float | None = None,
    ) -> ExecutionMetadata:
        assert isinstance(py_stats, _PyExecutionStats)
        meta = ExecutionMetadata.__new__(ExecutionMetadata)
        meta._py = py_stats
        meta._query_id = query_id
        meta._query_plan = query_plan
        meta._wall_time_us = wall_time_us
        return meta

    @property
    def query_id(self) -> str:
        return self._query_id

    @property
    def query_plan(self) -> PlanNode:
        return json.loads(self._query_plan)

    def encode(self) -> bytes:
        return self._py.encode()

    def to_recordbatch(self) -> RecordBatch:
        return RecordBatch._from_pyrecordbatch(self._py.to_recordbatch())

    @property
    def _profile_telemetry(self) -> dict[str, Any]:
        telemetry = getattr(self._py, "profile_telemetry", None)
        result = {} if telemetry is None else dict(telemetry)
        pushdowns = {key: dict(value) for key, value in result.get("scan_pushdowns", {}).items()}
        known_sources = {int(node_id) for node_id in pushdowns}

        def visit(node: PlanNode) -> set[int]:
            node_id = int(node["id"])
            node_type = str(node.get("type", "")).lower()
            category = str(node.get("category", "")).lower()
            children = node.get("children") or []
            source_ids = (
                {node_id}
                if node_id in known_sources
                or category == "source"
                or node_type in {"globscan", "inmemoryscan", "scantask"}
                else set()
            )
            for child in children:
                source_ids.update(visit(child))
            if node_type == "filter":
                for source_id in source_ids:
                    pushdowns.setdefault(source_id, {})["filter_requested"] = True
            elif node_type == "project":
                for source_id in source_ids:
                    pushdowns.setdefault(source_id, {})["projection_requested"] = True
            return source_ids

        visit(self.query_plan)
        result["scan_pushdowns"] = pushdowns
        return result

    def _profile_nodes(self) -> list[_ProfileNode]:
        nodes = []
        for row in self.to_recordbatch().to_pylist():
            stats: Metrics = row["stats"]
            nodes.append(
                _ProfileNode(
                    node_id=int(row["id"]),
                    name=str(row["name"]),
                    node_type=str(row["type"]),
                    category=str(row["category"]),
                    duration_us=_metric_duration_us(stats),
                    rows_in=_metric_value(stats, "rows.in"),
                    rows_out=_metric_value(stats, "rows.out"),
                    bytes_read=_metric_value(stats, "bytes.read"),
                    bytes_in=_metric_value(stats, "bytes.in"),
                    bytes_out=_metric_value(stats, "bytes.out"),
                )
            )
        return nodes

    def _format_profile(self, top_n: int = 5) -> str:
        """Return a concise human-readable profile for a completed query."""
        nodes = self._profile_nodes()
        duration_values = [node.duration_us for node in nodes if node.duration_us is not None]
        total_operator_us = sum(duration_values)
        source_nodes = [node for node in nodes if node.category.lower() == "source" or node.bytes_read is not None]
        rows_read_values = [node.rows_out for node in source_nodes if node.rows_out is not None]
        bytes_read_values = [node.bytes_read for node in source_nodes if node.bytes_read is not None]
        rows_read = sum(rows_read_values) if rows_read_values else None
        bytes_scanned = sum(bytes_read_values) if bytes_read_values else None
        telemetry = self._profile_telemetry

        lines = [
            "== Query Profile ==",
            "",
            f"Query ID: {self.query_id}",
            f"Wall time: {_format_duration(self._wall_time_us)}",
            f"Operator CPU time: {_format_duration(total_operator_us if duration_values else None)} "
            "(summed across operators; may exceed wall time)",
            f"Rows read: {_format_count(rows_read)}",
            f"Bytes scanned: {_format_bytes(bytes_scanned)}",
            f"Peak sampled process RSS: {_format_bytes(telemetry.get('peak_process_rss_bytes'))}",
            "",
            "Slowest operators:",
        ]

        for index, node in enumerate(
            sorted(nodes, key=lambda item: (-(item.duration_us or 0), item.node_id))[:top_n], start=1
        ):
            percentage = (
                f"{100 * node.duration_us / total_operator_us:.1f}%"
                if node.duration_us is not None and total_operator_us
                else "N/A"
            )
            byte_details = (
                f"bytes read: {_format_bytes(node.bytes_read)}"
                if node.bytes_read is not None
                else f"bytes in/out: {_format_bytes(node.bytes_in)} / {_format_bytes(node.bytes_out)}"
            )
            lines.append(
                f"{index}. {node.name} ({node.node_type}) - {_format_duration(node.duration_us)} "
                f"({percentage} CPU), rows in/out: {_format_count(node.rows_in)} / {_format_count(node.rows_out)}, "
                f"{byte_details}"
            )

        warnings_out = _profile_warnings(nodes, total_operator_us, telemetry)
        lines.extend(["", "Warnings:"])
        lines.extend(f"- {warning}" for warning in warnings_out or ["None"])
        return "\n".join(lines)

    @property
    def skipped_corrupt_files(self) -> list[tuple[str, str, bool]]:
        """Files skipped during execution due to ignore_corrupt_files=True.

        Returns a list of (path, reason, partial) tuples. ``partial=True`` means
        some batches were already emitted before corruption was detected; the file
        was not fully skipped.
        """
        return self._py.skipped_corrupt_files

    def _plan_to_mermaid_string(self) -> str:
        """Convert query_plan dict to mermaid diagram string (bottom-up)."""
        metrics = {int(item["id"]): item["stats"] for item in self.to_recordbatch().to_pylist()}
        nodes, edges = _traverse_plan(self.query_plan, metrics)

        return """\
---
query_id: {}
---

flowchart TB
{}

{}
""".format(self.query_id, "\n".join(nodes), "\n".join(f"{source} --> {target}" for source, target in edges.items()))

    def write_mermaid(self) -> None:
        """Generate mermaid from query_plan and write to a file.

        Only when DAFT_DEV_ENABLE_EXPLAIN_ANALYZE is enabled.
        """
        raw = os.environ.get("DAFT_DEV_ENABLE_EXPLAIN_ANALYZE", "")
        if not raw or raw.strip().lower() not in ("1", "true"):
            return
        warnings.warn(
            "DAFT_DEV_ENABLE_EXPLAIN_ANALYZE is an experimental feature and may change in the future", RuntimeWarning
        )

        mermaid = self._plan_to_mermaid_string()
        timestamp_ms = int(time.time() * 1000)
        path = f"explain-analyze-{timestamp_ms}-mermaid.md"
        with open(path, "w") as f:
            f.write("```mermaid\n")
            f.write(mermaid)
            f.write("\n```\n")
