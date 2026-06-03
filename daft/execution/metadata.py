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


@dataclass(frozen=True)
class _ProfileNode:
    name: str
    node_type: str
    category: str
    duration_us: float
    rows_in: float | None
    rows_out: float | None
    bytes_read: float | None
    bytes_in: float | None
    bytes_out: float | None


def _format_duration(duration_us: float) -> str:
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

    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    current = float(value)
    unit = units[0]
    for unit in units:
        if abs(current) < 1024 or unit == units[-1]:
            break
        current /= 1024

    if unit == "B":
        return f"{int(current)} {unit}"
    return f"{current:.2f} {unit}"


def _stats_to_dict(stats: Metrics) -> dict[str, Metric]:
    return {name: metric for name, metric in stats}


def _metric_value(stats: dict[str, Metric], name: str) -> float | None:
    metric = stats.get(name)
    if metric is None:
        return None
    return float(metric["value"])


def _metric_duration_us(stats: dict[str, Metric]) -> float:
    metric = stats.get("duration")
    if metric is None:
        return 0

    value = float(metric["value"])
    unit = metric["unit"]
    if unit in ("us", "µs"):
        return value
    if unit == "ms":
        return value * 1_000
    if unit == "s":
        return value * 1_000_000
    return value


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

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating ExecutionMetadata via __init__")

    @staticmethod
    def _from_runner_output(
        py_stats: _PyExecutionStats,
        query_id: str,
        query_plan: str,
    ) -> ExecutionMetadata:
        assert isinstance(py_stats, _PyExecutionStats)
        meta = ExecutionMetadata.__new__(ExecutionMetadata)
        meta._py = py_stats
        meta._query_id = query_id
        meta._query_plan = query_plan
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

    def _profile_nodes(self) -> list[_ProfileNode]:
        profile_nodes = []
        for row in self.to_recordbatch().to_pylist():
            stats = _stats_to_dict(row["stats"])
            profile_nodes.append(
                _ProfileNode(
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
        return profile_nodes

    def format_profile(self, top_n: int = 5) -> str:
        """Return a concise query profile summary from execution metrics."""
        nodes = self._profile_nodes()
        # total_operator_us is the sum of duration_us across all nodes, which may be less than total query duration
        # due to missing metrics or unaccounted time (e.g. scheduling, network).
        total_operator_us = sum(node.duration_us for node in nodes)
        source_nodes = [node for node in nodes if node.category.lower() == "source" or node.bytes_read is not None]
        rows_read = sum(node.rows_out or 0 for node in source_nodes)
        bytes_scanned = sum(node.bytes_read or 0 for node in nodes)
        slowest_nodes = sorted(nodes, key=lambda node: node.duration_us, reverse=True)[:top_n]

        lines = [
            "== Query Profile ==",
            "",
            f"Query ID: {self.query_id}",
            f"Total operator time: {_format_duration(total_operator_us)}",
            f"Rows read: {_format_count(rows_read)}",
            f"Bytes scanned: {_format_bytes(bytes_scanned)}",
            "",
            "Slowest operators:",
        ]

        if slowest_nodes:
            for index, node in enumerate(slowest_nodes, start=1):
                details = [
                    _format_duration(node.duration_us),
                    f"rows in: {_format_count(node.rows_in)}",
                    f"rows out: {_format_count(node.rows_out)}",
                ]
                if node.bytes_read is not None:
                    details.append(f"bytes read: {_format_bytes(node.bytes_read)}")
                elif node.bytes_in is not None or node.bytes_out is not None:
                    details.append(f"bytes in/out: {_format_bytes(node.bytes_in)} / {_format_bytes(node.bytes_out)}")
                lines.append(f"{index}. {node.name} ({node.node_type}) - {', '.join(details)}")
        else:
            lines.append("No operator metrics were recorded.")

        warnings_out = []
        if not nodes:
            warnings_out.append("No execution metrics were recorded.")
        elif total_operator_us <= 0:
            warnings_out.append("No operator duration metrics were recorded.")
        elif len(nodes) > 1 and slowest_nodes and slowest_nodes[0].duration_us / total_operator_us >= 0.8:
            warnings_out.append(f"Most operator time was spent in {slowest_nodes[0].name}.")

        if warnings_out:
            lines.extend(["", "Warnings:"])
            lines.extend(f"- {warning}" for warning in warnings_out)

        return "\n".join(lines)

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
