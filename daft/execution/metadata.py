from __future__ import annotations

import json
import os
import time
from datetime import timedelta
from typing import Any, TypeAlias, TypedDict

from daft.daft import PyExecutionMetadata as _PyExecutionMetadata
from daft.recordbatch.recordbatch import RecordBatch


class Metric(TypedDict):
    value: int | float
    unit: str | None


Metrics: TypeAlias = list[tuple[str, Metric]]
PlanNode: TypeAlias = dict[str, Any]


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
    _py: _PyExecutionMetadata

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating an ExecutionMetadata via __init__")

    @staticmethod
    def _from_py_execution_metadata(py_meta: _PyExecutionMetadata) -> ExecutionMetadata:
        assert isinstance(py_meta, _PyExecutionMetadata)
        meta = ExecutionMetadata.__new__(ExecutionMetadata)
        meta._py = py_meta
        return meta

    @property
    def query_id(self) -> str:
        return self._py.query_id

    @property
    def query_plan(self) -> PlanNode:
        plan_str = self._py.query_plan
        assert plan_str is not None
        return json.loads(plan_str)

    def encode(self) -> bytes:
        return self._py.encode()

    def to_recordbatch(self) -> RecordBatch:
        return RecordBatch._from_pyrecordbatch(self._py.to_recordbatch())

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

        mermaid = self._plan_to_mermaid_string()
        timestamp_ms = int(time.time() * 1000)
        path = f"explain-analyze-{timestamp_ms}-mermaid.md"
        with open(path, "w") as f:
            f.write("```mermaid\n")
            f.write(mermaid)
            f.write("\n```\n")
