"""Lineage introspection for Daft DataFrames.

This module provides structured lineage information from a Daft DataFrame's
logical plan. When ``openlineage-python`` is installed, it returns native
OpenLineage objects (``Dataset``, ``Run``, ``Job``). Otherwise, it falls
back to plain dicts with the same structure.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.dataframe import DataFrame

# Try to import OpenLineage SDK
try:
    from openlineage.client.event_v2 import Dataset, Job, Run
    from openlineage.client.facet_v2 import datasource_dataset, schema_dataset
    from openlineage.client.uuid import generate_new_uuid

    _HAS_OL_SDK = True
except ImportError:
    _HAS_OL_SDK = False


def build_lineage(df: DataFrame) -> dict[str, Any]:
    """Extract OpenLineage-compatible lineage information from a DataFrame.

    Parses the logical plan JSON (with schema information) to identify
    input and output datasets, their schemas, and source metadata.

    Args:
        df: A Daft DataFrame.

    Returns:
        A dictionary with keys ``inputs``, ``outputs``, ``run``, and ``job``.
        When ``openlineage-python`` is installed, values are native OL objects.
        Otherwise, plain dicts with the same field structure.
    """
    plan_json_str = df._builder.repr_json(include_schema=True)
    plan = json.loads(plan_json_str)

    inputs_raw = _collect_inputs(plan)
    outputs_raw = _collect_outputs(plan)

    if _HAS_OL_SDK:
        return {
            "inputs": [_to_ol_dataset(d) for d in inputs_raw],
            "outputs": [_to_ol_dataset(d) for d in outputs_raw],
            "run": Run(runId=str(generate_new_uuid())),
            "job": Job(namespace="daft", name="daft_query"),
        }

    run_id = str(uuid.uuid4())
    return {
        "inputs": inputs_raw,
        "outputs": outputs_raw,
        "run": run_id,
        "job": {
            "namespace": "daft",
            "name": "daft_query",
        },
    }


# ---------------------------------------------------------------------------
# OL SDK conversion
# ---------------------------------------------------------------------------


def _to_ol_dataset(d: dict[str, Any]) -> Dataset:
    """Convert a plain dict dataset to an OpenLineage Dataset object."""
    facets: dict[str, Any] = {}

    raw_schema = d.get("facets", {}).get("schema", {})
    if raw_schema.get("fields"):
        facets["schema"] = schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(
                    name=f["name"],
                    type=f["type"],
                    description="",
                )
                for f in raw_schema["fields"]
            ]
        )

    raw_ds = d.get("facets", {}).get("dataSource", {})
    if raw_ds:
        facets["dataSource"] = datasource_dataset.DatasourceDatasetFacet(
            name=raw_ds.get("name", ""),
            uri=raw_ds.get("uri", ""),
        )

    return Dataset(
        namespace=d.get("namespace", "daft"),
        name=d.get("name", ""),
        facets=facets,
    )


# ---------------------------------------------------------------------------
# Plan traversal
# ---------------------------------------------------------------------------


def _collect_inputs(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Traverse the plan tree and collect input datasets from Source nodes."""
    inputs: list[dict[str, Any]] = []
    _walk_plan(plan, _input_visitor, inputs)
    return inputs


def _collect_outputs(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Traverse the plan tree and collect output datasets from Sink nodes."""
    outputs: list[dict[str, Any]] = []
    _walk_plan(plan, _output_visitor, outputs)
    return outputs


def _walk_plan(
    node: dict[str, Any],
    visitor_fn: Callable[[dict[str, Any], list[dict[str, Any]]], None],
    result_list: list[dict[str, Any]],
) -> None:
    """Depth-first traversal of the plan tree."""
    visitor_fn(node, result_list)
    for child in node.get("children", []):
        _walk_plan(child, visitor_fn, result_list)


def _input_visitor(node: dict[str, Any], inputs: list[dict[str, Any]]) -> None:
    """Handle Source nodes as input datasets."""
    if node.get("type") != "Source":
        return

    source_type = node.get("source_type", "Unknown")
    dataset = _build_dataset_from_source(node, source_type)
    if dataset is not None:
        inputs.append(dataset)


def _output_visitor(node: dict[str, Any], outputs: list[dict[str, Any]]) -> None:
    """Handle Sink nodes as output datasets."""
    if node.get("type") != "Sink":
        return

    sink_type = node.get("sink_type", "Unknown")
    dataset = _build_dataset_from_sink(node, sink_type)
    if dataset is not None:
        outputs.append(dataset)


# ---------------------------------------------------------------------------
# Source / Sink builders
# ---------------------------------------------------------------------------


def _build_dataset_from_source(node: dict[str, Any], source_type: str) -> dict[str, Any] | None:
    """Build an OpenLineage-compatible dataset dict from a Source node."""
    schema = _extract_schema_facets(node.get("schema", []))

    if source_type in ("Physical", "GlobScan"):
        paths = node.get("file_paths", [])
        if paths:
            name = _common_path_prefix(paths) if len(paths) > 1 else paths[0]
        else:
            # Extract path from operator_info (pre-materialization)
            name = _extract_path_from_operator_info(
                node.get("operator_info", []),
                node.get("glob_paths", []),
            )
        namespace = _infer_namespace(name)

        return {
            "namespace": namespace,
            "name": name,
            "facets": {
                "schema": schema,
                "dataSource": {"name": name, "uri": name},
            },
        }

    if source_type == "InMemory":
        return None

    if source_type == "PlaceHolder":
        return {
            "namespace": "daft",
            "name": "placeholder",
            "facets": {
                "schema": schema,
                "dataSource": {"name": "placeholder", "uri": "placeholder://"},
            },
        }

    return None


def _extract_path_from_operator_info(
    operator_info: list[str],
    glob_paths: list[str],
) -> str:
    """Extract a dataset name from operator_info or glob_paths."""
    # Prefer glob_paths if available
    if glob_paths:
        return _common_path_prefix(glob_paths) if len(glob_paths) > 1 else glob_paths[0]

    # Parse operator_info lines for path patterns
    for line in operator_info:
        # Match "Glob paths = [.../*.parquet]" or "File paths = [...]"
        m = re.search(r"(?:Glob|File) paths?\s*=\s*\[([^\]]+)\]", line)
        if m:
            paths_str = m.group(1)
            paths = [p.strip() for p in paths_str.split(",")]
            if paths:
                return _common_path_prefix(paths) if len(paths) > 1 else paths[0]

    return "unknown_source"


def _build_dataset_from_sink(node: dict[str, Any], sink_type: str) -> dict[str, Any] | None:
    """Build an OpenLineage-compatible dataset dict from a Sink node."""
    schema = _extract_schema_facets(node.get("schema", []))

    if sink_type == "OutputFile":
        root_dir = node.get("root_dir", "")
        file_format = node.get("file_format", "")
        return {
            "namespace": _infer_namespace(root_dir),
            "name": root_dir,
            "facets": {
                "schema": schema,
                "dataSource": {"name": root_dir, "uri": root_dir},
                "outputStatistics": {"fileFormat": file_format},
            },
        }

    if sink_type == "Catalog":
        catalog_type = node.get("catalog_type", "")
        table_name = node.get("table_name", "") or node.get("path", "")
        table_location = node.get("table_location", "")
        name = table_name or table_location
        return {
            "namespace": catalog_type.lower() if catalog_type else "daft",
            "name": name,
            "facets": {
                "schema": schema,
                "dataSource": {"name": name, "uri": table_location or name},
            },
        }

    if sink_type == "DataSink":
        name = node.get("name", "custom_sink")
        return {
            "namespace": "daft",
            "name": name,
            "facets": {
                "schema": schema,
                "dataSource": {"name": name, "uri": name},
            },
        }

    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_schema_facets(schema_fields: list[dict[str, Any]]) -> dict[str, Any]:
    """Convert Daft schema fields to OpenLineage SchemaDatasetFacet format."""
    fields = []
    for field in schema_fields:
        fields.append(
            {
                "name": field.get("name", ""),
                "type": field.get("dtype", "Unknown"),
            }
        )
    return {"fields": fields}


def _common_path_prefix(paths: list[str]) -> str:
    """Find the common path prefix of a list of file paths."""
    if not paths:
        return ""
    if len(paths) == 1:
        return paths[0]

    common = os.path.commonpath(paths)
    # Ensure trailing separator to distinguish directory from file
    if not common.endswith(os.sep) and any(p != common for p in paths):
        common += os.sep
    return common


def _infer_namespace(uri: str) -> str:
    """Infer the OpenLineage namespace from a URI string."""
    if "://" in uri:
        scheme, rest = uri.split("://", 1)
        if "/" in rest:
            authority = rest.split("/", 1)[0]
            return f"{scheme}://{authority}"
        return f"{scheme}://{rest}"
    return "file"
