from __future__ import annotations

import fsspec

from daft.context import get_context
from daft.daft import FileFormatConfig, LogicalPlanBuilder
from daft.datatype import DataType
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema


def _get_schema_from_hints(hints: dict[str, DataType]) -> Schema:
    if isinstance(hints, dict):
        return Schema._from_field_name_and_types([(fname, dtype) for fname, dtype in hints.items()])
    else:
        raise NotImplementedError(f"Unsupported schema hints: {type(hints)}")


def _get_tabular_files_scan(
    path: str | list[str],
    schema_hints: dict[str, DataType] | None,
    file_format_config: FileFormatConfig,
    fs: fsspec.AbstractFileSystem | None,
) -> LogicalPlanBuilder:
    """Returns a TabularFilesScan LogicalPlan for a given glob filepath."""
    paths = path if isinstance(path, list) else [str(path)]
    schema_hint = _get_schema_from_hints(schema_hints) if schema_hints is not None else None
    # Construct plan
    builder_cls = get_context().logical_plan_builder_class()
    builder = builder_cls.from_tabular_scan(
        paths=paths,
        schema_hint=schema_hint,
        file_format_config=file_format_config,
        fs=fs,
    )
    return builder
