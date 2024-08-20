from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import FileFormatConfig, ScanOperatorHandle, StorageConfig
from daft.datatype import DataType
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema

if TYPE_CHECKING:
    pass


def _get_schema_from_dict(fields: dict[str, DataType]) -> Schema:
    if isinstance(fields, dict):
        return Schema._from_field_name_and_types([(fname, dtype) for fname, dtype in fields.items()])
    else:
        raise NotImplementedError(f"Unsupported schema fields: {type(fields)}")


def get_tabular_files_scan(
    path: str | list[str],
    infer_schema: bool,
    schema: dict[str, DataType] | None,
    file_format_config: FileFormatConfig,
    storage_config: StorageConfig,
    is_ray_runner: bool,
) -> LogicalPlanBuilder:
    """Returns a TabularFilesScan LogicalPlan for a given glob filepath."""
    # Glob the path using the Runner

    if isinstance(path, list):
        paths = path
    elif isinstance(path, str):
        paths = [path]
    else:
        raise NotImplementedError(f"get_tabular_files_scan cannot construct ScanOperatorHandle for input: {path}")

    scan_op = ScanOperatorHandle.glob_scan(
        paths,
        file_format_config,
        storage_config,
        infer_schema=infer_schema,
        schema=_get_schema_from_dict(schema)._schema if schema is not None else None,
        is_ray_runner=is_ray_runner,
    )

    builder = LogicalPlanBuilder.from_tabular_scan(
        scan_operator=scan_op,
    )
    return builder
