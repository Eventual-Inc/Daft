from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import (
    FileFormatConfig,
    NativeStorageConfig,
    PythonStorageConfig,
    ScanOperatorHandle,
    StorageConfig,
)
from daft.datatype import DataType
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema

if TYPE_CHECKING:
    pass


def _get_schema_from_hints(hints: dict[str, DataType]) -> Schema:
    if isinstance(hints, dict):
        return Schema._from_field_name_and_types([(fname, dtype) for fname, dtype in hints.items()])
    else:
        raise NotImplementedError(f"Unsupported schema hints: {type(hints)}")


def _get_tabular_files_scan(
    path: str | list[str],
    schema_hints: dict[str, DataType] | None,
    file_format_config: FileFormatConfig,
    storage_config: StorageConfig,
) -> LogicalPlanBuilder:
    """Returns a TabularFilesScan LogicalPlan for a given glob filepath."""
    # Glob the path using the Runner
    # NOTE: Globbing will always need the IOConfig, regardless of whether "native reads" are used
    if isinstance(storage_config.config, NativeStorageConfig):
        storage_config.config.io_config
    elif isinstance(storage_config.config, PythonStorageConfig):
        storage_config.config.io_config
    else:
        raise NotImplementedError(f"Tabular scan with config not implemented: {storage_config.config}")

    schema_hint = _get_schema_from_hints(schema_hints) if schema_hints is not None else None

    scan_op: ScanOperatorHandle
    if isinstance(path, list):
        scan_op = ScanOperatorHandle.glob_scan(
            path,
            file_format_config,
            storage_config,
            schema_hint=schema_hint._schema if schema_hint is not None else None,
        )
    elif isinstance(path, str):
        scan_op = ScanOperatorHandle.glob_scan(
            [path],
            file_format_config,
            storage_config,
            schema_hint=schema_hint._schema if schema_hint is not None else None,
        )
    else:
        raise NotImplementedError(f"_get_tabular_files_scan cannot construct ScanOperatorHandle for input: {path}")

    builder = LogicalPlanBuilder.from_tabular_scan_with_scan_operator(
        scan_operator=scan_op,
    )
    return builder
