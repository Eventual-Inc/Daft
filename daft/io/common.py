from __future__ import annotations

import os
from typing import TYPE_CHECKING

from daft.context import get_context
from daft.daft import (
    CsvSourceConfig,
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
    io_config = None
    if isinstance(storage_config.config, NativeStorageConfig):
        io_config = storage_config.config.io_config
    elif isinstance(storage_config.config, PythonStorageConfig):
        io_config = storage_config.config.io_config
    else:
        raise NotImplementedError(f"Tabular scan with config not implemented: {storage_config.config}")

    schema_hint = _get_schema_from_hints(schema_hints) if schema_hints is not None else None

    ### FEATURE_FLAG: $DAFT_MICROPARTITIONS
    #
    # This environment variable will make Daft use the new "v2 scans" and MicroPartitions when building Daft logical plans
    if os.getenv("DAFT_MICROPARTITIONS", "0") == "1":
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

    paths = path if isinstance(path, list) else [str(path)]
    runner_io = get_context().runner().runner_io()
    file_infos = runner_io.glob_paths_details(paths, file_format_config=file_format_config, io_config=io_config)

    # Infer schema
    schema = runner_io.get_schema_from_first_filepath(file_infos, file_format_config, storage_config)

    # Apply hints from schema_hints if provided
    if schema_hint is not None:
        # If CSV and no headers, then use the schema hint as the schema
        if isinstance(file_format_config.config, CsvSourceConfig) and file_format_config.config.has_headers == False:
            if len(schema) != len(schema_hint):
                raise ValueError(
                    f"For CSV with no headers, number of columns in schema hint ({len(schema_hint)} columns were provided) must match number of columns in data: {len(schema)}."
                )
            schema = schema_hint
        else:
            schema = schema.apply_hints(schema_hint)
    # Construct plan
    builder = LogicalPlanBuilder.from_tabular_scan(
        file_infos=file_infos,
        schema=schema,
        file_format_config=file_format_config,
        storage_config=storage_config,
    )
    return builder
