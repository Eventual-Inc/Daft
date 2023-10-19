from __future__ import annotations

from typing import TYPE_CHECKING

from daft.context import get_context
from daft.daft import FileFormatConfig, StorageConfig
from daft.datatype import DataType
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema

if TYPE_CHECKING:
    import fsspec


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
    fs: fsspec.AbstractFileSystem | None = None,
) -> LogicalPlanBuilder:
    """Returns a TabularFilesScan LogicalPlan for a given glob filepath."""
    paths = path if isinstance(path, list) else [str(path)]
    schema_hint = _get_schema_from_hints(schema_hints) if schema_hints is not None else None
    # Glob the path using the Runner
    runner_io = get_context().runner().runner_io()
    file_infos = runner_io.glob_paths_details(paths, file_format_config, fs=fs, storage_config=storage_config)

    # Infer schema if no hints provided
    inferred_or_provided_schema = (
        schema_hint
        if schema_hint is not None
        else runner_io.get_schema_from_first_filepath(file_infos, file_format_config, storage_config)
    )
    # Construct plan
    builder_cls = get_context().logical_plan_builder_class()
    builder = builder_cls.from_tabular_scan(
        file_infos=file_infos,
        schema=inferred_or_provided_schema,
        file_format_config=file_format_config,
        storage_config=storage_config,
    )
    return builder
