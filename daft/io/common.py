from __future__ import annotations

import fsspec

from daft.context import get_context
from daft.daft import LogicalPlanBuilder
from daft.datasources import SourceInfo
from daft.datatype import DataType
from daft.logical import logical_plan, rust_logical_plan
from daft.logical.schema import Schema


def _get_schema_from_hints(hints: dict[str, DataType]) -> Schema:
    if isinstance(hints, dict):
        return Schema._from_field_name_and_types([(fname, dtype) for fname, dtype in hints.items()])
    else:
        raise NotImplementedError(f"Unsupported schema hints: {type(hints)}")


def _get_tabular_files_scan(
    path: str | list[str],
    schema_hints: dict[str, DataType] | None,
    source_info: SourceInfo,
    fs: fsspec.AbstractFileSystem | None,
) -> logical_plan.TabularFilesScan:
    """Returns a TabularFilesScan LogicalPlan for a given glob filepath."""
    # Glob the path using the Runner
    runner_io = get_context().runner().runner_io()

    paths = path if isinstance(path, list) else [str(path)]
    listing_details_partition_set = runner_io.glob_paths_details(paths, source_info, fs)

    # Infer schema if no hints provided
    inferred_or_provided_schema = (
        _get_schema_from_hints(schema_hints)
        if schema_hints is not None
        else runner_io.get_schema_from_first_filepath(listing_details_partition_set, source_info, fs)
    )

    # Construct plan
    cache_entry = get_context().runner().put_partition_set_into_cache(listing_details_partition_set)
    filepath_plan = logical_plan.InMemoryScan(
        cache_entry=cache_entry,
        schema=runner_io.FS_LISTING_SCHEMA,
        partition_spec=logical_plan.PartitionSpec(
            logical_plan.PartitionScheme.UNKNOWN, listing_details_partition_set.num_partitions()
        ),
    )
    return logical_plan.TabularFilesScan(
        schema=inferred_or_provided_schema,
        predicate=None,
        columns=None,
        source_info=source_info,
        fs=fs,
        filepaths_child=filepath_plan,
        filepaths_column_name=runner_io.FS_LISTING_PATH_COLUMN_NAME,
        # WARNING: This is currently hardcoded to be the same number of partitions as rows!! This is because we emit
        # one partition per filepath. This will change in the future and our logic here should change accordingly.
        num_partitions=len(listing_details_partition_set),
    )


def _get_files_scan_rustplan(
    path: str | list[str],
    schema_hints: dict[str, DataType] | None,
    source_info: SourceInfo,
    fs: fsspec.AbstractFileSystem | None,
) -> rust_logical_plan.RustLogicalPlanBuilder:
    """Returns a LogicalPlanBuilder with the file scan."""
    # Glob the path using the Runner
    runner_io = get_context().runner().runner_io()

    paths = path if isinstance(path, list) else [str(path)]
    listing_details_partition_set = runner_io.glob_paths_details(paths, source_info, fs)

    # Infer schema if no hints provided
    inferred_or_provided_schema = (
        _get_schema_from_hints(schema_hints)
        if schema_hints is not None
        else runner_io.get_schema_from_first_filepath(listing_details_partition_set, source_info, fs)
    )

    # Construct plan
    paths_details = listing_details_partition_set.to_pydict()

    filepaths = paths_details[runner_io.FS_LISTING_PATH_COLUMN_NAME]
    rs_schema = inferred_or_provided_schema._schema

    builder = LogicalPlanBuilder.read_parquet(filepaths, rs_schema)
    pybuilder = rust_logical_plan.RustLogicalPlanBuilder(builder)

    return pybuilder
