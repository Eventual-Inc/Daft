from __future__ import annotations

import fsspec

from daft.context import get_context
from daft.datasources import SourceInfo
from daft.logical import logical_plan
from daft.runners.partitioning import vPartitionSchemaInferenceOptions


def _get_tabular_files_scan(
    path: str,
    source_info: SourceInfo,
    fs: fsspec.AbstractFileSystem | None,
    schema_inference_options: vPartitionSchemaInferenceOptions,
) -> logical_plan.TabularFilesScan:
    """Returns a TabularFilesScan LogicalPlan for a given glob filepath."""
    # Glob the path using the Runner
    runner_io = get_context().runner().runner_io()
    listing_details_partition_set = runner_io.glob_paths_details(path, source_info, fs)

    # TODO: We should have a more sophisticated schema inference mechanism (sample >1 file and resolve schemas across files)
    # Infer schema from the first filepath in the listings PartitionSet
    data_schema = runner_io.get_schema_from_first_filepath(
        listing_details_partition_set, source_info, fs, schema_inference_options
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
        schema=data_schema,
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
