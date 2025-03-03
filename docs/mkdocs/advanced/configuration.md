# Configuration

Daft can be configured with a `config.toml` that lives in `DAFT_HOME` which defaults to `~/.daft`.

## Defaults

Below is the default configuration which documents the available options.

```toml
[execution]
#
# Execution Configuration controls various aspects of Daft execution.
#
runner = "native"
#
#   Configure Daft to execute dataframes using native multi-threaded
#   processing (native) or using the Ray computing framework (ray).
#
scan_tasks_min_size_bytes = "96MB"
#
#   Minimum size in bytes when merging ScanTasks when reading files from
#   storage. Increasing this value will make Daft perform more merging of
#   files into a single partition before yielding, which leads to bigger
#   but fewer partitions. Defaults to 96MB.
#
scan_tasks_max_size_bytes = "384MB"
#
#   Maximum size in bytes when merging ScanTasks when reading files from
#   storage. Increasing this value will increase the upper bound of the
#   size of merged ScanTasks, which leads to bigger but fewer partitions.
#   Defaults to 384MB.
#
max_sources_per_scan_task = 10
#
#   Maximum number of sources in a single ScanTask. Defaults to 10.
#
broadcast_join_size_bytes_threshold = "10MB"
#
#   If one side of a join is smaller than this threshold, a broadcast join
#   will be used. Defaults to 10MB.
#
parquet_split_row_groups_max_files = 10
#
#   Maximum number of files to read in which the row group splitting should
#   happen. Defaults to 10.
#
sort_merge_join_sort_with_aligned_boundaries = false
#
#   Whether to use a specialized algorithm for sorting both sides of a
#   sort-merge join such that they have aligned boundaries. This can lead
#   to a faster merge-join at the cost of more skewed sorted join inputs,
#   increasing the risk of OOMs. Defaults to false.
#
hash_join_partition_size_leniency = 0.5
#
#   If the left side of a hash join is already correctly partitioned and
#   the right side isn’t, and the ratio between the left and right size is
#   at least this value, then the right side is repartitioned to have an
#   equal number of partitions as the left. Defaults to 0.5.
#
sample_size_for_sort = 20
#
#   Number of elements to sample from each partition when running sort.
#   Default to 20.
#
num_preview_rows = 8
#
#   Number of rows to show when displaying a dataframe preview. Defaults to 8.
#
parquet_target_filesize = "512MB"
#
#   Target File Size when writing out Parquet Files. Defaults to 512 MB.
#
parquet_target_row_group_size = "128MB"
#
#   Target Row Group Size when writing out Parquet Files. Defaults to 128MB.
#
parquet_inflation_factor = 3.0
#
#   Inflation Factor of parquet files (In-Memory-Size / File-Size) ratio.
#   Defaults to 3.0.
#
csv_target_filesize = "512MB"
#
#   Target File Size when writing out CSV Files. Defaults to 512MB.
#
csv_inflation_factor = 0.5
#
#   Inflation Factor of CSV files (In-Memory-Size / File-Size) ratio.
#   Defaults to 0.5.
#
shuffle_aggregation_default_partitions = 200
#
#   Maximum number of partitions to create when performing aggregations on
#   the Ray Runner. Defaults to 200, unless the number of input partitions
#   is less than 200.
#
partial_aggregation_threshold = 10_000
#
#   Threshold for performing partial aggregations on the Native Runner.
#   Defaults to 10_000 rows.
#
high_cardinality_aggregation_threshold = 0.8
#
#   Threshold selectivity for performing high cardinality aggregations on
#   the Native Runner. Defaults to 0.8.
#
read_sql_partition_size_bytes = "512MB"
#
#   Target size of partition when reading from SQL databases. Defaults to
#   512MB.
#
enable_aqe = false
#
#   Enables Adaptive Query Execution. Defaults to False.
#
enable_native_executor = false
#
#   Enables the native executor. Defaults to False.
#
default_morsel_size = 131072
#
#   Default size of morsels used for the local executor. Defaults to
#   131072 rows (2^17).
#
shuffle_algorithm = "auto"
#
#   The shuffle algorithm to use. Defaults to “auto”, which will let Daft
#   determine the algorithm. Options are “map_reduce” and “pre_shuffle_merge”.
#
pre_shuffle_merge_threshold = "1GB"
#
#   Memory threshold in bytes for pre-shuffle merge. Defaults to 1GB.
#
enable_ray_tracing = false
#
#   Enable tracing for Ray. Accessible in /tmp/ray/session_latest/logs/daft
#   after the run completes. Defaults to False.
#
scantask_splitting_level = 1
#
#   How aggressively to split scan tasks. Setting this to 2 will use a more
#   aggressive ScanTask splitting algorithm which might be more expensive
#   to run but results in more even splits of partitions. Defaults to 1.
#

[telemetry]
#
# Daft collects non-identifiable data via our own analytics as well as Scarf.
# https://www.getdaft.io/projects/docs/en/stable/resources/telemetry/
#
#
do_not_track = true
#
#   Disable both Daft and Scarf analytics. This is slightly different than
#   the environment variable which only disables scarf. Defaults to false.
#
analytics_enabled = true
#
#   Report versions (OS, Python, Daft) common bugs and performance bottlenecks.
#   Defaults to true.
#
scarf_enabled = true
#
#   Scarf usage analytics (https://about.scarf.sh/) Defaults to true.
#
```

## Environment Variables

In addition to `config.toml` there are several environment variables to control execution and telemetry.

> For boolean values, both 1 and 'true' (case-insensitively) are truthy

**Execution**

| Variable                        | Description                                                                | Default       |
| ------------------------------- | -------------------------------------------------------------------------- | ------------- |
| `DAFT_HOME`                     | Set the configuration directory                                            | `$HOME/.daft` |
| `DAFT_RUNNER`                   | Set the runner to *native* or *ray*                                        | `native`      |
| `DAFT_ENABLE_AQE`               | Enable adaptive query execution                                            | `false`       |
| `DAFT_ENABLE_RAY_TRACING`       | Enable Ray tracing                                                         | `false`       |
| `DAFT_SHUFFLE_ALGORITHM`        | Set the shuffle algorithm to *pre_shuffle_merge*, *map_reduce*,  or *auto* | `auto`        |
| `DAFT_SCANTASK_SPLITTING_LEVEL` | Set how aggressively to split scan tasks.                                  | `1`           |


**Telemetry**

| Variable                 | Description                              | Default |
| ------------------------ | ---------------------------------------- | ------- |
| `DAFT_ANALYTICS_ENABLED` | Enable Daft analytics (disable with `0`) | `true`  |
| `SCARF_NO_ANALYTICS`     | Disable scarf.sh usage metrics           | `false` |
| `DO_NOT_TRACK`           | Disable scarf.sh usage metrics           | `false` |
