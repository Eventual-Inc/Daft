use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_runtime::get_io_runtime;
use daft_io::{get_io_client, IOStatsContext};
use daft_parquet::read::read_parquet_metadata;
use daft_stats::TableMetadata;
use futures::future::try_join_all;
use itertools::Itertools;
use parquet2::metadata::{FileMetaData, RowGroupList};

use super::BoxScanTaskIter;
use crate::{storage_config::StorageConfig, ChunkSpec, DataSource, ScanTask, ScanTaskRef};

enum SplitSingleParquetFileByRowGroupsState {
    Accumulating(usize, Vec<usize>, f64),
    ReadyToEmit(usize, Vec<usize>),
}

/// Splits a single Parquet-based ScanTask by rowgroups into smaller chunked ScanTasks
///
/// This is used as an Iterator, yielding ScanTaskRefs after splitting the Parquet file
struct SplitSingleParquetFileByRowGroups<'a> {
    state: SplitSingleParquetFileByRowGroupsState,
    scan_task: &'a ScanTask,
    file_metadata: FileMetaData,
    config: &'a DaftExecutionConfig,
}

impl<'a> Iterator for SplitSingleParquetFileByRowGroups<'a> {
    type Item = ScanTaskRef;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.state {
                SplitSingleParquetFileByRowGroupsState::Accumulating(
                    idx,
                    accumulated,
                    estimated_accumulated_size_bytes,
                ) => {
                    let idx = *idx;

                    // Finished iterating: idx exceeds the number of available rowgroups
                    if idx >= self.file_metadata.row_groups.len() {
                        if accumulated.is_empty() {
                            return None;
                        } else {
                            self.state = SplitSingleParquetFileByRowGroupsState::ReadyToEmit(
                                idx,
                                std::mem::take(accumulated),
                            );
                            continue;
                        }
                    }

                    // Grab the next rowgroup index
                    let (rg_idx, rg) = self.file_metadata.row_groups.get_index(idx).unwrap();
                    let total_rg_compressed_size: usize = self
                        .file_metadata
                        .row_groups
                        .iter()
                        .map(|rg| rg.1.compressed_size())
                        .sum();
                    let rg_estimated_in_memory_size_bytes = self
                        .scan_task
                        .estimate_in_memory_size_bytes(Some(self.config))
                        .map_or(0., |est_materialized_size| {
                            (rg.compressed_size() as f64 / total_rg_compressed_size as f64)
                                * est_materialized_size as f64
                        });

                    // State updates
                    let mut new_accumulated = std::mem::take(accumulated);
                    new_accumulated.push(*rg_idx);
                    let new_estimated_size_bytes =
                        *estimated_accumulated_size_bytes + rg_estimated_in_memory_size_bytes;
                    let new_idx = idx + 1;

                    // Decide which state to jump to
                    //
                    // If the estimated size in bytes of the materialized data is past our configured threshold, we perform a split.
                    // NOTE: If the ScanTask is missing memory estimates for some reason, we will naively split it
                    let scan_task_missing_memory_estimates = self
                        .scan_task
                        .estimate_in_memory_size_bytes(Some(self.config))
                        .is_none();
                    let accumulated_size_bytes_past_threshold =
                        new_estimated_size_bytes as usize >= self.config.scan_tasks_min_size_bytes;
                    if scan_task_missing_memory_estimates || accumulated_size_bytes_past_threshold {
                        self.state = SplitSingleParquetFileByRowGroupsState::ReadyToEmit(
                            new_idx,
                            new_accumulated,
                        );
                    } else {
                        self.state = SplitSingleParquetFileByRowGroupsState::Accumulating(
                            new_idx,
                            new_accumulated,
                            new_estimated_size_bytes,
                        );
                    }
                    continue;
                }
                SplitSingleParquetFileByRowGroupsState::ReadyToEmit(idx, finalized_rg_idxs) => {
                    let num_rows = finalized_rg_idxs
                        .iter()
                        .map(|rg_idx| {
                            self.file_metadata
                                .row_groups
                                .get(rg_idx)
                                .unwrap()
                                .num_rows()
                        })
                        .sum();

                    // Create a new DataSource by mutating a clone of the old one
                    let mut new_source = match self.scan_task.sources.as_slice() {
                        [source] => source,
                        _ => unreachable!(
                            "SplitByRowGroupsAccumulator should only have one DataSource in its ScanTask"
                        ),
                    }.clone();
                    if let DataSource::File {
                        chunk_spec,
                        size_bytes,
                        parquet_metadata,
                        metadata,
                        ..
                    } = &mut new_source
                    {
                        // Create a new Parquet FileMetaData, only keeping the relevant row groups
                        let row_group_list =
                            RowGroupList::from_iter(finalized_rg_idxs.iter().map(|idx| {
                                (
                                    *idx,
                                    self.file_metadata.row_groups.get(idx).unwrap().clone(),
                                )
                            }));
                        let new_metadata = self
                            .file_metadata
                            .clone_with_row_groups(num_rows, row_group_list);
                        *parquet_metadata = Some(Arc::new(new_metadata));

                        // Mutate other necessary metadata
                        *chunk_spec = Some(ChunkSpec::Parquet(
                            finalized_rg_idxs.iter().map(|&idx| idx as i64).collect(),
                        ));
                        *size_bytes = Some(
                            finalized_rg_idxs
                                .iter()
                                .map(|rg_idx| {
                                    self.file_metadata
                                        .row_groups
                                        .get(rg_idx)
                                        .unwrap()
                                        .compressed_size()
                                })
                                .sum::<usize>() as u64,
                        );
                        *metadata = Some(TableMetadata { length: num_rows });
                    } else {
                        unreachable!(
                            "Parquet file format should only be used with DataSource::File"
                        );
                    }

                    // Move state back to Accumulating, and emit data
                    self.state =
                        SplitSingleParquetFileByRowGroupsState::Accumulating(*idx, Vec::new(), 0.);
                    let new_scan_task = ScanTask::new(
                        vec![new_source],
                        self.scan_task.file_format_config.clone(),
                        self.scan_task.schema.clone(),
                        self.scan_task.storage_config.clone(),
                        self.scan_task.pushdowns.clone(),
                        self.scan_task.generated_fields.clone(),
                    )
                    .into();
                    return Some(new_scan_task);
                }
            }
        }
    }
}

/// Information for splitting a ScanTask by RowGroups
///
/// TODO: this can be extended to hold other useful information to inform the splitting *without* requiring
/// a Parquet metadata fetch. For example, rowgroup indices provided by a catalog/parquet metadata provider.
struct SplitScanTaskInfo<'a> {
    to_split: &'a ScanTask,
    path: &'a str,
    parquet_source_config: &'a ParquetSourceConfig,
}

impl<'a> SplitScanTaskInfo<'a> {
    /// Optionally creates [`SplitScanTaskInfo`] depending on whether or not a given ScanTask can be split
    pub fn from_scan_task(scan_task: &'a ScanTask, config: &DaftExecutionConfig) -> Option<Self> {
        /* Only split parquet tasks if they:
            - have one source
            - use native storage config
            - have no specified chunk spec or number of rows
            - have size past split threshold
            - no iceberg delete files
        */
        if let (
            FileFormatConfig::Parquet(parquet_source_config @ ParquetSourceConfig { .. }),
            StorageConfig::Native(_),
            [source],
            Some(None),
            None,
            est_materialized_size,
        ) = (
            scan_task.file_format_config.as_ref(),
            scan_task.storage_config.as_ref(),
            &scan_task.sources[..],
            scan_task.sources.first().map(DataSource::get_chunk_spec),
            scan_task.pushdowns.limit,
            scan_task.estimate_in_memory_size_bytes(Some(config)),
        ) && est_materialized_size.map_or(true, |est| est > config.scan_tasks_max_size_bytes)
            && source
                .get_iceberg_delete_files()
                .map_or(true, std::vec::Vec::is_empty)
        {
            Some(SplitScanTaskInfo {
                to_split: scan_task,
                path: source.get_path(),
                parquet_source_config,
            })
        } else {
            None
        }
    }

    /// Uses Parquet FileMetaData to create an Iterator of ScanTasks that are the result of splitting the existing Parquet ScanTask
    pub fn get_scan_task_iter(
        &self,
        file_metadata: FileMetaData,
        config: &'a DaftExecutionConfig,
    ) -> SplitSingleParquetFileByRowGroups {
        SplitSingleParquetFileByRowGroups {
            state: SplitSingleParquetFileByRowGroupsState::Accumulating(0, Vec::new(), 0.),
            scan_task: self.to_split,
            file_metadata,
            config,
        }
    }
}

/// Helper method to split ScanTasks using [`SplitScanTaskInfo`]
///
/// Returns a HashMap of {idx: split_results}
fn split_scan_tasks_by_parquet_metadata(
    scan_tasks_to_split: &[(usize, SplitScanTaskInfo)],
    config: &DaftExecutionConfig,
) -> DaftResult<HashMap<usize, Vec<ScanTaskRef>>> {
    // Perform a bulk Parquet metadata fetch
    let io_runtime = get_io_runtime(true);
    let io_stats = IOStatsContext::new(format!(
        "Performing batched Parquet metadata read for {} ScanTasks",
        scan_tasks_to_split.len()
    ));
    let parquet_metadata_futures = scan_tasks_to_split
        .iter()
        .map(|(_, split_info)| {
            let io_config = split_info.to_split.storage_config.get_io_config();
            let io_client = get_io_client(true, io_config)?;
            let io_stats = io_stats.clone();
            let field_id_mapping = split_info.parquet_source_config.field_id_mapping.clone();
            let path = split_info.path.to_string();
            Ok(io_runtime.spawn(async move {
                read_parquet_metadata(path.as_str(), io_client, Some(io_stats), field_id_mapping)
                    .await
            }))
        })
        .collect::<DaftResult<Vec<_>>>()?;
    let file_metadatas = io_runtime
        .block_on_current_thread(try_join_all(parquet_metadata_futures))
        .and_then(|results| results.into_iter().collect::<Result<Vec<_>, _>>())?;

    // Use fetched file metadatas to perform splitting of each ScanTask
    Ok(scan_tasks_to_split
        .iter()
        .zip(file_metadatas)
        .map(|((idx, split_info), file_metadata)| {
            let split_tasks = split_info.get_scan_task_iter(file_metadata, config);
            (*idx, split_tasks.collect_vec())
        })
        .collect())
}

/// Maximum number of Parquet Metadata fetches to perform at once while iterating through ScanTasks
static WINDOW_SIZE_MAX_PARQUET_METADATA_FETCHES: usize = 16;

enum SplitParquetFilesByRowGroupsState {
    ConstructingWindow(Vec<ScanTaskRef>),
    WindowFinalized(Vec<ScanTaskRef>),
    EmittingSplitWindow(VecDeque<ScanTaskRef>),
}

struct SplitParquetFilesByRowGroups<'a> {
    iter: BoxScanTaskIter<'a>,
    state: SplitParquetFilesByRowGroupsState,
    config: &'a DaftExecutionConfig,
}

impl<'a> Iterator for SplitParquetFilesByRowGroups<'a> {
    type Item = DaftResult<ScanTaskRef>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.state {
                // Constructing window:
                //
                // Grab the next ScanTask. Then choose between:
                // 1. Keep constructing the window
                // 2. Move to WindowFinalized state
                // 3. End iteration
                SplitParquetFilesByRowGroupsState::ConstructingWindow(window) => {
                    if let Some(next_scan_task) = self.iter.next() {
                        match next_scan_task {
                            Err(e) => return Some(Err(e)),
                            Ok(next_scan_task) => {
                                window.push(next_scan_task);

                                // Windows are "finalized" when there are >= 16 Parquet Metadatas to be fetched
                                let should_finalize_window = window
                                    .iter()
                                    .filter(|scan_task| {
                                        SplitScanTaskInfo::from_scan_task(
                                            scan_task.as_ref(),
                                            self.config,
                                        )
                                        .is_some()
                                    })
                                    .count()
                                    >= WINDOW_SIZE_MAX_PARQUET_METADATA_FETCHES;
                                if should_finalize_window {
                                    let window = std::mem::take(window);
                                    self.state =
                                        SplitParquetFilesByRowGroupsState::WindowFinalized(window);
                                    continue;
                                }
                            }
                        }
                    } else if !window.is_empty() {
                        let window = std::mem::take(window);
                        self.state = SplitParquetFilesByRowGroupsState::WindowFinalized(window);
                    } else {
                        return None;
                    }
                }
                // Window finalized:
                //
                // Perform the necessary I/O operations to split the ScanTasks. Perform the splitting and
                // then construct a new window of (split) ScanTasks and enter the EmittingSplitWindow state.
                SplitParquetFilesByRowGroupsState::WindowFinalized(finalized_window) => {
                    let finalized_window = std::mem::take(finalized_window);
                    let scan_tasks_to_split =
                        finalized_window
                            .iter()
                            .enumerate()
                            .filter_map(|(idx, maybe_split)| {
                                SplitScanTaskInfo::from_scan_task(maybe_split.as_ref(), self.config)
                                    .map(|split_info| (idx, split_info))
                            });

                    // Perform a split of ScanTasks: note that this might trigger expensive I/O to retrieve Parquet metadata
                    let split_scan_tasks = split_scan_tasks_by_parquet_metadata(
                        scan_tasks_to_split.collect_vec().as_slice(),
                        self.config,
                    );
                    let mut split_scan_tasks = match split_scan_tasks {
                        Err(e) => {
                            self.state =
                                SplitParquetFilesByRowGroupsState::ConstructingWindow(vec![]);
                            return Some(Err(e));
                        }
                        Ok(split_scan_tasks) => split_scan_tasks,
                    };

                    // Zip the split_scan_tasks back into the iterator by substituting entries that have been split using their index
                    let mut window_after_split = VecDeque::<ScanTaskRef>::new();
                    for (idx, maybe_split) in finalized_window.into_iter().enumerate() {
                        if let Some(splits) = split_scan_tasks.remove(&idx) {
                            window_after_split.extend(splits);
                        } else {
                            window_after_split.push_back(maybe_split);
                        }
                    }
                    self.state =
                        SplitParquetFilesByRowGroupsState::EmittingSplitWindow(window_after_split);
                    continue;
                }
                // Emitting split window:
                //
                // Exhaust the window after splitting, and when completely exhausted, re-enter the ConstructingWindow state.
                SplitParquetFilesByRowGroupsState::EmittingSplitWindow(window_after_split) => {
                    if let Some(item) = window_after_split.pop_front() {
                        return Some(Ok(item));
                    } else {
                        self.state =
                            SplitParquetFilesByRowGroupsState::ConstructingWindow(Vec::new());
                        continue;
                    }
                }
            }
        }
    }
}

#[must_use]
pub(crate) fn split_by_row_groups<'a>(
    scan_tasks: BoxScanTaskIter<'a>,
    config: &'a DaftExecutionConfig,
) -> BoxScanTaskIter<'a> {
    Box::new(SplitParquetFilesByRowGroups {
        iter: scan_tasks,
        state: SplitParquetFilesByRowGroupsState::ConstructingWindow(Vec::new()),
        config,
    }) as BoxScanTaskIter
}
