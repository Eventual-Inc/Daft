use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_runtime::get_io_runtime;
use common_scan_info::{ScanTaskLike, ScanTaskLikeRef, SPLIT_AND_MERGE_PASS};
use daft_io::{get_io_client, IOStatsContext};
use daft_parquet::read::read_parquet_metadata;
use daft_stats::TableMetadata;
use futures::future::try_join_all;
use itertools::Itertools;
use parquet2::metadata::{FileMetaData, RowGroupList};

use crate::{
    storage_config::StorageConfig, ChunkSpec, DataSource, Pushdowns, ScanTask, ScanTaskRef,
};

pub(crate) type BoxScanTaskIter<'a> = Box<dyn Iterator<Item = DaftResult<ScanTaskRef>> + 'a>;

/// Coalesces ScanTasks by their [`ScanTask::estimate_in_memory_size_bytes()`]
///
/// NOTE: `min_size_bytes` and `max_size_bytes` are only parameters for the algorithm used for merging ScanTasks,
/// and do not provide any guarantees about the sizes of ScanTasks yielded by the resultant iterator.
/// This function may still yield ScanTasks with smaller sizes than `min_size_bytes`, or larger sizes
/// than `max_size_bytes` if **existing non-merged ScanTasks** with those sizes exist in the iterator.
///
/// # Arguments:
///
/// * `scan_tasks`: A Boxed Iterator of ScanTaskRefs to perform merging on
/// * `min_size_bytes`: Minimum size in bytes of a ScanTask, after which no more merging will be performed
/// * `max_size_bytes`: Maximum size in bytes of a ScanTask, capping the maximum size of a merged ScanTask
#[must_use]
pub(crate) fn merge_by_sizes<'a>(
    scan_tasks: BoxScanTaskIter<'a>,
    pushdowns: &Pushdowns,
    cfg: &'a DaftExecutionConfig,
) -> BoxScanTaskIter<'a> {
    if let Some(limit) = pushdowns.limit {
        // If LIMIT pushdown is present, perform a more conservative merge using the estimated size of the LIMIT
        let mut scan_tasks = scan_tasks.peekable();
        let first_scantask = scan_tasks
            .peek()
            .and_then(|x| x.as_ref().map(std::clone::Clone::clone).ok());
        if let Some(first_scantask) = first_scantask {
            let estimated_bytes_for_reading_limit_rows = first_scantask
                .as_ref()
                .estimate_in_memory_size_bytes(Some(cfg))
                .and_then(|est_materialized_bytes| {
                    first_scantask
                        .as_ref()
                        .approx_num_rows(Some(cfg))
                        .map(|approx_num_rows| {
                            (est_materialized_bytes as f64) / approx_num_rows * (limit as f64)
                        })
                });
            if let Some(limit_bytes) = estimated_bytes_for_reading_limit_rows {
                return Box::new(MergeByFileSize {
                    iter: Box::new(scan_tasks),
                    cfg,
                    target_upper_bound_size_bytes: (limit_bytes * 1.5) as usize,
                    target_lower_bound_size_bytes: (limit_bytes / 2.) as usize,
                    accumulator: None,
                }) as BoxScanTaskIter;
            }
        }
        // If we are unable to determine an estimation on the LIMIT size, so we don't perform a merge
        Box::new(scan_tasks)
    } else {
        Box::new(MergeByFileSize {
            iter: scan_tasks,
            cfg,
            target_upper_bound_size_bytes: cfg.scan_tasks_max_size_bytes,
            target_lower_bound_size_bytes: cfg.scan_tasks_min_size_bytes,
            accumulator: None,
        }) as BoxScanTaskIter
    }
}

struct MergeByFileSize<'a> {
    iter: BoxScanTaskIter<'a>,
    cfg: &'a DaftExecutionConfig,

    // The target upper/lower bound for in-memory size_bytes of a merged ScanTask
    target_upper_bound_size_bytes: usize,
    target_lower_bound_size_bytes: usize,

    // Current element being accumulated on
    accumulator: Option<ScanTaskRef>,
}

impl<'a> MergeByFileSize<'a> {
    /// Returns whether or not the current accumulator is "ready" to be emitted as a finalized merged ScanTask
    ///
    /// "Readiness" is determined by a combination of factors based on how large the accumulated ScanTask is
    /// in estimated bytes, as well as other factors including any limit pushdowns.
    fn accumulator_ready(&self) -> bool {
        // Emit the accumulator as soon as it is bigger than the specified `target_lower_bound_size_bytes`
        if let Some(acc) = &self.accumulator
            && let Some(acc_bytes) = acc.estimate_in_memory_size_bytes(Some(self.cfg))
            && acc_bytes >= self.target_lower_bound_size_bytes
        {
            true
        } else {
            false
        }
    }

    /// Checks if the current accumulator can be merged with the provided ScanTask
    fn can_merge(&self, other: &ScanTask) -> bool {
        let accumulator = self
            .accumulator
            .as_ref()
            .expect("accumulator should be populated");
        let child_matches_accumulator = other.partition_spec() == accumulator.partition_spec()
            && other.file_format_config == accumulator.file_format_config
            && other.schema == accumulator.schema
            && other.storage_config == accumulator.storage_config
            && other.pushdowns == accumulator.pushdowns;

        // Merge only if the resultant accumulator is smaller than the targeted upper bound
        let sum_smaller_than_max_size_bytes = if let Some(child_bytes) =
            other.estimate_in_memory_size_bytes(Some(self.cfg))
            && let Some(accumulator_bytes) =
                accumulator.estimate_in_memory_size_bytes(Some(self.cfg))
        {
            child_bytes + accumulator_bytes <= self.target_upper_bound_size_bytes
        } else {
            false
        };

        child_matches_accumulator && sum_smaller_than_max_size_bytes
    }
}

impl<'a> Iterator for MergeByFileSize<'a> {
    type Item = DaftResult<ScanTaskRef>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Create accumulator if not already present
            if self.accumulator.is_none() {
                self.accumulator = match self.iter.next() {
                    Some(Ok(item)) => Some(item),
                    e @ Some(Err(_)) => return e,
                    None => return None,
                };
            }

            // Emit accumulator if ready
            if self.accumulator_ready() {
                return self.accumulator.take().map(Ok);
            }

            let next_item = match self.iter.next() {
                Some(Ok(item)) => item,
                e @ Some(Err(_)) => return e,
                None => return self.accumulator.take().map(Ok),
            };

            // Emit accumulator if `next_item` cannot be merged
            if next_item
                .estimate_in_memory_size_bytes(Some(self.cfg))
                .is_none()
                || !self.can_merge(&next_item)
            {
                return self.accumulator.replace(next_item).map(Ok);
            }

            // Merge into a new accumulator
            self.accumulator = Some(Arc::new(
                ScanTask::merge(
                    self.accumulator
                        .as_ref()
                        .expect("accumulator should be populated"),
                    next_item.as_ref(),
                )
                .expect("ScanTasks should be mergeable in MergeByFileSize"),
            ));
        }
    }
}

enum SplitSingleParquetFileByRowGroupsState {
    Accumulating(usize, Vec<usize>, f64),
    Finalized(usize, Vec<usize>),
}

/// Splits a single Parquet file by its rowgroups
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
                            self.state = SplitSingleParquetFileByRowGroupsState::Finalized(
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
                        self.state = SplitSingleParquetFileByRowGroupsState::Finalized(
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
                SplitSingleParquetFileByRowGroupsState::Finalized(idx, finalized_rg_idxs) => {
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

struct MaybeSplitScanTask(ScanTaskRef);

struct SplitScanTaskInfo<'a> {
    path: &'a str,
    parquet_source_config: &'a ParquetSourceConfig,
}

impl MaybeSplitScanTask {
    fn split_info<'a>(&'a self, config: &DaftExecutionConfig) -> Option<SplitScanTaskInfo<'a>> {
        let scan_task = self.0.as_ref();

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
                path: source.get_path(),
                parquet_source_config,
            })
        } else {
            None
        }
    }
}

fn split_scan_tasks(
    scan_tasks_to_split: &[(usize, &ScanTask, SplitScanTaskInfo)],
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
        .map(|(_, scan_task, split_info)| {
            let io_config = scan_task.storage_config.get_io_config();
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
        .map(|((idx, scan_task, _), file_metadata)| {
            let splitter = SplitSingleParquetFileByRowGroups {
                state: SplitSingleParquetFileByRowGroupsState::Accumulating(0, Vec::new(), 0.),
                scan_task,
                file_metadata,
                config,
            };
            (*idx, splitter.collect_vec())
        })
        .collect())
}

enum SplitParquetFilesByRowGroupsState {
    ConstructingWindow(Vec<MaybeSplitScanTask>),
    WindowFinalized(Vec<MaybeSplitScanTask>),
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
                                window.push(MaybeSplitScanTask(next_scan_task));

                                // TODO: Naively construct windows of size 1 for now, but should do it conditionally
                                let should_finalize_window = |_window| true;
                                if should_finalize_window(&window) {
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
                                maybe_split
                                    .split_info(self.config)
                                    .map(|split_info| (idx, maybe_split.0.as_ref(), split_info))
                            });
                    let split_scan_tasks =
                        split_scan_tasks(scan_tasks_to_split.collect_vec().as_slice(), self.config);
                    match split_scan_tasks {
                        Err(e) => {
                            self.state =
                                SplitParquetFilesByRowGroupsState::ConstructingWindow(vec![]);
                            return Some(Err(e));
                        }
                        Ok(mut split_scan_tasks) => {
                            // Zip the split_scan_tasks into scan_tasks_to_split by substituting all entries with the found index
                            let mut window_after_split = VecDeque::<ScanTaskRef>::new();
                            for (idx, maybe_split) in finalized_window.into_iter().enumerate() {
                                if let Some(splits) = split_scan_tasks.remove(&idx) {
                                    window_after_split.extend(splits);
                                } else {
                                    window_after_split.push_back(maybe_split.0);
                                }
                            }
                            self.state = SplitParquetFilesByRowGroupsState::EmittingSplitWindow(
                                window_after_split,
                            );
                            continue;
                        }
                    }
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

fn split_and_merge_pass(
    scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    pushdowns: &Pushdowns,
    cfg: &DaftExecutionConfig,
) -> DaftResult<Arc<Vec<ScanTaskLikeRef>>> {
    // Perform scan task splitting and merging if there are only ScanTasks (i.e. no DummyScanTasks).
    if scan_tasks
        .iter()
        .all(|st| st.as_any().downcast_ref::<ScanTask>().is_some())
    {
        // TODO(desmond): Here we downcast Arc<dyn ScanTaskLike> to Arc<ScanTask>. ScanTask and DummyScanTask (test only) are
        // the only non-test implementer of ScanTaskLike. It might be possible to avoid the downcast by implementing merging
        // at the trait level, but today that requires shifting around a non-trivial amount of code to avoid circular dependencies.
        let iter: BoxScanTaskIter = Box::new(scan_tasks.as_ref().iter().map(|st| {
            st.clone()
                .as_any_arc()
                .downcast::<ScanTask>()
                .map_err(|e| DaftError::TypeError(format!("Expected Arc<ScanTask>, found {:?}", e)))
        }));
        let split_tasks = split_by_row_groups(iter, cfg);
        let merged_tasks = merge_by_sizes(split_tasks, pushdowns, cfg);
        let scan_tasks: Vec<Arc<dyn ScanTaskLike>> = merged_tasks
            .map(|st| st.map(|task| task as Arc<dyn ScanTaskLike>))
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(Arc::new(scan_tasks))
    } else {
        Ok(scan_tasks)
    }
}

#[ctor::ctor]
fn set_pass() {
    let _ = SPLIT_AND_MERGE_PASS.set(&split_and_merge_pass);
}
