use std::sync::Arc;

mod split_jsonl;

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_scan_info::{SPLIT_AND_MERGE_PASS, ScanTaskLike, ScanTaskLikeRef};
use daft_io::IOStatsContext;
use daft_parquet::read::read_parquet_metadata;
use parquet2::metadata::RowGroupList;

use crate::{ChunkSpec, DataSource, Pushdowns, ScanTask, ScanTaskRef};

type BoxScanTaskIter<'a> = Box<dyn Iterator<Item = DaftResult<ScanTaskRef>> + 'a>;

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
/// * `max_source_count`: Maximum number of ScanTasks to merge
#[must_use]
fn merge_by_sizes<'a>(
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
                    max_source_count: cfg.max_sources_per_scan_task,
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
            max_source_count: cfg.max_sources_per_scan_task,
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

    // Maximum number of files in a merged ScanTask
    max_source_count: usize,
}

impl MergeByFileSize<'_> {
    /// Returns whether or not the current accumulator is "ready" to be emitted as a finalized merged ScanTask
    ///
    /// "Readiness" is determined by a combination of factors based on how large the accumulated ScanTask is
    /// in estimated bytes, as well as other factors including any limit pushdowns.
    fn accumulator_ready(&self) -> bool {
        // Emit the accumulator as soon as it is bigger than the specified `target_lower_bound_size_bytes`
        if let Some(acc) = &self.accumulator {
            acc.sources.len() >= self.max_source_count
                || acc
                    .estimate_in_memory_size_bytes(Some(self.cfg))
                    .is_some_and(|bytes| bytes >= self.target_lower_bound_size_bytes)
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

        // Respect max_source_count: do not merge if we would exceed the cap
        if accumulator.sources.len() >= self.max_source_count {
            return false;
        }

        let child_matches_accumulator = other.partition_spec() == accumulator.partition_spec()
            && other.file_format_config == accumulator.file_format_config
            && other.schema == accumulator.schema
            && other.storage_config == accumulator.storage_config
            && other.pushdowns == accumulator.pushdowns;

        // Merge only if the resultant accumulator is smaller than the targeted upper bound
        let sum_smaller_than_max_size_bytes = if let (Some(child_bytes), Some(accumulator_bytes)) = (
            other.estimate_in_memory_size_bytes(Some(self.cfg)),
            accumulator.estimate_in_memory_size_bytes(Some(self.cfg)),
        ) {
            child_bytes + accumulator_bytes <= self.target_upper_bound_size_bytes
        } else {
            false
        };

        child_matches_accumulator && sum_smaller_than_max_size_bytes
    }
}

impl Iterator for MergeByFileSize<'_> {
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

            // Emit accumulator if ready or if merge count limit is reached
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

#[must_use]
fn split_by_row_groups(
    scan_tasks: BoxScanTaskIter,
    max_tasks: usize,
    min_size_bytes: usize,
    max_size_bytes: usize,
) -> BoxScanTaskIter {
    let mut scan_tasks = itertools::peek_nth(scan_tasks);

    // only split if we have a small amount of files
    if scan_tasks.peek_nth(max_tasks).is_some() {
        Box::new(scan_tasks)
    } else {
        Box::new(
            scan_tasks
                .map(move |t| -> DaftResult<BoxScanTaskIter> {
                    let t = t?;

                    /* Only split parquet tasks if they:
                        - have one source
                        - use native storage config
                        - have no specified chunk spec or number of rows
                        - have size past split threshold
                        - no iceberg delete files
                    */
                    if let (
                        FileFormatConfig::Parquet(ParquetSourceConfig {
                            field_id_mapping, ..
                        }),
                        [source],
                        Some(None),
                        None,
                    ) = (
                        t.file_format_config.as_ref(),
                        &t.sources[..],
                        t.sources.first().map(DataSource::get_chunk_spec),
                        t.pushdowns.limit,
                    ) && source
                        .get_size_bytes()
                        .is_none_or(|s| s > max_size_bytes as u64)
                      && source
                        .get_iceberg_delete_files()
                        .is_none_or(std::vec::Vec::is_empty)
                    {
                        let (io_runtime, io_client) =
                            t.storage_config.get_io_client_and_runtime()?;

                        let path = source.get_path();

                        let io_stats =
                            IOStatsContext::new(format!("split_by_row_groups for {path:#?}"));

                        let mut file = io_runtime.block_on_current_thread(read_parquet_metadata(
                            path,
                            io_client,
                            Some(io_stats),
                            field_id_mapping.clone(),
                        ))?;

                        let mut new_tasks: Vec<DaftResult<ScanTaskRef>> = Vec::new();
                        let mut curr_row_group_indices = Vec::new();
                        let mut curr_row_groups = Vec::new();
                        let mut curr_size_bytes = 0;
                        let mut curr_num_rows = 0;

                        let row_groups = std::mem::take(&mut file.row_groups);
                        let num_row_groups = row_groups.len();
                        for (i, rg) in row_groups {
                            curr_row_groups.push((i, rg));
                            let rg = &curr_row_groups.last().unwrap().1;
                            curr_row_group_indices.push(i as i64);
                            curr_size_bytes += rg.compressed_size();
                            curr_num_rows += rg.num_rows();

                            if curr_size_bytes >= min_size_bytes || i == num_row_groups - 1 {
                                let mut new_source = source.clone();

                                if let DataSource::File {
                                    chunk_spec,
                                    size_bytes,
                                    parquet_metadata,
                                    ..
                                } = &mut new_source
                                {
                                    // only keep relevant row groups in the metadata
                                    let row_group_list = RowGroupList::from_iter(curr_row_groups.into_iter());
                                    let new_metadata = file.clone_with_row_groups(curr_num_rows, row_group_list);
                                    *parquet_metadata = Some(Arc::new(new_metadata));

                                    *chunk_spec = Some(ChunkSpec::Parquet(curr_row_group_indices));
                                    *size_bytes = Some(curr_size_bytes as u64);
                                } else {
                                    unreachable!("Parquet file format should only be used with DataSource::File");
                                }

                                if let DataSource::File {
                                    metadata: Some(metadata),
                                    ..
                                } = &mut new_source
                                {
                                    metadata.length = curr_num_rows;
                                }

                                // Reset accumulators
                                curr_row_groups = Vec::new();
                                curr_row_group_indices = Vec::new();
                                curr_size_bytes = 0;
                                curr_num_rows = 0;

                                new_tasks.push(Ok(ScanTask::new(
                                    vec![new_source],
                                    t.file_format_config.clone(),
                                    t.schema.clone(),
                                    t.storage_config.clone(),
                                    t.pushdowns.clone(),
                                    t.generated_fields.clone(),
                                )
                                .into()));
                            }
                        }

                        Ok(Box::new(new_tasks.into_iter()))
                    } else {
                        Ok(Box::new(std::iter::once(Ok(t))))
                    }
                })
                .flat_map(|t| t.unwrap_or_else(|e| Box::new(std::iter::once(Err(e))))),
        )
    }
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
        && !scan_tasks
            .iter()
            .any(|st| matches!(st.file_format_config().as_ref(), FileFormatConfig::Warc(_)))
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
        // Split JSONL by byte ranges aligned to line boundaries for JSONFileFormat, other formats will be leaked through.
        // If there are other file formats in the future, a pipeline can be constructed to pass split_tasks.
        let split_jsonl_tasks = split_jsonl::split_by_jsonl_ranges(iter, cfg);
        let split_tasks = split_by_row_groups(
            split_jsonl_tasks,
            cfg.parquet_split_row_groups_max_files,
            cfg.scan_tasks_min_size_bytes,
            cfg.scan_tasks_max_size_bytes,
        );
        let merged_tasks = merge_by_sizes(split_tasks, pushdowns, cfg);
        let scan_tasks: Vec<Arc<dyn ScanTaskLike>> = merged_tasks
            .map(|st| st.map(|task| task as Arc<dyn ScanTaskLike>))
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(Arc::new(scan_tasks))
    } else {
        Ok(scan_tasks)
    }
}

/// Sets ``SPLIT_AND_MERGE_PASS``, which is the publicly-available pass that the query optimizer can use
#[ctor::ctor]
fn set_pass() {
    let _ = SPLIT_AND_MERGE_PASS.set(&split_and_merge_pass);
}
