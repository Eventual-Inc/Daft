use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_file_formats::{FileFormatConfig, ParquetSourceConfig};
use common_scan_info::{ScanTaskLike, ScanTaskLikeRef, SPLIT_AND_MERGE_PASS};
use daft_io::IOStatsContext;
use daft_parquet::read::read_parquet_metadata;
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

#[derive(Default)]
struct SplitParquetByRowGroupsAccumulator {
    row_group_indices: Vec<usize>,
    size_bytes: f64,
    num_rows: usize,
}

impl SplitParquetByRowGroupsAccumulator {
    fn accumulate(
        &mut self,
        rg_idx: &usize,
        ctx: &SplitParquetByRowGroupsContext,
    ) -> Option<DataSource> {
        let rg = ctx.file_metadata.row_groups.get(rg_idx).unwrap();

        // Estimate the materialized size of this rowgroup and add it to curr_size_bytes
        self.size_bytes += ctx
            .scantask_estimated_size_bytes
            .map_or(0., |est_materialized_size| {
                (rg.compressed_size() as f64 / ctx.total_rg_compressed_size() as f64)
                    * est_materialized_size as f64
            });
        self.num_rows += rg.num_rows();
        self.row_group_indices.push(*rg_idx);

        // Flush the accumulator if necessary
        let reached_accumulator_limit =
            self.size_bytes >= ctx.config.scan_tasks_min_size_bytes as f64;
        let materialized_size_estimation_not_available =
            ctx.scantask_estimated_size_bytes.is_none();
        if materialized_size_estimation_not_available || reached_accumulator_limit {
            self.flush(ctx)
        } else {
            None
        }
    }

    fn flush(&mut self, ctx: &SplitParquetByRowGroupsContext) -> Option<DataSource> {
        // If nothing to flush, return None early
        if self.row_group_indices.is_empty() {
            return None;
        }

        // Grab accumulated values and reset accumulators
        let curr_row_group_indices = std::mem::take(&mut self.row_group_indices);
        let curr_size_bytes = std::mem::take(&mut self.size_bytes);
        let curr_num_rows = std::mem::take(&mut self.num_rows);

        // Create a new DataSource by mutating a clone of the old one
        let mut new_source = ctx.data_source().clone();
        if let DataSource::File {
            chunk_spec,
            size_bytes,
            parquet_metadata,
            metadata: table_metadata,
            ..
        } = &mut new_source
        {
            // Create a new Parquet FileMetaData, only keeping the relevant row groups
            let row_group_list = RowGroupList::from_iter(
                curr_row_group_indices
                    .iter()
                    .map(|idx| (*idx, ctx.file_metadata.row_groups.get(idx).unwrap().clone())),
            );
            let new_metadata = ctx
                .file_metadata
                .clone_with_row_groups(curr_num_rows, row_group_list);
            *parquet_metadata = Some(Arc::new(new_metadata));

            // Mutate other necessary metadata
            *chunk_spec = Some(ChunkSpec::Parquet(
                curr_row_group_indices
                    .into_iter()
                    .map(|idx| idx as i64)
                    .collect(),
            ));
            *size_bytes = Some(curr_size_bytes as u64);
            if let Some(metadata) = table_metadata {
                metadata.length = curr_num_rows;
            }
        } else {
            unreachable!("Parquet file format should only be used with DataSource::File");
        }

        Some(new_source)
    }
}

struct SplitParquetByRowGroupsContext<'a> {
    config: &'a DaftExecutionConfig,
    file_metadata: FileMetaData,
    scan_task_ref: ScanTaskRef,
    scantask_estimated_size_bytes: Option<usize>,
}

impl<'a> SplitParquetByRowGroupsContext<'a> {
    pub fn new(
        scan_task_ref: ScanTaskRef,
        file_metadata: FileMetaData,
        config: &'a DaftExecutionConfig,
    ) -> Self {
        let scantask_estimated_size_bytes =
            scan_task_ref.estimate_in_memory_size_bytes(Some(config));
        Self {
            config,
            file_metadata,
            scan_task_ref,
            scantask_estimated_size_bytes,
        }
    }

    #[inline]
    fn total_rg_compressed_size(&self) -> usize {
        self.file_metadata
            .row_groups
            .iter()
            .map(|rg| rg.1.compressed_size())
            .sum()
    }

    #[inline]
    fn data_source(&self) -> &DataSource {
        match self.scan_task_ref.sources.as_slice() {
            [source] => source,
            _ => unreachable!(
                "SplitByRowGroupsAccumulator should only have one DataSource in its ScanTask"
            ),
        }
    }
}

#[must_use]
pub(crate) fn split_by_row_groups<'a>(
    scan_tasks: BoxScanTaskIter<'a>,
    config: &'a DaftExecutionConfig,
) -> BoxScanTaskIter<'a> {
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
                    StorageConfig::Native(_),
                    [source],
                    Some(None),
                    None,
                    est_materialized_size,
                ) = (
                    t.file_format_config.as_ref(),
                    t.storage_config.as_ref(),
                    &t.sources[..],
                    t.sources.first().map(DataSource::get_chunk_spec),
                    t.pushdowns.limit,
                    t.estimate_in_memory_size_bytes(Some(config)),
                ) && est_materialized_size
                    .map_or(true, |est| est > config.scan_tasks_max_size_bytes)
                    && source
                        .get_iceberg_delete_files()
                        .map_or(true, std::vec::Vec::is_empty)
                {
                    // Retrieve Parquet FileMetaData and construct SplitParquetByRowGroupsAccumulator
                    let (io_runtime, io_client) = t.storage_config.get_io_client_and_runtime()?;
                    let path = source.get_path();
                    let io_stats =
                        IOStatsContext::new(format!("split_by_row_groups for {path:#?}"));
                    let file_metadata =
                        io_runtime.block_on_current_thread(read_parquet_metadata(
                            path,
                            io_client,
                            Some(io_stats),
                            field_id_mapping.clone(),
                        ))?;
                    let mut accumulator = SplitParquetByRowGroupsAccumulator::default();
                    let accumulator_ctx =
                        SplitParquetByRowGroupsContext::new(t, file_metadata, config);

                    // Run accumulation over all rowgroups, make sure to flush last result
                    let mut new_data_sources = Vec::new();
                    for idx in accumulator_ctx.file_metadata.row_groups.keys() {
                        if let Some(accumulated) = accumulator.accumulate(idx, &accumulator_ctx) {
                            new_data_sources.push(accumulated);
                        }
                    }
                    if let Some(accumulated) = accumulator.flush(&accumulator_ctx) {
                        new_data_sources.push(accumulated);
                    }

                    // Construct ScanTasks with the new DataSources
                    let new_scan_tasks = new_data_sources
                        .into_iter()
                        .map(|data_source| {
                            ScanTask::new(
                                vec![data_source],
                                accumulator_ctx.scan_task_ref.file_format_config.clone(),
                                accumulator_ctx.scan_task_ref.schema.clone(),
                                accumulator_ctx.scan_task_ref.storage_config.clone(),
                                accumulator_ctx.scan_task_ref.pushdowns.clone(),
                                accumulator_ctx.scan_task_ref.generated_fields.clone(),
                            )
                            .into()
                        })
                        .collect_vec();
                    Ok(Box::new(new_scan_tasks.into_iter().map(Ok)))
                } else {
                    Ok(Box::new(std::iter::once(Ok(t))))
                }
            })
            .flat_map(|t| t.unwrap_or_else(|e| Box::new(std::iter::once(Err(e))))),
    )
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
