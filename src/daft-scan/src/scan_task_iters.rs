use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::{DaftError, DaftResult};
use common_scan_info::{ScanTaskLike, ScanTaskLikeRef, SPLIT_AND_MERGE_PASS};
use split_parquet_files_by_rowgroup::split_by_row_groups;

mod split_parquet_files_by_rowgroup;

use crate::{Pushdowns, ScanTask, ScanTaskRef};

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
