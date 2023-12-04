use std::sync::Arc;

use common_error::DaftResult;

use crate::{ScanTask, ScanTaskRef};

type BoxScanTaskIter = Box<dyn Iterator<Item = DaftResult<ScanTaskRef>>>;

/// Coalesces ScanTasks by their filesizes
pub fn merge_by_sizes(
    scan_tasks: BoxScanTaskIter,
    min_filesize: usize,
    max_filesize: usize,
) -> BoxScanTaskIter {
    Box::new(MergeByFileSize {
        iter: scan_tasks,
        min_filesize,
        max_filesize,
        accumulator: None,
    })
}

struct MergeByFileSize {
    iter: BoxScanTaskIter,
    min_filesize: usize,
    max_filesize: usize,

    // Current element being accumulated on
    accumulator: Option<ScanTaskRef>,
}

impl Iterator for MergeByFileSize {
    type Item = DaftResult<ScanTaskRef>;

    fn next(&mut self) -> Option<Self::Item> {
        // Grabs the accumulator, leaving a `None` in its place
        let accumulator = self.accumulator.take();

        match (self.iter.next(), accumulator) {
            // When no accumulator exists, trivially place the ScanTask into the accumulator
            (Some(Ok(child_item)), None) => {
                self.accumulator = Some(child_item);
                self.next()
            }
            // When an accumulator exists, attempt a merge and yield the result
            (Some(Ok(child_item)), Some(accumulator)) => {
                // Whether or not the accumulator and the current item should be merged
                let should_merge = {
                    let child_matches_accumulator = child_item.partition_spec()
                        == accumulator.partition_spec()
                        && child_item.file_format_config == accumulator.file_format_config
                        && child_item.schema == accumulator.schema
                        && child_item.storage_config == accumulator.storage_config
                        && child_item.pushdowns == accumulator.pushdowns;
                    let smaller_than_max_filesize = matches!(
                        (child_item.size_bytes(), accumulator.size_bytes()),
                        (Some(child_item_size), Some(buffered_item_size)) if child_item_size + buffered_item_size <= self.max_filesize
                    );
                    child_matches_accumulator && smaller_than_max_filesize
                };

                if should_merge {
                    let merged_result = Some(Arc::new(
                        ScanTask::merge(accumulator.as_ref(), child_item.as_ref())
                            .expect("ScanTasks should be mergeable in MergeByFileSize"),
                    ));

                    // Whether or not we should immediately yield the merged result, or keep accumulating
                    let should_yield = matches!(
                        (child_item.size_bytes(), accumulator.size_bytes()),
                        (Some(child_item_size), Some(buffered_item_size)) if child_item_size + buffered_item_size >= self.min_filesize
                    );
                    if should_yield {
                        Ok(merged_result).transpose()
                    } else {
                        self.accumulator = merged_result;
                        self.next()
                    }
                } else {
                    self.accumulator = Some(child_item);
                    Some(Ok(accumulator))
                }
            }
            // Bubble up errors from child iterator, making sure to replace the accumulator which we moved
            (Some(Err(e)), acc) => {
                self.accumulator = acc;
                Some(Err(e))
            }
            // Iterator ran out of elements: ensure that we flush the last buffered ScanTask
            (None, Some(last_scan_task)) => Some(Ok(last_scan_task)),
            (None, None) => None,
        }
    }
}
