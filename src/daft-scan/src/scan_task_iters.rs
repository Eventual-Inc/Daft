use std::sync::Arc;

use common_error::DaftResult;

use crate::{ScanTask, ScanTaskRef};

type BoxScanTaskIter = Box<dyn Iterator<Item = DaftResult<ScanTaskRef>>>;

/// Coalesces ScanTasks by their filesizes
pub fn merge_by_sizes(scan_tasks: BoxScanTaskIter, target_filesize: usize) -> BoxScanTaskIter {
    Box::new(MergeByFileSize {
        iter: scan_tasks,
        target_filesize,
        accumulator: None,
    })
}

struct MergeByFileSize {
    iter: BoxScanTaskIter,
    target_filesize: usize,

    // Current element being accumulated on
    accumulator: Option<ScanTaskRef>,
}

impl Iterator for MergeByFileSize {
    type Item = DaftResult<ScanTaskRef>;

    fn next(&mut self) -> Option<Self::Item> {
        // Grabs the accumulator, leaving a `None` in its place
        let accumulator = self.accumulator.take();

        match (self.iter.next(), accumulator) {
            // On first iteration, place ScanTask into the accumulator
            (Some(Ok(child_item)), None) => {
                self.accumulator = Some(child_item);
                self.next()
            }
            // On subsequent iterations, check if ScanTask can be merged with accumulator
            (Some(Ok(child_item)), Some(accumulator)) => {
                let child_matches_accumulator = child_item.partition_spec()
                    == accumulator.partition_spec()
                    && child_item.file_format_config == accumulator.file_format_config
                    && child_item.schema == accumulator.schema
                    && child_item.storage_config == accumulator.storage_config
                    && child_item.pushdowns == accumulator.pushdowns;
                let can_merge_filesize = matches!(
                    (child_item.size_bytes(), accumulator.size_bytes()),
                    (Some(child_item_size), Some(buffered_item_size)) if child_item_size + buffered_item_size <= self.target_filesize
                );
                // Merge the accumulator and continue iteration
                if child_matches_accumulator && can_merge_filesize {
                    self.accumulator = Some(Arc::new(
                        ScanTask::merge(child_item.as_ref(), accumulator.as_ref())
                            .expect("ScanTasks should be mergeable in MergeByFileSize"),
                    ));
                    self.next()
                // Replace the accumulator with the new ScanTask and then yield the old accumulator
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
