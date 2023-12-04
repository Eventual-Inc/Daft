use std::sync::Arc;

use common_error::DaftResult;

use crate::{ScanTask, ScanTaskRef};

type BoxScanTaskIter = Box<dyn Iterator<Item = DaftResult<ScanTaskRef>>>;

/// Coalesces ScanTasks by their filesizes
pub fn merge_by_sizes(scan_tasks: BoxScanTaskIter, target_filesize: usize) -> BoxScanTaskIter {
    Box::new(MergeByFileSize {
        iter: scan_tasks,
        target_filesize,
        buffer: std::default::Default::default(),
    })
}

struct MergeByFileSize {
    iter: BoxScanTaskIter,
    target_filesize: usize,

    // HACK: we use a Vec instead of IndexMap because PartitionSpec is not Hashable
    // This might potentially be very slow. We should explore another option here?
    // Could be easier to make `PartitionSpec: Hash` if we don't use a `Table` under the hood.
    buffer: Vec<ScanTaskRef>,
}

impl Iterator for MergeByFileSize {
    type Item = DaftResult<ScanTaskRef>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(child_item)) => {
                // Try to find matches in the buffer to perform merging operations
                for (idx, buffered_item) in self.buffer.iter().enumerate() {
                    if child_item.partition_spec() == buffered_item.partition_spec()
                        && child_item.file_format_config == buffered_item.file_format_config
                        && child_item.schema == buffered_item.schema
                        && child_item.storage_config == buffered_item.storage_config
                        && child_item.pushdowns == buffered_item.pushdowns
                    {
                        // Remove the matched ScanTask from the buffer to either return, or be merged
                        let matched_item = self.buffer.remove(idx);

                        match (child_item.size_bytes(), matched_item.size_bytes()) {
                            // Merge if combined size will still be under the target size, and keep iterating
                            (Some(child_item_size), Some(buffered_item_size))
                                if child_item_size + buffered_item_size <= self.target_filesize =>
                            {
                                let merged_scan_task = ScanTask::merge(
                                    child_item.as_ref(),
                                    &matched_item,
                                )
                                .expect(
                                    "Should be able to merge ScanTasks if all invariants are met",
                                );
                                self.buffer.push(Arc::new(merged_scan_task));
                                return self.next();
                            }
                            // Otherwise place the current child ScanTask into the buffer and yield the matched ScanTask
                            _ => {
                                self.buffer.push(child_item);
                                return Some(Ok(matched_item));
                            }
                        }
                    }
                }
                // No match found, place it in the buffer and keep going
                self.buffer.push(child_item);
                self.next()
            }
            // Bubble up errors from child iterator
            Some(Err(e)) => Some(Err(e)),
            // Iterator ran out of elements, we now flush the buffer
            None => Ok(self.buffer.pop()).transpose(),
        }
    }
}
