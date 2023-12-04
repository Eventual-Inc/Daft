use crate::ScanTask;

trait ScanTaskIterator: Iterator<Item = ScanTask> + Sized {
    /// Coalesces ScanTasks in the iterator by their filesize
    fn merge_by_sizes(self, target_filesize: usize) -> MergeByFileSize<Self> {
        MergeByFileSize {
            iter: self,
            target_filesize,
            buffer: std::default::Default::default(),
        }
    }
}

struct MergeByFileSize<I: ScanTaskIterator> {
    iter: I,
    target_filesize: usize,

    // HACK: we use a Vec instead of IndexMap because PartitionSpec is not Hashable
    // This might potentially be very slow. We should explore another option here?
    // Could be easier to make `PartitionSpec: Hash` if we don't use a `Table` under the hood.
    buffer: Vec<ScanTask>,
}

impl<I: ScanTaskIterator> Iterator for MergeByFileSize<I> {
    type Item = ScanTask;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(child_item) => {
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
                                let merged_scan_task = ScanTask::merge(child_item, matched_item)
                                    .expect(
                                    "Should be able to merge ScanTasks if all invariants are met",
                                );
                                self.buffer.push(merged_scan_task);
                                return self.next();
                            }
                            // Otherwise place the current child ScanTask into the buffer and yield the matched ScanTask
                            _ => {
                                self.buffer.push(child_item);
                                return Some(matched_item);
                            }
                        }
                    }
                }
                // No match found, place it in the buffer and keep going
                self.buffer.push(child_item);
                self.next()
            }
            // Iterator ran out of elements, we now flush the buffer
            None => self.buffer.pop(),
        }
    }
}

impl<I: ScanTaskIterator> ScanTaskIterator for MergeByFileSize<I> {}
