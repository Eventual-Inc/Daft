use std::sync::Arc;

use common_error::DaftResult;
use daft_io::IOStatsContext;
use daft_parquet::read::read_parquet_metadata;

use crate::{
    file_format::FileFormatConfig, storage_config::StorageConfig, ChunkSpec, DataFileSource,
    ScanTask, ScanTaskRef,
};

type BoxScanTaskIter = Box<dyn Iterator<Item = DaftResult<ScanTaskRef>>>;

/// Coalesces ScanTasks by their [`ScanTask::size_bytes()`]
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
pub fn merge_by_sizes(
    scan_tasks: BoxScanTaskIter,
    min_size_bytes: usize,
    max_size_bytes: usize,
) -> BoxScanTaskIter {
    Box::new(MergeByFileSize {
        iter: scan_tasks,
        min_size_bytes,
        max_size_bytes,
        accumulator: None,
    })
}

struct MergeByFileSize {
    iter: BoxScanTaskIter,
    min_size_bytes: usize,
    max_size_bytes: usize,

    // Current element being accumulated on
    accumulator: Option<ScanTaskRef>,
}

impl Iterator for MergeByFileSize {
    type Item = DaftResult<ScanTaskRef>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Grabs the accumulator, leaving a `None` in its place
            let accumulator = self.accumulator.take();

            match (self.iter.next(), accumulator) {
                // When no accumulator exists, trivially place the ScanTask into the accumulator
                (Some(Ok(child_item)), None) => {
                    self.accumulator = Some(child_item);
                    continue;
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
                        let smaller_than_max_size_bytes = matches!(
                            (child_item.size_bytes(), accumulator.size_bytes()),
                            (Some(child_item_size), Some(buffered_item_size)) if child_item_size + buffered_item_size <= self.max_size_bytes
                        );
                        child_matches_accumulator && smaller_than_max_size_bytes
                    };

                    if should_merge {
                        let merged_result = Some(Arc::new(
                            ScanTask::merge(accumulator.as_ref(), child_item.as_ref())
                                .expect("ScanTasks should be mergeable in MergeByFileSize"),
                        ));

                        // Whether or not we should immediately yield the merged result, or keep accumulating
                        let should_yield = matches!(
                            (child_item.size_bytes(), accumulator.size_bytes()),
                            (Some(child_item_size), Some(buffered_item_size)) if child_item_size + buffered_item_size >= self.min_size_bytes
                        );

                        // Either yield eagerly, or keep looping with a merged accumulator
                        if should_yield {
                            return Ok(merged_result).transpose();
                        } else {
                            self.accumulator = merged_result;
                            continue;
                        }
                    } else {
                        self.accumulator = Some(child_item);
                        return Some(Ok(accumulator));
                    }
                }
                // Bubble up errors from child iterator, making sure to replace the accumulator which we moved
                (Some(Err(e)), acc) => {
                    self.accumulator = acc;
                    return Some(Err(e));
                }
                // Iterator ran out of elements: ensure that we flush the last buffered ScanTask
                (None, Some(last_scan_task)) => {
                    return Some(Ok(last_scan_task));
                }
                (None, None) => {
                    return None;
                }
            }
        }
    }
}

pub fn split_by_row_groups(
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
                    */
                    if let (
                        FileFormatConfig::Parquet(_),
                        StorageConfig::Native(_),
                        [source],
                        Some(None),
                        None,
                    ) = (
                        t.file_format_config.as_ref(),
                        t.storage_config.as_ref(),
                        &t.sources[..],
                        t.sources.get(0).map(DataFileSource::get_chunk_spec),
                        t.pushdowns.limit,
                    ) && source.get_size_bytes().map_or(true, |s| s > max_size_bytes as u64) {
                        let (io_runtime, io_client) =
                            t.storage_config.get_io_client_and_runtime()?;

                        let path = source.get_path();

                        let io_stats =
                            IOStatsContext::new(format!("split_by_row_groups for {:#?}", path));

                        let runtime_handle = io_runtime.handle();

                        let file = runtime_handle.block_on(read_parquet_metadata(
                            path,
                            io_client,
                            Some(io_stats),
                        ))?;

                        let mut new_tasks: Vec<DaftResult<ScanTaskRef>> = Vec::new();
                        let mut curr_row_groups = Vec::new();
                        let mut curr_size_bytes = 0;

                        for (i, rg) in file.row_groups.iter().enumerate() {
                            curr_row_groups.push(i as i64);
                            curr_size_bytes += rg.compressed_size();

                            if curr_size_bytes >= min_size_bytes || i == file.row_groups.len() - 1 {
                                let mut new_source = source.clone();
                                match &mut new_source {
                                    DataFileSource::AnonymousDataFile {
                                        chunk_spec,
                                        size_bytes,
                                        ..
                                    }
                                    | DataFileSource::CatalogDataFile {
                                        chunk_spec,
                                        size_bytes,
                                        ..
                                    } => {
                                        *chunk_spec = Some(ChunkSpec::Parquet(curr_row_groups));
                                        *size_bytes = Some(curr_size_bytes as u64);

                                        curr_row_groups = Vec::new();
                                        curr_size_bytes = 0;
                                    }
                                };

                                new_tasks.push(Ok(ScanTask::new(
                                    vec![new_source],
                                    t.file_format_config.clone(),
                                    t.schema.clone(),
                                    t.storage_config.clone(),
                                    t.pushdowns.clone(),
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
