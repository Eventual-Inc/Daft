use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_io::IOStatsContext;
use daft_parquet::read::read_parquet_metadata;

use crate::{
    file_format::{FileFormatConfig, ParquetSourceConfig},
    storage_config::StorageConfig,
    ChunkSpec, DataSource, ScanTask, ScanTaskRef,
};

type BoxScanTaskIter<'a> = Box<dyn Iterator<Item = DaftResult<ScanTaskRef>> + 'a>;

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
pub fn merge_by_sizes<'a>(
    scan_tasks: BoxScanTaskIter<'a>,
    cfg: &'a DaftExecutionConfig,
) -> BoxScanTaskIter<'a> {
    Box::new(MergeByFileSize {
        iter: scan_tasks,
        cfg,
        accumulator: None,
    })
}

struct MergeByFileSize<'a> {
    iter: BoxScanTaskIter<'a>,
    cfg: &'a DaftExecutionConfig,

    // Current element being accumulated on
    accumulator: Option<ScanTaskRef>,
}

impl<'a> MergeByFileSize<'a> {
    fn accumulator_ready(&self) -> bool {
        if let Some(acc) = &self.accumulator
            && let Some(acc_bytes) = acc.estimate_in_memory_size_bytes(Some(self.cfg))
            && acc_bytes >= self.cfg.scan_tasks_min_size_bytes
        {
            true
        } else {
            false
        }
    }

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

        let sum_smaller_than_max_size_bytes = if let Some(child_bytes) =
            other.estimate_in_memory_size_bytes(Some(self.cfg))
            && let Some(accumulator_bytes) =
                accumulator.estimate_in_memory_size_bytes(Some(self.cfg))
        {
            child_bytes + accumulator_bytes <= self.cfg.scan_tasks_max_size_bytes
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
            if self.accumulator.is_none() {
                self.accumulator = match self.iter.next() {
                    Some(Ok(item)) => Some(item),
                    e @ Some(Err(_)) => return e,
                    None => return None,
                };
            }

            if self.accumulator_ready() {
                return self.accumulator.take().map(Ok);
            }

            let next_item = match self.iter.next() {
                Some(Ok(item)) => item,
                e @ Some(Err(_)) => return e,
                None => return self.accumulator.take().map(Ok),
            };

            if next_item
                .estimate_in_memory_size_bytes(Some(self.cfg))
                .is_none()
                || !self.can_merge(&next_item)
            {
                return self.accumulator.replace(next_item).map(Ok);
            }

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
                    ) = (
                        t.file_format_config.as_ref(),
                        t.storage_config.as_ref(),
                        &t.sources[..],
                        t.sources.first().map(DataSource::get_chunk_spec),
                        t.pushdowns.limit,
                    ) && source
                        .get_size_bytes()
                        .map_or(true, |s| s > max_size_bytes as u64)
                      && source
                        .get_iceberg_delete_files()
                        .map_or(true, |f| f.is_empty())
                    {
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
                            field_id_mapping.clone(),
                        ))?;

                        let mut new_tasks: Vec<DaftResult<ScanTaskRef>> = Vec::new();
                        let mut curr_row_groups = Vec::new();
                        let mut curr_size_bytes = 0;
                        let mut curr_num_rows = 0;

                        for (i, rg) in file.row_groups.iter().enumerate() {
                            curr_row_groups.push(i as i64);
                            curr_size_bytes += rg.compressed_size();
                            curr_num_rows += rg.num_rows();

                            if curr_size_bytes >= min_size_bytes || i == file.row_groups.len() - 1 {
                                let mut new_source = source.clone();

                                if let DataSource::File {
                                    chunk_spec,
                                    size_bytes,
                                    ..
                                } = &mut new_source
                                {
                                    *chunk_spec = Some(ChunkSpec::Parquet(curr_row_groups));
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
                                curr_size_bytes = 0;
                                curr_num_rows = 0;

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
