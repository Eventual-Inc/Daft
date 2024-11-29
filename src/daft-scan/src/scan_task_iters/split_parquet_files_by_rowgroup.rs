use std::sync::Arc;

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use daft_stats::TableMetadata;
use parquet2::metadata::{FileMetaData, RowGroupList};

use super::BoxScanTaskIter;
use crate::{ChunkSpec, DataSource, ScanTask, ScanTaskRef};

/// Performs splitting of an iterator of [`ScanTaskSplitByRowgroup`] into a flattened iterator of [`ScanTaskRef`].
#[must_use]
pub(crate) fn split_by_rowgroup<'cfg>(
    splitters: impl Iterator<Item = DaftResult<SplittableScanTaskRef<'cfg>>> + 'cfg,
) -> BoxScanTaskIter<'cfg> {
    Box::new(
        splitters
            .into_iter()
            .flat_map(|scan_task_to_split| match scan_task_to_split {
                Err(e) => EitherOnceOrMany::Once(std::iter::once(Err(e))),
                Ok(scan_task_to_split) => {
                    EitherOnceOrMany::Many(scan_task_to_split.into_iter().map(Ok))
                }
            }),
    ) as BoxScanTaskIter
}

/// A ScanTask that can be split by Parquet rowgroups
pub(crate) enum SplittableScanTaskRef<'cfg> {
    NoSplit(ScanTaskRef),
    Split(ScanTaskRef, FileMetaData, &'cfg DaftExecutionConfig),
}

/// Iterator for [`SplittableScanTaskRef`] which produces [`ScanTaskRef`]
pub(crate) enum SplittableScanTaskRefIterator<'cfg> {
    NoSplitIterOnce(ScanTaskRef, bool),
    SplitIterMany(ParquetFileSplitter<'cfg>),
}

/// Iterator that performs a split on the provided [`ScanTaskRef`]
pub(crate) struct ParquetFileSplitter<'cfg> {
    // Iterator state that tracks iteration progress
    current_idx: usize,
    accumulated_rowgroup_ids: Vec<usize>,
    estimated_accumulated_materialized_size: f64,

    // Cached items
    scan_task: ScanTaskRef,
    file_metadata: FileMetaData,
    config: &'cfg DaftExecutionConfig,

    // Call once and cache this, since it can be expensive when called in a loop
    scan_task_in_memory_size_estimate: Option<usize>,
}

impl<'cfg> IntoIterator for SplittableScanTaskRef<'cfg> {
    type IntoIter = SplittableScanTaskRefIterator<'cfg>;
    type Item = ScanTaskRef;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::NoSplit(st) => SplittableScanTaskRefIterator::NoSplitIterOnce(st, false),
            Self::Split(st, fm, cfg) => {
                let scan_task_in_memory_size_estimate = st.estimate_in_memory_size_bytes(Some(cfg));
                SplittableScanTaskRefIterator::SplitIterMany(ParquetFileSplitter {
                    current_idx: 0,
                    accumulated_rowgroup_ids: Vec::new(),
                    estimated_accumulated_materialized_size: 0.,
                    scan_task: st,
                    file_metadata: fm,
                    config: cfg,
                    scan_task_in_memory_size_estimate,
                })
            }
        }
    }
}

impl<'cfg> Iterator for SplittableScanTaskRefIterator<'cfg> {
    type Item = ScanTaskRef;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::NoSplitIterOnce(st, has_returned) => {
                if !*has_returned {
                    *has_returned = true;
                    Some(st.clone())
                } else {
                    None
                }
            }
            Self::SplitIterMany(splitter) => splitter.next(),
        }
    }
}

impl<'cfg> Iterator for ParquetFileSplitter<'cfg> {
    type Item = ScanTaskRef;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.exhausted() {
            let emitted_result = if self.current_idx < self.file_metadata.row_groups.len() {
                self.accumulate()
            } else {
                self.flush()
            };
            if let Some(emitted_result) = emitted_result {
                return Some(emitted_result);
            }
        }
        None
    }
}

impl<'cfg> ParquetFileSplitter<'cfg> {
    /// Returns whether or not this Splitter is done producing items
    fn exhausted(&self) -> bool {
        self.accumulated_rowgroup_ids.is_empty()
            && self.current_idx >= self.file_metadata.row_groups.len()
    }

    /// Helper to update internal state with the results of retrieving a new rowgroup
    fn update_state(&mut self, new_rg_idx: usize, rg_estimated_in_memory_size_bytes: f64) {
        self.current_idx += 1;
        self.accumulated_rowgroup_ids.push(new_rg_idx);
        self.estimated_accumulated_materialized_size += rg_estimated_in_memory_size_bytes;
    }

    /// Helper to take the currently accumulated data, replacing them with empty accumulators
    fn take_accumulator(&mut self) -> Vec<usize> {
        let accumulated_rowgroup_ids = std::mem::take(&mut self.accumulated_rowgroup_ids);
        self.estimated_accumulated_materialized_size = 0.;

        accumulated_rowgroup_ids
    }

    /// Accumulates the next rowgroup from ``self.file_metadata``
    ///
    /// Optionally emits a [`ScanTaskRef`] if the conditions for flushing the accumulator are met
    fn accumulate(&mut self) -> Option<ScanTaskRef> {
        // Grab the next rowgroup index
        let (rg_idx, rg) = self
            .file_metadata
            .row_groups
            .get_index(self.current_idx)
            .unwrap();
        let total_rg_compressed_size: usize = self
            .file_metadata
            .row_groups
            .iter()
            .map(|rg| rg.1.compressed_size())
            .sum();
        let rg_estimated_in_memory_size_bytes =
            self.scan_task_in_memory_size_estimate
                .map_or(0., |est_materialized_size| {
                    (rg.compressed_size() as f64 / total_rg_compressed_size as f64)
                        * est_materialized_size as f64
                });

        // Update the state of the accumulator
        self.update_state(*rg_idx, rg_estimated_in_memory_size_bytes);

        // If the ScanTask is missing memory estimates for some reason, we will naively split
        if self.scan_task_in_memory_size_estimate.is_none() {
            return self.flush();
        }

        // If the estimated size in bytes of the materialized data is past our configured threshold, we perform a split
        if self.estimated_accumulated_materialized_size as usize
            >= self.config.scan_tasks_min_size_bytes
        {
            return self.flush();
        }

        None
    }

    /// Flushes the accumulator and emits the currently accumulated data as a ScanTaskRef
    fn flush(&mut self) -> Option<ScanTaskRef> {
        // If nothing is accumulated, then return None
        if self.accumulated_rowgroup_ids.is_empty() {
            return None;
        }

        // Retrieve accumulated state, resetting state to null
        let accumulated_rowgroup_ids_to_emit = self.take_accumulator();

        let num_rows = accumulated_rowgroup_ids_to_emit
            .iter()
            .map(|rg_idx| {
                self.file_metadata
                    .row_groups
                    .get(rg_idx)
                    .unwrap()
                    .num_rows()
            })
            .sum();

        // Construct the new ScanTask by mutating a clone of the source ScanTask's DataSource
        let mut new_source = match self.scan_task.sources.as_slice() {
            [source] => source,
            _ => unreachable!(
                "SplitSingleParquetFileByRowGroupsState should only have one DataSource in its ScanTask"
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
                RowGroupList::from_iter(accumulated_rowgroup_ids_to_emit.iter().map(|idx| {
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
                accumulated_rowgroup_ids_to_emit
                    .iter()
                    .map(|&idx| idx as i64)
                    .collect(),
            ));
            *size_bytes = Some(
                accumulated_rowgroup_ids_to_emit
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
            unreachable!("Parquet file format should only be used with DataSource::File");
        }
        let scantask_to_emit = ScanTask::new(
            vec![new_source],
            self.scan_task.file_format_config.clone(),
            self.scan_task.schema.clone(),
            self.scan_task.storage_config.clone(),
            self.scan_task.pushdowns.clone(),
            self.scan_task.generated_fields.clone(),
        )
        .into();

        Some(scantask_to_emit)
    }
}

enum EitherOnceOrMany<T, IterT>
where
    IterT: Iterator<Item = T>,
{
    Once(std::iter::Once<T>),
    Many(IterT),
}

impl<T, IterT> Iterator for EitherOnceOrMany<T, IterT>
where
    IterT: Iterator<Item = T>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Once(it) => it.next(),
            Self::Many(many) => many.next(),
        }
    }
}
