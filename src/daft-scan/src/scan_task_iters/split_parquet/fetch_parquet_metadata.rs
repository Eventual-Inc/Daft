use std::collections::{HashMap, VecDeque};

use common_error::DaftResult;
use common_runtime::get_io_runtime;
use daft_io::{get_io_client, IOStatsContext};
use daft_parquet::read::read_parquet_metadata;
use futures::future::try_join_all;
use itertools::Itertools;
use parquet2::metadata::FileMetaData;

use super::{
    split_parquet_decision::{self, Decision},
    split_parquet_file,
};
use crate::ScanTaskRef;

/// Retrieves Parquet metadata for the incoming "Decisions".
///
/// # Returns
///
/// Returns [`ParquetSplitScanTaskGenerator`] instances which are themselves iterators that can yield:
/// - A single [`ScanTaskRef`] if no split was needed
/// - Multiple [`ScanTaskRef`]s if the task was split
///
/// # Implementation Details
///
/// Retrieval of Parquet metadata is performed in batches using a windowed approach for efficiency.
pub(super) struct RetrieveParquetMetadataIterator<'cfg> {
    decider: split_parquet_decision::DecideSplitIterator<'cfg>,
    state: State,
    max_num_fetches_per_window: usize,
}

impl<'cfg> RetrieveParquetMetadataIterator<'cfg> {
    pub(super) fn new(decider: split_parquet_decision::DecideSplitIterator<'cfg>) -> Self {
        Self {
            decider,
            max_num_fetches_per_window: 16,
            state: State::new(),
        }
    }

    fn accumulate(&mut self) -> DaftResult<Vec<Decision>> {
        let mut window = Vec::new();

        for item in &mut self.decider {
            let next_decision = item?;

            window.push(next_decision);

            // Windows are "finalized" when there are sufficient needs_metadatas in the window
            let should_finalize_window = window
                .iter()
                .filter(|input| matches!(input, Decision::Split(_)))
                .count()
                >= self.max_num_fetches_per_window;

            if should_finalize_window {
                return Ok(window);
            }
        }

        Ok(window)
    }

    fn batched_parquet_metadata_retrieval(
        &self,
        window: Vec<Decision>,
    ) -> DaftResult<VecDeque<ParquetSplitScanTaskGenerator>> {
        let inputs_to_fetch_metadata = window
            .iter()
            .enumerate()
            .filter_map(|(idx, needs_metadata)| {
                if matches!(needs_metadata, Decision::Split(_)) {
                    Some((idx, needs_metadata))
                } else {
                    None
                }
            })
            .collect_vec();

        // Retrieve FileMetaData as a batched request over an async tokio runtime
        let file_metadatas: DaftResult<Vec<FileMetaData>> = {
            let io_runtime = get_io_runtime(true);
            let io_stats = IOStatsContext::new(format!(
                "Performing batched Parquet metadata read for {} files",
                inputs_to_fetch_metadata.len()
            ));
            let parquet_metadata_futures = inputs_to_fetch_metadata
                .iter()
                .map(|(_, needs_metadata)| {
                    let io_config = needs_metadata.get_io_config();
                    let io_client = get_io_client(true, io_config)?;
                    let io_stats = io_stats.clone();
                    let field_id_mapping = needs_metadata
                        .get_parquet_source_config()
                        .field_id_mapping
                        .clone();
                    let path = needs_metadata.get_parquet_path().to_string();
                    Ok(io_runtime.spawn(async move {
                        read_parquet_metadata(
                            path.as_str(),
                            io_client,
                            Some(io_stats),
                            field_id_mapping,
                        )
                        .await
                    }))
                })
                .collect::<DaftResult<Vec<_>>>();
            parquet_metadata_futures.and_then(|parquet_metadata_futures| {
                io_runtime
                    .block_on_current_thread(try_join_all(parquet_metadata_futures))
                    .and_then(|results| results.into_iter().collect::<Result<Vec<_>, _>>())
            })
        };
        let file_metadatas = file_metadatas?;

        // Apply the retrieved FileMetaDatas to each input Decision
        let mut file_metadatas: HashMap<usize, FileMetaData> = inputs_to_fetch_metadata
            .iter()
            .map(|x| x.0)
            .zip(file_metadatas)
            .collect();
        let fetched_window = window
            .into_iter()
            .enumerate()
            .map(|(idx, needs_metadata)| needs_metadata.apply_metadata(file_metadatas.remove(&idx)))
            .collect();

        Ok(fetched_window)
    }
}

pub(super) enum ParquetSplitScanTaskGenerator {
    NoSplit(std::iter::Once<DaftResult<ScanTaskRef>>),
    Split(split_parquet_file::ParquetFileSplitter),
}

impl<'cfg> Iterator for RetrieveParquetMetadataIterator<'cfg> {
    type Item = ParquetSplitScanTaskGenerator;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.state {
                // Accumulating: accumulate a window.
                //   -> Exhausted: if nothing left to accumulate
                //   -> Accumulated: if there is a window that was successfully accumulated
                //   -> Accumulating: if encountered an error
                State::Accumulating => {
                    let accumulated = self.accumulate();
                    match accumulated {
                        Ok(accumulated) => {
                            if accumulated.is_empty() {
                                self.state = State::Exhausted;
                            } else {
                                self.state = State::Accumulated(accumulated);
                            }
                        }
                        Err(e) => {
                            return Some(ParquetSplitScanTaskGenerator::NoSplit(std::iter::once(
                                Err(e),
                            )));
                        }
                    }
                }
                // Accumulated: retrieve Parquet metadata for the accumulated window
                //   -> Emitting: if successfully retrieved Parquet metadata
                //   -> Accumulating: if encountered an error during retrieval
                State::Accumulated(accumulated_decisions) => {
                    let accumulated_decisions = std::mem::take(accumulated_decisions);
                    let retrieved = self.batched_parquet_metadata_retrieval(accumulated_decisions);
                    match retrieved {
                        Ok(retrieved) => {
                            self.state = State::Emitting(retrieved);
                        }
                        Err(e) => {
                            self.state = State::Accumulating;
                            return Some(ParquetSplitScanTaskGenerator::NoSplit(std::iter::once(
                                Err(e),
                            )));
                        }
                    }
                }
                // Emitting: emit the retrieved data exhaustively
                //  -> Emitting: if still more items to emit
                //  -> Accumulating: if no more items to emit
                State::Emitting(window) => {
                    if let Some(emitted) = window.pop_front() {
                        return Some(emitted);
                    } else {
                        self.state = State::Accumulating;
                    }
                }
                // Exhausted: no more items
                //  -> Exhausted: always
                State::Exhausted => return None,
            }
        }
    }
}

impl Iterator for ParquetSplitScanTaskGenerator {
    type Item = DaftResult<ScanTaskRef>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::NoSplit(iter) => iter.next(),
            Self::Split(iter) => iter.next(),
        }
    }
}

enum State {
    Accumulating,
    Accumulated(Vec<Decision>),
    Emitting(VecDeque<ParquetSplitScanTaskGenerator>),
    Exhausted,
}

impl State {
    fn new() -> Self {
        Self::Accumulating
    }
}
