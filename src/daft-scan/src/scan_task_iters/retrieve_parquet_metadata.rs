use std::collections::{HashMap, VecDeque};

use common_error::DaftResult;
use common_runtime::get_io_runtime;
use daft_io::{get_io_client, IOStatsContext};
use daft_parquet::read::read_parquet_metadata;
use futures::future::try_join_all;
use itertools::Itertools;
use parquet2::metadata::FileMetaData;

use super::{
    mark_scan_tasks_for_split::ScanTaskSplitDecision,
    split_parquet_files_by_rowgroup::SplittableScanTaskRef,
};

/// Performs Parquet metadata retrieval for the provided iterator of [`NeedsParquetMetadata`]s
///
/// This will transform each [`NeedsParquetMetadata`] into its specified [`NeedsParquetMetadata::WithMetadata`] by
/// performing batched Parquet metadata retrieval whenever [`NeedsParquetMetadata::should_fetch`] is ``true``.
pub fn batched_parquet_metadata_retrieval<'cfg>(
    inputs: impl Iterator<Item = DaftResult<ScanTaskSplitDecision<'cfg>>>,
    max_retrieval_batch_size: usize,
) -> impl Iterator<Item = DaftResult<SplittableScanTaskRef<'cfg>>> {
    FetchParquetMetadataByWindows {
        inputs,
        state: State::ConstructingWindow(Vec::new()),
        max_num_fetches_per_window: max_retrieval_batch_size,
    }
}

struct FetchParquetMetadataByWindows<'cfg, InputIter>
where
    InputIter: Iterator<Item = DaftResult<ScanTaskSplitDecision<'cfg>>>,
{
    inputs: InputIter,
    state: State<'cfg>,
    max_num_fetches_per_window: usize,
}

enum State<'cfg> {
    ConstructingWindow(Vec<ScanTaskSplitDecision<'cfg>>),
    WindowFinalized(Vec<ScanTaskSplitDecision<'cfg>>),
    WindowWithMetadata(VecDeque<SplittableScanTaskRef<'cfg>>),
    Exhausted,
}

impl<'cfg, InputIter> Iterator for FetchParquetMetadataByWindows<'cfg, InputIter>
where
    InputIter: Iterator<Item = DaftResult<ScanTaskSplitDecision<'cfg>>>,
{
    type Item = DaftResult<SplittableScanTaskRef<'cfg>>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.state.exhausted() {
            // Advance the state
            let result = self
                .state
                .advance(&mut self.inputs, &self.max_num_fetches_per_window);

            // Return a result (if emitted as a result of state advancement)
            if let Some(result) = result {
                return Some(result);
            }
        }
        None
    }
}

impl<'cfg> State<'cfg> {
    /// Advances the state machine forward by one tick.
    ///
    /// May emit [`T::WithMetadata`] as a result of advancement. If ``None`` is emitted, it does not
    /// mean that the data has been exhausted.
    ///
    /// To check whether the state will return ``None`` in perpetuity, use [`Self::exhausted`]
    pub fn advance(
        &mut self,
        inputs: &mut impl Iterator<Item = DaftResult<ScanTaskSplitDecision<'cfg>>>,
        max_num_needs_metadatas_per_window: &usize,
    ) -> Option<DaftResult<SplittableScanTaskRef<'cfg>>> {
        match &self {
            Self::ConstructingWindow(_) => {
                let next_input = match inputs.next().transpose() {
                    Err(e) => return Some(Err(e)),
                    Ok(next_input) => next_input,
                };
                self.handle_constructing_window(next_input, max_num_needs_metadatas_per_window);
                None
            }
            Self::WindowFinalized(_) => match self.handle_window_finalized() {
                Err(e) => Some(Err(e)),
                Ok(()) => None,
            },
            Self::WindowWithMetadata(_) => self.handle_window_fetched(),
            Self::Exhausted => None,
        }
    }

    /// Check whether the state is exhausted ([`Self::advance`] will return ``None`` in perpetuity).
    pub fn exhausted(&self) -> bool {
        matches!(self, Self::Exhausted)
    }

    fn set_state(&mut self, state: Self) {
        *self = state;
    }

    fn handle_constructing_window(
        &mut self,
        next_input: Option<ScanTaskSplitDecision<'cfg>>,
        max_num_fetches_per_window: &usize,
    ) {
        let accumulating_window = if let Self::ConstructingWindow(accumulating_window) = self {
            accumulating_window
        } else {
            unreachable!(
                "handle_constructing_window must be called from the ConstructingWindow state"
            )
        };

        let next_state = if let Some(next_input) = next_input {
            accumulating_window.push(next_input);

            // Windows are "finalized" when there are sufficient needs_metadatas in the window
            let should_finalize_window = accumulating_window
                .iter()
                .filter(|input| input.should_fetch_parquet_metadata())
                .count()
                >= *max_num_fetches_per_window;
            if should_finalize_window {
                let window = std::mem::take(accumulating_window);
                Some(Self::WindowFinalized(window))
            } else {
                None
            }
        } else if !accumulating_window.is_empty() {
            Some(Self::WindowFinalized(std::mem::take(accumulating_window)))
        } else {
            Some(Self::Exhausted)
        };

        if let Some(next_state) = next_state {
            self.set_state(next_state);
        }
    }

    fn handle_window_finalized(&mut self) -> DaftResult<()> {
        let finalized_window = if let Self::WindowFinalized(finalized_window) = self {
            finalized_window
        } else {
            unreachable!("handle_window_finalized must be called from the WindowFinalized state")
        };

        let finalized_window = std::mem::take(finalized_window);
        let inputs_to_fetch_metadata = finalized_window
            .iter()
            .enumerate()
            .filter_map(|(idx, needs_metadata)| {
                if needs_metadata.should_fetch_parquet_metadata() {
                    Some((idx, needs_metadata))
                } else {
                    None
                }
            })
            .collect_vec();

        // Perform a bulk Parquet metadata fetch
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

        // Error handling: set to Exhausted state and propagate error
        let file_metadatas = match file_metadatas {
            Err(e) => {
                self.set_state(Self::Exhausted);
                return Err(e);
            }
            Ok(file_metadatas) => file_metadatas,
        };

        // Apply the retrieved FileMetaDatas to each NeedsMetadata input
        let mut file_metadatas: HashMap<usize, FileMetaData> = inputs_to_fetch_metadata
            .iter()
            .map(|x| x.0)
            .zip(file_metadatas)
            .collect();
        let fetched_window = finalized_window
            .into_iter()
            .enumerate()
            .map(|(idx, needs_metadata)| needs_metadata.apply_metadata(file_metadatas.remove(&idx)))
            .collect();

        // Move to next state: WindowWithMetadata
        self.set_state(Self::WindowWithMetadata(fetched_window));
        Ok(())
    }

    fn handle_window_fetched(&mut self) -> Option<DaftResult<SplittableScanTaskRef<'cfg>>> {
        let fetched_window = if let Self::WindowWithMetadata(fetched_window) = self {
            fetched_window
        } else {
            unreachable!("handle_window_fetched must be called from the WindowWithMetadata state")
        };

        if let Some(item) = fetched_window.pop_front() {
            Some(Ok(item))
        } else {
            self.set_state(Self::ConstructingWindow(Vec::new()));
            None
        }
    }
}
