mod dyn_strategy;
mod latency_constrained_strategy;
mod static_strategy;
use std::time::Duration;

use common_error::DaftResult;
use daft_micropartition::MicroPartition;
pub use dyn_strategy::*;
pub use latency_constrained_strategy::*;
pub use static_strategy::*;

use crate::{buffer::RowBasedBuffer, pipeline::MorselSizeRequirement, runtime_stats::RuntimeStats};

#[cfg(not(debug_assertions))]
pub trait BatchingStrategy: Send + Sync {
    type State: BatchingState + Send + Sync + Unpin;
    fn make_state(&self) -> Self::State;
    fn calculate_new_requirements(&self, state: &mut Self::State) -> MorselSizeRequirement;
    fn initial_requirements(&self) -> MorselSizeRequirement;

    fn next_batch(
        &self,
        _state: &mut Self::State,
        buffer: &mut RowBasedBuffer,
    ) -> DaftResult<Option<MicroPartition>> {
        buffer.next_batch_if_ready()
    }
}

/// Pluggable strategy that controls how batches are sized and extracted.
///
/// `BatchManager` calls these methods to decide batch size requirements and to
/// extract batches from per-input buffers. The default `next_batch` implementation
/// uses row-count-based extraction via `RowBasedBuffer::next_batch_if_ready`.
/// Data-aware strategies (e.g. explode) can override `next_batch` to inspect
/// buffer contents and call `buffer.take_rows(n)` with a precisely computed count.
///
/// Bounds are applied to the buffer by `BatchManager` before `next_batch` is called,
/// so strategies that use the default extraction don't need to manage bounds themselves.
#[cfg(debug_assertions)]
pub trait BatchingStrategy: Send + Sync + std::fmt::Debug {
    type State: BatchingState + Send + Sync + Unpin;

    /// Creates a new instance of strategy-specific state.
    fn make_state(&self) -> Self::State;

    /// Recalculates batch size requirements based on accumulated execution metrics.
    /// Called by `BatchManager::record_completion` after each worker finishes.
    fn calculate_new_requirements(&self, state: &mut Self::State) -> MorselSizeRequirement;

    /// Returns the batch size requirements to use before any execution metrics are available.
    fn initial_requirements(&self) -> MorselSizeRequirement;

    /// Extracts the next batch from the buffer. The buffer's bounds have already been
    /// updated by `BatchManager` before this is called. Override this for data-aware
    /// strategies that need to inspect buffer contents to determine batch boundaries.
    fn next_batch(
        &self,
        _state: &mut Self::State,
        buffer: &mut RowBasedBuffer,
    ) -> DaftResult<Option<MicroPartition>> {
        buffer.next_batch_if_ready()
    }
}

/// Accumulator for execution metrics that the strategy uses to adjust batch sizes.
pub trait BatchingState {
    /// Records metrics from a single completed batch execution.
    fn record_execution_stat(
        &mut self,
        stats: &dyn RuntimeStats,
        batch_size: usize,
        duration: Duration,
    );
}
