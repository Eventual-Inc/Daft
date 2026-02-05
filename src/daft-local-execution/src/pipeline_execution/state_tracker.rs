use std::{num::NonZeroUsize, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::buffer::RowBasedBuffer;

/// Tracks state for pipeline execution, managing available states,
/// completion status, and buffered partitions for a given input_id.
pub(crate) struct StateTracker<S> {
    // Total number of states created (used to check if all are available)
    total_states: usize,
    // Available states (states currently not in use)
    available_states: Vec<S>,
    // Whether this input_id has received flush (is completed)
    is_completed: bool,
    // Buffered partitions waiting for a state to become available
    buffered_partitions: RowBasedBuffer,
}

impl<S> StateTracker<S> {
    /// Create a new StateTracker with the given states and buffer bounds.
    /// For blocking sinks that don't need batching, use lower=0 and upper=usize::MAX.
    pub(crate) fn new(states: Vec<S>, lower_bound: usize, upper_bound: NonZeroUsize) -> Self {
        Self {
            total_states: states.len(),
            available_states: states,
            is_completed: false,
            buffered_partitions: RowBasedBuffer::new(lower_bound, upper_bound),
        }
    }

    pub(crate) fn has_available_states(&self) -> bool {
        !self.available_states.is_empty()
    }

    pub(crate) fn take_state(&mut self) -> Option<S> {
        self.available_states.pop()
    }

    pub(crate) fn return_state(&mut self, state: S) {
        self.available_states.push(state);
    }

    pub(crate) fn all_states_available(&self) -> bool {
        self.available_states.len() == self.total_states
    }

    pub(crate) fn take_all_states(self) -> Vec<S> {
        self.available_states
    }

    pub(crate) fn mark_completed(&mut self) {
        self.is_completed = true;
    }

    pub(crate) fn is_completed(&self) -> bool {
        self.is_completed
    }

    pub(crate) fn buffer_partition(&mut self, partition: Arc<MicroPartition>) {
        self.buffered_partitions.push(partition);
    }

    pub(crate) fn has_buffered_partitions(&self) -> bool {
        !self.buffered_partitions.is_empty()
    }

    /// Get the next batch from the buffer if it's ready (within bounds).
    pub(crate) fn next_batch_if_ready(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        self.buffered_partitions.next_batch_if_ready()
    }

    /// Pop all buffered partitions regardless of bounds.
    pub(crate) fn pop_all(&mut self) -> DaftResult<Option<Arc<MicroPartition>>> {
        self.buffered_partitions.pop_all()
    }

    /// Update the buffer bounds.
    pub(crate) fn update_bounds(&mut self, lower: usize, upper: NonZeroUsize) {
        self.buffered_partitions.update_bounds(lower, upper);
    }
}
