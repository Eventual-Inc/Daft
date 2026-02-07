use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::{buffer::RowBasedBuffer, pipeline::MorselSizeRequirement, pipeline_message::InputId};

/// completion status, and buffered partitions for a given input_id.
pub(crate) struct StateTracker<S> {
    // Total number of states created (used to check if all are available)
    total_states: usize,
    // Available states (states currently not in use)
    available_states: Vec<S>,
    // Whether this input_id has received flush (is completed)
    is_completed: bool,
    // Buffered morsels waiting to hit batch size or task availability
    buffer: RowBasedBuffer,
}

impl<S> StateTracker<S> {
    pub(crate) fn new(available_states: Vec<S>, buffer: RowBasedBuffer) -> Self {
        let total_states = available_states.len();
        Self {
            total_states,
            available_states,
            is_completed: false,
            buffer,
        }
    }
}

/// Wrapper around HashMap<InputId, StateTracker<S>> that provides
/// a clean API for state management, buffered partition processing, and finalization logic.
pub(crate) struct InputStatesTracker<S> {
    trackers: HashMap<InputId, StateTracker<S>>,
    state_creator: Box<dyn Fn(InputId) -> DaftResult<StateTracker<S>> + Send + Sync>,
}

impl<S> InputStatesTracker<S> {
    /// Create a new InputStatesTracker with a state creation function.
    /// The state creator will be called when the first partition for an input_id arrives,
    /// and will receive the input_id as a parameter.
    pub(crate) fn new(
        state_creator: Box<dyn Fn(InputId) -> DaftResult<StateTracker<S>> + Send + Sync>,
    ) -> Self {
        Self {
            trackers: HashMap::new(),
            state_creator,
        }
    }

    /// Buffer a partition for the given input_id.
    /// If the tracker doesn't exist, calls the stored state_creator to create it.
    /// Always buffers the partition (implements "always buffer first" pattern).
    pub(crate) fn buffer_partition(
        &mut self,
        input_id: InputId,
        partition: Arc<MicroPartition>,
    ) -> DaftResult<()> {
        if !self.trackers.contains_key(&input_id) {
            let tracker = (self.state_creator)(input_id)?;
            self.trackers.insert(input_id, tracker);
        }
        let tracker = self
            .trackers
            .get_mut(&input_id)
            .expect("Tracker should exist after insertion");
        tracker.buffer.push(partition);
        Ok(())
    }

    /// Check if finalization can proceed for the given input_id.
    /// Returns Some(states) if finalization is ready (completed, all states available, no buffered partitions),
    /// and removes the tracker entry. Returns None otherwise.
    pub(crate) fn try_take_states_for_finalize(&mut self, input_id: InputId) -> Option<Vec<S>> {
        let tracker = self.trackers.get(&input_id)?;
        let should_finalize = tracker.is_completed
            && tracker.available_states.len() == tracker.total_states
            && tracker.buffer.is_empty();

        if should_finalize {
            let tracker = self.trackers.remove(&input_id)?;
            Some(tracker.available_states)
        } else {
            None
        }
    }

    /// Get the next partition/batch and state for execution if available.
    /// Returns Some((partition, state)) if buffered partitions and available states exist.
    /// Returns None otherwise.
    ///
    /// This method respects batching bounds. For blocking sinks with bounds (0, usize::MAX),
    /// it behaves the same as popping all partitions. For streaming sinks, it respects
    /// dynamic bounds for proper batching.
    pub(crate) fn get_next_morsel_for_execute(
        &mut self,
        input_id: InputId,
    ) -> Option<DaftResult<(Arc<MicroPartition>, S)>> {
        let tracker = match self.trackers.get_mut(&input_id) {
            Some(tracker) => tracker,
            None => return None,
        };

        if tracker.buffer.is_empty() || tracker.available_states.is_empty() {
            return None;
        }

        let batch = match tracker.buffer.next_batch_if_ready() {
            Ok(Some(batch)) => Some(batch),
            Ok(None) => {
                // If tracker is completed and has buffered partitions but next_batch_if_ready returned None,
                // try pop_all to get all remaining partitions
                if tracker.is_completed && !tracker.buffer.is_empty() {
                    tracker.buffer.pop_all().ok()?
                } else {
                    None
                }
            }
            Err(e) => return Some(Err(e)),
        };

        // Only take state if we have a batch
        let state = if batch.is_some() {
            tracker.available_states.pop()
        } else {
            None
        };

        match (batch, state) {
            (Some(batch), Some(state)) => Some(Ok((batch, state))),
            (Some(_batch), None) => unreachable!(),
            (None, Some(_state)) => unreachable!(),
            (None, None) => None,
        }
    }

    /// Return a state after task completion
    pub(crate) fn return_state(&mut self, input_id: InputId, state: S) {
        if let Some(tracker) = self.trackers.get_mut(&input_id) {
            tracker.available_states.push(state);
        }
    }

    /// Mark an input_id as completed (flush received)
    pub(crate) fn mark_completed(&mut self, input_id: InputId) {
        if let Some(tracker) = self.trackers.get_mut(&input_id) {
            tracker.is_completed = true;
        }
    }

    /// Mark all trackers as completed
    pub(crate) fn mark_all_completed(&mut self) {
        for tracker in self.trackers.values_mut() {
            if !tracker.is_completed {
                tracker.is_completed = true;
            }
        }
    }

    /// Check if there are any unfinalized states remaining
    pub(crate) fn is_empty(&self) -> bool {
        self.trackers.is_empty()
    }

    /// Update bounds for all input_ids (used by streaming sinks)
    pub(crate) fn update_all_bounds(&mut self, morsel_size_requirement: MorselSizeRequirement) {
        for tracker in self.trackers.values_mut() {
            tracker.buffer.update_bounds(morsel_size_requirement);
        }
    }

    /// Check if a tracker exists for the given input_id
    pub(crate) fn contains_key(&self, input_id: InputId) -> bool {
        self.trackers.contains_key(&input_id)
    }

    /// Get all input_ids that have trackers
    pub(crate) fn input_ids(&self) -> Vec<InputId> {
        self.trackers.keys().copied().collect()
    }
}
