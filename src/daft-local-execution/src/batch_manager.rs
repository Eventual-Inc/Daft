use std::{collections::HashMap, time::Duration};

use common_error::DaftResult;
use daft_micropartition::MicroPartition;

use crate::{
    buffer::RowBasedBuffer,
    dynamic_batching::{BatchingState, BatchingStrategy},
    pipeline::{InputId, MorselSizeRequirement},
    runtime_stats::RuntimeStats,
};

struct InputBuffer {
    buffer: RowBasedBuffer,
    pending_flush: bool,
}

/// Manages per-input buffering and batch extraction for pipeline operators.
///
/// `BatchManager` is the single abstraction for getting data in and out of an operator's
/// input buffers. It owns per-input `RowBasedBuffer`s, manages flush lifecycle,
/// and delegates batch extraction to a pluggable `BatchingStrategy`.
///
/// # Usage
/// ```rust,ignore
/// let mut manager = BatchManager::new(strategy);
///
/// // Data in
/// manager.push(input_id, partition);
///
/// // Data out (strategy-controlled, flush-aware)
/// while let Some(batch) = manager.next_batch(input_id)? {
///     // spawn worker with batch...
/// }
///
/// // Worker completes
/// manager.record_completion(stats, batch_size, duration);
/// ```
pub struct BatchManager<S: BatchingStrategy> {
    state: S::State,
    pub(crate) strategy: S,
    inputs: HashMap<InputId, InputBuffer>,
    current_requirements: MorselSizeRequirement,
}

impl<S> BatchManager<S>
where
    S: BatchingStrategy + 'static,
    S::State: 'static,
{
    /// Creates a new `BatchManager` with the given batching strategy.
    /// Initializes strategy state and sets initial batch size requirements.
    pub fn new(strategy: S) -> Self {
        let state = strategy.make_state();
        let current_requirements = strategy.initial_requirements();

        Self {
            state,
            strategy,
            inputs: HashMap::new(),
            current_requirements,
        }
    }

    /// Buffers an incoming partition for the given input. Creates the input's
    /// buffer on first push, sized according to the current batch requirements.
    pub fn push(&mut self, input_id: InputId, partition: MicroPartition) {
        let input = self.inputs.entry(input_id).or_insert_with(|| {
            let (lower, upper) = self.current_requirements.values();
            InputBuffer {
                buffer: RowBasedBuffer::new(lower, upper),
                pending_flush: false,
            }
        });
        input.buffer.push(partition);
    }

    /// Extracts the next batch for the given input by delegating to the
    /// batching strategy. Returns `None` when the buffer doesn't have enough
    /// data to form a batch. During flush, drains any remaining buffered data
    /// regardless of batch size requirements.
    pub fn next_batch(&mut self, input_id: InputId) -> DaftResult<Option<MicroPartition>> {
        let input = self
            .inputs
            .get_mut(&input_id)
            .expect("Input should be present");
        input.buffer.update_bounds(self.current_requirements);

        let batch = self
            .strategy
            .next_batch(&mut self.state, &mut input.buffer)?;
        match batch {
            Some(b) => Ok(Some(b)),
            None if input.pending_flush => input.buffer.pop_all(),
            None => Ok(None),
        }
    }

    /// Records execution metrics from a completed worker and recalculates
    /// batch size requirements. The updated requirements are applied lazily
    /// on the next `next_batch` call.
    pub fn record_completion(
        &mut self,
        stats: &dyn RuntimeStats,
        batch_size: usize,
        duration: Duration,
    ) {
        self.state
            .record_execution_stat(stats, batch_size, duration);
        self.current_requirements = self.strategy.calculate_new_requirements(&mut self.state);
    }

    /// Signals that the given input has finished sending data. `next_batch`
    /// will drain remaining buffered data, and `can_flush` will return true
    /// once the buffer is empty.
    pub fn set_pending_flush(&mut self, input_id: InputId) {
        if let Some(input) = self.inputs.get_mut(&input_id) {
            input.pending_flush = true;
        }
    }

    /// Returns true when the input's buffer is fully drained after a flush signal.
    /// The caller must separately verify that all in-flight workers have completed
    /// before propagating the flush downstream.
    pub fn can_flush(&self, input_id: InputId) -> bool {
        self.inputs
            .get(&input_id)
            .is_some_and(|input| input.pending_flush && input.buffer.is_empty())
    }

    /// Removes the input and returns any remaining buffered data.
    /// If the input was fully drained (e.g. after `can_flush` returned true),
    /// this returns `Ok(None)`.
    pub fn drain(&mut self, input_id: InputId) -> DaftResult<Option<MicroPartition>> {
        if let Some(mut input) = self.inputs.remove(&input_id) {
            input.buffer.pop_all()
        } else {
            Ok(None)
        }
    }

    /// Returns true if the given input has been seen (via `push`).
    pub fn has_input(&self, input_id: InputId) -> bool {
        self.inputs.contains_key(&input_id)
    }

    /// Returns an iterator over all active input IDs.
    pub fn input_ids(&self) -> impl Iterator<Item = InputId> + '_ {
        self.inputs.keys().copied()
    }

    /// Signals flush for all active inputs. Used when the input channel closes.
    pub fn set_all_pending_flush(&mut self) {
        for input in self.inputs.values_mut() {
            input.pending_flush = true;
        }
    }

    #[cfg(test)]
    pub fn initial_requirements(&self) -> MorselSizeRequirement {
        self.strategy.initial_requirements()
    }

    #[cfg(test)]
    pub fn current_requirements(&self) -> MorselSizeRequirement {
        self.current_requirements
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use common_metrics::{Meter, ops::NodeInfo};

    use super::*;
    use crate::{dynamic_batching::StaticBatchingStrategy, runtime_stats::RuntimeStats};

    pub(crate) struct MockRuntimeStats;
    impl RuntimeStats for MockRuntimeStats {
        fn new(_meter: &Meter, _node_info: &NodeInfo) -> Self {
            Self {}
        }

        fn build_snapshot(
            &self,
            _ordering: std::sync::atomic::Ordering,
        ) -> common_metrics::StatSnapshot {
            unimplemented!()
        }

        fn add_rows_in(&self, _rows: u64) {
            unimplemented!()
        }

        fn add_rows_out(&self, _rows: u64) {
            unimplemented!()
        }

        fn add_duration_us(&self, _cpu_us: u64) {
            unimplemented!()
        }
    }

    struct MockBatchingState {
        measurement_count: usize,
    }

    impl BatchingState for MockBatchingState {
        fn record_execution_stat(
            &mut self,
            _stats: &dyn RuntimeStats,
            _batch_size: usize,
            _duration: Duration,
        ) {
            self.measurement_count += 1;
        }
    }

    #[derive(Clone, Debug)]
    struct MockBatchingStrategy {
        call_counter: Arc<AtomicUsize>,
        initial_req: MorselSizeRequirement,
    }

    impl MockBatchingStrategy {
        fn new(initial_req: MorselSizeRequirement) -> Self {
            Self {
                call_counter: Arc::new(AtomicUsize::new(0)),
                initial_req,
            }
        }

        fn call_count(&self) -> usize {
            self.call_counter.load(Ordering::SeqCst)
        }
    }

    impl BatchingStrategy for MockBatchingStrategy {
        type State = MockBatchingState;

        fn make_state(&self) -> Self::State {
            MockBatchingState {
                measurement_count: 0,
            }
        }

        fn initial_requirements(&self) -> MorselSizeRequirement {
            self.initial_req
        }

        fn calculate_new_requirements(&self, state: &mut Self::State) -> MorselSizeRequirement {
            self.call_counter.fetch_add(1, Ordering::SeqCst);

            match state.measurement_count {
                0 => self.initial_req,
                1 => MorselSizeRequirement::Flexible(1, NonZeroUsize::new(10).unwrap()),
                2..=5 => MorselSizeRequirement::Flexible(5, NonZeroUsize::new(20).unwrap()),
                _ => MorselSizeRequirement::Flexible(10, NonZeroUsize::new(50).unwrap()),
            }
        }
    }

    #[test]
    fn test_batch_manager_creation() {
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(
            1,
            NonZeroUsize::new(32).unwrap(),
        ));
        let manager = BatchManager::new(strategy.clone());

        assert_eq!(
            manager.initial_requirements(),
            MorselSizeRequirement::Flexible(1, NonZeroUsize::new(32).unwrap())
        );
        assert_eq!(strategy.call_count(), 0);
    }

    #[test]
    fn test_batch_manager_no_completions() {
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(
            2,
            NonZeroUsize::new(64).unwrap(),
        ));
        let manager = BatchManager::new(strategy.clone());

        assert_eq!(
            manager.current_requirements(),
            MorselSizeRequirement::Flexible(2, NonZeroUsize::new(64).unwrap())
        );
        assert_eq!(strategy.call_count(), 0);
    }

    #[test]
    fn test_batch_manager_record_completion() {
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(
            1,
            NonZeroUsize::new(16).unwrap(),
        ));
        let mut manager = BatchManager::new(strategy.clone());

        manager.record_completion(
            Arc::new(MockRuntimeStats).as_ref(),
            32,
            Duration::from_millis(100),
        );

        assert_eq!(
            manager.current_requirements(),
            MorselSizeRequirement::Flexible(1, NonZeroUsize::new(10).unwrap())
        );
        assert_eq!(strategy.call_count(), 1);
    }

    #[test]
    fn test_batch_manager_multiple_completions() {
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(
            1,
            NonZeroUsize::new(8).unwrap(),
        ));
        let mut manager = BatchManager::new(strategy.clone());

        manager.record_completion(
            Arc::new(MockRuntimeStats).as_ref(),
            10,
            Duration::from_millis(50),
        );
        assert_eq!(
            manager.current_requirements(),
            MorselSizeRequirement::Flexible(1, NonZeroUsize::new(10).unwrap())
        );

        manager.record_completion(
            Arc::new(MockRuntimeStats).as_ref(),
            20,
            Duration::from_millis(75),
        );
        assert_eq!(
            manager.current_requirements(),
            MorselSizeRequirement::Flexible(5, NonZeroUsize::new(20).unwrap())
        );
        assert_eq!(strategy.call_count(), 2);
    }

    #[test]
    fn test_batch_manager_accumulates_measurements() {
        let strategy = MockBatchingStrategy::new(MorselSizeRequirement::Flexible(
            2,
            NonZeroUsize::new(8).unwrap(),
        ));
        let mut manager = BatchManager::new(strategy.clone());

        manager.record_completion(
            Arc::new(MockRuntimeStats).as_ref(),
            10,
            Duration::from_millis(30),
        );
        assert_eq!(
            manager.current_requirements(),
            MorselSizeRequirement::Flexible(1, NonZeroUsize::new(10).unwrap())
        );

        manager.record_completion(
            Arc::new(MockRuntimeStats).as_ref(),
            15,
            Duration::from_millis(40),
        );
        manager.record_completion(
            Arc::new(MockRuntimeStats).as_ref(),
            20,
            Duration::from_millis(60),
        );
        assert_eq!(
            manager.current_requirements(),
            MorselSizeRequirement::Flexible(5, NonZeroUsize::new(20).unwrap())
        );
        assert_eq!(strategy.call_count(), 3);
    }

    #[test]
    fn test_batch_manager_with_static_strategy() {
        let static_req = MorselSizeRequirement::Flexible(16, NonZeroUsize::new(128).unwrap());
        let strategy = StaticBatchingStrategy::new(static_req);
        let mut manager = BatchManager::new(strategy);

        assert_eq!(manager.initial_requirements(), static_req);

        manager.record_completion(
            Arc::new(MockRuntimeStats).as_ref(),
            64,
            Duration::from_millis(200),
        );

        assert_eq!(manager.current_requirements(), static_req);
    }
}
