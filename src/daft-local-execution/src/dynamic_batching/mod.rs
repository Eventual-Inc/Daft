mod aimd;
mod latency_constrained;

use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub use aimd::*;
pub use latency_constrained::*;

use crate::{
    ExecutionRuntimeContext, RuntimeHandle,
    channel::{Receiver, Sender, create_channel},
    pipeline::MorselSizeRequirement,
    runtime_stats::RuntimeStats,
};

/// Trait for algorithms that dynamically adjust batch sizes based on execution performance.
///
/// Dynamic batching is a technique used to optimize throughput and latency by adjusting
/// the number of items processed together in a batch based on runtime performance metrics.
/// Different algorithms use various strategies to find the optimal batch size.
///
///
/// # Examples
///
/// ```rust,ignore
/// let algorithm = AimdBatching::default();
/// let mut state = algorithm.make_state();
/// let rt_stats = DefaultRuntimeStats::default();
///
/// // After processing a batch that took 100ms
/// let new_batch_size = algorithm.adjust_batch_size(&mut state, &rt_stats, Duration::from_millis(100));
/// ```
pub trait BatchingStrategy: Send + Sync {
    type State: Send + Sync + Unpin;
    fn make_state(&self) -> Self::State;
    /// adjust the batch size based on runtime performance metrics
    fn adjust_batch_size(
        &self,
        state: &mut Self::State,
        _runtime_stats: &dyn RuntimeStats,
        current_run_duration: Duration,
    ) -> usize;
    fn current_batch_size(&self) -> usize;
}
pub struct DefaultBatchingStrategy {
    current_batch_size: usize,
}
impl DefaultBatchingStrategy {
    pub fn new(morsel_size_req: &MorselSizeRequirement) -> Self {
        let current_batch_size = match morsel_size_req {
            MorselSizeRequirement::Strict(v) => *v,
            MorselSizeRequirement::Flexible(_, v) => *v,
        };

        DefaultBatchingStrategy { current_batch_size }
    }
}

impl BatchingStrategy for DefaultBatchingStrategy {
    type State = ();
    fn make_state(&self) -> Self::State {}
    fn adjust_batch_size(
        &self,
        _state: &mut Self::State,
        _runtime_stats: &dyn RuntimeStats,
        _current_run_duration: Duration,
    ) -> usize {
        self.current_batch_size
    }
    fn current_batch_size(&self) -> usize {
        self.current_batch_size
    }
}

pub struct BatchingContext<S: BatchingStrategy> {
    current_batch_size: Arc<AtomicUsize>,
    _phantom: PhantomData<S>,
    tx: Sender<(Arc<dyn RuntimeStats>, Duration)>,
}

impl<S> BatchingContext<S>
where
    S: BatchingStrategy + 'static,
    S::State: 'static,
{
    pub fn new(strategy: S, runtime_handle: &mut RuntimeHandle) -> Self {
        let (tx, rx) = create_channel::<(Arc<dyn RuntimeStats>, Duration)>(0);
        let mut state = strategy.make_state();
        let initial_size = 1000; // or whatever default
        let current_batch_size = Arc::new(AtomicUsize::new(initial_size));

        // Spawn background task
        //
        let current_batch_size_clone = current_batch_size.clone();
        runtime_handle.spawn(async move {
            while let Some(report) = rx.recv().await {
                dbg!(&report.1);
                let new_size = strategy.adjust_batch_size(&mut state, report.0.as_ref(), report.1);
                current_batch_size_clone.store(new_size, Ordering::Relaxed);
            }
            Ok(())
        });

        Self {
            current_batch_size,
            _phantom: PhantomData,
            tx,
        }
    }

    pub fn current_batch_size(&self) -> usize {
        self.current_batch_size.load(Ordering::Relaxed)
    }
}
