mod aimd;
mod latency_constrained;

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub use aimd::*;
pub use latency_constrained::*;
use tokio::sync::Mutex;

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

    fn adjust_batch_size_batched(
        &self,
        state: &mut Self::State,
        batch: Vec<(Arc<dyn RuntimeStats>, usize, Duration)>,
    ) -> usize {
        self.current_batch_size(state)
    }

    fn current_batch_size(&self, state: &Self::State) -> usize;
}
pub struct DefaultBatchingStrategy {
    current_batch_size: usize,
}
impl DefaultBatchingStrategy {
    pub fn new(morsel_size_req: Option<&MorselSizeRequirement>) -> Self {
        let current_batch_size = match morsel_size_req {
            Some(MorselSizeRequirement::Strict(v)) => *v,
            Some(MorselSizeRequirement::Flexible(_, v)) => *v,
            None => 128 * 1024,
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

    fn current_batch_size(&self, _state: &Self::State) -> usize {
        self.current_batch_size
    }
}

pub struct BatchingContext<S: BatchingStrategy> {
    current_batch_size: Arc<AtomicUsize>,
    tx: Sender<(Arc<dyn RuntimeStats>, usize, Duration)>,
    pending_reports: Arc<Mutex<VecDeque<(Arc<dyn RuntimeStats>, usize, Duration)>>>,
    strategy: S,
    state: Arc<Mutex<S::State>>,
}

impl<S> BatchingContext<S>
where
    S: BatchingStrategy + 'static,
    S::State: 'static,
{
    pub fn new(strategy: S, runtime_handle: &mut RuntimeHandle) -> Self {
        let (tx, rx) = create_channel::<(Arc<dyn RuntimeStats>, usize, Duration)>(0);
        let state = strategy.make_state();
        let current_batch_size = Arc::new(AtomicUsize::new(strategy.current_batch_size(&state)));
        let state = Arc::new(Mutex::new(state));
        let pending_reports = Arc::new(Mutex::new(VecDeque::new()));

        let pending_reports_clone = pending_reports.clone();
        runtime_handle.spawn(async move {
            while let Some(report) = rx.recv().await {
                pending_reports_clone.lock().await.push_back(report);
            }
        });

        Self {
            current_batch_size,
            tx,
            strategy,
            state,
            pending_reports,
        }
    }

    pub async fn calculate_batch_size(&self) -> usize {
        // Drain pending reports and process them all
        let mut reports = self.pending_reports.lock().await;

        if !reports.is_empty() {
            let mut state = self.state.lock().await;
            let reports = reports.drain(..).collect::<Vec<_>>();
            let new_size = self.strategy.adjust_batch_size_batched(&mut state, reports);
            self.current_batch_size.store(new_size, Ordering::Relaxed);
        }
        self.current_batch_size.load(Ordering::Relaxed)
    }

    pub async fn record_execution_stats(
        &self,
        stats: Arc<dyn RuntimeStats>,
        batch_size: usize,
        duration: Duration,
    ) {
        self.tx.send((stats, batch_size, duration)).await.unwrap();
    }
}
