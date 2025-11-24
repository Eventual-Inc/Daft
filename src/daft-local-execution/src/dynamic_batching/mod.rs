mod latency_constrained;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub use latency_constrained::*;
use tokio::sync::Mutex;

use crate::{
    RuntimeHandle,
    channel::{Sender, create_channel},
    pipeline::MorselSizeRequirement,
    runtime_stats::RuntimeStats,
};
type BatchReport = Vec<(Arc<dyn RuntimeStats>, usize, Duration)>;

/// Trait for algorithms that dynamically adjust batch sizes based on execution performance.
///
/// Dynamic batching is a technique used to optimize throughput and latency by adjusting
/// the number of items processed together in a batch based on runtime performance metrics.
/// Different algorithms use various strategies to find the optimal batch size.
pub trait BatchingStrategy: Send + Sync {
    type State: BatchingState + Send + Sync + Unpin;
    fn make_state(&self) -> Self::State;
    /// adjust the batch size based on runtime performance metrics
    fn calculate_new_batch_size(&self, state: &mut Self::State, reports: BatchReport) -> usize;

    fn initial_batch_size(&self) -> usize;
}

pub trait BatchingState {}
impl BatchingState for () {}

#[derive(Clone)]
pub struct StaticBatchingStrategy {
    batch_size: usize,
}

impl StaticBatchingStrategy {
    pub fn new(morsel_size_req: Option<&MorselSizeRequirement>) -> Self {
        let batch_size = match morsel_size_req {
            Some(MorselSizeRequirement::Strict(v)) => *v,
            Some(MorselSizeRequirement::Flexible(_, v)) => *v,
            None => 128 * 1024,
        };

        Self { batch_size }
    }
}

impl BatchingStrategy for StaticBatchingStrategy {
    type State = ();
    fn make_state(&self) -> Self::State {}

    fn calculate_new_batch_size(&self, _state: &mut Self::State, _reports: BatchReport) -> usize {
        self.batch_size
    }

    fn initial_batch_size(&self) -> usize {
        self.batch_size
    }
}

#[allow(dead_code)]
pub struct DynBatchingState {
    update_fn: Box<dyn FnMut(BatchReport) -> usize + Send + Sync>,
}
impl BatchingState for DynBatchingState {}

#[allow(dead_code)]
pub struct DynBatchingStrategy {
    initial_size: usize,
    make_state_fn: Box<dyn Fn() -> DynBatchingState + Send + Sync>,
}

#[allow(dead_code)]
impl DynBatchingStrategy {
    pub fn new<S: BatchingStrategy + Clone + 'static>(strategy: S) -> Self
    where
        S::State: 'static,
    {
        let initial_size = strategy.initial_batch_size();

        Self {
            initial_size,
            make_state_fn: Box::new(move || {
                let mut state = strategy.make_state();
                let strategy_clone = strategy.clone();

                DynBatchingState {
                    update_fn: Box::new(move |reports| {
                        strategy_clone.calculate_new_batch_size(&mut state, reports)
                    }),
                }
            }),
        }
    }
}
impl<S: BatchingStrategy + Clone + 'static> From<S> for DynBatchingStrategy
where
    S::State: 'static,
{
    fn from(strategy: S) -> Self {
        Self::new(strategy)
    }
}

impl BatchingStrategy for DynBatchingStrategy {
    type State = DynBatchingState;

    fn make_state(&self) -> Self::State {
        (self.make_state_fn)()
    }

    fn calculate_new_batch_size(&self, state: &mut Self::State, reports: BatchReport) -> usize {
        (state.update_fn)(reports)
    }

    fn initial_batch_size(&self) -> usize {
        self.initial_size
    }
}

pub struct BatchingContext<S: BatchingStrategy> {
    current_batch_size: Arc<AtomicUsize>,
    pending_reports: Arc<Mutex<BatchReport>>,
    state: Arc<Mutex<S::State>>,
    strategy: S,
    tx: Sender<(Arc<dyn RuntimeStats>, usize, Duration)>,
}

impl<S> BatchingContext<S>
where
    S: BatchingStrategy + 'static,
    S::State: 'static,
{
    pub fn new(strategy: S, runtime_handle: &RuntimeHandle) -> Self {
        let (tx, rx) = create_channel::<(Arc<dyn RuntimeStats>, usize, Duration)>(0);
        let state = strategy.make_state();
        let current_batch_size = Arc::new(AtomicUsize::new(strategy.initial_batch_size()));
        let state = Arc::new(Mutex::new(state));
        let pending_reports = Arc::new(Mutex::new(Vec::new()));

        let pending_reports_clone = pending_reports.clone();
        runtime_handle.spawn(async move {
            while let Some(report) = rx.recv().await {
                pending_reports_clone.lock().await.push(report);
            }
        });

        Self {
            current_batch_size,
            pending_reports,
            state,
            strategy,
            tx,
        }
    }

    pub async fn calculate_batch_size(&self) -> usize {
        // Drain pending reports and process them all
        let mut reports = self.pending_reports.lock().await;

        if !reports.is_empty() {
            let mut state = self.state.lock().await;
            let new_size = self
                .strategy
                .calculate_new_batch_size(&mut state, std::mem::take(&mut *reports));
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
