mod latency_constrained;

use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwap;
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
    type State: Send + Sync + Unpin;
    fn make_state(&self) -> Self::State;
    /// adjust the batch size based on runtime performance metrics
    fn calculate_new_requirements(
        &self,
        state: &mut Self::State,
        reports: BatchReport,
    ) -> MorselSizeRequirement;

    fn initial_requirements(&self) -> MorselSizeRequirement;
}

#[derive(Clone)]
pub struct StaticBatchingStrategy {
    req: MorselSizeRequirement,
}

impl StaticBatchingStrategy {
    pub fn new(req: MorselSizeRequirement) -> Self {
        Self { req }
    }
}

impl BatchingStrategy for StaticBatchingStrategy {
    type State = ();
    fn make_state(&self) -> Self::State {}

    fn calculate_new_requirements(
        &self,
        _state: &mut Self::State,
        _reports: BatchReport,
    ) -> MorselSizeRequirement {
        self.req
    }

    fn initial_requirements(&self) -> MorselSizeRequirement {
        self.req
    }
}

#[allow(dead_code)]
pub struct DynBatchingState {
    update_fn: Box<dyn FnMut(BatchReport) -> MorselSizeRequirement + Send + Sync>,
}

#[allow(dead_code)]
pub struct DynBatchingStrategy {
    initial_req: MorselSizeRequirement,
    make_state_fn: Box<dyn Fn() -> DynBatchingState + Send + Sync>,
}

#[allow(dead_code)]
impl DynBatchingStrategy {
    pub fn new<S: BatchingStrategy + Clone + 'static>(strategy: S) -> Self
    where
        S::State: 'static,
    {
        let initial_req = strategy.initial_requirements();

        Self {
            initial_req,
            make_state_fn: Box::new(move || {
                let mut state = strategy.make_state();
                let strategy_clone = strategy.clone();

                DynBatchingState {
                    update_fn: Box::new(move |reports| {
                        strategy_clone.calculate_new_requirements(&mut state, reports)
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

    fn calculate_new_requirements(
        &self,
        state: &mut Self::State,
        reports: BatchReport,
    ) -> MorselSizeRequirement {
        (state.update_fn)(reports)
    }

    fn initial_requirements(&self) -> MorselSizeRequirement {
        self.initial_req
    }
}

pub struct BatchingContext<S: BatchingStrategy> {
    requirements: ArcSwap<MorselSizeRequirement>,
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
        let requirements = strategy.initial_requirements();
        let requirements = ArcSwap::new(Arc::new(requirements));
        let state = Arc::new(Mutex::new(state));
        let pending_reports = Arc::new(Mutex::new(Vec::new()));

        let pending_reports_clone = pending_reports.clone();
        runtime_handle.spawn(async move {
            while let Some(report) = rx.recv().await {
                pending_reports_clone.lock().await.push(report);
            }
        });

        Self {
            requirements,
            pending_reports,
            state,
            strategy,
            tx,
        }
    }

    pub async fn calculate_batch_size(&self) -> MorselSizeRequirement {
        // Drain pending reports and process them all
        let mut reports = self.pending_reports.lock().await;

        if !reports.is_empty() {
            let mut state = self.state.lock().await;
            let new_reqs = self
                .strategy
                .calculate_new_requirements(&mut state, std::mem::take(&mut *reports));
            self.requirements.store(Arc::new(new_reqs));
        }

        *self.requirements.load().as_ref()
    }

    pub async fn record_execution_stats(
        &self,
        stats: Arc<dyn RuntimeStats>,
        batch_size: usize,
        duration: Duration,
    ) {
        self.tx.send((stats, batch_size, duration)).await.unwrap();
    }

    pub fn initial_requirements(&self) -> MorselSizeRequirement {
        self.strategy.initial_requirements()
    }
}
