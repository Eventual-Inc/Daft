pub mod partition_cache;
#[cfg(feature = "python")]
pub mod python;
pub mod subscribers;

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock},
};

use common_daft_config::{DaftExecutionConfig, DaftPlanningConfig, IOConfig};
use common_error::{DaftError, DaftResult};
use common_metrics::{QueryID, QueryPlan};
use daft_micropartition::{MicroPartitionRef, partitioning::Partition};
#[cfg(feature = "python")]
use pyo3::prelude::*;
pub use subscribers::{Event, QueryMetadata, QueryResult, Subscriber};

use crate::subscribers::{
    event_header,
    events::{
        ExecEndEvent, ExecStartEvent, OperatorEndEvent, OperatorMeta, OperatorStartEvent,
        OptimizationCompleteEvent, OptimizationStartEvent, QueryEndEvent, QueryHeartbeatEvent,
        QueryStartEvent, ResultOutEvent, StatsEvent,
    },
};

#[derive(Default)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct Config {
    pub execution: Arc<DaftExecutionConfig>,
    pub planning: Arc<DaftPlanningConfig>,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            execution: Arc::new(DaftExecutionConfig::from_env()),
            planning: Arc::new(DaftPlanningConfig::from_env()),
        }
    }
}

#[cfg_attr(debug_assertions, derive(Debug))]
struct ContextState {
    /// Shared configuration for the context
    config: Config,
    subscribers: HashMap<String, Arc<dyn Subscriber>>,
}

/// Wrapper around the ContextState to provide a thread-safe interface.
/// IMPORTANT: Do not create this directly, use `get_context` instead.
/// This is a singleton, and should only be created once.
#[derive(Clone)]
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct DaftContext {
    /// Private state field - access only through state() and state_mut() methods
    state: Arc<RwLock<ContextState>>,
}
#[cfg(not(debug_assertions))]
impl std::fmt::Debug for DaftContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaftContext").finish()
    }
}

impl DaftContext {
    fn with_state<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&ContextState) -> R,
    {
        let guard = self
            .state
            .read()
            .expect("Failed to acquire read lock on DaftContext");
        f(&guard)
    }

    fn with_state_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut ContextState) -> R,
    {
        let mut guard = self
            .state
            .write()
            .expect("Failed to acquire write lock on DaftContext");
        f(&mut guard)
    }

    /// get the execution config
    pub fn execution_config(&self) -> Arc<DaftExecutionConfig> {
        self.with_state(|state| state.config.execution.clone())
    }

    /// get the planning config
    pub fn planning_config(&self) -> Arc<DaftPlanningConfig> {
        self.with_state(|state| state.config.planning.clone())
    }

    /// set the execution config
    pub fn set_execution_config(&self, config: Arc<DaftExecutionConfig>) {
        self.with_state_mut(|state| state.config.execution = config);
    }

    /// set the planning config
    pub fn set_planning_config(&self, config: Arc<DaftPlanningConfig>) {
        self.with_state_mut(|state| state.config.planning = config);
    }

    pub fn io_config(&self) -> IOConfig {
        self.with_state(|state| state.config.planning.default_io_config.clone())
    }

    pub fn subscribers(&self) -> Vec<Arc<dyn Subscriber>> {
        self.with_state(|state| state.subscribers.values().cloned().collect())
    }

    /// Attaches a subscriber to this context.
    pub fn attach_subscriber(&self, alias: String, subscriber: Arc<dyn Subscriber>) {
        self.with_state_mut(|state| state.subscribers.insert(alias, subscriber));
    }

    /// Detaches a subscriber from this context, err if does not exist.
    pub fn detach_subscriber(&self, alias: &str) -> DaftResult<()> {
        self.with_state_mut(|state| {
            if !state.subscribers.contains_key(alias) {
                return Err(DaftError::ValueError(format!(
                    "Subscriber `{}` not found in this context",
                    alias
                )));
            }

            state.subscribers.remove(alias);
            Ok(())
        })
    }

    pub fn notify_query_start(
        &self,
        query_id: QueryID,
        metadata: Arc<QueryMetadata>,
    ) -> DaftResult<()> {
        let event = Event::QueryStart(QueryStartEvent {
            header: event_header(query_id),
            metadata,
        });
        self.dispatch_event(&event, "notify query start")
    }

    pub fn notify_query_heartbeat(&self, query_id: QueryID) -> DaftResult<()> {
        let event = Event::QueryHeartbeat(QueryHeartbeatEvent {
            header: event_header(query_id),
        });
        self.dispatch_event(&event, "notify query heartbeat")
    }

    pub fn notify_query_end(&self, query_id: QueryID, result: QueryResult) {
        let event = Event::QueryEnd(QueryEndEvent {
            header: event_header(query_id),
            result,
            duration_ms: None,
        });
        if let Err(e) = self.dispatch_event(&event, "notify query end") {
            log::error!("Failed to dispatch query end event: {}", e);
        }
    }

    pub fn notify_result_out(
        &self,
        query_id: QueryID,
        result: MicroPartitionRef,
    ) -> DaftResult<()> {
        let event = Event::ResultOut(ResultOutEvent {
            header: event_header(query_id),
            num_rows: (result.num_rows() as u64),
            data: Some(result),
        });
        self.dispatch_event(&event, "notify result out")
    }

    pub fn notify_optimization_start(&self, query_id: QueryID) -> DaftResult<()> {
        let event = Event::OptimizationStart(OptimizationStartEvent {
            header: event_header(query_id),
        });
        self.dispatch_event(&event, "notify optimization start")
    }

    pub fn notify_optimization_end(
        &self,
        query_id: QueryID,
        optimized_plan: QueryPlan,
    ) -> DaftResult<()> {
        let event = Event::OptimizationComplete(OptimizationCompleteEvent {
            header: event_header(query_id),
            optimized_plan,
        });
        self.dispatch_event(&event, "notify optimization complete")
    }

    pub fn notify_exec_start(&self, query_id: QueryID, physical_plan: String) -> DaftResult<()> {
        let event = Event::ExecStart(ExecStartEvent {
            header: event_header(query_id),
            physical_plan: physical_plan.into(),
        });
        self.dispatch_event(&event, "notify exec start")
    }

    pub fn notify_exec_end(&self, query_id: QueryID) -> DaftResult<()> {
        let event = Event::ExecEnd(ExecEndEvent {
            header: event_header(query_id),
            duration_ms: None,
        });
        self.dispatch_event(&event, "notify exec end")
    }

    pub fn notify_exec_operator_start(&self, query_id: QueryID, node_id: usize) -> DaftResult<()> {
        let event = Event::OperatorStart(OperatorStartEvent {
            header: event_header(query_id),
            operator: Arc::new(OperatorMeta::from_id(node_id)),
        });
        self.dispatch_event(&event, "notify exec operator start")
    }

    pub fn notify_exec_operator_end(&self, query_id: QueryID, node_id: usize) -> DaftResult<()> {
        let event = Event::OperatorEnd(OperatorEndEvent {
            header: event_header(query_id),
            operator: Arc::new(OperatorMeta::from_id(node_id)),
        });
        self.dispatch_event(&event, "notify exec operator end")
    }

    pub fn notify_exec_emit_stats(
        &self,
        query_id: QueryID,
        stats: Vec<(usize, common_metrics::Stats)>,
    ) -> DaftResult<()> {
        let event = Event::Stats(StatsEvent {
            header: event_header(query_id),
            stats: Arc::new(stats),
        });
        self.dispatch_event(&event, "notify exec emit stats")
    }

    pub fn notify(&self, event: &Event) -> DaftResult<()> {
        self.dispatch_event(event, "notify event")
    }

    fn dispatch_event(&self, event: &Event, err_context: &'static str) -> DaftResult<()> {
        let subscribers = self.with_state(|state| {
            state
                .subscribers
                .values()
                .cloned()
                .collect::<Vec<Arc<dyn Subscriber>>>()
        });
        for subscriber in subscribers {
            if let Err(e) = subscriber.on_event(event.clone()) {
                log::error!("Failed to {}: {}", err_context, e);
            }
        }
        Ok(())
    }
}

static DAFT_CONTEXT: OnceLock<DaftContext> = OnceLock::new();

#[cfg(feature = "python")]
pub fn get_context() -> DaftContext {
    match DAFT_CONTEXT.get() {
        Some(ctx) => ctx.clone(),
        None => {
            use crate::subscribers::default_subscribers;

            let state = ContextState {
                config: Config::from_env(),
                subscribers: default_subscribers(),
            };
            let state = RwLock::new(state);
            let state = Arc::new(state);
            let ctx = DaftContext { state };

            DAFT_CONTEXT
                .set(ctx.clone())
                .expect("Failed to set DaftContext");
            ctx
        }
    }
}

// Non-Python builds are only used in Rust-only test runs (`cargo test`).
// Production always enables the python feature.
#[cfg(not(feature = "python"))]
pub fn get_context() -> DaftContext {
    match DAFT_CONTEXT.get() {
        Some(ctx) => ctx.clone(),
        None => {
            let state = ContextState {
                config: Config::from_env(),
                subscribers: HashMap::new(),
            };
            let state = RwLock::new(state);
            let state = Arc::new(state);
            let ctx = DaftContext { state };

            // If another thread already set the context, use theirs.
            let _ = DAFT_CONTEXT.set(ctx);
            DAFT_CONTEXT.get().unwrap().clone()
        }
    }
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(python::get_context, parent)?)?;
    parent.add_function(wrap_pyfunction!(
        python::refresh_dashboard_subscriber,
        parent
    )?)?;
    parent.add_class::<python::PyDaftContext>()?;
    parent.add_class::<python::PyQueryMetadata>()?;
    parent.add_class::<python::PyQueryResult>()?;
    Ok(())
}
