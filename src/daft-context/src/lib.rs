pub mod partition_cache;
#[cfg(feature = "python")]
pub mod python;
mod subscribers;

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock},
};

use common_daft_config::{DaftExecutionConfig, DaftPlanningConfig, IOConfig};
use common_error::{DaftError, DaftResult};
use common_metrics::{QueryID, QueryPlan};
use daft_micropartition::MicroPartitionRef;
#[cfg(feature = "python")]
use pyo3::prelude::*;

pub use crate::subscribers::Subscriber;
use crate::subscribers::{QueryMetadata, QueryResult};

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
        self.with_state(|state| {
            for subscriber in state.subscribers.values() {
                subscriber.on_query_start(query_id.clone(), metadata.clone())?;
            }
            Ok::<(), DaftError>(())
        })
    }

    pub fn notify_query_end(&self, query_id: QueryID, result: QueryResult) -> DaftResult<()> {
        self.with_state(move |state| {
            for subscriber in state.subscribers.values() {
                subscriber.on_query_end(query_id.clone(), result.clone())?;
            }
            Ok::<(), DaftError>(())
        })
    }

    pub fn notify_result_out(
        &self,
        query_id: QueryID,
        result: MicroPartitionRef,
    ) -> DaftResult<()> {
        self.with_state(|state| {
            for subscriber in state.subscribers.values() {
                subscriber.on_result_out(query_id.clone(), result.clone())?;
            }
            Ok::<(), DaftError>(())
        })
    }

    pub fn notify_optimization_start(&self, query_id: QueryID) -> DaftResult<()> {
        self.with_state(|state| {
            for subscriber in state.subscribers.values() {
                subscriber.on_optimization_start(query_id.clone())?;
            }
            Ok::<(), DaftError>(())
        })
    }

    pub fn notify_optimization_end(
        &self,
        query_id: QueryID,
        optimized_plan: QueryPlan,
    ) -> DaftResult<()> {
        self.with_state(|state| {
            for subscriber in state.subscribers.values() {
                subscriber.on_optimization_end(query_id.clone(), optimized_plan.clone())?;
            }
            Ok::<(), DaftError>(())
        })
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

#[cfg(not(feature = "python"))]
pub fn get_context() -> DaftContext {
    unimplemented!()
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(python::get_context, parent)?)?;
    parent.add_class::<python::PyDaftContext>()?;
    parent.add_class::<python::PyQueryMetadata>()?;
    parent.add_class::<python::PyQueryResult>()?;
    Ok(())
}
