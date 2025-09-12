pub mod partition_cache;
#[cfg(feature = "python")]
pub mod python;
mod subscribers;

use std::sync::{Arc, OnceLock, RwLock};

use common_daft_config::{DaftExecutionConfig, DaftPlanningConfig, IOConfig};
use common_error::{DaftError, DaftResult};
#[cfg(feature = "python")]
use pyo3::prelude::*;

pub use crate::subscribers::QuerySubscriber;

#[derive(Debug, Default)]
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

#[derive(Debug)]
struct ContextState {
    /// Shared configuration for the context
    config: Config,
    subscribers: Vec<Arc<dyn QuerySubscriber>>,
}

/// Wrapper around the ContextState to provide a thread-safe interface.
/// IMPORTANT: Do not create this directly, use `get_context` instead.
/// This is a singleton, and should only be created once.
#[derive(Debug, Clone)]
pub struct DaftContext {
    /// Private state field - access only through state() and state_mut() methods
    state: Arc<RwLock<ContextState>>,
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

    pub fn subscribers(&self) -> Vec<Arc<dyn QuerySubscriber>> {
        self.with_state(|state| state.subscribers.clone())
    }

    pub fn attach_subscriber(&self, subscriber: Arc<dyn QuerySubscriber>) {
        self.with_state_mut(|state| state.subscribers.push(subscriber));
    }

    pub fn notify_query_start(&self, query_id: String) -> DaftResult<()> {
        self.with_state(|state| {
            for subscriber in &state.subscribers {
                subscriber.on_query_start(query_id.clone())?;
            }
            Ok::<(), DaftError>(())
        })
    }

    pub fn notify_query_end(&self, query_id: String) -> DaftResult<()> {
        self.with_state(|state| {
            for subscriber in &state.subscribers {
                subscriber.on_query_end(query_id.clone())?;
            }
            Ok::<(), DaftError>(())
        })
    }

    pub fn notify_plan_start(&self, query_id: String) -> DaftResult<()> {
        self.with_state(|state| {
            for subscriber in &state.subscribers {
                subscriber.on_plan_start(query_id.clone())?;
            }
            Ok::<(), DaftError>(())
        })
    }

    pub fn notify_plan_end(&self, query_id: String) -> DaftResult<()> {
        self.with_state(|state| {
            for subscriber in &state.subscribers {
                subscriber.on_plan_end(query_id.clone())?;
            }
            Ok::<(), DaftError>(())
        })
    }

    pub fn notify_exec_start(&self, query_id: String) -> DaftResult<()> {
        self.with_state(|state| {
            for subscriber in &state.subscribers {
                subscriber.on_exec_start(query_id.clone())?;
            }
            Ok::<(), DaftError>(())
        })
    }

    pub fn notify_exec_end(&self, query_id: String) -> DaftResult<()> {
        self.with_state(|state| {
            for subscriber in &state.subscribers {
                subscriber.on_exec_end(query_id.clone())?;
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
    Ok(())
}
