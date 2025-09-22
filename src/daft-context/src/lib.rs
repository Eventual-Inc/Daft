use std::sync::{Arc, OnceLock, RwLock};

use common_daft_config::{DaftExecutionConfig, DaftPlanningConfig, IOConfig};
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
mod python;

pub mod partition_cache;

#[derive(Debug)]
struct ContextState {
    /// Shared configuration for the context
    config: Config,
}

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

/// Wrapper around the ContextState to provide a thread-safe interface.
/// IMPORTANT: Do not create this directly, use `get_context` instead.
/// This is a singleton, and should only be created once.
#[derive(Debug, Clone)]
pub struct DaftContext {
    /// Private state field - access only through state() and state_mut() methods
    state: Arc<RwLock<ContextState>>,
}

#[cfg(feature = "python")]
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
}

#[cfg(not(feature = "python"))]
impl DaftContext {
    /// Execute a callback with read access to the state.
    /// The guard is automatically released when the callback returns.
    pub fn with_state<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&ContextState) -> R,
    {
        unimplemented!()
    }

    /// Execute a callback with mutable access to the state.
    /// The guard is automatically released when the callback returns.
    pub fn with_state_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut ContextState) -> R,
    {
        unimplemented!()
    }
}

static DAFT_CONTEXT: OnceLock<DaftContext> = OnceLock::new();

#[cfg(feature = "python")]
pub fn get_context() -> DaftContext {
    match DAFT_CONTEXT.get() {
        Some(ctx) => ctx.clone(),
        None => {
            let state = ContextState {
                config: Config::from_env(),
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
