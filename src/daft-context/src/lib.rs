#![feature(mapped_lock_guards)]
use std::sync::{Arc, RwLock, RwLockReadGuard};

use common_daft_config::{DaftExecutionConfig, DaftPlanningConfig};
use common_error::{DaftError, DaftResult};
#[cfg(feature = "python")]
use daft_py_runners::{NativeRunner, PyRunner, RayRunner};
use daft_py_runners::{Runner, RunnerConfig};
use once_cell::sync::OnceCell;
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
mod python;

/// Wrapper around the ContextState to provide a thread-safe interface.
/// IMPORTANT: Do not create this directly, use `get_context` instead.
/// This is a singleton, and should only be created once.
#[derive(Debug, Clone)]
pub struct DaftContext {
    state: Arc<RwLock<ContextState>>,
}

#[derive(Debug)]
struct ContextState {
    /// Shared configuration for the context
    config: Config,
    /// The runner to use for executing queries.
    /// most scenarios of etting it more than once will result in an error.
    /// Since native and py both use the same partition set, they can be swapped out freely
    /// valid runner changes are:
    /// native -> py
    /// py -> native
    /// but not:
    /// native -> ray
    /// py -> ray
    /// ray -> native
    /// ray -> py
    runner: Option<Arc<Runner>>,
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

#[cfg(feature = "python")]
impl ContextState {
    /// Retrieves the runner.
    ///
    /// WARNING: This will set the runner if it has not yet been set.
    fn get_or_create_runner(&mut self) -> DaftResult<Arc<Runner>> {
        if let Some(runner) = self.runner.as_ref() {
            return Ok(runner.clone());
        }

        let runner_cfg = get_runner_config_from_env();
        let runner = runner_cfg.create_runner()?;

        let runner = Arc::new(runner);
        self.runner = Some(runner.clone());

        Ok(runner)
    }
}

#[cfg(not(feature = "python"))]
impl ContextState {
    fn get_or_create_runner(&mut self) -> DaftResult<Arc<Runner>> {
        unimplemented!()
    }
}

#[cfg(feature = "python")]
impl DaftContext {
    /// Retrieves the runner.
    ///
    /// WARNING: This will set the runner if it has not yet been set.
    pub fn get_or_create_runner(&self) -> DaftResult<Arc<Runner>> {
        let mut lock = self
            .state
            .write()
            .expect("Failed to acquire write lock on DaftContext");
        lock.get_or_create_runner()
    }

    /// Get the current runner, if one has been set.
    pub fn runner(&self) -> Option<Arc<Runner>> {
        self.state.read().unwrap().runner.clone()
    }

    /// Set the runner.
    /// IMPORTANT: This can only be set once. Setting it more than once will error.
    pub fn set_runner(&self, runner: Arc<Runner>) -> DaftResult<()> {
        use Runner::{Native, Py};
        if let Some(current_runner) = self.runner() {
            let runner = match (current_runner.as_ref(), runner.as_ref()) {
                (Native(_), Native(_)) | (Py(_), Py(_)) => return Ok(()),
                (Py(_), Native(_)) | (Native(_), Py(_)) => runner,

                _ => {
                    return Err(DaftError::InternalError(
                        "Cannot set runner more than once".to_string(),
                    ));
                }
            };

            let mut state = self.state.write().map_err(|_| {
                DaftError::InternalError("Failed to acquire write lock on DaftContext".to_string())
            })?;

            state.runner.replace(runner);

            Ok(())
        } else {
            let mut state = self.state.write().map_err(|_| {
                DaftError::InternalError("Failed to acquire write lock on DaftContext".to_string())
            })?;

            state.runner.replace(runner);
            Ok(())
        }
    }

    /// Get a read only reference to the state.
    fn state(&self) -> RwLockReadGuard<'_, ContextState> {
        self.state.read().unwrap()
    }

    /// get the execution config
    pub fn execution_config(&self) -> Arc<DaftExecutionConfig> {
        self.state().config.execution.clone()
    }

    /// get the planning config
    pub fn planning_config(&self) -> Arc<DaftPlanningConfig> {
        self.state().config.planning.clone()
    }
}

#[cfg(not(feature = "python"))]
impl DaftContext {
    pub fn get_or_create_runner(&self) -> DaftResult<Arc<Runner>> {
        unimplemented!()
    }

    pub fn runner(&self) -> Option<Arc<Runner>> {
        unimplemented!()
    }

    pub fn set_runner(&self, runner: Arc<Runner>) -> DaftResult<()> {
        unimplemented!()
    }

    pub fn state(&self) -> RwLockReadGuard<'_, ContextState> {
        unimplemented!()
    }

    pub fn state_mut(&self) -> std::sync::RwLockWriteGuard<'_, ContextState> {
        unimplemented!()
    }
}

static DAFT_CONTEXT: OnceCell<DaftContext> = OnceCell::new();

#[cfg(feature = "python")]
pub fn get_context() -> DaftContext {
    match DAFT_CONTEXT.get() {
        Some(ctx) => ctx.clone(),
        None => {
            let state = ContextState {
                config: Config::from_env(),
                runner: None,
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
pub fn set_runner_ray(
    address: Option<String>,
    max_task_backlog: Option<usize>,
    force_client_mode: Option<bool>,
) -> DaftResult<DaftContext> {
    let ctx = get_context();

    let runner = Runner::Ray(RayRunner::try_new(
        address,
        max_task_backlog,
        force_client_mode,
    )?);
    let runner = Arc::new(runner);
    ctx.set_runner(runner)?;

    Ok(ctx)
}

#[cfg(not(feature = "python"))]
pub fn set_runner_ray(
    _address: Option<String>,
    _max_task_backlog: Option<usize>,
    _force_client_mode: Option<bool>,
) -> DaftResult<DaftContext> {
    unimplemented!()
}

#[cfg(feature = "python")]
pub fn set_runner_native() -> DaftResult<DaftContext> {
    let ctx = get_context();

    let runner = Runner::Native(NativeRunner::try_new()?);
    let runner = Arc::new(runner);

    ctx.set_runner(runner)?;

    Ok(ctx)
}

#[cfg(not(feature = "python"))]
pub fn set_runner_native() -> DaftResult<DaftContext> {
    unimplemented!()
}

#[cfg(feature = "python")]
pub fn set_runner_py(use_thread_pool: Option<bool>) -> DaftResult<DaftContext> {
    let ctx = get_context();

    let runner = Runner::Py(PyRunner::try_new(use_thread_pool)?);
    let runner = Arc::new(runner);

    ctx.set_runner(runner)?;

    Ok(ctx)
}

#[cfg(not(feature = "python"))]
pub fn set_runner_py(_use_thread_pool: Option<bool>) -> DaftResult<DaftContext> {
    unimplemented!()
}

#[cfg(feature = "python")]
fn get_runner_config_from_env() -> RunnerConfig {
    const DAFT_RUNNER: &str = "DAFT_RUNNER";
    const DAFT_RAY_ADDRESS: &str = "DAFT_RAY_ADDRESS";
    const RAY_ADDRESS: &str = "RAY_ADDRESS";
    const DAFT_DEVELOPER_RAY_MAX_TASK_BACKLOG: &str = "DAFT_DEVELOPER_RAY_MAX_TASK_BACKLOG";
    const DAFT_RAY_FORCE_CLIENT_MODE: &str = "DAFT_RAY_FORCE_CLIENT_MODE";
    const DAFT_DEVELOPER_USE_THREAD_POOL: &str = "DAFT_DEVELOPER_USE_THREAD_POOL";

    let runner_from_envvar = std::env::var(DAFT_RUNNER).unwrap_or_default();
    let address = std::env::var(DAFT_RAY_ADDRESS).ok();
    let address = if address.is_some() {
        log::warn!(
            "Detected usage of the $DAFT_RAY_ADDRESS environment variable. This will be deprecated, please use $RAY_ADDRESS instead."
        );
        address
    } else {
        std::env::var(RAY_ADDRESS).ok()
    };

    let max_task_backlog = std::env::var(DAFT_DEVELOPER_RAY_MAX_TASK_BACKLOG)
        .ok()
        .map(|s| s.parse().unwrap());

    let force_client_mode = std::env::var(DAFT_RAY_FORCE_CLIENT_MODE)
        .ok()
        .map(|s| matches!(s.trim().to_lowercase().as_str(), "true" | "1"));

    let mut ray_is_in_job = false;
    let mut in_ray_worker = false;
    let mut ray_is_initialized = false;

    pyo3::Python::with_gil(|py| {
        let ray = py.import("ray").ok()?;

        ray_is_initialized = ray
            .call_method0("is_initialized")
            .ok()?
            .extract::<bool>()
            .ok()?;

        let worker_mode = ray
            .getattr("_private")
            .ok()?
            .getattr("worker")
            .ok()?
            .getattr("global_worker")
            .ok()?
            .getattr("mode")
            .ok()?;

        let ray_worker_mode = ray.getattr("WORKER_MODE").ok()?;
        if worker_mode.eq(ray_worker_mode).ok()? {
            in_ray_worker = true;
        }
        if std::env::var("RAY_JOB_ID").is_ok() {
            ray_is_in_job = true;
        }
        Some(())
    });

    match runner_from_envvar.to_lowercase().as_str() {
        "ray" => RunnerConfig::Ray {
            address,
            max_task_backlog,
            force_client_mode,
        },
        "py" => RunnerConfig::Py {
            use_thread_pool: std::env::var(DAFT_DEVELOPER_USE_THREAD_POOL)
                .ok()
                .map(|s| matches!(s.trim().to_lowercase().as_str(), "true" | "1")),
        },
        _ if !in_ray_worker && (ray_is_initialized || ray_is_in_job) => RunnerConfig::Ray {
            address: None,
            max_task_backlog,
            force_client_mode,
        },
        _ => RunnerConfig::Native,
    }
}

#[cfg(not(feature = "python"))]
fn get_runner_config_from_env() -> RunnerConfig {
    unimplemented!()
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> pyo3::PyResult<()> {
    parent.add_function(wrap_pyfunction!(
        python::get_runner_config_from_env,
        parent
    )?)?;
    parent.add_function(wrap_pyfunction!(python::get_context, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::set_runner_ray, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::set_runner_native, parent)?)?;
    parent.add_function(wrap_pyfunction!(python::set_runner_py, parent)?)?;
    parent.add_class::<python::PyDaftContext>()?;
    Ok(())
}
