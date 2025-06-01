#![feature(mapped_lock_guards)]
use std::sync::{Arc, OnceLock, RwLock, RwLockReadGuard};

use common_daft_config::{DaftExecutionConfig, DaftPlanningConfig, IOConfig};
use common_error::{DaftError, DaftResult};
#[cfg(feature = "python")]
use daft_py_runners::{NativeRunner, RayRunner};
use daft_py_runners::{Runner, RunnerConfig};
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
mod python;

pub mod partition_cache;
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

        let runner_cfg = get_runner_config_from_env()?;
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
        use Runner::Native;
        if let Some(current_runner) = self.runner() {
            match (current_runner.as_ref(), runner.as_ref()) {
                (Native(_), Native(_)) => Ok(()),
                _ => Err(DaftError::InternalError(
                    "Cannot set runner more than once".to_string(),
                )),
            }
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

    pub fn io_config(&self) -> IOConfig {
        self.state().config.planning.default_io_config.clone()
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

static DAFT_CONTEXT: OnceLock<DaftContext> = OnceLock::new();

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
pub fn set_runner_native(num_threads: Option<usize>) -> DaftResult<DaftContext> {
    let ctx = get_context();

    let runner = Runner::Native(NativeRunner::try_new(num_threads)?);
    let runner = Arc::new(runner);

    ctx.set_runner(runner)?;

    Ok(ctx)
}

#[cfg(not(feature = "python"))]
pub fn set_runner_native(num_threads: Option<usize>) -> DaftResult<DaftContext> {
    unimplemented!()
}

/// Helper function to parse a boolean environment variable.
fn parse_bool_env_var(var_name: &str) -> Option<bool> {
    std::env::var(var_name)
        .ok()
        .map(|s| matches!(s.trim().to_lowercase().as_str(), "true" | "1"))
}

/// Helper function to parse a numeric environment variable.
fn parse_usize_env_var(var_name: &str) -> Option<usize> {
    std::env::var(var_name).ok().and_then(|s| s.parse().ok())
}

/// Helper function to get the ray runner config from the environment.
#[cfg(feature = "python")]
fn get_ray_runner_config_from_env() -> RunnerConfig {
    const DAFT_RAY_ADDRESS: &str = "DAFT_RAY_ADDRESS";
    const RAY_ADDRESS: &str = "RAY_ADDRESS";
    const DAFT_DEVELOPER_RAY_MAX_TASK_BACKLOG: &str = "DAFT_DEVELOPER_RAY_MAX_TASK_BACKLOG";
    const DAFT_RAY_FORCE_CLIENT_MODE: &str = "DAFT_RAY_FORCE_CLIENT_MODE";

    let address = if let Ok(address) = std::env::var(DAFT_RAY_ADDRESS) {
        log::warn!(
            "Detected usage of the ${} environment variable. This will be deprecated, please use ${} instead.",
            DAFT_RAY_ADDRESS,
            RAY_ADDRESS
        );
        Some(address)
    } else {
        std::env::var(RAY_ADDRESS).ok()
    };
    let max_task_backlog = parse_usize_env_var(DAFT_DEVELOPER_RAY_MAX_TASK_BACKLOG);
    let force_client_mode = parse_bool_env_var(DAFT_RAY_FORCE_CLIENT_MODE);
    RunnerConfig::Ray {
        address,
        max_task_backlog,
        force_client_mode,
    }
}

/// Helper function to automatically detect whether to use the ray runner.
#[cfg(feature = "python")]
fn detect_ray_state() -> bool {
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

    !in_ray_worker && (ray_is_initialized || ray_is_in_job)
}

#[cfg(feature = "python")]
fn get_runner_config_from_env() -> DaftResult<RunnerConfig> {
    const DAFT_RUNNER: &str = "DAFT_RUNNER";

    let runner_from_envvar = std::env::var(DAFT_RUNNER)
        .unwrap_or_default()
        .to_lowercase();

    match runner_from_envvar.as_str() {
        "native" => Ok(RunnerConfig::Native { num_threads: None }),
        "ray" => Ok(get_ray_runner_config_from_env()),
        "py" => Err(DaftError::ValueError("The PyRunner was removed from Daft from v0.5.0 onwards. Please set the env to `DAFT_RUNNER=native` instead.".to_string())),
        "" => Ok(if detect_ray_state() { get_ray_runner_config_from_env() } else { RunnerConfig::Native { num_threads: None }}),
        other => Err(DaftError::ValueError(format!("Invalid runner type `DAFT_RUNNER={other}` specified through the env. Please use either `native` or `ray` instead.")))
    }
}

#[cfg(not(feature = "python"))]
fn get_runner_config_from_env() -> DaftResult<RunnerConfig> {
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
    parent.add_class::<python::PyDaftContext>()?;
    Ok(())
}
