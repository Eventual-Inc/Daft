use std::sync::{Arc, RwLock};

use common_daft_config::{DaftExecutionConfig, DaftPlanningConfig};
use common_error::DaftResult;
use daft_catalog::DaftCatalog;
use daft_py_runners::{NativeRunner, RayRunner};
use once_cell::sync::OnceCell;
#[cfg(feature = "python")]
use pyo3::prelude::*;

#[derive(Debug)]
pub struct ContextState {
    pub config: Arc<RwLock<Config>>,
    _catalog: Arc<RwLock<DaftCatalog>>,
    runner: OnceCell<Arc<Runner>>,
}

#[cfg_attr(feature = "python", pyo3::prelude::pyclass)]
#[derive(Debug, Default)]
pub struct Config {
    pub execution: Arc<DaftExecutionConfig>,
    pub planning: Arc<DaftPlanningConfig>,
}

#[derive(Debug)]
#[cfg(feature = "python")]
pub enum Runner {
    Ray(RayRunner),
    Native(NativeRunner),
}

#[derive(Debug)]
#[cfg(feature = "python")]
enum RunnerConfig {
    Native,
    Ray {
        address: Option<String>,
        max_task_backlog: Option<usize>,
        force_client_mode: Option<bool>,
    },
}

#[cfg(feature = "python")]
impl ContextState {
    /// Retrieves the runner.
    /// WARNING: This will set the runner if it has not yet been set.
    pub fn get_or_create_runner(&mut self) -> Arc<Runner> {
        if let Some(runner) = self.runner.get() {
            return runner.clone();
        }

        let runner_cfg = get_runner_config_from_env();
        match runner_cfg {
            RunnerConfig::Native => {
                let runner =
                    Runner::Native(NativeRunner::try_new().expect("Failed to create NativeRunner"));
                let runner = Arc::new(runner);
                self.runner
                    .set(runner.clone())
                    .expect("Failed to set runner");

                runner
            }
            #[cfg(feature = "python")]
            RunnerConfig::Ray {
                address,
                max_task_backlog,
                force_client_mode,
            } => {
                let ray_engine = RayRunner::try_new(address, max_task_backlog, force_client_mode)
                    .expect("Failed to create RayEngine");

                let runner = Runner::Ray(ray_engine);
                let runner = Arc::new(runner);
                self.runner
                    .set(runner.clone())
                    .expect("Failed to set runner");
                runner
            }
        }
    }
}

#[cfg(feature = "python")]
impl DaftContext {
    pub fn get_or_create_runner(&self) -> Arc<Runner> {
        let mut lock = self
            .write()
            .expect("Failed to acquire write lock on DaftContext");
        lock.get_or_create_runner()
    }
}

#[derive(Debug, Clone)]
pub struct DaftContext {
    state: Arc<RwLock<ContextState>>,
}

impl std::ops::Deref for DaftContext {
    type Target = Arc<RwLock<ContextState>>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl std::ops::DerefMut for DaftContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

static DAFT_CONTEXT: OnceCell<DaftContext> = OnceCell::new();

#[cfg(feature = "python")]
pub fn get_context() -> DaftContext {
    match DAFT_CONTEXT.get() {
        Some(ctx) => ctx.clone(),
        None => {
            let state = ContextState {
                config: Default::default(),
                _catalog: Arc::new(RwLock::new(DaftCatalog::default())),
                runner: OnceCell::new(),
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

#[cfg(feature = "python")]
pub fn set_runner_ray(
    address: Option<String>,
    max_task_backlog: Option<usize>,
    force_client_mode: Option<bool>,
) -> DaftResult<DaftContext> {
    let ctx = get_context();
    let lock = ctx
        .write()
        .map_err(|e| common_error::DaftError::InternalError(format!("{:?}", e)))?;
    if lock.runner.get().is_some() {
        return Err(common_error::DaftError::InternalError(
            "Cannot set runner more than once".to_string(),
        ))?;
    }

    let runner = Runner::Ray(RayRunner::try_new(
        address,
        max_task_backlog,
        force_client_mode,
    )?);
    let runner = Arc::new(runner);
    lock.runner
        .set(runner.clone())
        .expect("Failed to set runner");

    Ok(ctx.clone().into())
}

#[cfg(feature = "python")]
pub fn set_runner_native() -> DaftResult<DaftContext> {
    let ctx = get_context();
    let lock = ctx
        .write()
        .map_err(|e| common_error::DaftError::InternalError(format!("{:?}", e)))?;
    if lock.runner.get().is_some() {
        return Err(common_error::DaftError::InternalError(
            "Cannot set runner more than once".to_string(),
        ))?;
    }

    let runner = Runner::Native(NativeRunner::try_new()?);
    let runner = Arc::new(runner);
    lock.runner
        .set(runner.clone())
        .expect("Failed to set runner");

    Ok(ctx.clone().into())
}

#[cfg(feature = "python")]
fn get_runner_config_from_env() -> RunnerConfig {
    const DAFT_RUNNER: &str = "DAFT_RUNNER";
    const DAFT_RAY_ADDRESS: &str = "DAFT_RAY_ADDRESS";
    const RAY_ADDRESS: &str = "RAY_ADDRESS";
    const DAFT_DEVELOPER_RAY_MAX_TASK_BACKLOG: &str = "DAFT_DEVELOPER_RAY_MAX_TASK_BACKLOG";
    const DAFT_RAY_FORCE_CLIENT_MODE: &str = "DAFT_RAY_FORCE_CLIENT_MODE";

    let runner_from_envvar = std::env::var(DAFT_RUNNER).unwrap_or_default();
    match runner_from_envvar.as_str() {
        "ray" => {
            use pyo3::prelude::*;
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
                .map(|s| match s.trim().to_lowercase().as_str() {
                    "true" | "1" => true,
                    _ => false,
                });

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
                if let Ok(_) = std::env::var("RAY_JOB_ID") {
                    ray_is_in_job = true;
                }
                Some(())
            });

            if !in_ray_worker && (ray_is_initialized || ray_is_in_job) {
                RunnerConfig::Ray {
                    address: None,
                    max_task_backlog,
                    force_client_mode,
                }
            } else {
                RunnerConfig::Ray {
                    address,
                    max_task_backlog,
                    force_client_mode,
                }
            }
        }
        _ => RunnerConfig::Native,
    }
}

#[cfg(feature = "python")]
mod python {

    use common_daft_config::{PyDaftExecutionConfig, PyDaftPlanningConfig};
    use pyo3::prelude::*;

    use crate::{DaftContext, Runner, RunnerConfig};

    #[pyclass]
    pub struct PyRunnerConfig {
        _inner: RunnerConfig,
    }

    #[pyclass]
    pub struct PyDaftContext {
        inner: crate::DaftContext,
    }

    #[pymethods]
    impl PyDaftContext {
        #[new]
        pub fn new() -> Self {
            Self {
                inner: crate::get_context(),
            }
        }

        pub fn get_or_create_runner(&self, py: Python) -> PyResult<PyObject> {
            let mut lock = self
                .inner
                .write()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{:?}", e)))?;

            let runner = lock.get_or_create_runner();
            match runner.as_ref() {
                Runner::Ray(ray) => {
                    let pyobj = ray.ray_runner.as_ref();
                    Ok(pyobj.clone_ref(py))
                }
                Runner::Native(native) => {
                    let pyobj = native.instance.as_ref();
                    Ok(pyobj.clone_ref(py))
                }
            }
        }
        #[getter(_daft_execution_config)]
        pub fn get_daft_execution_config(&self) -> PyResult<PyDaftExecutionConfig> {
            let state = self.inner.state.read().unwrap();
            let config = state.config.read().unwrap().execution.clone();
            let config = PyDaftExecutionConfig { config };
            Ok(config)
        }

        #[getter(_daft_planning_config)]
        pub fn get_daft_planning_config(&self) -> PyResult<PyDaftPlanningConfig> {
            let state = self.inner.state.read().unwrap();
            let config = state.config.read().unwrap().planning.clone();
            let config = PyDaftPlanningConfig { config };
            Ok(config)
        }

        #[setter(_daft_execution_config)]
        pub fn set_daft_execution_config(&mut self, config: PyDaftExecutionConfig) {
            let state = self.inner.state.write().unwrap();
            state.config.write().unwrap().execution = config.config;
        }

        #[setter(_daft_planning_config)]
        pub fn set_daft_planning_config(&mut self, config: PyDaftPlanningConfig) {
            let state = self.inner.state.write().unwrap();
            state.config.write().unwrap().planning = config.config;
        }
    }
    impl From<DaftContext> for PyDaftContext {
        fn from(ctx: DaftContext) -> Self {
            Self { inner: ctx }
        }
    }

    #[pyfunction]
    pub fn get_runner_config_from_env() -> PyRunnerConfig {
        PyRunnerConfig {
            _inner: super::get_runner_config_from_env(),
        }
    }

    #[pyfunction]
    pub fn get_context() -> PyDaftContext {
        PyDaftContext {
            inner: super::get_context(),
        }
    }

    #[pyfunction(signature = (
    address = None,
    noop_if_initialized = false,
    max_task_backlog = None,
    force_client_mode = false
))]
    pub fn set_runner_ray(
        address: Option<String>,
        noop_if_initialized: Option<bool>,
        max_task_backlog: Option<usize>,
        force_client_mode: Option<bool>,
    ) -> PyResult<PyDaftContext> {
        let noop_if_initialized = noop_if_initialized.unwrap_or(false);
        let res = super::set_runner_ray(address, max_task_backlog, force_client_mode)
            .map(|ctx| ctx.into());
        if noop_if_initialized {
            match res {
                Err(_) => Ok(super::get_context().into()),
                Ok(ctx) => Ok(ctx),
            }
        } else {
            res.map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{:?}", e)))
        }
    }

    #[pyfunction]
    pub fn set_runner_native() -> PyResult<PyDaftContext> {
        super::set_runner_native()
            .map(|ctx| ctx.into())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{:?}", e)))
    }
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
    parent.add_class::<Config>()?;
    Ok(())
}
