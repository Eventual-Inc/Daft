//! Wrappers around the Python Runner ABC class
//! and utilities for runner detection
use std::sync::{Arc, OnceLock};

use common_error::{DaftError, DaftResult};
use daft_logical_plan::{LogicalPlanBuilder, PyLogicalPlanBuilder};
use daft_micropartition::{MicroPartitionRef, python::PyMicroPartition};
use pyo3::{
    exceptions::PyValueError,
    intern,
    prelude::*,
    types::{PyDict, PyIterator},
};

#[derive(Debug)]
pub struct RayRunner {
    pub pyobj: Arc<pyo3::Py<pyo3::PyAny>>,
}

impl RayRunner {
    pub const NAME: &'static str = "ray";

    pub fn try_new(
        address: Option<String>,
        max_task_backlog: Option<usize>,
        force_client_mode: Option<bool>,
    ) -> DaftResult<Self> {
        Python::attach(|py| {
            let ray_runner_module = py.import(intern!(py, "daft.runners.ray_runner"))?;
            let ray_runner = ray_runner_module.getattr(intern!(py, "RayRunner"))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item(intern!(py, "address"), address)?;
            kwargs.set_item(intern!(py, "max_task_backlog"), max_task_backlog)?;
            kwargs.set_item(intern!(py, "force_client_mode"), force_client_mode)?;

            let instance = ray_runner.call((), Some(&kwargs))?;
            let instance = instance.unbind();

            Ok(Self {
                pyobj: Arc::new(instance),
            })
        })
    }
}

#[derive(Debug)]
pub struct NativeRunner {
    pub pyobj: Arc<pyo3::Py<pyo3::PyAny>>,
}

impl NativeRunner {
    pub const NAME: &'static str = "native";

    pub fn try_new(num_threads: Option<usize>) -> DaftResult<Self> {
        Python::attach(|py| {
            let native_runner_module = py.import(intern!(py, "daft.runners.native_runner"))?;
            let native_runner = native_runner_module.getattr(intern!(py, "NativeRunner"))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item(intern!(py, "num_threads"), num_threads)?;

            let instance = native_runner.call((), Some(&kwargs))?;
            let instance = instance.unbind();

            Ok(Self {
                pyobj: Arc::new(instance),
            })
        })
    }
}

#[derive(Debug)]
pub enum Runner {
    Ray(RayRunner),
    Native(NativeRunner),
}

impl Runner {
    pub fn from_pyobj(obj: pyo3::Py<pyo3::PyAny>) -> PyResult<Self> {
        Python::attach(|py| {
            let name = obj.getattr(py, "name")?.extract::<String>(py)?;
            match name.as_ref() {
                RayRunner::NAME => {
                    let ray_runner = RayRunner {
                        pyobj: Arc::new(obj),
                    };
                    Ok(Self::Ray(ray_runner))
                }
                NativeRunner::NAME => {
                    let native_runner = NativeRunner {
                        pyobj: Arc::new(obj),
                    };
                    Ok(Self::Native(native_runner))
                }
                _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Unknown runner type: {name}"
                ))),
            }
        })
    }

    fn get_runner_ref(&self) -> &pyo3::Py<pyo3::PyAny> {
        match self {
            Self::Ray(RayRunner { pyobj }) => pyobj.as_ref(),
            Self::Native(NativeRunner { pyobj }) => pyobj.as_ref(),
        }
    }
    pub fn run_iter_tables<'py>(
        &self,
        py: Python<'py>,
        lp: LogicalPlanBuilder,
        results_buffer_size: Option<usize>,
    ) -> DaftResult<impl Iterator<Item = DaftResult<MicroPartitionRef>> + 'py> {
        let pyobj = self.get_runner_ref();
        let py_lp = PyLogicalPlanBuilder::new(lp);
        let builder = py.import(intern!(py, "daft.logical.builder"))?;
        let builder = builder.getattr(intern!(py, "LogicalPlanBuilder"))?;
        let builder = builder.call((py_lp,), None)?;
        let result = pyobj.call_method(
            py,
            intern!(py, "run_iter_tables"),
            (builder, results_buffer_size),
            None,
        )?;

        let result = result.bind(py);
        let iter = PyIterator::from_object(result)?;

        let iter = iter.map(move |item| {
            let item = item?;
            let partition = item.getattr(intern!(py, "_micropartition"))?;
            let partition = partition.extract::<PyMicroPartition>()?;
            let partition = partition.inner;
            Ok::<_, DaftError>(partition)
        });

        Ok(iter)
    }

    pub fn to_pyobj(self: Arc<Self>, py: Python) -> pyo3::Py<pyo3::PyAny> {
        let runner = self.get_runner_ref();
        runner.clone_ref(py)
    }
}

#[derive(Debug)]
#[pyclass]
pub enum RunnerConfig {
    Native {
        num_threads: Option<usize>,
    },
    Ray {
        address: Option<String>,
        max_task_backlog: Option<usize>,
        force_client_mode: Option<bool>,
    },
}

impl RunnerConfig {
    pub fn create_runner(self) -> DaftResult<Runner> {
        match self {
            Self::Native { num_threads } => Ok(Runner::Native(NativeRunner::try_new(num_threads)?)),
            Self::Ray {
                address,
                max_task_backlog,
                force_client_mode,
            } => Ok(Runner::Ray(RayRunner::try_new(
                address,
                max_task_backlog,
                force_client_mode,
            )?)),
        }
    }
}

// ---------------------- Detection utilities ---------------------- //

/// Helper function to automatically detect whether to use the ray runner.
pub fn detect_ray_state() -> (bool, bool) {
    Python::attach(|py| {
        py.import(pyo3::intern!(py, "daft.utils"))
            .and_then(|m| m.getattr(pyo3::intern!(py, "detect_ray_state")))
            .and_then(|m| m.call0())
            .and_then(|m| m.extract())
            .unwrap_or((false, false))
    })
}

// ----------------- Environment variable handling ----------------- //

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

/// Helper function to get the runner type from the environment.
pub(crate) fn get_runner_type_from_env() -> String {
    const DAFT_RUNNER: &str = "DAFT_RUNNER";

    std::env::var(DAFT_RUNNER)
        .unwrap_or_default()
        .to_lowercase()
}

pub(crate) fn get_runner_config_from_env() -> PyResult<RunnerConfig> {
    match get_runner_type_from_env().as_str() {
        NativeRunner::NAME => Ok(RunnerConfig::Native { num_threads: None }),
        RayRunner::NAME => Ok(get_ray_runner_config_from_env()),
        "py" => Err(PyValueError::new_err(
            "The PyRunner was removed from Daft from v0.5.0 onwards. \
            Please set the env to `DAFT_RUNNER=native`."
                .to_string(),
        )
        .into()),
        "" => Ok(if detect_ray_state() == (true, false) {
            // on ray but not in ray worker
            get_ray_runner_config_from_env()
        } else {
            RunnerConfig::Native { num_threads: None }
        }),
        other => Err(PyValueError::new_err(format!(
            "Invalid runner type `DAFT_RUNNER={other}` specified through the env. \
            Please use either `native` or `ray`."
        ))),
    }
}

// -------------------------- Singleton -------------------------- //

/// The global runner used to execute queries.
/// It is never possible to set more than once.
pub(crate) static DAFT_RUNNER: OnceLock<Arc<Runner>> = OnceLock::new();

/// Retrieves the runner.
///
/// WARNING: This will set the runner if it has not yet been set.
pub fn get_or_create_runner() -> DaftResult<Arc<Runner>> {
    DAFT_RUNNER
        .get_or_try_init(|| {
            let runner_cfg = get_runner_config_from_env()?;
            let runner = runner_cfg.create_runner()?;

            Ok(Arc::new(runner))
        })
        .cloned()
}
