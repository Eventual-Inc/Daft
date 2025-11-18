use std::sync::Arc;

use common_error::DaftError;
use pyo3::{IntoPyObjectExt, PyResult, Python, pyfunction};

use crate::runners::{self, DAFT_RUNNER, NativeRunner, RayRunner, Runner, RunnerConfig};

#[pyfunction]
pub fn get_runner(py: Python) -> PyResult<Option<pyo3::Py<pyo3::PyAny>>> {
    let runner = py.detach(|| DAFT_RUNNER.get().cloned());
    Ok(runner.map(|r| r.to_pyobj(py)))
}

#[pyfunction]
pub fn get_or_create_runner(py: Python) -> PyResult<pyo3::Py<pyo3::PyAny>> {
    let runner = py.detach(runners::get_or_create_runner)?;
    Ok(runner.to_pyobj(py))
}

#[pyfunction]
pub fn get_or_infer_runner_type(py: Python) -> PyResult<pyo3::Py<pyo3::PyAny>> {
    match runners::DAFT_RUNNER.get() {
        Some(runner) => match runner.as_ref() {
            Runner::Ray(_) => RayRunner::NAME,
            Runner::Native(_) => NativeRunner::NAME,
        },
        None => {
            if let (true, _) = runners::detect_ray_state() {
                RayRunner::NAME
            } else {
                match runners::get_runner_config_from_env()? {
                    RunnerConfig::Ray { .. } => RayRunner::NAME,
                    RunnerConfig::Native { .. } => NativeRunner::NAME,
                }
            }
        }
    }
    .into_py_any(py)
}

#[pyfunction(signature = (
    address = None,
    noop_if_initialized = false,
    max_task_backlog = None,
    force_client_mode = false
))]
pub fn set_runner_ray(
    py: Python,
    address: Option<String>,
    noop_if_initialized: Option<bool>,
    max_task_backlog: Option<usize>,
    force_client_mode: Option<bool>,
) -> PyResult<pyo3::Py<pyo3::PyAny>> {
    let noop_if_initialized = noop_if_initialized.unwrap_or(false);

    let runner_type = runners::get_runner_type_from_env();
    if !runner_type.is_empty() && runner_type != RayRunner::NAME {
        log::warn!(
            "Ignore inconsistent $DAFT_RUNNER='{}' env when setting runner as ray",
            runner_type
        );
    }

    let runner = Arc::new(Runner::Ray(RayRunner::try_new(
        address,
        max_task_backlog,
        force_client_mode,
    )?));

    match runners::DAFT_RUNNER.set(runner.clone()) {
        Ok(()) => Ok(runner.to_pyobj(py)),
        Err(_) if noop_if_initialized => {
            Ok(runners::DAFT_RUNNER.get().unwrap().clone().to_pyobj(py))
        }
        Err(_) => {
            Err(DaftError::InternalError("Cannot set runner more than once".to_string()).into())
        }
    }
}

#[pyfunction(signature = (num_threads = None))]
pub fn set_runner_native(
    py: Python,
    num_threads: Option<usize>,
) -> PyResult<pyo3::Py<pyo3::PyAny>> {
    let runner_type = runners::get_runner_type_from_env();
    if !runner_type.is_empty() && runner_type != NativeRunner::NAME {
        log::warn!(
            "Ignore inconsistent $DAFT_RUNNER='{}' env when setting runner as native",
            runner_type
        );
    }

    let runner = Arc::new(Runner::Native(NativeRunner::try_new(num_threads)?));
    match runners::DAFT_RUNNER.set(runner.clone()) {
        Ok(()) => Ok(runner.to_pyobj(py)),
        Err(_) => {
            Err(DaftError::InternalError("Cannot set runner more than once".to_string()).into())
        }
    }
}
