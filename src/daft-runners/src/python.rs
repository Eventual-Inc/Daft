use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::{pyfunction, PyObject, PyResult, Python};

use crate::runners;
use crate::runners::{RayRunner, NativeRunner, Runner};


#[cfg(feature = "python")]
#[pyfunction]
pub fn get_or_create_runner(py: Python) -> PyResult<PyObject> {
    Ok(runners::get_or_create_runner()?.to_pyobj(py))
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
) -> PyResult<PyObject> {
    let noop_if_initialized = noop_if_initialized.unwrap_or(false);

    let runner_type = runners::get_runner_type_from_env();
    if !runner_type.is_empty() && runner_type != RayRunner::NAME {
        log::warn!(
            "Ignore inconsistent $DAFT_RUNNER='{}' env when setting runner as ray",
            runner_type
        );
    }
    
    let runner = Arc::new(Runner::Ray(RayRunner::try_new(
        address.clone(),
        max_task_backlog,
        force_client_mode,
    )?));

    match runners::DAFT_RUNNER.set(runner.clone()) {
        Ok(()) => Ok(runner.to_pyobj(py)),
        Err(_) if noop_if_initialized => Ok(runners::DAFT_RUNNER.get().unwrap().clone().to_pyobj(py)),
        Err(_) => Err(PyValueError::new_err(
            "Cannot set runner more than once".to_string(),
        ).into()),
    }
}

#[pyfunction(signature = (num_threads = None))]
pub fn set_runner_native(py: Python, num_threads: Option<usize>) -> PyResult<PyObject> {
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
        Err(_) => Err(PyValueError::new_err(
            "Cannot set runner more than once".to_string(),
        ).into()),
    }
}
