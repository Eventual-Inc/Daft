#[cfg(feature = "python")]
use std::sync::OnceLock;

#[cfg(feature = "python")]
use common_error::DaftResult;
#[cfg(feature = "python")]
use pyo3::PyErr;
#[cfg(feature = "python")]
use pyo3::{Python, types::PyAnyMethods};

/// Global storage for Python async runtime task locals
#[cfg(feature = "python")]
static PYO3_ASYNC_RUNTIME_LOCALS: OnceLock<pyo3_async_runtimes::TaskLocals> = OnceLock::new();

#[cfg(feature = "python")]
/// Get or initialize the pyo3 async runtime task locals (which includes the asyncio event loop) from the current Python context.
///
/// This function checks if there is already a running event loop, and if not, it initializes one on a background thread.
fn get_or_init_task_locals() -> &'static pyo3_async_runtimes::TaskLocals {
    PYO3_ASYNC_RUNTIME_LOCALS.get_or_init(|| {
        Python::attach(|py| {
            let event_loop_module = py
                .import(pyo3::intern!(py, "daft.event_loop"))
                .expect("Failed to import event loop module");
            let event_loop = event_loop_module
                .call_method0(pyo3::intern!(py, "get_or_init_event_loop"))
                .expect("Failed to call get_or_init_event_loop method")
                .getattr(pyo3::intern!(py, "loop"))
                .expect("Failed to get event loop attribute");
            pyo3_async_runtimes::TaskLocals::new(event_loop)
                .copy_context(py)
                .expect("Failed to copy context")
        })
    })
}

#[cfg(feature = "python")]
/// Execute a Python coroutine and extract the result.
///
/// This function takes a coroutine builder that creates a Python coroutine,
/// executes it asynchronously with proper task locals scoping, and extracts
/// the result into a Rust type.
///
/// # Example
/// ```
/// let result: MyType = execute_python_coroutine(
///     |py| some_python_obj.call_method0(py, "async_method"),
///     task_locals,
/// ).await?;
/// ```
pub async fn execute_python_coroutine<F, R>(coroutine_builder: F) -> DaftResult<R>
where
    F: FnOnce(Python) -> pyo3::PyResult<pyo3::Bound<'_, pyo3::PyAny>> + Send + 'static,
    R: for<'a, 'py> pyo3::FromPyObject<'a, 'py> + Send + 'static,
    PyErr: for<'a, 'py> From<<R as pyo3::FromPyObject<'a, 'py>>::Error>,
{
    // Execute the coroutine with the task_locals
    let task_locals = get_or_init_task_locals();
    let result = Python::attach(|py| {
        let coroutine: pyo3::Bound<'_, pyo3::PyAny> = coroutine_builder(py)?;
        pyo3_async_runtimes::into_future_with_locals(task_locals, coroutine)
    })?
    .await?;

    // Extract the result
    let extracted = Python::attach(|py| result.extract::<R>(py).map_err(PyErr::from))?;
    Ok(extracted)
}
