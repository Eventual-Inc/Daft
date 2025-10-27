#[cfg(feature = "python")]
use std::sync::OnceLock;

#[cfg(feature = "python")]
use common_error::DaftResult;
#[cfg(feature = "python")]
use pyo3::Python;

/// Global storage for Python async runtime task locals
#[cfg(feature = "python")]
static PYO3_ASYNC_RUNTIME_LOCALS: OnceLock<pyo3_async_runtimes::TaskLocals> = OnceLock::new();

#[cfg(feature = "python")]
/// Initialize the Python task locals from the current Python context.
///
/// This should be called once during runtime initialization when the async runtime
/// has been set up and we have access to a Python instance.
///
/// # Panics
/// Panics if unable to get the current task locals from pyo3_async_runtimes.
pub fn init_task_locals(py: Python) {
    PYO3_ASYNC_RUNTIME_LOCALS.get_or_init(|| {
        pyo3_async_runtimes::tokio::get_current_locals(py)
            .expect("Failed to get current task locals")
    });
}

#[cfg(feature = "python")]
/// Get a clone of the Python task locals for scoping async operations.
///
/// # Panics
/// Panics if the task locals have not been initialized via `init_task_locals`.
pub fn get_task_locals() -> &'static pyo3_async_runtimes::TaskLocals {
    PYO3_ASYNC_RUNTIME_LOCALS
        .get()
        .expect("Python task locals not initialized. Call init_task_locals first.")
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
pub async fn execute_python_coroutine<F, R>(
    coroutine_builder: F,
    task_locals: pyo3_async_runtimes::TaskLocals,
) -> DaftResult<R>
where
    F: FnOnce(Python) -> pyo3::PyResult<pyo3::Bound<'_, pyo3::PyAny>> + Send + 'static,
    R: for<'py> pyo3::FromPyObject<'py> + Send + 'static,
{
    // Execute the coroutine with the task_locals
    let await_coroutine = async move {
        let result = Python::attach(|py| {
            let coroutine: pyo3::Bound<'_, pyo3::PyAny> = coroutine_builder(py)?;
            pyo3_async_runtimes::tokio::into_future(coroutine)
        })?
        .await?;
        DaftResult::Ok(result)
    };

    let result = pyo3_async_runtimes::tokio::scope(task_locals, await_coroutine).await?;

    // Extract the result
    let extracted = Python::attach(|py| result.extract::<R>(py))?;
    Ok(extracted)
}
