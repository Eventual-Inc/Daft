#[cfg(feature = "python")]
use common_error::DaftResult;
#[cfg(feature = "python")]
use pyo3::Python;

#[cfg(feature = "python")]
/// Execute a Python coroutine and extract the result.
///
/// This function takes a coroutine builder that creates a Python coroutine,
/// executes it asynchronously with proper task locals scoping, and extracts
/// the result into a Rust type.
///
/// # Arguments
/// * `coroutine_builder` - A closure that creates the Python coroutine
/// * `task_locals` - The pyo3-async-runtimes task locals for scoping
///
/// # Example
/// ```ignore
/// let result: MyType = execute_python_coroutine(
///     |py| {
///         let coro = some_python_obj.call_method0(py, "async_method")?;
///         Ok(coro.into_bound(py))
///     },
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
