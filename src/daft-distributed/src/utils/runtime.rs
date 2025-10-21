use std::sync::{Once, OnceLock};

use common_runtime::{PoolType, Runtime, RuntimeRef};

pub static RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();
pub static PYO3_RUNTIME_INITIALIZED: Once = Once::new();
#[cfg(feature = "python")]
pub static PYO3_ASYNC_RUNTIME_LOCALS: OnceLock<pyo3_async_runtimes::TaskLocals> = OnceLock::new();

pub fn get_or_init_runtime() -> &'static Runtime {
    let runtime_ref = RUNTIME.get_or_init(|| {
        let mut tokio_runtime_builder = tokio::runtime::Builder::new_multi_thread();
        tokio_runtime_builder.enable_all();
        tokio_runtime_builder.worker_threads(1);
        tokio_runtime_builder.thread_name_fn(move || "Daft-Scheduler".to_string());
        let tokio_runtime = tokio_runtime_builder
            .build()
            .expect("Failed to build runtime");
        Runtime::new(
            tokio_runtime,
            PoolType::Custom("daft-scheduler".to_string()),
        )
    });
    #[cfg(feature = "python")]
    {
        PYO3_RUNTIME_INITIALIZED.call_once(|| {
            pyo3_async_runtimes::tokio::init_with_runtime(&runtime_ref.runtime)
                .expect("Failed to initialize python runtime");
            PYO3_ASYNC_RUNTIME_LOCALS.get_or_init(|| {
                use pyo3::Python;

                Python::attach(|py| {
                    pyo3_async_runtimes::tokio::get_current_locals(py)
                        .expect("Failed to get current task locals")
                })
            });
        });
    }
    runtime_ref
}

#[cfg(feature = "python")]
/// Get a clone of the Python task locals for scoping async operations
pub fn get_task_locals(py: pyo3::Python) -> pyo3_async_runtimes::TaskLocals {
    PYO3_ASYNC_RUNTIME_LOCALS
        .get()
        .expect("Python task locals not initialized")
        .clone_ref(py)
}

#[cfg(feature = "python")]
/// Execute a Python coroutine and extract the result.
pub async fn execute_python_coroutine<F, R>(
    coroutine_builder: F,
    task_locals: Option<pyo3_async_runtimes::TaskLocals>,
) -> common_error::DaftResult<R>
where
    F: FnOnce(pyo3::Python) -> pyo3::PyResult<pyo3::Bound<'_, pyo3::PyAny>> + Send + 'static,
    R: for<'py> pyo3::FromPyObject<'py> + Send + 'static,
{
    use pyo3::Python;

    // Get task_locals if not provided
    let task_locals = if let Some(task_locals) = task_locals {
        task_locals
    } else {
        Python::attach(get_task_locals)
    };

    common_runtime::python::execute_python_coroutine(coroutine_builder, task_locals).await
}
