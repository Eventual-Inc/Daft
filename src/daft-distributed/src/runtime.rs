use std::sync::{Once, OnceLock};

use tokio::runtime::Runtime;

pub static RUNTIME: OnceLock<Runtime> = OnceLock::new();
pub static PYO3_RUNTIME_INITIALIZED: Once = Once::new();
#[cfg(feature = "python")]
pub static TASK_LOCALS: OnceLock<pyo3_async_runtimes::TaskLocals> = OnceLock::new();

pub fn get_or_init_runtime() -> &'static Runtime {
    let runtime = RUNTIME.get_or_init(|| {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        runtime.enable_all();
        runtime.worker_threads(1);
        runtime.build().unwrap()
    });
    #[cfg(feature = "python")]
    {
        PYO3_RUNTIME_INITIALIZED.call_once(|| {
            pyo3_async_runtimes::tokio::init_with_runtime(runtime).unwrap();
        });
    }
    runtime
}

#[cfg(feature = "python")]
pub fn get_or_init_task_locals(py: pyo3::Python<'_>) -> &'static pyo3_async_runtimes::TaskLocals {
    TASK_LOCALS.get_or_init(|| pyo3_async_runtimes::tokio::get_current_locals(py).unwrap())
}
