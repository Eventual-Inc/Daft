use std::sync::{Once, OnceLock};

use tokio::runtime::Runtime;

pub static RUNTIME: OnceLock<Runtime> = OnceLock::new();
pub static PYO3_RUNTIME_INITIALIZED: Once = Once::new();

pub fn get_or_init_runtime() -> &'static Runtime {
    let runtime = RUNTIME.get_or_init(|| {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        runtime.enable_all();
        runtime.worker_threads(1);
        runtime.build().expect("Failed to build runtime")
    });
    #[cfg(feature = "python")]
    {
        PYO3_RUNTIME_INITIALIZED.call_once(|| {
            pyo3_async_runtimes::tokio::init_with_runtime(runtime)
                .expect("Failed to initialize python runtime");
        });
    }
    runtime
}

pub type JoinSet<T> = tokio::task::JoinSet<T>;
pub type JoinHandle<T> = tokio::task::JoinHandle<T>;

#[allow(dead_code)]
pub fn create_join_set<T>() -> JoinSet<T> {
    tokio::task::JoinSet::new()
}
