use std::sync::{Once, OnceLock};

use common_runtime::{PoolType, Runtime, RuntimeRef};

pub static RUNTIME: OnceLock<RuntimeRef> = OnceLock::new();
pub static PYO3_RUNTIME_INITIALIZED: Once = Once::new();

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
        });
    }
    runtime_ref
}
