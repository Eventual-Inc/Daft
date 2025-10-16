mod task;
mod worker;
mod worker_manager;

use common_error::DaftResult;
use pyo3::prelude::*;
pub(crate) use task::{RayPartitionRef, RaySwordfishTask, RayTaskResult};
pub(crate) use worker::RaySwordfishWorker;
pub(crate) use worker_manager::RayWorkerManager;

/// Call Python to clear shuffle directories on all Ray nodes
pub(crate) async fn clear_shuffle_dirs_on_all_nodes(shuffle_dirs: Vec<String>) -> DaftResult<()> {
    let task_locals = Python::with_gil(|py| {
        crate::utils::runtime::PYO3_ASYNC_RUNTIME_LOCALS
            .get()
            .expect("Python task locals not initialized")
            .clone_ref(py)
    });

    let cleanup_fut = async move {
        let result = Python::with_gil(|py| {
            let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;

            let coroutine = flotilla_module.call_method1(
                pyo3::intern!(py, "clear_flight_shuffle_dirs_on_all_nodes"),
                (shuffle_dirs,),
            )?;
            pyo3_async_runtimes::tokio::into_future(coroutine)
        })?
        .await?;
        DaftResult::Ok(result)
    };

    pyo3_async_runtimes::tokio::scope(task_locals, cleanup_fut).await?;

    Ok(())
}
