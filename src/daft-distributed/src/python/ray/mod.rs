mod task;
mod worker;
mod worker_manager;

use common_error::DaftResult;
pub use daft_partition_refs::RayPartitionRef;
use pyo3::prelude::*;
pub(crate) use task::{RaySwordfishTask, RayTaskResult};
pub(crate) use worker::RaySwordfishWorker;
pub(crate) use worker_manager::RayWorkerManager;

/// Call Python to clear shuffle directories: `shuffle_dirs` on every Ray node,
/// `shared_dirs` exactly once.
pub(super) async fn clear_shuffle_dirs_on_all_nodes(
    shuffle_dirs: Vec<String>,
    shared_dirs: Vec<String>,
) -> DaftResult<()> {
    common_runtime::python::execute_python_coroutine_noreturn(move |py| {
        let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;

        let coroutine = flotilla_module.call_method1(
            pyo3::intern!(py, "clear_flight_shuffle_dirs_on_all_nodes"),
            (shuffle_dirs, shared_dirs),
        )?;

        Ok(coroutine)
    })
    .await?;

    Ok(())
}

/// Await the per-worker calls that drop finished shuffles' Flight registrations.
///
/// Failures are logged Python-side rather than raised: a worker that died or is
/// unreachable has already lost the registry we were asking it to trim, and the
/// query is over either way.
pub(super) async fn await_shuffle_unregistrations(refs: Vec<Py<PyAny>>) -> DaftResult<()> {
    if refs.is_empty() {
        return Ok(());
    }
    common_runtime::python::execute_python_coroutine_noreturn(move |py| {
        let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;

        let coroutine = flotilla_module.call_method1(
            pyo3::intern!(py, "await_flight_shuffle_unregistrations"),
            (refs,),
        )?;

        Ok(coroutine)
    })
    .await?;

    Ok(())
}
