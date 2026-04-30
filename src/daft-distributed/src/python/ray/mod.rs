mod task;
mod worker;
mod worker_manager;

use daft_common::error::DaftResult;
pub use daft_local_plan::partition_refs::RayPartitionRef;
use pyo3::prelude::*;
pub(crate) use task::{RaySwordfishTask, RayTaskResult};
pub(crate) use worker::RaySwordfishWorker;
pub(crate) use worker_manager::RayWorkerManager;

/// Call Python to clear shuffle directories on all Ray nodes
pub(crate) async fn clear_shuffle_dirs_on_all_nodes(shuffle_dirs: Vec<String>) -> DaftResult<()> {
    daft_common::runtime::python::execute_python_coroutine_noreturn(move |py| {
        let flotilla_module = py.import(pyo3::intern!(py, "daft.runners.flotilla"))?;

        let coroutine = flotilla_module.call_method1(
            pyo3::intern!(py, "clear_flight_shuffle_dirs_on_all_nodes"),
            (shuffle_dirs,),
        )?;

        Ok(coroutine)
    })
    .await?;

    Ok(())
}
