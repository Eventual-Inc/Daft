use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::{PyErr, PyResult, exceptions, pyclass, pyfunction, pymethods};
use tokio::{
    runtime::{Builder, Runtime},
    sync::oneshot,
};

use crate::{DEFAULT_SERVER_PORT, state::GLOBAL_DASHBOARD_STATE};

static DASHBOARD_ENABLED: AtomicBool = AtomicBool::new(false);

#[pyclass]
pub struct ConnectionHandle {
    shutdown_signal: Option<oneshot::Sender<()>>,
    port: u16,
}

#[pymethods]
impl ConnectionHandle {
    pub fn shutdown(&mut self, noop_if_shutdown: bool) -> PyResult<()> {
        match (self.shutdown_signal.take(), noop_if_shutdown) {
            (Some(shutdown_signal), _) => shutdown_signal.send(()).map_err(|()| {
                PyErr::new::<exceptions::PyRuntimeError, _>("unable to send shutdown signal")
            }),
            (None, true) => Ok(()),
            (None, false) => Err(PyErr::new::<exceptions::PyRuntimeError, _>(
                "shutdown signal already sent",
            )),
        }
    }

    pub fn get_port(&self) -> u16 {
        self.port
    }
}

#[pyfunction]
pub fn register_dataframe_for_display(
    record_batch: daft_recordbatch::python::PyRecordBatch,
) -> PyResult<String> {
    let df_id = GLOBAL_DASHBOARD_STATE.register_dataframe_preview(record_batch.record_batch);
    Ok(df_id)
}

#[pyfunction]
pub fn generate_interactive_html(df_id: String) -> PyResult<String> {
    let record_batch = GLOBAL_DASHBOARD_STATE
        .get_dataframe_preview(&df_id)
        .ok_or_else(|| {
            PyErr::new::<exceptions::PyRuntimeError, _>(format!(
                "DataFrame with id '{}' does not exist",
                df_id
            ))
        })?;

    let html = super::generate_interactive_html(
        &record_batch,
        &df_id,
        &super::DEFAULT_SERVER_ADDR.to_string(),
        DEFAULT_SERVER_PORT,
    );
    Ok(html)
}

fn tokio_runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

#[pyfunction]
pub fn launch(noop_if_initialized: bool) -> PyResult<ConnectionHandle> {
    // Check if server is already running
    if DASHBOARD_ENABLED.load(Ordering::SeqCst) {
        if noop_if_initialized {
            return Ok(ConnectionHandle {
                shutdown_signal: None,
                port: super::DEFAULT_SERVER_PORT,
            });
        } else {
            return Err(PyErr::new::<exceptions::PyRuntimeError, _>(
                "Server is already running",
            ));
        }
    }

    let port = super::DEFAULT_SERVER_PORT;
    let (send, recv) = oneshot::channel::<()>();

    let handle = ConnectionHandle {
        shutdown_signal: Some(send),
        port,
    };
    let _ = std::thread::spawn(move || {
        DASHBOARD_ENABLED.store(true, Ordering::SeqCst);
        let res = tokio_runtime().block_on(async {
            super::launch_server(
                std::net::IpAddr::V4(super::DEFAULT_SERVER_ADDR),
                port,
                async move { recv.await.unwrap() },
            )
            .await
        });
        DASHBOARD_ENABLED.store(false, Ordering::SeqCst);
        res
    });

    DASHBOARD_ENABLED.store(true, Ordering::SeqCst);
    Ok(handle)
}
