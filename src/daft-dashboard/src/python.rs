use std::time::Duration;

use pyo3::{exceptions, pyclass, pyfunction, pymethods, PyErr, PyResult};
use reqwest::blocking::Client;
use tokio::{
    runtime::{Builder, Runtime},
    sync::oneshot,
};

/// Connection to a Daft Dashboard server
///
/// * If the server is running on a separate process, shutdown_signal will be None
/// * Otherwise, shutdown_signal will be Some(sender) and the sender can be used to
///   shutdown the server. Often for notebook environments.
pub enum ConnectionHandle {
    Remote {
        url: String,
    },
    Local {
        join_handle: std::thread::JoinHandle<()>,
        shutdown_tx: oneshot::Sender<()>,
        url: String, // Expected to be localhost:port
    },
}

impl ConnectionHandle {
    pub fn shutdown(self) {
        match self {
            Self::Remote { .. } => (),
            Self::Local {
                shutdown_tx,
                join_handle,
                ..
            } => {
                shutdown_tx
                    .send(())
                    .expect("Failed to send shutdown signal");
                join_handle.join().expect("Failed to join join handle");
            }
        }
    }

    pub fn url(&self) -> &str {
        match self {
            Self::Local { url, .. } => url,
            Self::Remote { url } => url,
        }
    }
}

#[pyclass]
pub struct PyConnectionHandle(Option<ConnectionHandle>);

impl PyConnectionHandle {
    fn new(handle: ConnectionHandle) -> Self {
        Self(Some(handle))
    }

    fn inner(&self) -> PyResult<&ConnectionHandle> {
        self.0.as_ref().ok_or_else(|| {
            PyErr::new::<exceptions::PyRuntimeError, _>("Dashboard connection has been closed")
        })
    }

    fn inner_take(&mut self) -> PyResult<ConnectionHandle> {
        self.0.take().ok_or_else(|| {
            PyErr::new::<exceptions::PyRuntimeError, _>("Dashboard connection has been closed")
        })
    }
}

#[pymethods]
impl PyConnectionHandle {
    fn shutdown(&mut self) -> PyResult<()> {
        self.inner_take()?.shutdown();
        Ok(())
    }

    fn url(&self) -> PyResult<&str> {
        Ok(self.inner()?.url())
    }

    pub fn register_dataframe_for_display(
        &self,
        query_id: String,
        duration: Duration,
        rb: daft_recordbatch::python::PyRecordBatch,
    ) -> PyResult<()> {
        let rb_bytes = rb.record_batch.to_ipc_stream()?;
        let body = serde_json::json!({
            "duration": duration,
            "rb_bytes": rb_bytes,
        });

        let client = Client::new();
        let res = client
            .post(format!("{}/queries/{}", self.url()?, query_id))
            .json(&body)
            .send()
            .map_err(|e| PyErr::new::<exceptions::PyRuntimeError, _>(e.to_string()))?;

        res.error_for_status()
            .map_err(|e| PyErr::new::<exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    pub fn generate_interactive_html(&self, query_id: String) -> PyResult<String> {
        let client = Client::new();
        let res = client
            .get(format!("{}/queries/{}/dataframe", self.url()?, query_id))
            .send()
            .map_err(|e| PyErr::new::<exceptions::PyRuntimeError, _>(e.to_string()))?;

        match res.error_for_status() {
            Ok(res) => {
                let html = res
                    .text()
                    .map_err(|e| PyErr::new::<exceptions::PyRuntimeError, _>(e.to_string()))?;
                Ok(html)
            }
            Err(e) => Err(PyErr::new::<exceptions::PyRuntimeError, _>(e.to_string())),
        }
    }
}

/// Connect to a Daft Dashboard server
///
/// * If DAFT_DASHBOARD_URL is set, connect to the server at the given URL.
/// * Otherwise, start a new server on an available port.
#[pyfunction]
pub fn connect() -> PyConnectionHandle {
    if let Ok(url) = std::env::var(super::DAFT_DASHBOARD_URL_ENV) {
        PyConnectionHandle::new(ConnectionHandle::Remote { url })
    } else {
        let listener = Runtime::new()
            .unwrap()
            .block_on(async { crate::make_listener().await.unwrap() });
        let url = listener.local_addr().unwrap().to_string();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = std::thread::spawn(move || {
            let rt = Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                super::launch_server(listener, async { shutdown_rx.await.unwrap() }).await;
            });
        });

        PyConnectionHandle::new(ConnectionHandle::Local {
            join_handle: handle,
            shutdown_tx,
            url,
        })
    }
}
