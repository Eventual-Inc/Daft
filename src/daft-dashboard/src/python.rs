use std::io::ErrorKind;

use pyo3::{exceptions, pyclass, pyfunction, pymethods, PyErr, PyResult};
use tokio::{
    runtime::{Builder, Runtime},
    sync::oneshot,
};

use crate::{DashboardState, GLOBAL_DASHBOARD_STATE};

#[pyclass]
pub struct ConnectionHandle {
    shutdown_signal: Option<oneshot::Sender<()>>,
    port: u16,
}

fn make_listener() -> std::io::Result<(std::net::TcpListener, u16)> {
    let mut port = super::DEFAULT_SERVER_PORT;
    let max_port = port + 100; // Try up to 100 ports after the default

    while port <= max_port {
        match std::net::TcpListener::bind((super::DEFAULT_SERVER_ADDR, port)) {
            Ok(listener) => {
                return Ok((listener, port));
            }
            Err(e) if e.kind() == ErrorKind::AddrInUse => {
                port += 1;
            }
            Err(e) => return Err(e),
        }
    }

    Err(std::io::Error::new(
        ErrorKind::AddrInUse,
        format!(
            "No available ports in range {}..={}",
            super::DEFAULT_SERVER_PORT,
            max_port
        ),
    ))
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
pub fn get_dashboard_url() -> Option<String> {
    let state = GLOBAL_DASHBOARD_STATE.lock();
    state.as_ref().map(|state| state.get_url())
}

#[pyfunction]
pub fn get_dashboard_queries_url() -> Option<String> {
    let state = GLOBAL_DASHBOARD_STATE.lock();
    state.as_ref().map(|state| state.get_queries_url())
}

#[pyfunction]
pub fn register_dataframe_for_display(
    record_batch: daft_recordbatch::python::PyRecordBatch,
) -> PyResult<String> {
    let mut state = GLOBAL_DASHBOARD_STATE.lock();
    if let Some(state) = state.as_mut() {
        let df_id = state.register_dataframe_preview(record_batch.record_batch);
        Ok(df_id)
    } else {
        Err(PyErr::new::<exceptions::PyRuntimeError, _>(
            "Dashboard is not running",
        ))
    }
}

#[pyfunction]
pub fn generate_interactive_html(df_id: String) -> PyResult<String> {
    let (record_batch, port) = {
        let state_guard = GLOBAL_DASHBOARD_STATE.lock();
        let state = state_guard.as_ref().ok_or_else(|| {
            PyErr::new::<exceptions::PyRuntimeError, _>("Dashboard is not running")
        })?;

        let record_batch = state.get_dataframe_preview(&df_id).ok_or_else(|| {
            PyErr::new::<exceptions::PyRuntimeError, _>(format!(
                "DataFrame with id '{}' does not exist",
                df_id
            ))
        })?;

        (record_batch, state.get_port())
    };

    let html = super::generate_interactive_html(
        &record_batch,
        &df_id,
        &super::DEFAULT_SERVER_ADDR.to_string(),
        port,
    );
    Ok(html)
}

#[pyfunction]
pub fn launch(noop_if_initialized: bool) -> PyResult<ConnectionHandle> {
    // Check if server is already running
    let mut dashboard_state = GLOBAL_DASHBOARD_STATE.lock();
    if let Some(dashboard_state) = dashboard_state.as_ref() {
        if noop_if_initialized {
            return Ok(ConnectionHandle {
                shutdown_signal: None,
                port: dashboard_state.get_port(),
            });
        } else {
            return Err(PyErr::new::<exceptions::PyRuntimeError, _>(
                "Server is already running",
            ));
        }
    }

    let (listener, port) = make_listener()?;
    let (send, recv) = oneshot::channel::<()>();

    let handle = ConnectionHandle {
        shutdown_signal: Some(send),
        port,
    };

    let new_dashboard_state = DashboardState::new(super::DEFAULT_SERVER_ADDR.to_string(), port);
    *dashboard_state = Some(new_dashboard_state);

    std::thread::spawn(move || tokio_runtime().block_on(async { run(listener, recv).await }));

    Ok(handle)
}

async fn run(
    listener: std::net::TcpListener,
    mut recv: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    listener.set_nonblocking(true).map_err(anyhow::Error::new)?;

    let listener = tokio::net::TcpListener::from_std(listener).map_err(anyhow::Error::new)?;

    loop {
        tokio::select! {
            stream = listener.accept() => match stream {
                Ok((stream, _)) => {
                    super::handle_stream(stream);
                },
                Err(error) => {
                    log::warn!("Unable to accept incoming connection: {error}");
                },
            },
            _ = &mut recv => {
                break;
            },
        }
    }

    Ok(())
}

fn tokio_runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}
