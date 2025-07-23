use std::{
    io::ErrorKind,
    sync::{Mutex, OnceLock},
};

use pyo3::{exceptions, pyclass, pyfunction, pymethods, PyErr, PyResult, Python};
use tokio::{
    runtime::{Builder, Runtime},
    sync::oneshot,
};

use crate::DashboardState;

// Global shared state
static GLOBAL_DASHBOARD_STATE: OnceLock<DashboardState> = OnceLock::new();
static SERVER_RUNNING: Mutex<bool> = Mutex::new(false);

fn get_global_state() -> &'static DashboardState {
    GLOBAL_DASHBOARD_STATE.get_or_init(DashboardState::new)
}

#[pyclass]
pub struct ConnectionHandle {
    shutdown_signal: Option<oneshot::Sender<()>>,
}

fn make_listener() -> std::io::Result<std::net::TcpListener> {
    std::net::TcpListener::bind((super::SERVER_ADDR, super::SERVER_PORT))
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
}

#[pyfunction]
pub fn register_dataframe_for_display(
    record_batch: daft_recordbatch::python::PyRecordBatch,
) -> PyResult<String> {
    let state = get_global_state();
    let df_id = state.register_dataframe(record_batch.record_batch);
    Ok(df_id)
}

#[pyfunction]
pub fn generate_interactive_html(df_id: String) -> PyResult<String> {
    let data_frame = get_global_state().get_dataframe(&df_id);
    let html = super::generate_interactive_html(
        data_frame.as_ref().unwrap(),
        &df_id,
        &super::SERVER_ADDR.to_string(),
        super::SERVER_PORT,
    );
    Ok(html)
}

#[pyfunction]
pub fn launch(noop_if_initialized: bool, py: Python) -> PyResult<ConnectionHandle> {
    // Check if server is already running
    {
        let running = SERVER_RUNNING.lock().unwrap();
        if *running {
            if noop_if_initialized {
                return Ok(ConnectionHandle {
                    shutdown_signal: None,
                });
            } else {
                return Err(PyErr::new::<exceptions::PyRuntimeError, _>(
                    "Server is already running",
                ));
            }
        }
    }

    match make_listener() {
        Err(e) if e.kind() == ErrorKind::AddrInUse => {
            if noop_if_initialized {
                // Port is in use but we're being lenient, assume server is running
                *SERVER_RUNNING.lock().unwrap() = true;
                Ok(ConnectionHandle {
                    shutdown_signal: None,
                })
            } else {
                Err(PyErr::new::<exceptions::PyRuntimeError, _>(
                    "Port is already in use",
                ))
            }
        }
        Err(e) => Err(PyErr::new::<exceptions::PyRuntimeError, _>(e)),
        Ok(listener) => {
            let (send, recv) = oneshot::channel::<()>();

            let handle = ConnectionHandle {
                shutdown_signal: Some(send),
            };

            // Mark server as running
            *SERVER_RUNNING.lock().unwrap() = true;

            py.allow_threads(move || {
                std::thread::spawn(move || {
                    let result = tokio_runtime().block_on(async { run(listener, recv).await });
                    // Mark server as not running when it exits
                    *SERVER_RUNNING.lock().unwrap() = false;
                    result
                });
            });
            Ok(handle)
        }
    }
}

async fn run(
    listener: std::net::TcpListener,
    mut recv: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    listener.set_nonblocking(true).map_err(anyhow::Error::new)?;

    let listener = tokio::net::TcpListener::from_std(listener).map_err(anyhow::Error::new)?;

    let state = get_global_state().clone();

    loop {
        tokio::select! {
            stream = listener.accept() => match stream {
                Ok((stream, _)) => {
                    super::handle_stream(stream, state.clone());
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
