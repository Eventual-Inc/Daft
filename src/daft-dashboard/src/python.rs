use std::{io::ErrorKind, sync::{OnceLock, Mutex}};

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
    GLOBAL_DASHBOARD_STATE.get_or_init(|| DashboardState::new())
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
pub fn register_dataframe_for_display(record_batch: daft_recordbatch::python::PyRecordBatch) -> PyResult<(String, String, u16)> {
    println!("[Dashboard Server] register_dataframe_for_display() called");
    // Use the global shared state
    let state = get_global_state();
    let df_id = state.register_dataframe(record_batch.record_batch);
    println!("[Dashboard Server] Registered dataframe with id: {}", df_id);
    Ok((df_id, super::SERVER_ADDR.to_string(), super::SERVER_PORT))
}

#[pyfunction]
pub fn generate_interactive_html(
    record_batch: daft_recordbatch::python::PyRecordBatch,
    df_id: String,
) -> PyResult<String> {
    println!("[Dashboard Server] generate_interactive_html() called for df_id: {}", df_id);
    let html = super::generate_interactive_html(
        &record_batch.record_batch,
        &df_id,
        &super::SERVER_ADDR.to_string(),
        super::SERVER_PORT,
    );
    println!("[Dashboard Server] Generated HTML (length: {})", html.len());
    Ok(html)
}

#[pyfunction]
pub fn launch(noop_if_initialized: bool, py: Python) -> PyResult<ConnectionHandle> {
    println!("[Dashboard Server] launch() called with noop_if_initialized={}", noop_if_initialized);
    
    // Check if server is already running
    {
        let running = SERVER_RUNNING.lock().unwrap();
        if *running {
            println!("[Dashboard Server] Server already running, noop_if_initialized={}", noop_if_initialized);
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

    println!("[Dashboard Server] Attempting to create listener on {}:{}", super::SERVER_ADDR, super::SERVER_PORT);
    match make_listener() {
        Err(e) if e.kind() == ErrorKind::AddrInUse => {
            println!("[Dashboard Server] Port {} already in use", super::SERVER_PORT);
            if noop_if_initialized {
                println!("[Dashboard Server] Port in use but noop_if_initialized=true, assuming server is running");
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
        Err(e) => {
            println!("[Dashboard Server] ERROR: Failed to create listener: {}", e);
            Err(PyErr::new::<exceptions::PyRuntimeError, _>(e))
        }
        Ok(listener) => {
            println!("[Dashboard Server] Successfully created listener, starting server thread");
            let (send, recv) = oneshot::channel::<()>();

            let handle = ConnectionHandle {
                shutdown_signal: Some(send),
            };

            // Mark server as running
            *SERVER_RUNNING.lock().unwrap() = true;
            println!("[Dashboard Server] Marked server as running, spawning background thread");

            py.allow_threads(move || {
                std::thread::spawn(move || {
                    println!("[Dashboard Server] Background thread started, entering tokio runtime");
                    let result = tokio_runtime().block_on(async { 
                        println!("[Dashboard Server] Tokio runtime started, calling run()");
                        run(listener, recv).await 
                    });
                    println!("[Dashboard Server] Server run() finished with result: {:?}", result);
                    // Mark server as not running when it exits
                    *SERVER_RUNNING.lock().unwrap() = false;
                    println!("[Dashboard Server] Marked server as not running");
                    result
                });
            });
            println!("[Dashboard Server] Successfully launched server");
            Ok(handle)
        }
    }
}

async fn run(
    listener: std::net::TcpListener,
    mut recv: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    println!("[Dashboard Server] run() function started");
    
    listener.set_nonblocking(true).map_err(anyhow::Error::new)?;
    println!("[Dashboard Server] Set listener to non-blocking");

    let listener = tokio::net::TcpListener::from_std(listener).map_err(anyhow::Error::new)?;
    println!("[Dashboard Server] Converted to tokio listener");

    let state = get_global_state().clone();
    println!("[Dashboard Server] Got global state, entering accept loop");

    loop {
        tokio::select! {
            stream = listener.accept() => match stream {
                Ok((stream, _)) => {
                    println!("[Dashboard Server] Accepted new connection");
                    super::handle_stream(stream, state.clone())
                },
                Err(error) => {
                    println!("[Dashboard Server] Error accepting connection: {}", error);
                    log::warn!("Unable to accept incoming connection: {error}")
                },
            },
            _ = &mut recv => {
                println!("[Dashboard Server] Received shutdown signal, breaking from loop");
                break;
            },
        }
    }

    println!("[Dashboard Server] run() function exiting");
    Ok(())
}

fn tokio_runtime() -> Runtime {
    Builder::new_multi_thread().enable_all().build().unwrap()
}
