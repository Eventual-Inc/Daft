use std::{io::ErrorKind, pin::pin};

use pyo3::{exceptions, pyclass, pyfunction, pymethods, PyErr, PyResult, Python};
use tokio::{
    runtime::{Builder, Runtime},
    sync::oneshot,
};

use crate::DashboardState;

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
pub fn launch(noop_if_initialized: bool, py: Python) -> PyResult<ConnectionHandle> {
    match (make_listener(), noop_if_initialized) {
        (Err(_), true) => Ok(ConnectionHandle {
            shutdown_signal: None,
        }),
        (Err(e), false) if e.kind() == ErrorKind::AddrInUse => {
            Err(PyErr::new::<exceptions::PyRuntimeError, _>(
                "Port is already in use",
            ))
        }
        (Err(e), _) => Err(PyErr::new::<exceptions::PyRuntimeError, _>(e)),
        (Ok(listener), _) => {
            let (send, recv) = oneshot::channel::<()>();

            let handle = ConnectionHandle {
                shutdown_signal: Some(send),
            };

            py.allow_threads(move || {
                std::thread::spawn(move || {
                    tokio_runtime().block_on(async { run(listener, recv).await })
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

    let mut api_signal = pin!(recv);
    let state = DashboardState::new();

    loop {
        tokio::select! {
            stream = listener.accept() => match stream {
                Ok((stream, _)) => super::handle_stream(stream, state.clone()),
                Err(error) => log::warn!("Unable to accept incoming connection: {error}"),
            },
            Ok(()) = &mut api_signal => break,
        }
    }

    Ok(())
}

fn tokio_runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}
