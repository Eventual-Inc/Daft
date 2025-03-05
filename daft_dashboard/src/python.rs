use std::{io::ErrorKind, pin::pin, process::exit, time::Duration};

use clap::Parser;
use http_body_util::Empty;
use hyper::{body::Bytes, client::conn::http1, Request};
use hyper_util::rt::TokioIo;
use pyo3::{
    exceptions,
    ffi::PyErr_CheckSignals,
    pyfunction, pymodule,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction, Bound, PyErr, PyResult, Python,
};
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::{Builder, Runtime},
    spawn,
    sync::mpsc,
    time::sleep,
};

use crate::DashboardState;

const NUMBER_OF_WORKER_THREADS: usize = 3;

#[pyfunction(signature = (detach = false, noop_if_initialized = false))]
fn launch(detach: bool, noop_if_initialized: bool) -> PyResult<()> {
    if detach {
        launch_detached(noop_if_initialized)
    } else {
        launch_attached(noop_if_initialized)
    }
    .map_err(|error| PyErr::new::<exceptions::PyRuntimeError, _>(error.to_string()))
}

#[pyfunction(signature = (noop_if_shutdown = false))]
fn shutdown(noop_if_shutdown: bool) -> PyResult<()> {
    tokio_runtime(false)
        .block_on(async {
            let stream = match TcpStream::connect((super::SERVER_ADDR, super::SERVER_PORT)).await {
                Ok(stream) => stream,
                Err(error) if error.kind() == ErrorKind::ConnectionRefused && noop_if_shutdown => {
                    return Ok(())
                }
                Err(error) => return Err(anyhow::anyhow!("{error}")),
            };
            let io = TokioIo::new(stream);
            let (mut sender, conn) = http1::handshake(io).await?;

            spawn(async move {
                if let Err(err) = conn.await {
                    log::error!("Connection failed: {:?}", err);
                }
            });

            let req = Request::post(format!(
                "http://{}:{}/api/shutdown",
                super::SERVER_ADDR,
                super::SERVER_PORT
            ))
            .body(Empty::<Bytes>::default())
            .unwrap();

            let response = sender.send_request(req).await?;
            let status = response.status();

            if status.is_success() {
                Ok(())
            } else {
                Err(anyhow::anyhow!(
                    "Failed to shutdown server; endpoint returned a {} status code",
                    status,
                ))
            }
        })
        .map_err(|err: anyhow::Error| PyErr::new::<exceptions::PyRuntimeError, _>(err.to_string()))
}

#[derive(Clone, PartialEq, Eq, Parser)]
enum Cli {
    Launch {
        /// Do not fail if a Daft dashboard server process is already bound to port 3238.
        ///
        /// Makes this command idempotent.
        #[arg(short, long)]
        noop_if_initialized: bool,
    },
    Shutdown {
        /// Do not fail if no Daft dashboard server process is bound to port 3238.
        ///
        /// Makes this command idempotent.
        #[arg(short, long)]
        noop_if_shutdown: bool,
    },
}

#[pyfunction(signature = (args))]
fn cli(args: Vec<String>) -> PyResult<()> {
    match Cli::parse_from(args) {
        Cli::Launch {
            noop_if_initialized,
        } => launch(true, noop_if_initialized),
        Cli::Shutdown { noop_if_shutdown } => shutdown(noop_if_shutdown),
    }
}

#[pymodule]
fn daft_dashboard(_: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(launch, m)?)?;
    m.add_function(wrap_pyfunction!(shutdown, m)?)?;
    m.add_function(wrap_pyfunction!(cli, m)?)?;
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BreakReason {
    PortAlreadyBound,
    PythonSignalInterrupt,
    ApiShutdownSignal,
}

fn launch_attached(noop_if_initialized: bool) -> anyhow::Result<()> {
    let break_reason = tokio_runtime(true).block_on(run())?;

    if break_reason == BreakReason::PortAlreadyBound && !noop_if_initialized {
        Err(already_bound_error())
    } else {
        Ok(())
    }
}

fn launch_detached(noop_if_initialized: bool) -> anyhow::Result<()> {
    #[cfg(not(unix))]
    {
        Err(PyErr::new::<exceptions::PyRuntimeError, _>("Daft dashboard's detaching feature is not available on this platform; unable to fork on Windows"))
    }

    #[cfg(unix)]
    {
        match fork::fork().unwrap() {
            fork::Fork::Parent(..) => Ok(()),
            fork::Fork::Child => {
                let break_reason = tokio_runtime(true).block_on(run())?;
                exit(match break_reason {
                    BreakReason::PortAlreadyBound if noop_if_initialized => 0,
                    BreakReason::PortAlreadyBound => {
                        log::error!("{}", already_bound_error());
                        1
                    }
                    BreakReason::PythonSignalInterrupt => {
                        unreachable!(
                            "Can't receive a python signal interrupt in an orphaned process"
                        )
                    }
                    BreakReason::ApiShutdownSignal => 0,
                });
            }
        }
    }
}

async fn run() -> anyhow::Result<BreakReason> {
    let listener = match TcpListener::bind((super::SERVER_ADDR, super::SERVER_PORT)).await {
        Ok(listener) => listener,
        Err(error) if error.kind() == ErrorKind::AddrInUse => {
            return Ok(BreakReason::PortAlreadyBound)
        }
        Err(error) => Err(error)?,
    };

    let mut python_signal = pin!(interrupt_handler());
    let (send, mut recv) = mpsc::channel::<()>(1);
    let mut api_signal = pin!(async { recv.recv().await.unwrap() });
    let state = DashboardState::new(send);

    Ok(loop {
        tokio::select! {
            stream = listener.accept() => match stream {
                Ok((stream, _)) => super::handle_stream(stream, state.clone()),
                Err(error) => log::warn!("Unable to accept incoming connection: {error}"),
            },
            () = &mut python_signal => break BreakReason::PythonSignalInterrupt,
            () = &mut api_signal => break BreakReason::ApiShutdownSignal,
        }
    })
}

async fn interrupt_handler() {
    loop {
        unsafe {
            if PyErr_CheckSignals() != 0 {
                break;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn tokio_runtime(multithreaded: bool) -> Runtime {
    if multithreaded {
        let mut builder = Builder::new_multi_thread();
        builder.worker_threads(NUMBER_OF_WORKER_THREADS);
        builder
    } else {
        Builder::new_current_thread()
    }
    .enable_all()
    .build()
    .unwrap()
}

fn already_bound_error() -> anyhow::Error {
    anyhow::anyhow!(
        r"There's another process already bound to {}:{}.
If this is the `daft-dashboard-client` (i.e., if you already ran `dashboard.launch()` inside of a python script previously), then you don't have to do anything else.

However, if this is another process, then kill that other server (by running `kill -9 $(lsof -t -i :3238)` inside of your shell) and then rerun `dashboard.launch()`.",
        super::SERVER_ADDR,
        super::SERVER_PORT,
    )
}
