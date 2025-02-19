use std::{pin::pin, process::exit, time::Duration};

use clap::Parser;
use http_body_util::Empty;
use hyper::{body::Bytes, client::conn::http1, Request};
use hyper_staticfile::Resolver;
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

const NUMBER_OF_WORKER_THREADS: usize = 3;

#[pyfunction(signature = (static_assets_path, detach = false))]
fn launch(static_assets_path: String, detach: Option<bool>) {
    async fn run(static_assets_path: String) -> BreakReason {
        let resolver = Resolver::new(static_assets_path);

        let Ok(listener) = TcpListener::bind((super::SERVER_ADDR, super::SERVER_PORT)).await else {
            return BreakReason::PortAlreadyBound;
        };

        let mut python_signal = pin!(interrupt_handler());
        let (send, mut recv) = mpsc::channel::<()>(1);
        let mut api_signal = pin!(async { recv.recv().await.unwrap() });

        loop {
            tokio::select! {
                stream = listener.accept() => match stream {
                    Ok((stream, _)) => super::handle_stream(stream, Some(resolver.clone()), send.clone()),
                    Err(error) => log::warn!("Unable to accept incoming connection: {error}"),
                },
                () = &mut python_signal => break BreakReason::PythonSignalInterrupt,
                () = &mut api_signal => break BreakReason::ApiShutdownSignal,
            }
        }
    }

    if detach.unwrap_or(false) {
        if matches!(fork::fork().unwrap(), fork::Fork::Child) {
            if matches!(
                tokio_runtime(true).block_on(run(static_assets_path)),
                BreakReason::PythonSignalInterrupt
            ) {
                unreachable!("Can't receive a python signal interrupt in an orphaned process");
            }
            exit(0);
        }
    } else if matches!(
        tokio_runtime(true).block_on(run(static_assets_path)),
        BreakReason::PortAlreadyBound,
    ) {
        panic!(
            r#"There's another process already bound to {}:{}.
If this is the `daft-dashboard-client` (i.e., if you already ran `daft.dashboard.launch(block=False)` inside of a python script previously), then you don't have to do anything else.

However, if this is another process, then kill that other server (by running `kill -9 $(lsof -t -i :3238)` inside of your shell) and then rerun `daft.dashboard.launch()`."#,
            super::SERVER_ADDR,
            super::SERVER_PORT,
        );
    }
}

#[pyfunction]
fn shutdown() -> PyResult<()> {
    tokio_runtime(true)
        .block_on(async {
            let stream = TcpStream::connect((super::SERVER_ADDR, super::SERVER_PORT)).await?;
            let io = TokioIo::new(stream);
            let (mut sender, conn) = http1::handshake(io).await?;

            spawn(async move {
                if let Err(err) = conn.await {
                    println!("Connection failed: {:?}", err);
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
    Start,
    Stop,
}

#[pyfunction(signature = (args, static_assets_path))]
fn cli(args: Vec<String>, static_assets_path: String) -> PyResult<()> {
    match Cli::parse_from(args) {
        Cli::Start => {
            launch(static_assets_path, Some(true));
            Ok(())
        }
        Cli::Stop => shutdown(),
    }
}

#[pymodule]
fn daft_dashboard(_: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(launch, m)?)?;
    m.add_function(wrap_pyfunction!(shutdown, m)?)?;
    m.add_function(wrap_pyfunction!(cli, m)?)?;
    Ok(())
}

enum BreakReason {
    PortAlreadyBound,
    PythonSignalInterrupt,
    ApiShutdownSignal,
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
