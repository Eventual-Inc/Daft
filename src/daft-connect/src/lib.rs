#![feature(iterator_try_collect)]
#![feature(let_chains)]
#![feature(try_trait_v2)]
#![feature(coroutines)]
#![feature(iter_from_coroutine)]
#![feature(stmt_expr_attributes)]
#![feature(try_trait_v2_residual)]

#[cfg(feature = "python")]
mod config;

#[cfg(feature = "python")]
mod connect_service;

#[cfg(feature = "python")]
mod functions;

#[cfg(feature = "python")]
mod display;
#[cfg(feature = "python")]
mod error;
#[cfg(feature = "python")]
mod execute;
#[cfg(feature = "python")]
mod response_builder;
#[cfg(feature = "python")]
mod session;
#[cfg(feature = "python")]
mod spark_analyzer;
#[cfg(feature = "python")]
pub mod util;

#[cfg(feature = "python")]
use connect_service::DaftSparkConnectService;
#[cfg(feature = "python")]
use pyo3::types::PyModuleMethods;
#[cfg(feature = "python")]
use snafu::{ResultExt, Whatever};
#[cfg(feature = "python")]
use spark_connect::spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer};
#[cfg(feature = "python")]
use tonic::transport::Server;
#[cfg(feature = "python")]
use tracing::info;

#[cfg(feature = "python")]
pub type ExecuteStream = <DaftSparkConnectService as SparkConnectService>::ExecutePlanStream;

#[cfg_attr(feature = "python", pyo3::pyclass)]
pub struct ConnectionHandle {
    shutdown_signal: Option<tokio::sync::oneshot::Sender<()>>,
    port: u16,
}

#[cfg_attr(feature = "python", pyo3::pymethods)]
impl ConnectionHandle {
    pub fn shutdown(&mut self) {
        let Some(shutdown_signal) = self.shutdown_signal.take() else {
            return;
        };
        shutdown_signal.send(()).unwrap();
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

#[cfg(feature = "python")]
pub fn start(addr: &str) -> Result<ConnectionHandle, Whatever> {
    info!("Daft-Connect server listening on {addr}");
    let addr = util::parse_spark_connect_address(addr).whatever_context("Invalid address")?;

    let listener =
        std::net::TcpListener::bind(addr).whatever_context("unable to bind to address")?;
    let port = listener
        .local_addr()
        .whatever_context("no local_addr")?
        .port();

    let service = DaftSparkConnectService::default();

    info!("Daft-Connect server listening on {addr}");

    let (shutdown_signal, shutdown_receiver) = tokio::sync::oneshot::channel();

    let handle = ConnectionHandle {
        shutdown_signal: Some(shutdown_signal),
        port,
    };
    let runtime = common_runtime::get_io_runtime(true);

    std::thread::spawn(move || {
        let result = runtime.block_on_current_thread(async {
            let incoming = {
                let listener = tokio::net::TcpListener::from_std(listener).expect("from_std");

                async_stream::stream! {
                    loop {
                        match listener.accept().await {
                            Ok((stream, _)) => yield Ok(stream),
                            Err(e) => yield Err(e),
                        }
                    }
                }
            };

            tokio::select! {
                result = Server::builder()
                    .add_service(SparkConnectServiceServer::new(service))
                    .serve_with_incoming(incoming)=> {
                    result
                }
                _ = shutdown_receiver => {
                    info!("Received shutdown signal");
                    Ok(())
                }
            }
        });

        if let Err(e) = result {
            eprintln!("Daft-Connect server error: {e:?}");
        }

        Ok::<_, error::ConnectError>(())
    });

    Ok(handle)
}

#[cfg(feature = "python")]
pub enum Runner {
    Ray,
    Native,
}

#[cfg(feature = "python")]
#[cfg_attr(feature = "python", pyo3::pyfunction)]
#[pyo3(name = "connect_start", signature = (addr = "sc://0.0.0.0:0"))]
pub fn py_connect_start(addr: &str) -> pyo3::PyResult<ConnectionHandle> {
    start(addr).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{e:?}")))
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &pyo3::Bound<pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    parent.add_function(pyo3::wrap_pyfunction!(py_connect_start, parent)?)?;
    parent.add_class::<ConnectionHandle>()?;
    Ok(())
}
