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
mod display;
#[cfg(feature = "python")]
mod err;
#[cfg(feature = "python")]
mod execute;
#[cfg(feature = "python")]
mod response_builder;
#[cfg(feature = "python")]
mod session;
#[cfg(feature = "python")]
mod translation;
#[cfg(feature = "python")]
pub mod util;
use connect_service::DaftSparkConnectService;
#[cfg(feature = "python")]
use eyre::Context;
#[cfg(feature = "python")]
use pyo3::types::PyModuleMethods;
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
pub fn start(addr: &str) -> eyre::Result<ConnectionHandle> {
    info!("Daft-Connect server listening on {addr}");
    let addr = util::parse_spark_connect_address(addr)?;

    let listener = std::net::TcpListener::bind(addr)?;
    let port = listener.local_addr()?.port();

    let service = DaftSparkConnectService::default();

    info!("Daft-Connect server listening on {addr}");

    let (shutdown_signal, shutdown_receiver) = tokio::sync::oneshot::channel();

    let handle = ConnectionHandle {
        shutdown_signal: Some(shutdown_signal),
        port,
    };

    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(async {
            let incoming = {
                let listener = tokio::net::TcpListener::from_std(listener)
                    .wrap_err("Failed to create TcpListener from std::net::TcpListener")?;

                async_stream::stream! {
                    loop {
                        match listener.accept().await {
                            Ok((stream, _)) => yield Ok(stream),
                            Err(e) => yield Err(e),
                        }
                    }
                }
            };

            let result = tokio::select! {
                result = Server::builder()
                    .add_service(SparkConnectServiceServer::new(service))
                    .serve_with_incoming(incoming)=> {
                    result
                }
                _ = shutdown_receiver => {
                    info!("Received shutdown signal");
                    Ok(())
                }
            };

            result.wrap_err_with(|| format!("Failed to start server on {addr}"))
        });

        if let Err(e) = result {
            eprintln!("Daft-Connect server error: {e:?}");
        }

        eyre::Result::<_>::Ok(())
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
