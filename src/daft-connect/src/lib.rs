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

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Sender;
#[cfg(feature = "python")]
use {
    common_py_serde::impl_bincode_py_state_serialization,
    connect_service::DaftSparkConnectService,
    pyo3::prelude::*,
    pyo3::types::PyModuleMethods,
    snafu::{ResultExt, Whatever},
    spark_connect::spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer},
    tonic::transport::Server,
    tracing::info,
};

#[cfg(feature = "python")]
pub type ExecuteStream = <DaftSparkConnectService as SparkConnectService>::ExecutePlanStream;

#[cfg_attr(feature = "python", pyo3::pyclass)]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectionHandle {
    pub port: u16,
}

#[cfg(feature = "python")]
impl_bincode_py_state_serialization!(ConnectionHandle);

#[derive(Default)]
struct ShutdownSignals {
    signals: HashMap<u16, Sender<()>>,
}

#[cfg_attr(feature = "python", pyo3::pymethods)]
impl ConnectionHandle {
    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn shutdown(&self) {
        let shutdown_signals = SHUTDOWN_SIGNALS
            .get()
            .expect("SHUTDOWN_SIGNALS not initialized");
        let mut lock = shutdown_signals.lock().expect("poisoned lock");
        let signal = lock.signals.remove(&self.port).expect("no signal for port");
        signal.send(()).expect("send failed");
    }
}

static SHUTDOWN_SIGNALS: OnceCell<Arc<Mutex<ShutdownSignals>>> = OnceCell::new();

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

    let handle = ConnectionHandle { port };

    let shutdown_signals = SHUTDOWN_SIGNALS.get_or_init(Default::default);
    let mut lock = shutdown_signals.lock().whatever_context("poisoned lock")?;

    lock.signals.insert(port, shutdown_signal);

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
