mod response;

#[cfg(not(feature = "python"))]
use std::path::Path;
use std::{io::Cursor, net::Ipv4Addr, pin::pin, sync::OnceLock};

use chrono::{DateTime, Utc};
use http_body_util::{combinators::BoxBody, BodyExt};
use hyper::{
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_staticfile::{AcceptEncoding, ResolveResult, Resolver, ResponseBuilder};
use hyper_util::rt::TokioIo;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    spawn,
    sync::mpsc::{self, Receiver, Sender},
};
#[cfg(feature = "python")]
use {
    pyo3::pymodule,
    pyo3::Python,
    pyo3::{
        ffi::PyErr_CheckSignals,
        pyfunction,
        types::{PyModule, PyModuleMethods},
        wrap_pyfunction, Bound, PyResult,
    },
    std::process::exit,
    std::time::Duration,
    tokio::time::sleep,
};

type Req<T = Incoming> = Request<T>;
type Res = Response<BoxBody<Bytes, std::io::Error>>;
type ServerResult<T> = Result<T, (StatusCode, anyhow::Error)>;

#[cfg(feature = "python")]
const NUMBER_OF_WORKER_THREADS: usize = 3;
const SERVER_ADDR: Ipv4Addr = Ipv4Addr::LOCALHOST;
const SERVER_PORT: u16 = 3238;

static QUERY_METADATAS: OnceLock<RwLock<Vec<QueryMetadata>>> = OnceLock::new();

trait ResultExt<T, E: Into<anyhow::Error>>: Sized {
    fn with_status_code(self, status_code: StatusCode) -> ServerResult<T>;
    fn with_internal_error(self) -> ServerResult<T>;
}

impl<T, E: Into<anyhow::Error>> ResultExt<T, E> for Result<T, E> {
    fn with_status_code(self, status_code: StatusCode) -> ServerResult<T> {
        self.map_err(|err| (status_code, err.into()))
    }

    fn with_internal_error(self) -> ServerResult<T> {
        self.with_status_code(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

fn query_metadatas() -> &'static RwLock<Vec<QueryMetadata>> {
    QUERY_METADATAS.get_or_init(RwLock::default)
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct QueryMetadata {
    id: String,
    mermaid_plan: String,
    plan_time_start: DateTime<Utc>,
    plan_time_end: DateTime<Utc>,
}

async fn deserialize<T: for<'de> Deserialize<'de>>(req: Req) -> ServerResult<Req<T>> {
    let (parts, body) = req.into_parts();
    let bytes = body.collect().await.with_internal_error()?.to_bytes();
    let mut cursor = Cursor::new(bytes);
    let body = serde_json::from_reader(&mut cursor).with_status_code(StatusCode::BAD_REQUEST)?;
    Ok(Request::from_parts(parts, body))
}

async fn http_server_application(
    req: Req,
    resolver: Option<Resolver>,
    shutdown_signal: Sender<()>,
) -> ServerResult<Res> {
    let request_path = req.uri().path();
    let paths = request_path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    Ok(match (req.method(), paths.as_slice()) {
        (&Method::POST, ["api", "queries"]) => {
            let req = deserialize::<QueryMetadata>(req).await?;
            query_metadatas().write().push(req.into_body());
            response::empty(StatusCode::OK)
        }
        (&Method::GET, ["api", "queries"]) => {
            let query_metadatas = query_metadatas().read();
            response::with_body(StatusCode::OK, query_metadatas.as_slice())
        }
        (&Method::POST, ["api", "shutdown"]) => {
            shutdown_signal
                .send(())
                .await
                .expect("Sending signal should always succeed");
            response::empty(StatusCode::OK)
        }
        (_, ["api", ..]) => response::empty(StatusCode::NOT_FOUND),

        // All other paths (that don't start with "api") will be treated as web-server requests.
        (&Method::GET, _) => {
            let Some(resolver) = resolver else {
                return Ok(response::empty(StatusCode::NOT_FOUND));
            };

            let request_path = req.uri().path();
            let result = resolver
                .resolve_path(request_path, AcceptEncoding::all())
                .await
                .with_internal_error()?;

            let result = if matches!(
                result,
                ResolveResult::NotFound | ResolveResult::IsDirectory { .. }
            ) {
                let request_path = request_path.strip_suffix('/').unwrap_or(request_path);
                let request_path = format!("{}.html", request_path);
                resolver
                    .resolve_path(&request_path, AcceptEncoding::all())
                    .await
                    .with_internal_error()?
            } else {
                result
            };

            ResponseBuilder::new()
                .request(&req)
                .build(result)
                .with_internal_error()?
                .map(|body| body.boxed())
        }

        _ => response::empty(StatusCode::NOT_FOUND),
    })
}

fn handle_stream(stream: TcpStream, resolver: Option<Resolver>, shutdown_signal: Sender<()>) {
    let io = TokioIo::new(stream);
    spawn(async move {
        http1::Builder::new()
            .serve_connection(
                io,
                service_fn(move |request| {
                    let resolver = resolver.clone();
                    let shutdown_signal = shutdown_signal.clone();
                    async move {
                        Ok::<_, std::convert::Infallible>(
                            match http_server_application(request, resolver, shutdown_signal).await
                            {
                                Ok(response) => response,
                                Err((status_code, error)) => {
                                    response::with_body(status_code, error.to_string())
                                }
                            },
                        )
                    }
                }),
            )
            .await
            .expect("Endpoint should always be able to be served");
    });
}

async fn handle_receiver(mut recv: Receiver<()>) {
    recv.recv()
        .await
        .expect("Receiving a message should always succeed");
}

#[cfg(not(feature = "python"))]
pub async fn launch(static_assets_path: Option<&Path>) {
    let listener = TcpListener::bind((SERVER_ADDR, SERVER_PORT))
        .await
        .unwrap_or_else(|error| panic!("Failed to bind to `{SERVER_ADDR}:{SERVER_PORT}`, another process is already bound to it; consider running `kill -9 $(lsof -t -i :3238)` in order to kill it; {error}"));

    let resolver = static_assets_path.map(Resolver::new);
    let (send, recv) = mpsc::channel::<()>(1);
    let mut api_signal = pin!(handle_receiver(recv));

    loop {
        tokio::select! {
            (stream, _) = async {
                listener
                    .accept()
                    .await
                    .unwrap_or_else(|error| panic!("Unable to accept incoming connection: {error}"))
            } => handle_stream(stream, resolver.clone(), send.clone()),
            () = &mut api_signal => break,
        }
    }
}

#[cfg(feature = "python")]
#[pyfunction(signature = (static_assets_path, block = false))]
#[pyo3(name = "launch_dashboard")]
fn launch(static_assets_path: String, block: Option<bool>) {
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

    async fn launch_async(resolver: Resolver) -> BreakReason {
        let Ok(listener) = TcpListener::bind((SERVER_ADDR, SERVER_PORT)).await else {
            return BreakReason::PortAlreadyBound;
        };

        let mut python_signal = pin!(interrupt_handler());
        let (send, recv) = mpsc::channel::<()>(1);
        let mut api_signal = pin!(handle_receiver(recv));

        loop {
            tokio::select! {
                stream = listener.accept() => match stream {
                    Ok((stream, _)) => handle_stream(stream, Some(resolver.clone()), send.clone()),
                    Err(error) => log::warn!("Unable to accept incoming connection: {error}"),
                },
                () = &mut python_signal => break BreakReason::PythonSignalInterrupt,
                () = &mut api_signal => break BreakReason::ApiShutdownSignal,
            }
        }
    }

    enum BreakReason {
        PortAlreadyBound,
        PythonSignalInterrupt,
        ApiShutdownSignal,
    }

    env_logger::try_init().ok().unwrap_or_default();
    let resolver = Resolver::new(static_assets_path);
    let block = block.unwrap_or(false);

    let launch_on_tokio_runtime = move || {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(NUMBER_OF_WORKER_THREADS)
            .enable_all()
            .build()
            .expect("Tokio runtime should always be able to be built")
            .block_on(launch_async(resolver))
    };

    if block {
        if matches!(launch_on_tokio_runtime(), BreakReason::PortAlreadyBound) {
            panic!(
                r#"There's another process already bound to {SERVER_ADDR}:{SERVER_PORT}.
If this is the `daft-dashboard-client` (i.e., if you already ran `daft.dashboard.launch(block=False)` inside of a python script previously), then you don't have to do anything else.

However, if this is another process, then kill that other server (by running `kill -9 $(lsof -t -i :3238)` inside of your shell) and then rerun `daft.dashboard.launch()`."#
            );
        };
    } else if matches!(
        fork::fork().expect("Failed to fork child process"),
        fork::Fork::Child
    ) {
        if matches!(
            launch_on_tokio_runtime(),
            BreakReason::PythonSignalInterrupt
        ) {
            unreachable!("Can't receive a python signal interrupt in an orphaned process");
        }
        exit(0);
    }
}

#[cfg(feature = "python")]
#[pymodule]
fn daft_dashboard(_: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(launch, m)?)?;
    Ok(())
}
