mod response;

use std::{future::Future, net::Ipv4Addr, process::exit, sync::OnceLock};

use chrono::{DateTime, Utc};
use http_body_util::{combinators::BoxBody, BodyExt};
use hyper::{
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use parking_lot::RwLock;
use pyo3::{
    pyfunction,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction, Bound, PyResult,
};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, spawn};

type Req<T = Incoming> = Request<T>;
type Res = Response<BoxBody<Bytes, std::convert::Infallible>>;
type ServerResult<T> = Result<T, (StatusCode, anyhow::Error)>;

const DAFT_PORT: u16 = 3238;
const DASHBOARD_PORT: u16 = DAFT_PORT + 1;

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
    let data = simdutf8::basic::from_utf8(&bytes).with_status_code(StatusCode::BAD_REQUEST)?;
    let body = serde_json::from_str(data).with_status_code(StatusCode::BAD_REQUEST)?;
    Ok(Request::from_parts(parts, body))
}

async fn daft_http_application(req: Req) -> ServerResult<Res> {
    Ok(match (req.method(), req.uri().path()) {
        (&Method::POST, "/") => {
            let req = deserialize::<QueryMetadata>(req).await?;
            query_metadatas().write().push(req.into_body());
            response::empty(StatusCode::OK)
        }
        (&Method::GET, "/health") => response::empty(StatusCode::OK),
        _ => response::empty(StatusCode::NOT_FOUND),
    })
}

async fn dashboard_http_application(req: Req) -> ServerResult<Res> {
    Ok(match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            let query_metadatas = query_metadatas().read();
            response::with_body(StatusCode::OK, query_metadatas.as_slice())
        }
        (&Method::GET, "/health") => response::empty(StatusCode::OK),
        _ => response::empty(StatusCode::NOT_FOUND),
    })
}

pub async fn run() {
    async fn run_http_application<F>(server: fn(Req) -> F, addr: Ipv4Addr, port: u16)
    where
        F: 'static + Send + Future<Output = ServerResult<Res>>,
    {
        let Ok(listener) = TcpListener::bind((addr, port)).await else {
            log::warn!(
                r#"Looks like there's another process already bound to {addr}:{port}.
If this is the `daft-dashboard-client` (i.e., if you've already ran `daft.dashboard.launch()` inside of a python script), then you don't have to do anything else.

However, if this is another process, then kill that other server (by running `kill -9 $(lsof -t -i :3238)` inside of your shell) and then rerun `daft.dashboard.launch()`."#
            );
            return;
        };
        loop {
            let (stream, _) = listener
                .accept()
                .await
                .expect("Unable to accept incoming connection");
            let io = TokioIo::new(stream);
            spawn(async move {
                http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(|request| async {
                            Ok::<_, std::convert::Infallible>(match server(request).await {
                                Ok(response) => response,
                                Err((status_code, error)) => {
                                    response::with_body(status_code, error.to_string())
                                }
                            })
                        }),
                    )
                    .await
                    .expect("Failed to serve endpoint");
            });
        }
    }

    env_logger::try_init().ok().unwrap_or_default();

    tokio::select! {
        () = run_http_application(daft_http_application, Ipv4Addr::LOCALHOST, DAFT_PORT) => (),
        () = run_http_application(
            dashboard_http_application,
            Ipv4Addr::LOCALHOST,
            DASHBOARD_PORT,
        ) => (),
    };
}

#[pyfunction]
fn launch() {
    if matches!(
        fork::daemon(false, true).expect("Failed to fork server process"),
        fork::Fork::Child,
    ) {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(3)
            .enable_all()
            .build()
            .expect("Failed to launch server")
            .block_on(run());
        exit(0);
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(launch, parent)?)?;
    Ok(())
}
