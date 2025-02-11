mod response;

use std::{
    net::Ipv4Addr,
    path::{Path, PathBuf},
    process::exit,
    sync::OnceLock,
};

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
use pyo3::{
    pyfunction,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction, Bound, PyResult,
};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, spawn};

type Req<T = Incoming> = Request<T>;
type Res = Response<BoxBody<Bytes, std::io::Error>>;
type ServerResult<T> = Result<T, (StatusCode, anyhow::Error)>;

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
    let data = simdutf8::basic::from_utf8(&bytes).with_status_code(StatusCode::BAD_REQUEST)?;
    let body = serde_json::from_str(data).with_status_code(StatusCode::BAD_REQUEST)?;
    Ok(Request::from_parts(parts, body))
}

async fn dashboard_server(req: Req, resolver: Option<Resolver>) -> ServerResult<Res> {
    let request_path = req.uri().path();
    let paths = request_path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    Ok(match (req.method(), paths.as_slice()) {
        // The daft broadcast server
        (&Method::POST, ["broadcast"]) => {
            let req = deserialize::<QueryMetadata>(req).await?;
            query_metadatas().write().push(req.into_body());
            response::empty(StatusCode::OK)
        }

        // The dashboard API server
        (&Method::GET, ["api"]) => {
            let query_metadatas = query_metadatas().read();
            response::with_body(StatusCode::OK, query_metadatas.as_slice())
        }

        // The dashboard static HTML file server
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

pub async fn run(static_assets_path: Option<&Path>) {
    env_logger::try_init().ok().unwrap_or_default();

    let Ok(listener) = TcpListener::bind((SERVER_ADDR, SERVER_PORT)).await else {
        log::warn!(
            r#"Looks like there's another process already bound to {SERVER_ADDR}:{SERVER_PORT}.
If this is the `daft-dashboard-client` (i.e., if you've already ran `daft.dashboard.launch()` inside of a python script), then you don't have to do anything else.

However, if this is another process, then kill that other server (by running `kill -9 $(lsof -t -i :3238)` inside of your shell) and then rerun `daft.dashboard.launch()`."#
        );
        return;
    };

    let resolver = static_assets_path.map(Resolver::new);

    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(error) => {
                log::warn!("Unable to accept incoming connection: {error}");
                continue;
            }
        };
        let io = TokioIo::new(stream);
        spawn({
            let resolver = resolver.clone();
            async move {
                http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(|request| {
                            let resolver = resolver.clone();
                            async move {
                                Ok::<_, std::convert::Infallible>(
                                    match dashboard_server(request, resolver).await {
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
            }
        });
    }
}

#[pyfunction(signature = (static_assets_path))]
fn launch(static_assets_path: String) {
    let static_assets_path = PathBuf::from(static_assets_path);

    if matches!(
        fork::fork().expect("Failed to fork server process"),
        fork::Fork::Child,
    ) {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(NUMBER_OF_WORKER_THREADS)
            .enable_all()
            .build()
            .expect("Failed to launch server")
            .block_on(run(Some(&static_assets_path)));
        exit(0);
    }
}

pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    parent.add_function(wrap_pyfunction!(launch, parent)?)?;
    Ok(())
}
