#[cfg(feature = "python")]
mod python;
mod response;

use std::{io::Cursor, net::Ipv4Addr, sync::Arc};

use chrono::{DateTime, Utc};
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::{
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use include_dir::{include_dir, Dir};
use parking_lot::RwLock;
#[cfg(feature = "python")]
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, spawn};

type StrRef = Arc<str>;
type Req<T = Incoming> = Request<T>;
type Res = Response<BoxBody<Bytes, std::io::Error>>;
type ServerResult<T> = Result<T, (StatusCode, anyhow::Error)>;

const SERVER_ADDR: Ipv4Addr = Ipv4Addr::LOCALHOST;
const SERVER_PORT: u16 = 3238;

static ASSETS_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/frontend/out");

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

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct QueryInformation {
    id: StrRef,
    mermaid_plan: StrRef,
    plan_time_start: DateTime<Utc>,
    plan_time_end: DateTime<Utc>,
    logs: StrRef,
}

#[derive(Clone, Debug)]
struct DashboardState {
    queries: Arc<RwLock<Vec<QueryInformation>>>,
}

impl DashboardState {
    fn new() -> Self {
        Self {
            queries: Arc::default(),
        }
    }

    fn queries(&self) -> Vec<QueryInformation> {
        // TODO: The cloning here is a little ugly.
        // The reason the list is cloned is because returning a `&[QueryInformation]` will not work due to borrowing rules.
        self.queries.read().clone()
    }

    fn add_query(&self, query_information: QueryInformation) {
        self.queries.write().push(query_information);
    }
}

async fn deserialize<T: for<'de> Deserialize<'de>>(req: Req) -> ServerResult<Req<T>> {
    let (parts, body) = req.into_parts();
    let bytes = body.collect().await.with_internal_error()?.to_bytes();
    let mut cursor = Cursor::new(bytes);
    let body = serde_json::from_reader(&mut cursor).with_status_code(StatusCode::BAD_REQUEST)?;
    Ok(Request::from_parts(parts, body))
}

async fn http_server_application(req: Req, state: DashboardState) -> ServerResult<Res> {
    let request_path = req.uri().path();
    let paths = request_path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    Ok(match (req.method(), paths.as_slice()) {
        (&Method::POST, ["api", "queries"]) => {
            let req = deserialize::<QueryInformation>(req).await?;
            state.add_query(req.into_body());
            response::empty(StatusCode::OK)
        }
        (&Method::GET, ["api", "queries"]) => {
            let query_informations = state.queries();
            response::with_body(StatusCode::OK, query_informations.as_slice())
        }
        (_, ["api", ..]) => response::empty(StatusCode::NOT_FOUND),

        // All other paths (that don't start with "api") will be treated as web-server requests.
        (&Method::GET, _) => {
            let request_path = req.uri().path();
            let path = request_path.trim_start_matches('/');

            let path = if path.is_empty() { "index.html" } else { path };

            // Try to get the file directly
            let file = ASSETS_DIR.get_file(path).or_else(|| {
                // If not found and doesn't end with .html, try with .html extension
                if !std::path::Path::new(path)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("html"))
                {
                    ASSETS_DIR.get_file(format!("{}.html", path))
                } else {
                    None
                }
            });

            match file {
                Some(file) => {
                    let content_type = match file.path().extension().and_then(|ext| ext.to_str()) {
                        Some("html") => "text/html",
                        Some("css") => "text/css",
                        Some("js") => "application/javascript",
                        Some("png") => "image/png",
                        Some("jpg") | Some("jpeg") => "image/jpeg",
                        _ => "application/octet-stream",
                    };

                    let bytes = Bytes::copy_from_slice(file.contents());

                    Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", content_type)
                        .body(
                            Full::new(bytes)
                                .map_err(|infallible| match infallible {})
                                .boxed(),
                        )
                        .unwrap()
                }
                None => response::empty(StatusCode::NOT_FOUND),
            }
        }

        _ => response::empty(StatusCode::NOT_FOUND),
    })
}

fn handle_stream(stream: TcpStream, state: DashboardState) {
    let io = TokioIo::new(stream);
    spawn(async move {
        http1::Builder::new()
            .serve_connection(
                io,
                service_fn(move |request| {
                    let state = state.clone();
                    async move {
                        Ok::<_, std::convert::Infallible>(
                            match http_server_application(request, state).await {
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
            .unwrap();
    });
}

#[cfg(feature = "python")]
pub fn register_modules(parent: &Bound<PyModule>) -> PyResult<()> {
    const DAFT_DASHBOARD_ENV_ENABLED: &str = "DAFT_DASHBOARD_ENABLED";
    const DAFT_DASHBOARD_ENV_NAME: &str = "DAFT_DASHBOARD";
    const DAFT_DASHBOARD_URL: &str = "http://localhost:3238";
    const DAFT_DASHBOARD_QUERIES_URL: &str = "http://localhost:3238/api/queries";

    let module = PyModule::new(parent.py(), "dashboard")?;
    module.add_wrapped(wrap_pyfunction!(python::launch))?;
    // module.add_wrapped(wrap_pyfunction!(python::shutdown))?;
    // module.add_wrapped(wrap_pyfunction!(python::cli))?;
    module.add("DAFT_DASHBOARD_ENV_NAME", DAFT_DASHBOARD_ENV_NAME)?;
    module.add("DAFT_DASHBOARD_URL", DAFT_DASHBOARD_URL)?;
    module.add("DAFT_DASHBOARD_QUERIES_URL", DAFT_DASHBOARD_QUERIES_URL)?;
    module.add("DAFT_DASHBOARD_ENV_ENABLED", DAFT_DASHBOARD_ENV_ENABLED)?;
    parent.add_submodule(&module)?;

    Ok(())
}
