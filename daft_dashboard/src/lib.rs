#[cfg(feature = "python")]
mod python;
mod response;
#[cfg(not(feature = "python"))]
pub mod rust;

use std::{io::Cursor, net::Ipv4Addr, sync::OnceLock};

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
use tokio::{net::TcpStream, spawn, sync::mpsc::Sender};

type Req<T = Incoming> = Request<T>;
type Res = Response<BoxBody<Bytes, std::io::Error>>;
type ServerResult<T> = Result<T, (StatusCode, anyhow::Error)>;

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
            shutdown_signal.send(()).await.unwrap();
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
            .unwrap();
    });
}
