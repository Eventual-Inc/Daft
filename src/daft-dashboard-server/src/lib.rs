mod response;

use std::{future::Future, net::Ipv4Addr, sync::OnceLock};

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
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, spawn};

type Req<T = Incoming> = Request<T>;
type Res = Response<BoxBody<Bytes, std::convert::Infallible>>;

const DAFT_PORT: u16 = 3238;
const DASHBOARD_PORT: u16 = DAFT_PORT + 1;

static QUERY_METADATAS: OnceLock<RwLock<Vec<QueryMetadata>>> = OnceLock::new();

fn query_metadatas() -> &'static RwLock<Vec<QueryMetadata>> {
    QUERY_METADATAS.get_or_init(RwLock::default)
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct QueryMetadata {
    id: String,
    mermaid_plan: String,
    plan_time_start: DateTime<Utc>,
    plan_time_end: DateTime<Utc>,
}

async fn deserialize<T: for<'de> Deserialize<'de>>(req: Req) -> anyhow::Result<Req<T>> {
    let (parts, body) = req.into_parts();
    let bytes = body.collect().await?.to_bytes();
    let data = simdutf8::basic::from_utf8(&bytes)?;
    let body = serde_json::from_str(data)?;
    Ok(Request::from_parts(parts, body))
}

async fn daft_http_application(req: Req) -> anyhow::Result<Res> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/") => {
            let req = deserialize::<QueryMetadata>(req).await?;
            query_metadatas().write().push(req.into_body());
            Ok(response::empty(StatusCode::OK))
        }
        _ => Ok(response::empty(StatusCode::NOT_FOUND)),
    }
}

async fn dashboard_http_application(req: Req) -> anyhow::Result<Res> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            let query_metadatas = query_metadatas().read();
            Ok(response::with_body(
                StatusCode::OK,
                query_metadatas.as_slice(),
            ))
        }
        _ => Ok(response::empty(StatusCode::NOT_FOUND)),
    }
}

pub async fn run() {
    async fn run_server<F>(f: fn(Req) -> F, addr: Ipv4Addr, port: u16)
    where
        F: 'static + Send + Future<Output = anyhow::Result<Res>>,
    {
        let listener = TcpListener::bind((addr, port))
            .await
            .expect("Failed to bind to port");
        loop {
            let (stream, _) = listener
                .accept()
                .await
                .expect("Unable to accept incoming connection");
            let io = TokioIo::new(stream);
            spawn(async move {
                http1::Builder::new()
                    .serve_connection(io, service_fn(f))
                    .await
                    .expect("Failed to serve endpoint");
            });
        }
    }

    tokio::join!(
        run_server(daft_http_application, Ipv4Addr::LOCALHOST, DAFT_PORT),
        run_server(
            dashboard_http_application,
            Ipv4Addr::LOCALHOST,
            DASHBOARD_PORT,
        ),
    );
    unreachable!("The daft and dashboard servers should be infinitely running processes");
}
