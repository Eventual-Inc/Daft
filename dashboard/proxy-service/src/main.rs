use std::{
    net::Ipv4Addr,
    sync::{Mutex, OnceLock},
};

use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::{Bytes, Incoming},
    header,
    server::conn::http1,
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, spawn};

type Req<T = Incoming> = Request<T>;
type Res = Response<BoxBody<Bytes, std::convert::Infallible>>;

const DAFT_PORT: u16 = 3238;
const DASHBOARD_PORT: u16 = DAFT_PORT + 1;

static QUERIES: OnceLock<Mutex<Vec<DaftBroadcast>>> = OnceLock::new();

fn queries() -> &'static Mutex<Vec<DaftBroadcast>> {
    QUERIES.get_or_init(Mutex::default)
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct DaftBroadcast {
    mermaid_plan: String,
}

fn response(status: StatusCode, body: impl Serialize) -> Res {
    let body = serde_json::to_string(&body).expect("Body should always be serializable");
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "http://localhost:3000")
        .body(Full::new(body.into()).boxed())
        .expect("Responses should always be able to be constructed")
}

fn empty_response(status: StatusCode) -> Res {
    Response::builder()
        .status(status)
        .body(Empty::default().boxed())
        .expect("Responses should always be able to be constructed")
}

async fn deserialize<T: for<'de> Deserialize<'de>>(req: Req) -> anyhow::Result<Req<T>> {
    let (parts, body) = req.into_parts();
    let bytes = body.collect().await?.to_bytes();
    let data = simdutf8::basic::from_utf8(&bytes)?;
    let body = serde_json::from_str(data)?;
    Ok(Request::from_parts(parts, body))
}

async fn run_daft_server() {
    async fn daft_http_application(req: Req) -> anyhow::Result<Res> {
        match (req.method(), req.uri().path()) {
            (&Method::POST, "/") => {
                let req = deserialize::<DaftBroadcast>(req).await?;
                let mut queries = queries().lock().unwrap();
                queries.push(req.into_body());
                // let _ = tx.send(req.into_body())?;
                Ok(empty_response(StatusCode::OK))
            }
            _ => Ok(empty_response(StatusCode::NOT_FOUND)),
        }
    }

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, DAFT_PORT))
        .await
        .unwrap();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let io = TokioIo::new(stream);
        spawn(async move {
            http1::Builder::new()
                .serve_connection(io, service_fn(daft_http_application))
                .await
                .ok();
        });
    }
}

async fn run_dashboard_server() {
    async fn dashboard_http_application(req: Req) -> anyhow::Result<Res> {
        let queries = queries().lock().unwrap();
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/") => Ok(response(StatusCode::OK, &*queries)),
            _ => Ok(empty_response(StatusCode::NOT_FOUND)),
        }
    }

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, DASHBOARD_PORT))
        .await
        .unwrap();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let io = TokioIo::new(stream);
        spawn(async move {
            http1::Builder::new()
                .serve_connection(io, service_fn(dashboard_http_application))
                .await
                .ok();
        });
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 3)]
async fn main() {
    tokio::join!(run_daft_server(), run_dashboard_server());
    unreachable!("The daft and dashboard servers should be infinitely running processes");
}
