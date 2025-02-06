use std::net::Ipv4Addr;

use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    spawn,
    sync::broadcast::{self, error::TryRecvError, Receiver, Sender},
};

type Message = DaftBroadcast;
type Req<T = Incoming> = Request<T>;
type Res = Response<BoxBody<Bytes, std::convert::Infallible>>;

const DAFT_PORT: u16 = 3238;
const DASHBOARD_PORT: u16 = DAFT_PORT + 1;

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

async fn run_daft_server(tx: Sender<Message>) {
    async fn daft_http_application(tx: Sender<Message>, req: Req) -> anyhow::Result<Res> {
        match (req.method(), req.uri().path()) {
            (&Method::POST, "/") => {
                let req = deserialize::<DaftBroadcast>(req).await?;
                let _ = tx.send(req.into_body())?;
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
        let tx = tx.clone();
        spawn(async move {
            http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| daft_http_application(tx.clone(), req)),
                )
                .await
                .ok();
        });
    }
}

async fn run_dashboard_server(mut rx: Receiver<Message>) {
    async fn dashboard_http_application(
        queries: &[DaftBroadcast],
        req: Req,
    ) -> anyhow::Result<Res> {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/") => Ok(response(StatusCode::OK, queries.to_vec())),
            _ => Ok(empty_response(StatusCode::NOT_FOUND)),
        }
    }

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, DASHBOARD_PORT))
        .await
        .unwrap();
    let mut queries = vec![];
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let io = TokioIo::new(stream);
        loop {
            match rx.try_recv() {
                Ok(daft_broadcast) => queries.push(daft_broadcast),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Lagged(..)) => continue,
                Err(TryRecvError::Closed) => panic!("Receiver has forcibly closed"),
            }
        }
        spawn({
            let queries = queries.clone();
            async move {
                http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(|req| dashboard_http_application(&queries, req)),
                    )
                    .await
                    .ok();
            }
        });
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 3)]
async fn main() {
    const CHANNEL_SIZE: usize = 256;
    let (tx, rx) = broadcast::channel(CHANNEL_SIZE);
    tokio::join!(run_daft_server(tx), run_dashboard_server(rx));
    unreachable!("The daft and dashboard servers should be infinitely running processes");
}
