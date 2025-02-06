use std::net::Ipv4Addr;

use http_body_util::{combinators::BoxBody, BodyExt, Empty};
use hyper::{
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use tokio::{
    net::TcpListener,
    spawn,
    sync::broadcast::{self, Receiver, Sender},
};

type Message = ();
type Req<T = Incoming> = Request<T>;
type Res = Response<BoxBody<Bytes, std::convert::Infallible>>;

const DAFT_PORT: u16 = 3238;

// fn response(status: StatusCode, body: impl Into<Bytes>) -> Res {
//     Response::builder()
//         .status(status)
//         .body(Full::new(body.into()).boxed())
//         .expect("Responses should always be able to be constructed")
// }

fn empty_response(status: StatusCode) -> Res {
    Response::builder()
        .status(status)
        .body(Empty::default().boxed())
        .expect("Responses should always be able to be constructed")
}

async fn daft_http_application(_: Sender<Message>, req: Req) -> anyhow::Result<Res> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/") => Ok(empty_response(StatusCode::OK)),
        _ => Ok(empty_response(StatusCode::NOT_FOUND)),
    }
}

async fn run_daft_server(tx: Sender<Message>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, DAFT_PORT))
        .await
        .unwrap();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let io = TokioIo::new(stream);
        let tx = tx.clone();
        spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| daft_http_application(tx.clone(), req)),
                )
                .await
            {
                eprintln!("Error: {err}");
            };
        });
    }
}

async fn run_dashboard_server(_: Receiver<Message>) {}

#[tokio::main(flavor = "multi_thread", worker_threads = 3)]
async fn main() {
    const CHANNEL_SIZE: usize = 16;
    let (tx, rx) = broadcast::channel(CHANNEL_SIZE);
    tokio::join!(run_daft_server(tx), run_dashboard_server(rx));
    unreachable!("The daft and dashboard servers should be infinitely running processes");
}
