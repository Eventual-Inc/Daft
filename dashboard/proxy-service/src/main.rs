use std::net::Ipv4Addr;

use http_body_util::Full;
use hyper::{body::Bytes, server::conn::http2, service::service_fn, Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::{
    net::TcpListener,
    spawn,
    sync::broadcast::{self, Receiver, Sender},
};

type Message = ();
const DAFT_PORT: u16 = 3238;

async fn daft_http_application(
    _: Sender<Message>,
    _: Request<hyper::body::Incoming>,
) -> anyhow::Result<Response<Full<Bytes>>> {
    todo!()
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
            if let Err(err) = http2::Builder::new(TokioExecutor::default())
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
