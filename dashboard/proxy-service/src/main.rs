use std::{io::ErrorKind, net::Ipv4Addr};

use futures::{SinkExt, StreamExt};
use tokio::{
    net::TcpListener,
    sync::broadcast::{self, Receiver, Sender},
};
use tokio_tungstenite::tungstenite::Message;

const DAFT_BROADCAST_PORT: u16 = 3238;
const DASHBOARD_WEBSOCKET_PORT: u16 = DAFT_BROADCAST_PORT + 1;

async fn run_daft_server(tx: Sender<String>) {
    async fn run(tx: Sender<String>) -> anyhow::Result<()> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, DAFT_BROADCAST_PORT)).await?;

        loop {
            let (stream, _) = listener.accept().await?;
            let mut string = String::default();
            let mut total_size = 0;

            loop {
                stream.readable().await?;

                const MAX_MESSAGE_SIZE: usize = 2usize.pow(20);
                if total_size >= MAX_MESSAGE_SIZE {
                    anyhow::bail!("Maximum message size exceeded; max: {MAX_MESSAGE_SIZE}, total-size: {total_size}");
                }

                const BUFFER_SIZE: usize = 2usize.pow(10);
                let mut buf = [0; BUFFER_SIZE];

                match stream.try_read(&mut buf) {
                    Ok(0) => break,
                    Ok(bytes_read) => {
                        let new = String::from_utf8_lossy(&buf[0..bytes_read]);
                        string.push_str(&new);
                        total_size += bytes_read;
                    }
                    Err(err) if err.kind() == ErrorKind::WouldBlock => (),
                    Err(err) => Err(err)?,
                }
            }

            // If `tx.subscribe()` is not called before this line executes, an error saying something like "No receivers found" will be thrown.
            tx.send(string)?;
        }
    }

    loop {
        if let Err(err) = run(tx.clone()).await {
            eprintln!("Error while running daft server: {err}");
        }
    }
}

async fn run_dashboard_server(mut rx: Receiver<String>) {
    async fn run(rx: &mut Receiver<String>) -> anyhow::Result<()> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, DASHBOARD_WEBSOCKET_PORT)).await?;
        let (stream, _) = listener.accept().await?;
        let (mut ws_send, _) = tokio_tungstenite::accept_async(stream).await?.split();

        loop {
            let string = rx.recv().await?;
            ws_send.send(Message::text(string)).await?;
        }
    }

    loop {
        if let Err(err) = run(&mut rx).await {
            eprintln!("Error while running dashboard server: {err}");
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 3)]
async fn main() -> anyhow::Result<()> {
    const CHANNEL_SIZE: usize = 16;
    let (tx, rx) = broadcast::channel(CHANNEL_SIZE);
    tokio::join!(run_daft_server(tx), run_dashboard_server(rx));
    unreachable!("The daft and dashboard servers should be infinitely running processes");
}
