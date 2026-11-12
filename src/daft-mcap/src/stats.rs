use std::{fmt::Write, sync::Arc};

use daft_io::{IOConfig, IOStatsContext, get_io_client};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{
    McapReadOptions, McapReader,
    test_utils::{collect_rows, make_reader, write_mcap_out_of_order_with_payload},
};

struct Server(tokio::task::JoinHandle<()>);

impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn serve(bytes: Vec<u8>) -> (String, Server) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/fixture.mcap", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await.unwrap());
                assert!(request.len() < 16384);
            }
            let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
            let head = request.starts_with("head ");
            let range = request
                .lines()
                .find_map(|line| line.strip_prefix("range: bytes="));
            let (start, end) = match range {
                Some(range) => {
                    let (start, end) = range.split_once('-').unwrap();
                    (
                        start.parse::<usize>().unwrap(),
                        end.parse::<usize>().unwrap() + 1,
                    )
                }
                None => (0, bytes.len()),
            };
            assert!(start < end && end <= bytes.len());
            let status = if range.is_some() {
                "206 Partial Content"
            } else {
                "200 OK"
            };
            let mut headers = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n",
                end - start
            );
            if range.is_some() {
                write!(
                    headers,
                    "Content-Range: bytes {start}-{}/{}\r\n",
                    end - 1,
                    bytes.len()
                )
                .expect("failed to write headers");
            }
            headers.push_str("\r\n");
            socket.write_all(headers.as_bytes()).await.unwrap();
            if !head {
                socket.write_all(&bytes[start..end]).await.unwrap();
            }
        }
    });
    (uri, Server(task))
}

async fn assert_remote_indexed_matches_local(payload_size: usize) -> usize {
    let fixture = write_mcap_out_of_order_with_payload(10, payload_size);
    let contents = std::fs::read(fixture.path()).unwrap();
    let file_size = contents.len();
    let (uri, _server) = serve(contents).await;

    let (mut local, _) = make_reader(&fixture, McapReadOptions::default())
        .await
        .unwrap();
    let expected = collect_rows(&mut local).await.unwrap();

    let io = IOStatsContext::new("MCAP remote indexed order");
    let client = get_io_client(true, Arc::new(IOConfig::default())).unwrap();
    let mut remote = McapReader::new(&uri, client, io, McapReadOptions::default())
        .await
        .unwrap();
    assert!(remote.indexed());
    assert_eq!(collect_rows(&mut remote).await.unwrap(), expected);
    file_size
}

#[tokio::test]
async fn small_remote_indexed_order_matches_local() {
    let file_size = assert_remote_indexed_matches_local(8).await;
    assert!(file_size <= 64 * 1024);
}

#[tokio::test]
async fn large_remote_indexed_order_matches_local() {
    let file_size = assert_remote_indexed_matches_local(4096).await;
    assert!(file_size > 64 * 1024);
}
