/// Logical MCAP operations, distinct from storage requests in `IOStatsContext`.
///
/// Seeks only update a parser offset; they do not themselves issue I/O. A read
/// can hit a buffer, and a fetch can satisfy many reads. Backend GET counts are
/// logical storage operations, not necessarily wire requests (retries,
/// redirects, and Xet reconstruction can issue additional requests).
#[derive(Clone, Debug, Default)]
pub struct McapReadStats {
    /// Footer/summary parser seek events, including fully buffered files.
    pub summary_seeks: usize,
    /// Footer/summary parser read events, including fully buffered files.
    pub summary_reads: usize,
    pub summary_buffer_hits: usize,
    /// Range fetches made by the summary reader (excludes the magic probe).
    pub summary_fetches: usize,
    /// Bytes from the footer's summary start through EOF, including the footer
    /// and trailing magic. None when no summary is present.
    pub summary_tail_bytes: Option<u64>,
    /// Selected indexed chunks requested by the decoder.
    pub chunk_reads: usize,
    pub chunk_buffer_hits: usize,
    pub chunk_fetches: usize,
    /// Decoded record requests in the unindexed fallback, not AsyncRead calls.
    pub linear_record_reads: usize,
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use daft_io::{IOConfig, IOStatsContext, get_io_client};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use crate::{
        McapReadOptions, NativeMcapReader,
        test_utils::{collect_rows, make_reader, write_mcap, write_mcap_out_of_order_with_payload},
    };

    #[derive(Default)]
    struct Requests {
        gets: AtomicUsize,
        heads: AtomicUsize,
        bytes: AtomicUsize,
    }

    struct Server(tokio::task::JoinHandle<()>);

    impl Drop for Server {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    async fn serve(bytes: Vec<u8>) -> (String, Arc<Requests>, Server) {
        serve_with_range_behavior(bytes, false).await
    }

    async fn serve_with_range_behavior(
        bytes: Vec<u8>,
        ignore_nonzero_ranges: bool,
    ) -> (String, Arc<Requests>, Server) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!("http://{}/fixture.mcap", listener.local_addr().unwrap());
        let counts = Arc::new(Requests::default());
        let server_counts = counts.clone();
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
                    .find_map(|line| line.strip_prefix("range: bytes="))
                    .filter(|range| !ignore_nonzero_ranges || range.starts_with("0-"));
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
                    headers.push_str(&format!(
                        "Content-Range: bytes {start}-{}/{}\r\n",
                        end - 1,
                        bytes.len()
                    ));
                }
                headers.push_str("\r\n");
                if head {
                    server_counts.heads.fetch_add(1, Ordering::Relaxed);
                } else {
                    server_counts.gets.fetch_add(1, Ordering::Relaxed);
                    server_counts
                        .bytes
                        .fetch_add(end - start, Ordering::Relaxed);
                }
                socket.write_all(headers.as_bytes()).await.unwrap();
                if !head {
                    socket.write_all(&bytes[start..end]).await.unwrap();
                }
            }
        });
        (uri, counts, Server(task))
    }

    #[tokio::test]
    async fn indexed_order_is_independent_of_remote_size_and_filters() {
        for payload_size in [8, 4096] {
            let fixture = write_mcap_out_of_order_with_payload(10, payload_size);
            let contents = std::fs::read(fixture.path()).unwrap();
            let small = payload_size == 8;
            assert_eq!(contents.len() <= 64 * 1024, small);
            let (uri, _, _server) = serve(contents).await;
            for filtered in [false, true] {
                let options = if filtered {
                    McapReadOptions {
                        topics: Some(vec!["/camera".to_string()]),
                        start_time: Some(2),
                        end_time: Some(105),
                        batch_size: 3,
                    }
                } else {
                    McapReadOptions {
                        batch_size: 3,
                        ..Default::default()
                    }
                };
                let (mut local, _) = make_reader(&fixture, options.clone()).await.unwrap();
                let expected = collect_rows(&mut local).await.unwrap();
                let times: Vec<_> = if filtered {
                    (2..10).chain(100..105).collect()
                } else {
                    (0..10).chain(100..110).collect()
                };
                assert_eq!(
                    expected
                        .iter()
                        .map(|(_, time, _)| *time)
                        .collect::<Vec<_>>(),
                    times
                );

                let io = IOStatsContext::new("MCAP remote ordering regression");
                let client = get_io_client(true, Arc::new(IOConfig::default())).unwrap();
                let mut remote = NativeMcapReader::new(&uri, client, io.clone(), options)
                    .await
                    .unwrap();
                assert!(remote.indexed());
                assert_eq!(collect_rows(&mut remote).await.unwrap(), expected);
                if small {
                    assert_eq!(io.load_get_requests(), 1);
                    assert_eq!(remote.read_stats().chunk_fetches, 0);
                } else {
                    assert!(io.load_get_requests() > 1);
                }
            }
        }
    }

    #[tokio::test]
    async fn rejects_nonzero_range_response_with_unknown_origin() {
        let fixture = write_mcap(true, None, 100, 4096);
        let contents = std::fs::read(fixture.path()).unwrap();
        assert!(contents.len() > 64 * 1024);
        // The initial offset-zero probe works, but the footer GET returns the
        // whole file. Slicing its first 37 bytes would not yield the footer.
        let (uri, requests, _server) = serve_with_range_behavior(contents, true).await;
        let io = IOStatsContext::new("MCAP inconsistent Range response regression");
        let client = get_io_client(true, Arc::new(IOConfig::default())).unwrap();
        let error = NativeMcapReader::new(uri, client, io, McapReadOptions::default())
            .await
            .err()
            .expect("inconsistent range origin must not be accepted");
        assert!(
            error.to_string().contains("unexpected range length"),
            "{error}"
        );
        assert_eq!(requests.gets.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn remote_requests_match_io_stats_and_logical_reads() {
        // Small files are fully buffered. Larger files use a magic GET, footer
        // GET, one summary-tail GET, and coalesced indexed chunk GETs.
        for payload_size in [8, 4096] {
            let fixture = write_mcap(true, None, 100, payload_size);
            let (uri, requests, _server) = serve(std::fs::read(fixture.path()).unwrap()).await;
            let io = IOStatsContext::new("MCAP HTTP stats regression");
            let client = get_io_client(true, Arc::new(IOConfig::default())).unwrap();
            let mut reader =
                NativeMcapReader::new(uri, client, io.clone(), McapReadOptions::default())
                    .await
                    .unwrap();
            let small = payload_size == 8;
            assert_eq!(io.load_head_requests(), 1);
            assert_eq!(io.load_get_requests(), if small { 1 } else { 3 });
            let stats = reader.read_stats();
            assert_eq!(stats.summary_seeks, 2);
            assert_eq!(stats.summary_fetches, if small { 0 } else { 2 });
            assert!(stats.summary_tail_bytes.unwrap() > 0);
            assert!(stats.summary_reads > stats.summary_fetches);
            assert_eq!(
                stats.summary_reads,
                stats.summary_fetches + stats.summary_buffer_hits
            );
            let mut rows = 0;
            while let Some(batch) = reader.next_batch().await.unwrap() {
                rows += batch.len();
            }
            assert_eq!(rows, 100);
            let stats = reader.read_stats();
            assert!(stats.chunk_reads > 1);
            if small {
                assert_eq!(stats.chunk_fetches, 0);
            } else {
                assert!(stats.chunk_fetches > 0);
                assert!(stats.chunk_fetches < stats.chunk_reads);
            }
            assert_eq!(
                stats.chunk_reads,
                stats.chunk_fetches + stats.chunk_buffer_hits
            );
            assert_eq!(stats.linear_record_reads, 0);
            assert_eq!(
                io.load_get_requests(),
                if small { 1 } else { 3 + stats.chunk_fetches }
            );
            eprintln!(
                "small={small} gets={} heads={} bytes={} logical={stats:?}",
                io.load_get_requests(),
                io.load_head_requests(),
                io.load_bytes_read()
            );
            assert_eq!(
                io.load_get_requests(),
                requests.gets.load(Ordering::Relaxed)
            );
            assert_eq!(
                io.load_head_requests(),
                requests.heads.load(Ordering::Relaxed)
            );
            assert_eq!(io.load_bytes_read(), requests.bytes.load(Ordering::Relaxed));
        }
    }
}
