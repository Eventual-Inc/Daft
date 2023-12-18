use std::{fs::OpenOptions, ops::Range, sync::Arc};

use arrow2::chunk;
use bytes::Bytes;
use daft_io::{GetResult, IOClient, IOStatsRef};
use futures::{stream::BoxStream, Stream, StreamExt};

struct BytesConsumer {
    start: usize,
    end: usize,
    curr: usize,
    last_index: Option<usize>,
    channel: tokio::sync::mpsc::UnboundedSender<Bytes>,
}

impl PartialEq for BytesConsumer {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start && self.end == other.end && self.curr == other.curr
    }
}

impl Eq for BytesConsumer {}

impl Ord for BytesConsumer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .curr
            .cmp(&self.curr)
            .then_with(|| other.start.cmp(&self.start))
            .then_with(|| other.end.cmp(&self.end))
    }
}

impl PartialOrd for BytesConsumer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct InFlightState {
    consumers: std::collections::BinaryHeap<BytesConsumer>,
}

enum DownloaderState {
    InFlight(InFlightState),
    Complete(Bytes),
}

impl DownloaderState {
    fn new() -> ProtectedState {
        ProtectedState(
            std::sync::Mutex::new(DownloaderState::InFlight(InFlightState {
                consumers: std::collections::BinaryHeap::new(),
            }))
            .into(),
        )
    }
}

#[derive(Clone)]
pub(crate) struct ProtectedState(Arc<std::sync::Mutex<DownloaderState>>);

impl ProtectedState {
    pub async fn get_stream(
        &self,
        range: Range<usize>,
    ) -> BoxStream<'static, std::result::Result<Bytes, daft_io::Error>> {
        let mut _g = self.0.lock().unwrap();
        match &mut (*_g) {
            DownloaderState::Complete(bytes) => {
                let result = bytes.slice(range);
                futures::stream::iter(vec![Ok(result)]).boxed()
            }
            DownloaderState::InFlight(InFlightState { ref mut consumers }) => {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let new_consumer = BytesConsumer {
                    start: range.start,
                    end: range.end,
                    curr: range.start,
                    last_index: None,
                    channel: tx,
                };
                let expected_bytes = range.len();
                consumers.push(new_consumer);
                let stream = async_stream::stream! {
                    let mut bytes_read = 0;
                    while bytes_read < expected_bytes && let Some(v) = rx.recv().await {
                        bytes_read += v.len();
                        yield Ok(v)
                    }
                };
                stream.boxed()
            }
        }
    }
}

pub(crate) fn start_worker(
    url: String,
    global_range: Range<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> ProtectedState {
    let state = DownloaderState::new();

    let owned_state = state.clone();
    let join_handle = tokio::spawn(async move {
        let expected_len = global_range.len();
        let expected_chunks = (expected_len + 4096 - 1) / 4096;

        let get_result = io_client
            .single_url_get(url, Some(global_range.clone()), io_stats)
            .await
            .unwrap();
        // dont unwrap
        let mut chunks = Vec::<Bytes>::with_capacity(expected_chunks);
        let mut start_offsets = Vec::<usize>::with_capacity(expected_chunks);
        let mut curr_pos = 0;
        let mut stream_active = true;
        let target_chunk_size = 256 * 1024;
        if let GetResult::Stream(mut stream, ..) = get_result {
            loop {
                {
                    let mut curr_chunk = Vec::with_capacity(target_chunk_size);
                    while stream_active && curr_chunk.len() < target_chunk_size {
                        if stream_active && let Some(chunk) = stream.next().await {
                            let chunk = chunk.unwrap();
                            // dont unwrap
                            curr_chunk.extend_from_slice(&chunk);
                        } else {
                            stream_active = false;
                            break;    
                        }
                    }
                    let chunk: Bytes = curr_chunk.into();
                    start_offsets.push(curr_pos);
                    curr_pos += chunk.len();
                    chunks.push(chunk);
                }
                {
                    let mut _g = owned_state.0.lock().unwrap();
                    if let DownloaderState::InFlight(InFlightState { ref mut consumers }) =
                        &mut (*_g)
                    {
                        while !consumers.is_empty() && consumers.peek().unwrap().curr < curr_pos {
                            let mut c = consumers.pop().unwrap();
                            let mut is_dropped = false;
                            assert!(
                                c.curr >= c.start && c.curr <= c.end,
                                "start: {} curr: {} end: {} ",
                                c.start,
                                c.curr,
                                c.end
                            );

                            if c.last_index.is_none() {
                                let start_point = start_offsets.binary_search(&c.curr);
                                let curr_index;
                                match start_point {
                                    Ok(index) => {
                                        let index_pos = start_offsets[index];
                                        let chunk = &chunks[index];
                                        let end_offset = chunk.len().min(c.end - c.curr);
                                        let start_offset = c.curr - index_pos;
                                        let _v = c.channel.send(chunk.slice(start_offset..end_offset));
                                        is_dropped = is_dropped | _v.is_err();
                                        assert_eq!(index_pos, c.curr);
                                        // let before = c.curr;
                                        c.curr += (end_offset - start_offset);
                                        // println!("ok before {before} after {}", c.curr);
                                        curr_index = index + 1;
                                    }
                                    Err(index) => {
                                        assert!(index > 0,);
                                        let index = index - 1;
                                        let index_pos = start_offsets[index];
                                        assert!(index_pos < c.curr);

                                        let chunk = &chunks[index];
                                        let start_offset = c.curr - index_pos;
                                        let end_offset = chunk.len().min(c.end - index_pos);
                                        assert!(
                                            c.curr >= c.start && c.curr < c.end,
                                            "start: {} curr: {} end: {} ",
                                            c.start,
                                            c.curr,
                                            c.end
                                        );
                                        let _v = c.channel.send(chunk.slice(start_offset..end_offset));
                                        is_dropped = is_dropped | _v.is_err();

                                        // let before = c.curr;
                                        c.curr += (end_offset - start_offset);
                                        // println!("err before {before} after {}", c.curr);
                                        curr_index = index + 1;
                                    }
                                };
                                c.last_index = Some(curr_index);
                            }
                            while !is_dropped && c.curr < curr_pos && c.last_index.unwrap() < chunks.len() {
                                let chunk = &chunks[c.last_index.unwrap()];
                                let start_offset = 0;
                                let end_offset = chunk.len().min(c.end - c.curr);

                                let chunk = if c.curr + chunk.len() > c.end {
                                    chunk.slice(start_offset..end_offset)
                                } else {
                                    chunk.clone()
                                };
                                let _v = c.channel.send(chunk);
                                is_dropped = is_dropped | _v.is_err();

                                // let before = c.curr;
                                c.curr += (end_offset - start_offset);
                                // println!("while before {before} after {}", c.curr);
                                c.last_index = Some(c.last_index.unwrap() + 1);
                            }
                            if !is_dropped && c.curr < c.end {
                                consumers.push(c)
                            }
                        }
                        if !stream_active && consumers.is_empty() {
                            let mut all_data = Vec::<u8>::with_capacity(expected_len);
                            for chunk in chunks.into_iter() {
                                all_data.extend_from_slice(&chunk);
                            }
                            *_g = DownloaderState::Complete(all_data.into());
                            break;
                        }
                    }
                }
            }
        } else {
            unreachable!("we shouldn't be using this worker for local files")
        }
    });
    return state;
}
