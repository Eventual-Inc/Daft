use std::{sync::Arc, ops::Range, fs::OpenOptions};

use bytes::Bytes;
use daft_io::{IOClient, IOStatsRef, GetResult};
use futures::{StreamExt, Stream, stream::BoxStream};
use itertools::all;


struct BytesConsumer {
    start: usize,
    end: usize,
    curr: usize,
    channel: tokio::sync::mpsc::UnboundedSender<Bytes>
}


struct InFlightState {
    consumers: Vec<BytesConsumer>
}

enum DownloaderState {
    InFlight(InFlightState),
    Complete(Vec<usize>, Vec<Bytes>)
}

impl DownloaderState {
    fn new() -> ProtectedState {
        ProtectedState(tokio::sync::Mutex::new(DownloaderState::InFlight(InFlightState { consumers: vec![] })).into())
    }
}

#[derive(Clone)]
struct ProtectedState(Arc<tokio::sync::Mutex<DownloaderState>>);


impl ProtectedState {
    async fn get_stream(&self, range: Range<usize>) -> BoxStream<'static, Bytes>{
        let mut _g = self.0.lock().await;
        match &mut (*_g) {
            DownloaderState::Complete(so , v) => {
                let mut current_pos = range.start;
                let mut curr_index;
                let start_point = so.binary_search(&current_pos);
                let mut bytes = vec![];
                match start_point {
                    Ok(index) => {
                        let index_pos = so[index];
                        let chunk = &v[index];
                        let end_offset = chunk.len().min(range.end - current_pos);
                        bytes.push(chunk.slice(current_pos..end_offset));
                        assert_eq!(index_pos, current_pos);
                        current_pos += (end_offset - current_pos);
                        curr_index = index + 1;
                    }
                    Err(index) => {
                        let index = index - 1;
                        let index_pos = so[index];
                        let chunk = &v[index];
                        let start_offset = current_pos - index_pos;
                        let end_offset = chunk.len().min(range.end - index_pos);
                        bytes.push(chunk.slice(start_offset..end_offset));
                        current_pos += (end_offset - start_offset);
                        curr_index = index + 1;

                    }
                };
                while current_pos < range.end && curr_index < v.len() {
                    let chunk = &v[curr_index];
                    let start_offset = 0;
                    let end_offset = chunk.len().min(range.end - current_pos);
                    bytes.push(chunk.slice(start_offset..end_offset));
                    current_pos += end_offset - start_offset;
                    curr_index += 1;
                }
                futures::stream::iter(bytes).boxed()
            },
            DownloaderState::InFlight(InFlightState { ref mut consumers }) => {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let new_consumer = BytesConsumer {
                    start: range.start,
                    end: range.end,
                    curr: range.start,
                    channel: tx
                };
                consumers.push(new_consumer);
                let stream = async_stream::stream! {
                    while let Some(v) = rx.recv().await{
                        yield v
                    }
                };
                stream.boxed()

            }
        }
    }
}


fn worker(url: String, range: Range<usize>, io_client: Arc<IOClient>, io_stats: Option<IOStatsRef>) -> ProtectedState {

    let state = DownloaderState::new();

    let owned_state = state.clone();
    let join_handle = tokio::spawn(async move {
        let expected_len = range.len();
        let expected_chunks = (expected_len + 4096 - 1) / 4096;
        
        let get_result = io_client
            .single_url_get(url, Some(range.clone()), io_stats)
            .await.unwrap();
        // dont unwrap

        let mut chunks = Vec::<Bytes>::with_capacity(expected_chunks);
        let mut start_offsets = Vec::<usize>::with_capacity(expected_chunks);
        let mut curr_pos = 0;
        if let GetResult::Stream(mut stream, ..) = get_result {

            while curr_pos < expected_len {
                if let Some(chunk) = stream.next().await {
                    let chunk = chunk.unwrap();
                    // dont unwrap
                    start_offsets.push(curr_pos);
                    curr_pos += chunk.len();
                    chunks.push(chunk);

                    {
                        let mut _g = owned_state.0.lock().await;
                        if let DownloaderState::InFlight(InFlightState { ref mut consumers }) = &mut (*_g) {
                            for c in consumers {                                
                                if c.curr <= curr_pos && curr_pos < c.end {
                                    let mut chunk_index_to_start = chunks.len() - 1;
                                    let mut offset = curr_pos;
                                    while c.curr <= offset {
                                        offset -= chunks[chunk_index_to_start].len();
                                        chunk_index_to_start -= 1;
                                    } 
                                    while c.curr < c.end && chunk_index_to_start < chunks.len() {
                                        let chunk = &chunks[chunk_index_to_start];
                                        let start_offset = c.curr - offset;
                                        let end_offset = chunk.len().min(c.end - c.curr);

                                        let sliced = chunk.slice(start_offset..end_offset);
                                        c.curr += sliced.len();
                                        c.channel.send(sliced).unwrap();
                                        offset = c.curr;
                                        chunk_index_to_start += 1;
                                    }

                                }
                            }

                        }
                    }
                } else {
                    break;
                }
            }
            {
                let mut _g = owned_state.0.lock().await;
                if let DownloaderState::InFlight(InFlightState { ref mut consumers }) = &mut (*_g) {
                    for c in consumers {                                
                        if c.curr <= curr_pos && curr_pos < c.end {
                            let mut chunk_index_to_start = chunks.len() - 1;
                            let mut offset = curr_pos;
                            while c.curr <= offset {
                                offset -= chunks[chunk_index_to_start].len();
                                chunk_index_to_start -= 1;
                            } 
                            while c.curr < c.end && chunk_index_to_start < chunks.len() {
                                let chunk = &chunks[chunk_index_to_start];
                                let start_offset = c.curr - offset;
                                let end_offset = chunk.len().min(c.end - c.curr);

                                let sliced = chunk.slice(start_offset..end_offset);
                                c.curr += sliced.len();
                                c.channel.send(sliced).unwrap();
                                offset = c.curr;
                                chunk_index_to_start += 1;
                            }

                        }
                    }
                }
                *_g = DownloaderState::Complete(start_offsets, chunks);
                return;
            }

        } else {
            unreachable!("we shouldn't be using this worker for local files")
        }
    });
    return state;

}