#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        sync::{Arc, Mutex},
    };

    use async_stream::stream;
    use async_trait::async_trait;
    use bytes::Bytes;
    use common_file_formats::FileFormat;
    use futures::{StreamExt, stream::BoxStream};

    use crate::{
        Error, FileMetadata, GetRange, GetResult, IOStatsRef, ObjectSource, Result,
        object_io::{LSResult, StreamingRetryParams},
    };

    /// The mocked source used for testing read operations.
    struct MockSource {
        // the object content data
        content: Arc<Vec<u8>>,
        // the object data will be chunked by this value
        chunk_size: usize,
        // will raise UnableToReadBytes error when accumulated read chunk number of a single request reach this value
        fail_at_chunk: Option<usize>,
        // the total received get requests
        requests: Arc<Mutex<Vec<Option<GetRange>>>>,
    }

    impl MockSource {
        fn new(data: Vec<u8>, chunk_size: usize, fail_at_chunk: Option<usize>) -> Self {
            Self {
                content: Arc::new(data),
                chunk_size,
                fail_at_chunk,
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn slice_for_range(&self, range: Option<GetRange>) -> std::ops::Range<usize> {
            let len = self.content.len();
            match range {
                Some(GetRange::Bounded(r)) => {
                    let end = r.end.min(len);
                    r.start.min(end)..end
                }
                Some(GetRange::Offset(o)) => o.min(len)..len,
                Some(GetRange::Suffix(n)) => len.saturating_sub(n)..len,
                None => 0..len,
            }
        }

        fn read_data(
            &self,
            range: Option<GetRange>,
            path: &str,
        ) -> BoxStream<'static, Result<Bytes>> {
            let slice = self.slice_for_range(range);
            let data = self.content[slice].to_vec();
            let chunk = self.chunk_size;
            let fail_idx = self.fail_at_chunk;
            let path_owned = path.to_string();

            stream! {
                let mut sent_chunks = 0usize;
                for chunk_data in data.chunks(chunk) {
                    if let Some(fail) = fail_idx {
                        if sent_chunks == fail {
                            yield Err(Error::UnableToReadBytes { path: path_owned.clone(), source: std::io::Error::new(std::io::ErrorKind::Interrupted, "mock fail") });
                            break;
                        }
                    }
                    sent_chunks += 1;
                    yield Ok(Bytes::copy_from_slice(chunk_data));
                }
            }.boxed()
        }
    }

    #[async_trait]
    impl ObjectSource for MockSource {
        async fn supports_range(&self, _uri: &str) -> Result<bool> {
            Ok(true)
        }

        async fn get(
            &self,
            uri: &str,
            range: Option<GetRange>,
            _io_stats: Option<IOStatsRef>,
        ) -> Result<GetResult> {
            self.requests.lock().unwrap().push(range.clone());
            let s = self.read_data(range, uri);
            Ok(GetResult::Stream(s, Some(self.content.len()), None, None))
        }

        async fn put(&self, _uri: &str, _data: Bytes, _io_stats: Option<IOStatsRef>) -> Result<()> {
            Err(Error::NotImplementedSource {
                store: "mock".to_string(),
            })
        }

        async fn get_size(&self, _uri: &str, _io_stats: Option<IOStatsRef>) -> Result<usize> {
            Ok(self.content.len())
        }

        async fn glob(
            self: Arc<Self>,
            _glob_path: &str,
            _fanout_limit: Option<usize>,
            _page_size: Option<i32>,
            _limit: Option<usize>,
            _io_stats: Option<IOStatsRef>,
            _file_format: Option<FileFormat>,
        ) -> Result<BoxStream<'static, Result<FileMetadata>>> {
            Err(Error::NotImplementedSource {
                store: "mock".to_string(),
            })
        }

        async fn ls(
            &self,
            _path: &str,
            _posix: bool,
            _continuation_token: Option<&str>,
            _page_size: Option<i32>,
            _io_stats: Option<IOStatsRef>,
        ) -> Result<LSResult> {
            Err(Error::NotImplementedSource {
                store: "mock".to_string(),
            })
        }

        fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
            self
        }
    }

    #[tokio::test]
    async fn test_resumable_bounded() {
        let data: Vec<u8> = (0..64_000u32).flat_map(|v| v.to_le_bytes()).collect();
        let data_len = data.len();
        let src = Arc::new(MockSource::new(data.clone(), 4096, Some(20)));
        let range = Some(GetRange::Bounded(0..data_len));
        let initial = src.get("mock://path", range.clone(), None).await.unwrap();
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src.clone(),
            "mock://path".to_string(),
            range.clone(),
            None,
        ));
        let out = wrapped.bytes().await.unwrap();
        assert_eq!(out.len(), data.len());
        assert_eq!(&out[..], &data[..]);
        let requests = src.requests.lock().unwrap().clone();
        assert_eq!(
            requests,
            vec![
                Some(GetRange::Bounded(0..data.len())),
                Some(GetRange::Bounded(81920..data.len())),
                Some(GetRange::Bounded(163840..data.len())),
                Some(GetRange::Bounded(245760..data_len))
            ]
        );
    }

    #[tokio::test]
    async fn test_resumable_offset() {
        let data: Vec<u8> = (0..64_000u32).flat_map(|v| v.to_le_bytes()).collect();
        let chunk_size = 8192;
        let fail_at_chunk = 10;
        let init_off = 10_000;

        let src = Arc::new(MockSource::new(
            data.clone(),
            chunk_size,
            Some(fail_at_chunk),
        ));
        let range = Some(GetRange::Offset(init_off));
        let initial = src.get("mock://path", range.clone(), None).await.unwrap();
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src.clone(),
            "mock://path".to_string(),
            range.clone(),
            None,
        ));
        let out = wrapped.bytes().await.unwrap();
        assert_eq!(&out[..], &data[init_off..]);

        let requests = src.requests.lock().unwrap().clone();
        assert_eq!(
            requests,
            vec![
                Some(GetRange::Offset(init_off)),
                Some(GetRange::Offset(chunk_size * fail_at_chunk + init_off)),
                Some(GetRange::Offset(chunk_size * fail_at_chunk * 2 + init_off)),
                Some(GetRange::Offset(chunk_size * fail_at_chunk * 3 + init_off)),
            ]
        )
    }

    #[tokio::test]
    async fn test_resumable_suffix() {
        let data: Vec<u8> = (0..32_000u32).flat_map(|v| v.to_le_bytes()).collect();
        let n = 10_000usize.min(data.len());
        let chunk_size = 2048;
        let fail_at_chunk = 2;

        let src = Arc::new(MockSource::new(
            data.clone(),
            chunk_size,
            Some(fail_at_chunk),
        ));
        let range = Some(GetRange::Suffix(n));
        let initial = src.get("mock://path", range.clone(), None).await.unwrap();
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src.clone(),
            "mock://path".to_string(),
            range.clone(),
            None,
        ));
        let out = wrapped.bytes().await.unwrap();
        assert_eq!(&out[..], &data[data.len().saturating_sub(n)..]);
        let requests = src.requests.lock().unwrap().clone();
        assert_eq!(
            requests,
            vec![
                Some(GetRange::Suffix(n)),
                Some(GetRange::Suffix(n - chunk_size * fail_at_chunk)),
                Some(GetRange::Suffix(n - chunk_size * fail_at_chunk * 2)),
            ]
        )
    }

    #[tokio::test]
    async fn test_non_retry_error_bubbles() {
        let data: Vec<u8> = (0..8_000u32).flat_map(|v| v.to_le_bytes()).collect();
        // fail at chunk 0 so first read errors; but simulate non-retry by producing NotImplementedSource (mapped as Generic)
        let src = Arc::new(MockSource::new(data.clone(), 1024, Some(0)));
        // Override stream to emit non-retry error by creating an initial GetResult::Stream with an immediate error
        let s = stream! { yield Err(Error::NotImplementedSource { store: "mock".to_string() }); }
            .boxed();
        let initial = GetResult::Stream(s, Some(data.len()), None, None);
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src.clone(),
            "mock://path".to_string(),
            None,
            None,
        ));
        let err = wrapped.bytes().await.err().unwrap();
        match err {
            Error::NotImplementedSource { .. } => {}
            _ => panic!("expected NotImplementedSource error"),
        }
    }

    #[tokio::test]
    async fn test_max_retries_exceeded() {
        // With default max retries = 3, total get calls should be 1 (initial) + 3 (retries) = 4
        let data: Vec<u8> = (0..2_000u32).flat_map(|v| v.to_le_bytes()).collect();
        let _data_size = data.len();
        let chunk_size = 1024;
        let src = Arc::new(MockSource::new(data.clone(), chunk_size, Some(0)));
        let initial = src.get("mock://path", None, None).await.unwrap();
        let wrapped = initial.with_retry(StreamingRetryParams::new(
            src.clone(),
            "mock://path".to_string(),
            None,
            None,
        ));

        let err = wrapped.bytes().await.err().unwrap();
        match err {
            Error::UnableToReadBytes { .. } | Error::SocketError { .. } => {}
            _ => panic!("expected stream read error"),
        }
        let requests = src.requests.lock().unwrap().clone();
        assert_eq!(
            requests,
            vec![
                None,
                Some(GetRange::Offset(0)),
                Some(GetRange::Offset(0)),
                Some(GetRange::Offset(0)),
            ]
        );
    }
}
