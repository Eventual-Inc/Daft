use std::{collections::HashMap, ops::Range, sync::Arc};

use bytes::Bytes;
use common_io_config::HuggingFaceConfig;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use reqwest_middleware::ClientWithMiddleware;
use serde::Deserialize;
use tokio::sync::Mutex;
use xet::xet_session::{
    HeaderMap, HeaderValue, XetDownloadStream, XetDownloadStreamGroup, XetFileInfo, XetSession,
    XetSessionBuilder, header,
};

use super::{
    error::Error,
    path::{HFPathParts, HFRepoType},
};
use crate::range::GetRange;

const XET_FILEINFO_ACCEPT: &str = "application/vnd.xet-fileinfo+json";

#[derive(Clone, Debug)]
pub(super) struct XetResolvedFile {
    pub file_info: XetFileInfo,
    pub file_size: u64,
}

#[derive(Deserialize)]
struct XetFileResponse {
    hash: String,
    size: u64,
}

pub(super) fn xet_reads_enabled(hf_config: &HuggingFaceConfig) -> bool {
    hf_config.use_xet && std::env::var("HF_HUB_DISABLE_XET").ok().as_deref() != Some("1")
}

fn xet_read_token_url(parts: &HFPathParts) -> String {
    if parts.repo_type == HFRepoType::Buckets {
        format!(
            "https://huggingface.co/api/buckets/{}/xet-read-token",
            parts.repository
        )
    } else {
        format!(
            "https://huggingface.co/api/{}/{}/xet-read-token/{}",
            parts.repo_type, parts.repository, parts.revision
        )
    }
}

fn auth_headers(hf_config: &HuggingFaceConfig) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if !hf_config.anonymous
        && let Some(token) = hf_config.token.as_ref()
        && let Ok(val) = HeaderValue::from_str(&format!("Bearer {}", token.as_string()))
    {
        headers.insert(header::AUTHORIZATION, val);
    }
    headers
}

pub(super) struct XetContext {
    hf_config: HuggingFaceConfig,
    session: Mutex<Option<Arc<XetSession>>>,
    resolve_cache: Mutex<HashMap<String, Option<XetResolvedFile>>>,
}

impl XetContext {
    pub(super) fn new(hf_config: HuggingFaceConfig) -> Self {
        Self {
            hf_config,
            session: Mutex::new(None),
            resolve_cache: Mutex::new(HashMap::new()),
        }
    }

    pub(super) async fn resolve_xet_file(
        &self,
        parts: &HFPathParts,
        client: &ClientWithMiddleware,
    ) -> Result<Option<XetResolvedFile>, Error> {
        let cache_key = parts.resolve_url();
        if let Some(cached) = self.resolve_cache.lock().await.get(&cache_key) {
            return Ok(cached.clone());
        }

        let result = self.probe_xet_file(parts, client).await?;
        self.resolve_cache
            .lock()
            .await
            .insert(cache_key, result.clone());
        Ok(result)
    }

    async fn probe_xet_file(
        &self,
        parts: &HFPathParts,
        client: &ClientWithMiddleware,
    ) -> Result<Option<XetResolvedFile>, Error> {
        let url = parts.resolve_url();
        let response = client
            .get(&url)
            .header(reqwest::header::ACCEPT, XET_FILEINFO_ACCEPT)
            .send()
            .await
            .map_err(|source| Error::UnableToConnect {
                path: url.clone(),
                source,
            })?;

        if response.status().as_u16() == 401 {
            return Err(Error::Unauthorized);
        }

        if !response.status().is_success() {
            return Ok(None);
        }

        if !response.headers().contains_key("x-xet-hash") {
            return Ok(None);
        }

        let info: XetFileResponse =
            response
                .json()
                .await
                .map_err(|source| Error::UnableToReadBytes {
                    path: url.clone(),
                    source,
                })?;

        Ok(Some(XetResolvedFile {
            file_info: XetFileInfo::new(info.hash, info.size),
            file_size: info.size,
        }))
    }

    async fn get_session(&self) -> Result<Arc<XetSession>, Error> {
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_ref() {
            return Ok(session.clone());
        }
        let session = XetSessionBuilder::new()
            .with_tokio_handle(tokio::runtime::Handle::current())
            .build()
            .map_err(|source| Error::XetOperationFailed {
                path: "xet-session".to_string(),
                message: source.to_string(),
            })?;
        let session = Arc::new(session);
        *guard = Some(session.clone());
        Ok(session)
    }

    async fn get_download_group(&self, refresh_url: &str) -> Result<XetDownloadStreamGroup, Error> {
        let session = self.get_session().await?;
        let refresh_headers = auth_headers(&self.hf_config);
        session
            .new_download_stream_group()
            .map_err(|source| Error::XetOperationFailed {
                path: refresh_url.to_string(),
                message: source.to_string(),
            })?
            .with_token_refresh_url(refresh_url.to_string(), refresh_headers)
            .build()
            .boxed()
            .await
            .map_err(|source| Error::XetOperationFailed {
                path: refresh_url.to_string(),
                message: source.to_string(),
            })
    }

    pub(super) async fn download_stream(
        &self,
        parts: &HFPathParts,
        resolved: &XetResolvedFile,
        range: Option<GetRange>,
    ) -> Result<XetDownloadStream, Error> {
        let group = self.get_download_group(&xet_read_token_url(parts)).await?;
        let xet_range = get_range_to_xet_range(range, resolved.file_size);

        let mut stream = group
            .download_stream(resolved.file_info.clone(), xet_range)
            .await
            .map_err(|source| Error::XetOperationFailed {
                path: parts.path.clone(),
                message: source.to_string(),
            })?;
        stream.start();
        Ok(stream)
    }
}

fn get_range_to_xet_range(range: Option<GetRange>, file_size: u64) -> Option<Range<u64>> {
    match range {
        None => None,
        Some(GetRange::Bounded(r)) => Some(r.start as u64..r.end as u64),
        Some(GetRange::Offset(offset)) => Some(offset as u64..file_size),
        Some(GetRange::Suffix(n)) => {
            let start = file_size.saturating_sub(n as u64);
            Some(start..file_size)
        }
    }
}

pub(super) fn xet_download_stream_to_bytes_stream(
    mut stream: XetDownloadStream,
    path: String,
) -> BoxStream<'static, crate::Result<Bytes>> {
    async_stream::stream! {
        loop {
            match stream.next().await {
                Ok(Some(bytes)) => yield Ok(bytes),
                Ok(None) => break,
                Err(source) => {
                    yield Err(Error::XetOperationFailed {
                        path: path.clone(),
                        message: source.to_string(),
                    }.into());
                    break;
                }
            }
        }
    }
    .boxed()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::huggingface::path::HFRepoType;

    // Exercise the real Xet group builder without Hugging Face credentials or CAS traffic.
    struct TokenServer {
        url: String,
        requests: Arc<AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }

    impl TokenServer {
        async fn start(fail_first: bool) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let body =
                format!(r#"{{"accessToken":"test-token","exp":9999999999,"casUrl":"{url}/cas"}}"#);
            let requests = Arc::new(AtomicUsize::new(0));
            let count = requests.clone();
            let task = tokio::spawn(async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let mut buf = [0; 1024];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        let n = socket.read(&mut buf).await.unwrap();
                        assert!(n > 0);
                        request.extend_from_slice(&buf[..n]);
                    }
                    let n = count.fetch_add(1, Ordering::SeqCst);
                    let status = if fail_first && n == 0 {
                        "401 Unauthorized"
                    } else {
                        "200 OK"
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                }
            });
            Self {
                url,
                requests,
                task,
            }
        }

        fn requests(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }
    }

    impl Drop for TokenServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[tokio::test]
    async fn test_download_group_reuses_token_request() {
        let server = TokenServer::start(false).await;
        let context = XetContext::new(HuggingFaceConfig::default());
        futures::future::try_join_all((0..8).map(|_| context.get_download_group(&server.url)))
            .await
            .unwrap();
        context.get_download_group(&server.url).await.unwrap();
        assert_eq!(server.requests(), 1, "one token request per auth scope");
    }

    #[test]
    fn test_xet_read_token_url_datasets() {
        let parts = HFPathParts {
            repo_type: HFRepoType::Datasets,
            repository: "user/repo".to_string(),
            revision: "main".to_string(),
            path: "file.parquet".to_string(),
        };
        assert_eq!(
            xet_read_token_url(&parts),
            "https://huggingface.co/api/datasets/user/repo/xet-read-token/main"
        );
    }
}
