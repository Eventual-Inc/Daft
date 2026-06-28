use std::{collections::HashMap, ops::Range, sync::Arc};

use bytes::Bytes;
use common_io_config::HuggingFaceConfig;
use futures::{stream::BoxStream, StreamExt};
use reqwest_middleware::ClientWithMiddleware;
use serde::Deserialize;
use tokio::sync::Mutex;
use xet::xet_session::{
    HeaderMap, HeaderValue, XetDownloadStream, XetFileInfo, XetSession, XetSessionBuilder, header,
};

use crate::range::GetRange;

use super::{error::Error, path::{HFPath, HFPathParts}};

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

/// Parse `https://huggingface.co/.../resolve/...` URLs into [`HFPathParts`].
pub(super) fn parse_huggingface_https_url(url: &str) -> Option<HFPathParts> {
    let parsed = url::Url::parse(url).ok()?;
    if parsed.host_str()? != "huggingface.co" {
        return None;
    }
    let path = parsed.path().trim_start_matches('/');

    let (bucket, rest) = if let Some(rest) = path.strip_prefix("datasets/") {
        ("datasets", rest)
    } else if let Some(rest) = path.strip_prefix("spaces/") {
        ("spaces", rest)
    } else if let Some(rest) = path.strip_prefix("models/") {
        ("models", rest)
    } else if let Some(rest) = path.strip_prefix("buckets/") {
        ("buckets", rest)
    } else {
        ("models", path)
    };

    let resolve_marker = "/resolve/";
    let resolve_idx = rest.find(resolve_marker)?;
    let repository = rest[..resolve_idx].to_string();
    let after_resolve = &rest[resolve_idx + resolve_marker.len()..];
    let (revision, file_path) = after_resolve.split_once('/')?;
    if file_path.is_empty() {
        return None;
    }

    Some(HFPathParts {
        bucket: bucket.to_string(),
        repository,
        revision: revision.to_string(),
        path: file_path.to_string(),
    })
}

pub(super) fn hf_path_parts_from_uri(uri: &str) -> Result<Option<HFPathParts>, Error> {
    let path = uri.parse::<HFPath>()?;
    match path {
        HFPath::Hf(parts) if !parts.path.is_empty() => Ok(Some(parts)),
        HFPath::Http(url) => Ok(parse_huggingface_https_url(&url)),
        _ => Ok(None),
    }
}

fn repo_url_segment(parts: &HFPathParts) -> String {
    match parts.bucket.as_str() {
        "models" | "model" => parts.repository.clone(),
        _ => format!("{}/{}", parts.bucket, parts.repository),
    }
}

fn resolve_url_from_parts(parts: &HFPathParts) -> String {
    format!(
        "https://huggingface.co/{}/resolve/{}/{}",
        repo_url_segment(parts),
        parts.revision,
        parts.path,
    )
}

fn xet_read_token_url(parts: &HFPathParts) -> String {
    let repo_type = match parts.bucket.as_str() {
        "model" => "models",
        "dataset" => "datasets",
        "space" => "spaces",
        other => other,
    };
    if repo_type == "buckets" || repo_type == "bucket" {
        format!(
            "https://huggingface.co/api/buckets/{}/xet-read-token",
            parts.repository
        )
    } else {
        format!(
            "https://huggingface.co/api/{}/{}/xet-read-token/{}",
            repo_type, parts.repository, parts.revision
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
        let cache_key = resolve_url_from_parts(parts);
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
        let url = resolve_url_from_parts(parts);
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

        let info: XetFileResponse = response.json().await.map_err(|source| Error::UnableToReadBytes {
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

    pub(super) async fn download_stream(
        &self,
        parts: &HFPathParts,
        resolved: &XetResolvedFile,
        range: Option<GetRange>,
    ) -> Result<XetDownloadStream, Error> {
        let session = self.get_session().await?;
        let refresh_url = xet_read_token_url(parts);
        let refresh_headers = auth_headers(&self.hf_config);
        let xet_range = get_range_to_xet_range(range, resolved.file_size);

        let group = session
            .new_download_stream_group()
            .map_err(|source| Error::XetOperationFailed {
                path: refresh_url.clone(),
                message: source.to_string(),
            })?
            .with_token_refresh_url(refresh_url, refresh_headers)
            .build()
            .await
            .map_err(|source| Error::XetOperationFailed {
                path: parts.path.clone(),
                message: source.to_string(),
            })?;

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
    use super::*;

    #[test]
    fn test_parse_huggingface_https_resolve_url() {
        let url = "https://huggingface.co/datasets/google/FACTS-grounding-public/resolve/main/README.md";
        let parts = parse_huggingface_https_url(url).unwrap();
        assert_eq!(parts.bucket, "datasets");
        assert_eq!(parts.repository, "google/FACTS-grounding-public");
        assert_eq!(parts.revision, "main");
        assert_eq!(parts.path, "README.md");
    }

    #[test]
    fn test_parse_huggingface_https_model_resolve_url() {
        let url = "https://huggingface.co/Qwen/Qwen2.5-0.5B/resolve/main/config.json";
        let parts = parse_huggingface_https_url(url).unwrap();
        assert_eq!(parts.bucket, "models");
        assert_eq!(parts.repository, "Qwen/Qwen2.5-0.5B");
        assert_eq!(parts.revision, "main");
        assert_eq!(parts.path, "config.json");
    }

    #[test]
    fn test_xet_read_token_url_datasets() {
        let parts = HFPathParts {
            bucket: "datasets".to_string(),
            repository: "user/repo".to_string(),
            revision: "main".to_string(),
            path: "file.parquet".to_string(),
        };
        assert_eq!(
            xet_read_token_url(&parts),
            "https://huggingface.co/api/datasets/user/repo/xet-read-token/main"
        );
    }

    #[test]
    fn test_resolve_url_models_omits_prefix() {
        let parts = HFPathParts {
            bucket: "models".to_string(),
            repository: "Qwen/Qwen2.5".to_string(),
            revision: "main".to_string(),
            path: "config.json".to_string(),
        };
        assert_eq!(
            resolve_url_from_parts(&parts),
            "https://huggingface.co/Qwen/Qwen2.5/resolve/main/config.json"
        );
    }
}
