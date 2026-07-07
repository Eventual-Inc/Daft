mod error;
mod path;
mod xet;

use std::{any::Any, collections::HashMap, sync::Arc};

use async_recursion::async_recursion;
use async_trait::async_trait;
use common_io_config::{HTTPConfig, HuggingFaceConfig};
use futures::{
    StreamExt, TryStreamExt,
    stream::{self, BoxStream},
};
use reqwest::StatusCode;
use reqwest_middleware::{
    ClientWithMiddleware,
    reqwest::header::{CONTENT_LENGTH, RANGE},
};
use serde::{Deserialize, Serialize};
use snafu::{IntoError, ResultExt};
use xet::{XetContext, xet_download_stream_to_bytes_stream, xet_reads_enabled};

use super::object_io::{GetResult, ObjectSource};
use crate::{
    FileFormat, InvalidRangeRequestSnafu,
    http::HttpSource,
    huggingface::{
        error::Error,
        path::{HFPath, HFPathParts, HFRepoType, hf_path_parts_from_uri},
    },
    object_io::{FileMetadata, FileType, LSResult},
    range::GetRange,
    stats::IOStatsRef,
    stream_utils::io_stats_on_bytestream,
};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
enum ItemType {
    File,
    Directory,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
struct Item {
    r#type: ItemType,
    oid: String,
    size: u64,
    path: String,
}

pub struct HFSource {
    http_source: HttpSource,
    hf_config: HuggingFaceConfig,
    xet: XetContext,
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::{
            PrivateDataset, UnableToConnect, UnableToDetermineSize, UnableToOpenFile,
            UnableToReadBytes, Unauthorized, XetOperationFailed,
        };
        match error {
            UnableToOpenFile { path, source } => {
                // Preserve the existing 401 -> Unauthorized mapping, but
                // delegate the remaining classification to the shared
                // typed-error helpers in `http.rs` so 429 / 5xx /
                // connect+read timeouts surface as DaftTransientError
                // subclasses instead of opaque `External` strings.
                if source.status().map(|s| s.as_u16()) == Some(401) {
                    return Self::Unauthorized {
                        store: super::SourceType::HF,
                        path,
                        source: source.into(),
                    };
                }
                crate::http::classify_reqwest_error(path, source)
            }
            UnableToConnect { path, source } => {
                crate::http::classify_middleware_error(path, source)
            }
            UnableToReadBytes { path, source } => {
                if source.is_timeout() {
                    Self::ReadTimeout {
                        path,
                        source: source.into(),
                    }
                } else {
                    Self::SocketError {
                        path,
                        source: source.into(),
                    }
                }
            }
            UnableToDetermineSize { path } => Self::UnableToDetermineSize { path },
            Unauthorized => Self::Unauthorized {
                store: super::SourceType::HF,
                path: String::new(),
                source: error.into(),
            },
            PrivateDataset => Self::Unauthorized {
                store: super::SourceType::HF,
                path: String::new(),
                source: error.into(),
            },
            XetOperationFailed { path, message } => Self::Generic {
                store: super::SourceType::HF,
                source: format!("Xet error at {path}: {message}").into(),
            },
            _ => Self::Generic {
                store: super::SourceType::Http,
                source: error.into(),
            },
        }
    }
}

impl HFSource {
    pub async fn get_client(
        hf_config: &HuggingFaceConfig,
        http_config: &HTTPConfig,
    ) -> super::Result<Arc<Self>> {
        if http_config.bearer_token.is_some() {
            log::warn!(
                "Using `HttpConfig.bearer_token` to authenticate Hugging Face requests is deprecated and will be removed in Daft v0.6. Instead, specify your Hugging Face token in `daft.io.HuggingFaceConfig`."
            );
        }

        let mut combined_config = http_config.clone();
        if hf_config.anonymous {
            combined_config.bearer_token = None;
        } else if hf_config.token.is_some() {
            combined_config.bearer_token.clone_from(&hf_config.token);
        }

        let http_source = HttpSource::get_client(&combined_config).await?;
        let http_source = Arc::try_unwrap(http_source).expect("Could not unwrap Arc<HttpSource>");
        let client = http_source.client;
        Ok(Self {
            http_source: HttpSource { client },
            hf_config: hf_config.clone(),
            xet: XetContext::new(hf_config.clone()),
        }
        .into())
    }

    #[async_recursion]
    async fn request(
        &self,
        uri: &str,
        cache_bust: bool,
        range: Option<&GetRange>,
        client: &reqwest_middleware::ClientWithMiddleware,
    ) -> super::Result<(reqwest::Response, bool)> {
        let (use_range, range) = if cache_bust {
            if uri.contains("cachebust") {
                // Hugging Face is buggy with range requests. If we already attempted cache busting, try removing the range request.
                (false, None)
            } else {
                (range.is_some(), range)
            }
        } else {
            (range.is_some(), range)
        };
        let path = uri.parse::<HFPath>()?;
        let uri = &path.get_file_uri(cache_bust);
        let req = client.get(uri);
        let req = match &range {
            None => req,
            Some(range) => {
                range.validate().context(InvalidRangeRequestSnafu)?;
                req.header(RANGE, range.to_string())
            }
        };

        let response = req
            .send()
            .await
            .context(error::UnableToConnectSnafu::<String> { path: uri.clone() })?;

        match response.error_for_status() {
            Err(e) => {
                let status_code = e.status();
                match status_code {
                    // HTTP 416 (Range Not Satisfiable) occurs due to Hugging Face's buggy caching that incorrectly serves the data.
                    // Retry with cache busting to bypass the improperly cached response and get correct file metadata.
                    Some(StatusCode::RANGE_NOT_SATISFIABLE) => {
                        self.request(uri, true, range, client).await
                    }
                    _ => Err(e)
                        .context(error::UnableToOpenFileSnafu::<String> { path: uri.clone() })?,
                }
            }
            Ok(res) => {
                // Check if we got a 206 (Partial Content) response with zero content length.
                // This can happen due to Hugging Face's buggy caching. Retry with cache busting.
                if res.status() == StatusCode::PARTIAL_CONTENT && res.content_length() == Some(0) {
                    self.request(uri, true, range, client).await
                } else {
                    Ok((res, use_range))
                }
            }
        }
    }

    async fn get_via_http(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        let (response, range_applied) = self
            .request(uri, false, range.as_ref(), &self.http_source.client)
            .await?;

        let response = response.error_for_status().map_err(|e| {
            if e.status().map(|s| s.as_u16()) == Some(401) {
                Error::Unauthorized
            } else {
                Error::UnableToOpenFile {
                    path: uri.to_string(),
                    source: e,
                }
            }
        })?;

        if let Some(is) = io_stats.as_ref() {
            is.mark_get_requests(1);
        }
        let size_bytes = response.content_length().map(|s| s as usize);
        let stream = response.bytes_stream();
        let owned_string = uri.to_owned();
        let stream = stream.map_err(move |e| {
            error::UnableToReadBytesSnafu::<String> {
                path: owned_string.clone(),
            }
            .into_error(e)
            .into()
        });

        let result = GetResult::Stream(
            io_stats_on_bytestream(stream, io_stats.clone()),
            size_bytes,
            None,
            None,
        );

        if !range_applied && let Some(range) = range {
            // TODO: make this more efficient
            let bytes = result.bytes().await?;
            let len = bytes.len();
            let range = range.as_range(len).context(InvalidRangeRequestSnafu)?;
            let bytes_in_range = bytes.slice(range);

            let range_stream = Box::pin(futures::stream::once(async { Ok(bytes_in_range) }));

            Ok(GetResult::Stream(
                io_stats_on_bytestream(range_stream, io_stats),
                Some(len),
                None,
                None,
            ))
        } else {
            Ok(result)
        }
    }

    async fn get_via_xet(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> Result<Option<GetResult>, Error> {
        let Some(parts) = hf_path_parts_from_uri(uri)? else {
            return Ok(None);
        };

        let resolved = match self
            .xet
            .resolve_xet_file(&parts, &self.http_source.client)
            .await?
        {
            Some(resolved) => resolved,
            None => return Ok(None),
        };

        let size_hint = range
            .as_ref()
            .and_then(|r| match r {
                GetRange::Bounded(b) => Some(b.end - b.start),
                GetRange::Suffix(n) => Some(*n),
                GetRange::Offset(offset) => resolved
                    .file_size
                    .checked_sub(*offset as u64)
                    .map(|s| s as usize),
            })
            .or(Some(resolved.file_size as usize));

        let stream = self.xet.download_stream(&parts, &resolved, range).await?;

        if let Some(is) = io_stats.as_ref() {
            is.mark_get_requests(1);
        }

        let byte_stream = xet_download_stream_to_bytes_stream(stream, uri.to_string());
        Ok(Some(GetResult::Stream(
            io_stats_on_bytestream(byte_stream, io_stats),
            size_hint,
            None,
            None,
        )))
    }

    async fn get_size_via_http(
        &self,
        uri: &str,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<usize> {
        let path = uri.parse::<HFPath>()?;
        let uri = &path.get_file_uri(false);

        let request = self.http_source.client.head(uri);
        let response = request
            .send()
            .await
            .context(error::UnableToConnectSnafu::<String> { path: uri.into() })?;
        let response = response.error_for_status().map_err(|e| {
            if e.status().map(|s| s.as_u16()) == Some(401) {
                Error::Unauthorized
            } else {
                Error::UnableToOpenFile {
                    path: uri.clone(),
                    source: e,
                }
            }
        })?;

        if let Some(is) = io_stats.as_ref() {
            is.mark_head_requests(1);
        }

        let headers = response.headers();
        match headers.get(CONTENT_LENGTH) {
            Some(v) => {
                let size_bytes = String::from_utf8(v.as_bytes().to_vec()).with_context(|_| {
                    error::UnableToParseUtf8HeaderSnafu::<String> { path: uri.into() }
                })?;

                Ok(size_bytes
                    .parse()
                    .with_context(|_| error::UnableToParseIntegerSnafu::<String> {
                        path: uri.into(),
                    })?)
            }
            None => Err(Error::UnableToDetermineSize { path: uri.into() }.into()),
        }
    }
}

#[async_trait]
impl ObjectSource for HFSource {
    async fn supports_range(&self, uri: &str) -> super::Result<bool> {
        if xet_reads_enabled(&self.hf_config) && hf_path_parts_from_uri(uri)?.is_some() {
            return Ok(true);
        }
        let path = uri.parse::<HFPath>()?;
        let uri = path.get_file_uri(false);

        self.http_source.supports_range(&uri).await
    }

    async fn get(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        if xet_reads_enabled(&self.hf_config) {
            match self.get_via_xet(uri, range.clone(), io_stats.clone()).await {
                Ok(Some(result)) => return Ok(result),
                Ok(None) => {}
                Err(Error::Unauthorized) => return Err(Error::Unauthorized.into()),
                Err(e) => {
                    log::debug!("Xet read failed for {uri}, falling back to HTTP: {e}");
                }
            }
        }
        self.get_via_http(uri, range, io_stats).await
    }

    async fn put(
        &self,
        _uri: &str,
        _data: bytes::Bytes,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<()> {
        todo!("PUTs to HTTP URLs are not yet supported! Please file an issue.");
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        if xet_reads_enabled(&self.hf_config)
            && let Some(parts) = hf_path_parts_from_uri(uri)?
        {
            match self
                .xet
                .resolve_xet_file(&parts, &self.http_source.client)
                .await
            {
                Ok(Some(resolved)) => return Ok(resolved.file_size as usize),
                Ok(None) => {}
                Err(Error::Unauthorized) => return Err(Error::Unauthorized.into()),
                Err(e) => {
                    log::debug!("Xet size probe failed for {uri}, falling back to HTTP: {e}");
                }
            }
        }

        self.get_size_via_http(uri, io_stats).await
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        _fanout_limit: Option<usize>,
        _page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
        file_format: Option<FileFormat>,
    ) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>> {
        use crate::object_store_glob::glob;

        let path = glob_path.parse::<HFPath>()?;
        let glob_path = match &path {
            HFPath::Hf(parts) if parts.repo_type == HFRepoType::Buckets => parts.to_string(),
            _ => glob_path.to_string(),
        };

        // Ensure fanout_limit is None because HTTP ObjectSource does not support prefix listing
        let fanout_limit = None;
        let page_size = None;

        match path {
            HFPath::Http(_) => {
                glob(self, &glob_path, fanout_limit, page_size, limit, io_stats).await
            }
            HFPath::Hf(parts) => {
                // Huggingface has a special API for parquet files
                // So we'll try to use that API to get the parquet files
                // This allows us compatibility with datasets that are not natively uploaded as parquet, such as image datasets

                // We only want to use this api for datasets, not specific files
                // such as
                // hf://datasets/user/repo
                // but not
                // hf://datasets/user/repo/file.parquet
                if file_format == Some(FileFormat::Parquet) {
                    let res =
                        try_parquet_api(parts, limit, io_stats.clone(), &self.http_source.client)
                            .await;
                    match res {
                        Ok(Some(stream)) => return Ok(stream),
                        Err(e) => return Err(e.into()),
                        Ok(None) => {
                            // INTENTIONALLY EMPTY
                            // If we can't determine if the dataset is private, we'll fall back to the default globbing
                        }
                    }
                }

                glob(self, &glob_path, fanout_limit, page_size, limit, io_stats).await
            }
        }
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult> {
        if !posix {
            unimplemented!("Prefix-listing is not implemented for HTTP listing");
        }

        let hf_path = path.parse::<HFPath>()?;

        let HFPath::Hf(path_parts) = hf_path else {
            return self
                .http_source
                .ls(path, posix, continuation_token, page_size, io_stats)
                .await;
        };

        let api_uri = HFPath::Hf(path_parts.clone()).get_api_uri();

        let request = self.http_source.client.get(api_uri.clone());
        let response = request
            .send()
            .await
            .context(error::UnableToConnectSnafu::<String> {
                path: api_uri.clone(),
            })?;

        let response = response.error_for_status().map_err(|e| {
            if e.status().map(|s| s.as_u16()) == Some(401) {
                Error::Unauthorized
            } else {
                Error::UnableToOpenFile {
                    path: api_uri.clone(),
                    source: e,
                }
            }
        })?;

        if let Some(is) = io_stats.as_ref() {
            is.mark_list_requests(1);
        }
        let response =
            response
                .json::<Vec<Item>>()
                .await
                .context(error::UnableToReadBytesSnafu {
                    path: api_uri.clone(),
                })?;

        let files = response
            .into_iter()
            .map(|item| {
                let filepath = HFPathParts {
                    repo_type: path_parts.repo_type.clone(),
                    repository: path_parts.repository.clone(),
                    revision: path_parts.revision.clone(),
                    path: item.path,
                };

                let size = match item.size {
                    0 => None,
                    size => Some(size),
                };
                let filepath = filepath.to_string();

                let filetype = match item.r#type {
                    ItemType::File => FileType::File,
                    ItemType::Directory => FileType::Directory,
                };

                FileMetadata {
                    filepath,
                    size,
                    filetype,
                }
            })
            .collect();
        Ok(LSResult {
            files,
            continuation_token: None,
        })
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

async fn try_parquet_api(
    hf_glob_path: HFPathParts,
    limit: Option<usize>,
    io_stats: Option<IOStatsRef>,
    client: &ClientWithMiddleware,
) -> Result<Option<BoxStream<'static, super::Result<FileMetadata>>>, Error> {
    if hf_glob_path.path.is_empty() {
        let api_path = HFPath::Hf(hf_glob_path.clone()).get_parquet_api_uri();

        let response = client
            .get(api_path.clone())
            .send()
            .await
            .with_context(|_| error::UnableToConnectSnafu {
                path: api_path.clone(),
            })?;

        if response.status() == 400 {
            if let Some(error_message) = response
                .headers()
                .get("x-error-message")
                .and_then(|v| v.to_str().ok())
            {
                const PRIVATE_DATASET_ERROR: &str = "Private datasets are only supported for PRO users and Enterprise Hub organizations.";
                if error_message.ends_with(PRIVATE_DATASET_ERROR) {
                    return Err(Error::PrivateDataset);
                }
            } else {
                return Err(Error::Unauthorized);
            }
        }
        let response =
            response
                .error_for_status()
                .with_context(|_| error::UnableToOpenFileSnafu {
                    path: api_path.clone(),
                })?;

        if let Some(is) = io_stats.as_ref() {
            is.mark_list_requests(1);
        }

        // {<dataset_name>: {<split_name>: [<uri>, ...]}}
        type DatasetResponse = HashMap<String, HashMap<String, Vec<String>>>;
        let body =
            response
                .json::<DatasetResponse>()
                .await
                .context(error::UnableToReadBytesSnafu {
                    path: api_path.clone(),
                })?;

        let files = body
            .into_values()
            .flat_map(std::collections::HashMap::into_values)
            .flatten()
            .map(|uri| {
                Ok(FileMetadata {
                    filepath: uri,
                    size: None,
                    filetype: FileType::File,
                })
            });

        Ok(Some(
            stream::iter(files).take(limit.unwrap_or(16 * 1024)).boxed(),
        ))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use common_io_config::{HTTPConfig, HuggingFaceConfig};

    use crate::{huggingface::HFSource, object_io::ObjectSource};

    #[tokio::test]
    async fn test_full_get_from_hf() -> crate::Result<()> {
        let test_file_path = "hf://datasets/google/FACTS-grounding-public/README.md";
        let expected_md5 = "46df309e52cf88f458a4e3e2fb692fc1";

        let config = HuggingFaceConfig {
            use_xet: true,
            ..Default::default()
        };
        let client = HFSource::get_client(&config, &HTTPConfig::default()).await?;

        let parquet_file = client.get(test_file_path, None, None).await?;
        let bytes = parquet_file.bytes().await?;
        let all_bytes = bytes.as_ref();
        let checksum = format!("{:x}", md5::compute(all_bytes));
        assert_eq!(checksum, expected_md5);

        Ok(())
    }

    #[tokio::test]
    async fn test_full_get_from_hf_without_xet() -> crate::Result<()> {
        let test_file_path = "hf://datasets/google/FACTS-grounding-public/README.md";
        let expected_md5 = "46df309e52cf88f458a4e3e2fb692fc1";

        let config = HuggingFaceConfig {
            use_xet: false,
            ..Default::default()
        };
        let client = HFSource::get_client(&config, &HTTPConfig::default()).await?;

        let parquet_file = client.get(test_file_path, None, None).await?;
        let bytes = parquet_file.bytes().await?;
        let checksum = format!("{:x}", md5::compute(bytes.as_ref()));
        assert_eq!(checksum, expected_md5);

        Ok(())
    }
}
