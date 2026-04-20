use std::{
    any::Any, collections::HashMap, num::ParseIntError, str::FromStr, string::FromUtf8Error,
    sync::Arc,
};

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
use snafu::{IntoError, ResultExt, Snafu};
use uuid::Uuid;

use super::object_io::{GetResult, ObjectSource};
use crate::{
    FileFormat, InvalidRangeRequestSnafu,
    http::HttpSource,
    multipart::MultipartWriter,
    object_io::{FileMetadata, FileType, LSResult},
    opendal_source::OpenDALSource,
    range::GetRange,
    stats::IOStatsRef,
    stream_utils::io_stats_on_bytestream,
};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to connect to {}: {}", path, source))]
    UnableToConnect {
        path: String,
        source: reqwest_middleware::Error,
    },

    #[snafu(display("Unable to open {}: {}", path, source))]
    UnableToOpenFile {
        path: String,
        source: reqwest_middleware::reqwest::Error,
    },

    #[snafu(display("Unable to determine size of {}", path))]
    UnableToDetermineSize { path: String },

    #[snafu(display("Unable to read data from {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: reqwest_middleware::reqwest::Error,
    },

    #[snafu(display(
        "Unable to parse data as Utf8 while reading header for file: {path}. {source}"
    ))]
    UnableToParseUtf8Header { path: String, source: FromUtf8Error },

    #[snafu(display(
        "Unable to parse data as Integer while reading header for file: {path}. {source}"
    ))]
    UnableToParseInteger { path: String, source: ParseIntError },

    #[snafu(display("Invalid path: {}", path))]
    InvalidPath { path: String },

    #[snafu(display(r"
Implicit Parquet conversion not supported for private datasets.
You can use glob patterns, or request a specific file to access your dataset instead.
Example:
    instead of `hf://datasets/username/dataset_name`, use `hf://datasets/username/dataset_name/file_name.parquet`
    or `hf://datasets/username/dataset_name/*.parquet
"))]
    PrivateDataset,
    #[snafu(display("Unauthorized access to dataset, please check your credentials."))]
    Unauthorized,
}

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

enum HFPath {
    Http(String),
    Hf(HFPathParts),
}

#[derive(Clone, Debug, PartialEq)]
struct HFPathParts {
    bucket: String,
    repository: String,
    revision: String,
    path: String,
}

impl HFPathParts {
    fn is_storage_bucket(&self) -> bool {
        self.bucket == "buckets"
    }

    fn is_dataset(&self) -> bool {
        matches!(self.bucket.as_str(), "dataset" | "datasets")
    }

    fn opendal_uri(&self) -> String {
        if self.path.is_empty() {
            "huggingface://repo/".to_string()
        } else {
            format!("huggingface://repo/{}", self.path)
        }
    }

    fn from_opendal_uri(&self, uri: &str) -> super::Result<String> {
        let parsed = url::Url::parse(uri).context(super::InvalidUrlSnafu { path: uri })?;
        let path = parsed.path().trim_start_matches('/');

        if path.is_empty() {
            Ok(format!("hf://buckets/{}", self.repository))
        } else {
            Ok(format!("hf://buckets/{}/{}", self.repository, path))
        }
    }
}

fn decode_hf_revision_component(revision: &str) -> String {
    revision.replace("%2F", "/").replace("%2f", "/")
}

fn parse_hf_revision_and_path(revision_and_path: &str) -> (String, String) {
    if let Some(rest) = revision_and_path.strip_prefix("refs/convert/") {
        if let Some((revision_tail, path)) = rest.split_once('/') {
            return (format!("refs/convert/{revision_tail}"), path.to_string());
        }
        return (revision_and_path.to_string(), String::new());
    }

    if let Some(rest) = revision_and_path.strip_prefix("refs/pr/") {
        if let Some((revision_tail, path)) = rest.split_once('/') {
            return (format!("refs/pr/{revision_tail}"), path.to_string());
        }
        return (revision_and_path.to_string(), String::new());
    }

    if let Some((revision, path)) = revision_and_path.split_once('/') {
        (revision.to_string(), path.to_string())
    } else {
        (revision_and_path.to_string(), String::new())
    }
}

fn encode_hf_path_component(component: &str) -> String {
    if component.is_empty() {
        return String::new();
    }

    let mut url = url::Url::parse("https://huggingface.co").expect("hardcoded URL should be valid");
    {
        let mut segments = url
            .path_segments_mut()
            .expect("hardcoded URL should allow path mutation");
        segments.push(component);
    }
    url.path().trim_start_matches('/').to_string()
}

fn encode_hf_path(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }

    path.split('/')
        .map(encode_hf_path_component)
        .collect::<Vec<_>>()
        .join("/")
}

impl FromStr for HFPath {
    type Err = Error;

    fn from_str(uri: &str) -> Result<Self, Self::Err> {
        if uri.starts_with("http://") || uri.starts_with("https://") {
            Ok(Self::Http(uri.to_string()))
        } else {
            uri.parse().map(Self::Hf)
        }
    }
}

impl FromStr for HFPathParts {
    type Err = Error;
    /// Extracts path components from a hugging face path:
    /// `hf:// [datasets | spaces | buckets] / {username} / {reponame} @ {revision} / {path from root}`
    fn from_str(uri: &str) -> Result<Self, Self::Err> {
        // hf:// [datasets] / {username} / {reponame} @ {revision} / {path from root}
        //       !>
        if !uri.starts_with("hf://") {
            return Err(Error::InvalidPath {
                path: uri.to_string(),
            });
        }
        (|| {
            let uri = &uri[5..];

            // [datasets] / {username} / {reponame} @ {revision} / {path from root}
            // ^--------^   !>
            let (bucket, uri) = uri.split_once('/')?;
            // {username} / {reponame} @ {revision} / {path from root}
            // ^--------^   !>
            let (username, uri) = uri.split_once('/')?;

            let (repository_segment, remaining_path) =
                if let Some((repository, path)) = uri.split_once('/') {
                    (repository, Some(path))
                } else {
                    (uri, None)
                };

            let (repository, revision, path) =
                if let Some((repository, revision_prefix)) = repository_segment.split_once('@') {
                    let revision_and_path = match remaining_path {
                        Some(path) if !path.is_empty() => {
                            decode_hf_revision_component(&format!("{revision_prefix}/{path}"))
                        }
                        _ => decode_hf_revision_component(revision_prefix),
                    };
                    let (revision, path) = parse_hf_revision_and_path(&revision_and_path);
                    (repository.to_string(), revision, path)
                } else {
                    (
                        repository_segment.to_string(),
                        "main".to_string(),
                        remaining_path
                            .unwrap_or("")
                            .trim_end_matches('/')
                            .to_string(),
                    )
                };

            Some(Self {
                bucket: bucket.to_string(),
                repository: format!("{username}/{repository}"),
                revision,
                path: path.trim_end_matches('/').to_string(),
            })
        })()
        .ok_or_else(|| Error::InvalidPath {
            path: uri.to_string(),
        })
    }
}

impl std::fmt::Display for HFPathParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "hf://{BUCKET}/{REPOSITORY}/{PATH}",
            BUCKET = self.bucket,
            REPOSITORY = self.repository,
            PATH = self.path
        )
    }
}

impl HFPath {
    // There is a bug within huggingface apis that is incorrectly caching files
    // https://github.com/huggingface/datasets/issues/7685
    //
    // So to bypass this, we add a unique parameter to the url to prevent CDN caching.
    fn get_file_uri(&self, cache_bust: bool) -> String {
        let base = match self {
            Self::Http(base) => base.clone(),
            Self::Hf(parts) => match parts.is_storage_bucket() {
                true => format!(
                    "https://huggingface.co/buckets/{REPOSITORY}/resolve/{PATH}",
                    REPOSITORY = parts.repository,
                    PATH = encode_hf_path(&parts.path),
                ),
                false => format!(
                    "https://huggingface.co/{BUCKET}/{REPOSITORY}/resolve/{REVISION}/{PATH}",
                    BUCKET = parts.bucket,
                    REPOSITORY = parts.repository,
                    REVISION = encode_hf_path_component(&parts.revision),
                    PATH = encode_hf_path(&parts.path),
                ),
            },
        };
        if cache_bust {
            let cachebuster = Uuid::new_v4();
            let cachebuster = cachebuster.to_string();
            if base.contains('?') {
                format!("{base}&cachebust={cachebuster}")
            } else {
                format!("{base}?cachebust={cachebuster}")
            }
        } else {
            base
        }
    }

    fn get_api_uri(&self) -> String {
        match self {
            Self::Http(path) => path.clone(),
            Self::Hf(parts) => match parts.is_storage_bucket() {
                true => format!(
                    "https://huggingface.co/api/buckets/{REPOSITORY}/tree/{PATH}?expand=True&recursive=false",
                    REPOSITORY = parts.repository,
                    PATH = encode_hf_path(&parts.path),
                ),
                false => {
                    // "https://huggingface.co/api/ [datasets] / {username} / {reponame} / tree / {revision} / {path from root}"
                    format!(
                        "https://huggingface.co/api/{BUCKET}/{REPOSITORY}/tree/{REVISION}/{PATH}",
                        BUCKET = parts.bucket,
                        REPOSITORY = parts.repository,
                        REVISION = encode_hf_path_component(&parts.revision),
                        PATH = encode_hf_path(&parts.path),
                    )
                }
            },
        }
    }

    fn get_parquet_api_uri(&self) -> String {
        match self {
            Self::Http(path) => path.clone(),
            Self::Hf(parts) => format!(
                "https://huggingface.co/api/{BUCKET}/{REPOSITORY}/parquet",
                BUCKET = parts.bucket,
                REPOSITORY = parts.repository,
            ),
        }
    }
}

pub struct HFSource {
    http_source: HttpSource,
    hf_config: HuggingFaceConfig,
    bucket_sources: tokio::sync::RwLock<HashMap<String, Arc<dyn ObjectSource>>>,
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::{UnableToDetermineSize, UnableToOpenFile};
        match error {
            UnableToOpenFile { path, source } => match source.status().map(|v| v.as_u16()) {
                Some(404 | 410) => Self::NotFound {
                    path,
                    source: source.into(),
                },
                None | Some(_) => Self::UnableToOpenFile {
                    path,
                    source: source.into(),
                },
            },
            UnableToDetermineSize { path } => Self::UnableToDetermineSize { path },
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
        let hf_config = if hf_config.token.is_none() && !hf_config.anonymous {
            HuggingFaceConfig {
                token: combined_config.bearer_token.clone(),
                ..hf_config.clone()
            }
        } else {
            hf_config.clone()
        };

        Ok(Self {
            http_source: HttpSource { client },
            hf_config,
            bucket_sources: tokio::sync::RwLock::new(HashMap::new()),
        }
        .into())
    }

    async fn bucket_source(
        &self,
        path_parts: &HFPathParts,
    ) -> super::Result<Option<Arc<dyn ObjectSource>>> {
        if !path_parts.is_storage_bucket() {
            return Ok(None);
        }

        let source_key = path_parts.repository.clone();

        if let Some(source) = self.bucket_sources.read().await.get(&source_key) {
            return Ok(Some(source.clone()));
        }

        let mut write_handle = self.bucket_sources.write().await;
        if let Some(source) = write_handle.get(&source_key) {
            return Ok(Some(source.clone()));
        }

        let config = self
            .hf_config
            .to_opendal_config("bucket", &path_parts.repository, None);
        let source =
            OpenDALSource::get_client("huggingface", &config).await? as Arc<dyn ObjectSource>;
        write_handle.insert(source_key, source.clone());
        Ok(Some(source))
    }

    async fn bucket_source_and_uri(
        &self,
        uri: &str,
    ) -> super::Result<Option<(Arc<dyn ObjectSource>, String)>> {
        let HFPath::Hf(path_parts) = uri.parse::<HFPath>()? else {
            return Ok(None);
        };

        let Some(source) = self.bucket_source(&path_parts).await? else {
            return Ok(None);
        };

        Ok(Some((source, path_parts.opendal_uri())))
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
            .context(UnableToConnectSnafu::<String> { path: uri.clone() })?;

        match response.error_for_status() {
            Err(e) => {
                let status_code = e.status();
                match status_code {
                    // HTTP 416 (Range Not Satisfiable) occurs due to Hugging Face's buggy caching that incorrectly serves the data.
                    // Retry with cache busting to bypass the improperly cached response and get correct file metadata.
                    Some(StatusCode::RANGE_NOT_SATISFIABLE) => {
                        self.request(uri, true, range, client).await
                    }
                    _ => Err(e).context(UnableToOpenFileSnafu::<String> { path: uri.clone() })?,
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
}

#[async_trait]
impl ObjectSource for HFSource {
    async fn supports_range(&self, uri: &str) -> super::Result<bool> {
        if let Some((source, bucket_uri)) = self.bucket_source_and_uri(uri).await? {
            return source.supports_range(&bucket_uri).await;
        }

        let path = uri.parse::<HFPath>()?;
        let uri = path.get_file_uri(false);

        self.http_source.supports_range(&uri).await
    }

    async fn create_multipart_writer(
        self: Arc<Self>,
        uri: &str,
    ) -> super::Result<Option<Box<dyn MultipartWriter>>> {
        let Some((source, bucket_uri)) = self.bucket_source_and_uri(uri).await? else {
            return Ok(None);
        };

        source.create_multipart_writer(&bucket_uri).await
    }

    async fn get(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        if let Some((source, bucket_uri)) = self.bucket_source_and_uri(uri).await? {
            return source.get(&bucket_uri, range, io_stats).await;
        }

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
            UnableToReadBytesSnafu::<String> {
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

    async fn put(
        &self,
        uri: &str,
        data: bytes::Bytes,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<()> {
        if let Some((source, bucket_uri)) = self.bucket_source_and_uri(uri).await? {
            return source.put(&bucket_uri, data, io_stats).await;
        }

        todo!("PUTs to HTTP URLs are not yet supported! Please file an issue.");
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        if let Some((source, bucket_uri)) = self.bucket_source_and_uri(uri).await? {
            return source.get_size(&bucket_uri, io_stats).await;
        }

        let path = uri.parse::<HFPath>()?;
        let uri = &path.get_file_uri(false);

        let request = self.http_source.client.head(uri);
        let response = request
            .send()
            .await
            .context(UnableToConnectSnafu::<String> { path: uri.into() })?;
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
                    UnableToParseUtf8HeaderSnafu::<String> { path: uri.into() }
                })?;

                Ok(size_bytes
                    .parse()
                    .with_context(|_| UnableToParseIntegerSnafu::<String> { path: uri.into() })?)
            }
            None => Err(Error::UnableToDetermineSize { path: uri.into() }.into()),
        }
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

        if let Some((source, bucket_uri)) = self.bucket_source_and_uri(glob_path).await? {
            let HFPath::Hf(path_parts) = glob_path.parse::<HFPath>()? else {
                unreachable!("bucket_source_and_uri only returns Some for Hugging Face paths");
            };

            return source
                .glob(
                    &bucket_uri,
                    _fanout_limit,
                    _page_size,
                    limit,
                    io_stats,
                    file_format,
                )
                .await
                .map(|stream| {
                    stream
                        .map(move |result| {
                            result.and_then(|mut file| {
                                file.filepath = path_parts.from_opendal_uri(&file.filepath)?;
                                Ok(file)
                            })
                        })
                        .boxed()
                });
        }

        let path = glob_path.parse::<HFPath>()?;

        // Ensure fanout_limit is None because HTTP ObjectSource does not support prefix listing
        let fanout_limit = None;
        let page_size = None;

        match path {
            HFPath::Http(_) => {
                glob(self, glob_path, fanout_limit, page_size, limit, io_stats).await
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
                if file_format == Some(FileFormat::Parquet) && parts.is_dataset() {
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

                glob(self, glob_path, fanout_limit, page_size, limit, io_stats).await
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

        if let Some((source, bucket_uri)) = self.bucket_source_and_uri(path).await? {
            let HFPath::Hf(path_parts) = path.parse::<HFPath>()? else {
                unreachable!("bucket_source_and_uri only returns Some for Hugging Face paths");
            };
            let mut result = source
                .ls(&bucket_uri, posix, continuation_token, page_size, io_stats)
                .await?;
            for file in &mut result.files {
                file.filepath = path_parts.from_opendal_uri(&file.filepath)?;
            }
            return Ok(result);
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
            .context(UnableToConnectSnafu::<String> {
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
        let response = response
            .json::<Vec<Item>>()
            .await
            .context(UnableToReadBytesSnafu {
                path: api_uri.clone(),
            })?;

        let files = response
            .into_iter()
            .map(|item| {
                let filepath = HFPathParts {
                    bucket: path_parts.bucket.clone(),
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

    async fn delete(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<()> {
        let Some((source, bucket_uri)) = self.bucket_source_and_uri(uri).await? else {
            return Err(super::Error::NotImplementedMethod {
                method: "Deletes is not yet supported for non-bucket Hugging Face paths."
                    .to_string(),
            });
        };

        source.delete(&bucket_uri, io_stats).await
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
            .with_context(|_| UnableToConnectSnafu {
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
        let response = response
            .error_for_status()
            .with_context(|_| UnableToOpenFileSnafu {
                path: api_path.clone(),
            })?;

        if let Some(is) = io_stats.as_ref() {
            is.mark_list_requests(1);
        }

        // {<dataset_name>: {<split_name>: [<uri>, ...]}}
        type DatasetResponse = HashMap<String, HashMap<String, Vec<String>>>;
        let body = response
            .json::<DatasetResponse>()
            .await
            .context(UnableToReadBytesSnafu {
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
    use std::env;

    use bytes::Bytes;
    use common_error::DaftResult;
    use common_io_config::{HTTPConfig, HuggingFaceConfig, ObfuscatedString};
    use uuid::Uuid;

    use crate::{
        huggingface::{HFPath, HFPathParts, HFSource},
        integrations::test_full_get,
        object_io::ObjectSource,
    };

    #[tokio::test]
    async fn test_full_get_from_hf() -> crate::Result<()> {
        let test_file_path = "hf://datasets/google/FACTS-grounding-public/README.md";
        let expected_md5 = "46df309e52cf88f458a4e3e2fb692fc1";

        let client =
            HFSource::get_client(&HuggingFaceConfig::default(), &HTTPConfig::default()).await?;
        let parquet_file = client.get(test_file_path, None, None).await?;
        let bytes = parquet_file.bytes().await?;
        let all_bytes = bytes.as_ref();
        let checksum = format!("{:x}", md5::compute(all_bytes));
        assert_eq!(checksum, expected_md5);

        test_full_get(client, test_file_path, &bytes).await
    }

    #[test]
    fn test_parse_hf_parts() -> DaftResult<()> {
        let uri = "hf://datasets/wikimedia/wikipedia/20231101.ab/*.parquet";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            bucket: "datasets".to_string(),
            repository: "wikimedia/wikipedia".to_string(),
            revision: "main".to_string(),
            path: "20231101.ab/*.parquet".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }

    #[test]
    fn test_parse_hf_parts_with_revision() -> DaftResult<()> {
        let uri = "hf://datasets/wikimedia/wikipedia@dev/20231101.ab/*.parquet";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            bucket: "datasets".to_string(),
            repository: "wikimedia/wikipedia".to_string(),
            revision: "dev".to_string(),
            path: "20231101.ab/*.parquet".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }

    #[test]
    fn test_parse_hf_parts_with_exact_path() -> DaftResult<()> {
        let uri = "hf://datasets/user/repo@dev/config/my_file.parquet";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            bucket: "datasets".to_string(),
            repository: "user/repo".to_string(),
            revision: "dev".to_string(),
            path: "config/my_file.parquet".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }

    #[test]
    fn test_parse_hf_parts_with_wildcard() -> DaftResult<()> {
        let uri = "hf://datasets/wikimedia/wikipedia/**/*.parquet";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            bucket: "datasets".to_string(),
            repository: "wikimedia/wikipedia".to_string(),
            revision: "main".to_string(),
            path: "**/*.parquet".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }

    #[test]
    fn test_parse_hf_parts_with_refs_convert_revision() -> DaftResult<()> {
        let uri = "hf://datasets/user/repo@refs/convert/parquet/data/file.parquet";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            bucket: "datasets".to_string(),
            repository: "user/repo".to_string(),
            revision: "refs/convert/parquet".to_string(),
            path: "data/file.parquet".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }

    #[test]
    fn test_parse_hf_parts_with_encoded_refs_pr_revision() -> DaftResult<()> {
        let uri = "hf://datasets/user/repo@refs%2Fpr%2F10/config.json";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            bucket: "datasets".to_string(),
            repository: "user/repo".to_string(),
            revision: "refs/pr/10".to_string(),
            path: "config.json".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }

    #[test]
    fn test_parse_hf_parts_with_at_in_path() -> DaftResult<()> {
        let uri = "hf://datasets/user/repo/path/to/@not-a-revision.txt";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            bucket: "datasets".to_string(),
            repository: "user/repo".to_string(),
            revision: "main".to_string(),
            path: "path/to/@not-a-revision.txt".to_string(),
        };

        assert_eq!(parts, expected);

        Ok(())
    }

    #[test]
    fn test_parse_bucket_hf_parts() -> DaftResult<()> {
        let uri = "hf://buckets/user/bucket/path/to/file.parquet";
        let parts = uri.parse::<HFPathParts>().unwrap();
        let expected = HFPathParts {
            bucket: "buckets".to_string(),
            repository: "user/bucket".to_string(),
            revision: "main".to_string(),
            path: "path/to/file.parquet".to_string(),
        };

        assert_eq!(parts, expected);
        assert!(parts.is_storage_bucket());
        assert_eq!(
            parts.opendal_uri(),
            "huggingface://repo/path/to/file.parquet"
        );
        assert_eq!(
            parts.from_opendal_uri("huggingface://repo/path/to/file.parquet")?,
            "hf://buckets/user/bucket/path/to/file.parquet"
        );

        Ok(())
    }

    #[test]
    fn test_get_hf_bucket_uris() {
        let parts = HFPathParts {
            bucket: "buckets".to_string(),
            repository: "user/bucket".to_string(),
            revision: "main".to_string(),
            path: "data/my file.parquet".to_string(),
        };
        let path = HFPath::Hf(parts);

        assert_eq!(
            path.get_file_uri(false),
            "https://huggingface.co/buckets/user/bucket/resolve/data/my%20file.parquet"
        );
        assert_eq!(
            path.get_api_uri(),
            "https://huggingface.co/api/buckets/user/bucket/tree/data/my%20file.parquet?expand=True&recursive=false"
        );
    }

    #[test]
    fn test_get_hf_dataset_uris_encode_ref_revisions() {
        let parts = HFPathParts {
            bucket: "datasets".to_string(),
            repository: "user/repo".to_string(),
            revision: "refs/convert/parquet".to_string(),
            path: "data/my file.parquet".to_string(),
        };
        let path = HFPath::Hf(parts);

        assert_eq!(
            path.get_file_uri(false),
            "https://huggingface.co/datasets/user/repo/resolve/refs%2Fconvert%2Fparquet/data/my%20file.parquet"
        );
        assert_eq!(
            path.get_api_uri(),
            "https://huggingface.co/api/datasets/user/repo/tree/refs%2Fconvert%2Fparquet/data/my%20file.parquet"
        );
    }

    fn setup_online_bucket_test_config() -> Option<(HuggingFaceConfig, String)> {
        let token = env::var("HF_TOKEN").ok()?;
        let bucket = env::var("HF_BUCKET").ok()?;

        Some((
            HuggingFaceConfig {
                token: Some(ObfuscatedString::from(token)),
                anonymous: false,
                ..Default::default()
            },
            bucket,
        ))
    }

    fn generate_test_object_prefix() -> String {
        format!("daft-io-tests/{}", Uuid::new_v4())
    }

    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_full_get_from_xet_backed_hf_dataset() -> crate::Result<()> {
        const PARQUET_MAGIC: &[u8] = b"PAR1";

        let test_file_path =
            "hf://datasets/google-research-datasets/mbpp/full/train-00000-of-00001.parquet";
        let client =
            HFSource::get_client(&HuggingFaceConfig::default(), &HTTPConfig::default()).await?;
        let parquet_file = client.get(test_file_path, None, None).await?;
        let bytes = parquet_file.bytes().await?;

        assert!(bytes.len() > 8);
        assert_eq!(&bytes[..4], PARQUET_MAGIC);
        assert_eq!(&bytes[bytes.len() - 4..], PARQUET_MAGIC);

        test_full_get(client, test_file_path, &bytes).await
    }

    #[tokio::test]
    #[ignore]
    async fn test_hf_bucket_put_get_roundtrip() -> crate::Result<()> {
        let (cfg, bucket) = match setup_online_bucket_test_config() {
            Some(c) => c,
            None => return Ok(()),
        };

        let uri = format!(
            "hf://buckets/{}/{}/hello.txt",
            bucket,
            generate_test_object_prefix()
        );
        let client = HFSource::get_client(&cfg, &HTTPConfig::default()).await?;

        let data = Bytes::from_static(b"hello huggingface bucket");
        client.put(&uri, data.clone(), None).await?;

        let bytes = client.get(&uri, None, None).await?.bytes().await?;
        assert_eq!(bytes, data);

        client.delete(&uri, None).await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_hf_bucket_multipart_and_ls_roundtrip() -> crate::Result<()> {
        let (cfg, bucket) = match setup_online_bucket_test_config() {
            Some(c) => c,
            None => return Ok(()),
        };

        let prefix = generate_test_object_prefix();
        let dir_uri = format!("hf://buckets/{}/{}/", bucket, prefix);
        let file_uri = format!("{dir_uri}multipart.txt");
        let client = HFSource::get_client(&cfg, &HTTPConfig::default()).await?;

        let mut writer = client
            .clone()
            .create_multipart_writer(&file_uri)
            .await?
            .expect("bucket multipart writer should be available");
        writer.put_part(Bytes::from_static(b"multipart ")).await?;
        writer.put_part(Bytes::from_static(b"roundtrip")).await?;
        writer.complete().await?;

        let bytes = client.get(&file_uri, None, None).await?.bytes().await?;
        assert_eq!(bytes, Bytes::from_static(b"multipart roundtrip"));

        let listing = client.ls(&dir_uri, true, None, None, None).await?;
        assert!(listing.files.iter().any(|file| file.filepath == file_uri));

        client.delete(&file_uri, None).await?;
        Ok(())
    }
}
