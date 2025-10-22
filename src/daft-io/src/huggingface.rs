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
    object_io::{FileMetadata, FileType, LSResult},
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
    /// `hf:// [datasets | spaces] / {username} / {reponame} @ {revision} / {path from root}`
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
            // {reponame} @ {revision} / {path from root}
            // ^--------^   !>
            let (repository, uri) = if let Some((repo, uri)) = uri.split_once('/') {
                (repo, uri)
            } else {
                return Some(Self {
                    bucket: bucket.to_string(),
                    repository: format!("{username}/{uri}"),
                    revision: "main".to_string(),
                    path: String::new(),
                });
            };

            // {revision} / {path from root}
            // ^--------^   !>
            let (repository, revision) = if let Some((repo, rev)) = repository.split_once('@') {
                (repo, rev.to_string())
            } else {
                (repository, "main".to_string())
            };

            // {username}/{reponame}
            let repository = format!("{username}/{repository}");
            // {path from root}
            // ^--------------^
            let path = uri.to_string().trim_end_matches('/').to_string();

            Some(Self {
                bucket: bucket.to_string(),
                repository,
                revision,
                path,
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
            Self::Hf(parts) => {
                format!(
                    "https://huggingface.co/{BUCKET}/{REPOSITORY}/resolve/{REVISION}/{PATH}",
                    BUCKET = parts.bucket,
                    REPOSITORY = parts.repository,
                    REVISION = parts.revision,
                    PATH = parts.path,
                )
            }
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
            Self::Hf(parts) => {
                // "https://huggingface.co/api/ [datasets] / {username} / {reponame} / tree / {revision} / {path from root}"
                format!(
                    "https://huggingface.co/api/{BUCKET}/{REPOSITORY}/tree/{REVISION}/{PATH}",
                    BUCKET = parts.bucket,
                    REPOSITORY = parts.repository,
                    REVISION = parts.revision,
                    PATH = parts.path,
                )
            }
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
        Ok(Self {
            http_source: HttpSource { client },
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
        _uri: &str,
        _data: bytes::Bytes,
        _io_stats: Option<IOStatsRef>,
    ) -> super::Result<()> {
        todo!("PUTs to HTTP URLs are not yet supported! Please file an issue.");
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize> {
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
    use common_error::DaftResult;
    use common_io_config::{HTTPConfig, HuggingFaceConfig};

    use crate::{
        huggingface::{HFPathParts, HFSource},
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

        test_full_get(client, &test_file_path, &bytes).await
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
}
