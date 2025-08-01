use std::{
    any::Any, collections::HashMap, num::ParseIntError, str::FromStr, string::FromUtf8Error,
    sync::Arc,
};

use async_trait::async_trait;
use common_io_config::HTTPConfig;
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use reqwest::StatusCode;
use reqwest_middleware::{
    reqwest::header::{CONTENT_LENGTH, RANGE},
    ClientWithMiddleware,
};
use serde::{Deserialize, Serialize};
use snafu::{IntoError, ResultExt, Snafu};
use uuid::Uuid;

use super::object_io::{GetResult, ObjectSource};
use crate::{
    http::HttpSource,
    object_io::{FileMetadata, FileType, LSResult},
    range::GetRange,
    stats::IOStatsRef,
    stream_utils::io_stats_on_bytestream,
    FileFormat, InvalidRangeRequestSnafu,
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

#[derive(Debug, PartialEq)]
struct HFPathParts {
    bucket: String,
    repository: String,
    revision: String,
    path: String,
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

impl HFPathParts {
    // There is a bug within huggingface apis that is incorrectly caching files
    // https://github.com/huggingface/datasets/issues/7685
    //
    // So to bypass this, we add a unique parameter to the url to prevent CDN caching.
    fn get_file_uri(&self, cache_bust: bool) -> String {
        let base = format!(
            "https://huggingface.co/{BUCKET}/{REPOSITORY}/resolve/{REVISION}/{PATH}",
            BUCKET = self.bucket,
            REPOSITORY = self.repository,
            REVISION = self.revision,
            PATH = self.path,
        );
        if cache_bust {
            let cachebuster = Uuid::new_v4();
            let cachebuster = cachebuster.to_string();
            format!("{base}?cachebust={cachebuster}")
        } else {
            base
        }
    }

    fn get_api_uri(&self) -> String {
        // "https://huggingface.co/api/ [datasets] / {username} / {reponame} / tree / {revision} / {path from root}"
        format!(
            "https://huggingface.co/api/{BUCKET}/{REPOSITORY}/tree/{REVISION}/{PATH}",
            BUCKET = self.bucket,
            REPOSITORY = self.repository,
            REVISION = self.revision,
            PATH = self.path,
        )
    }

    fn get_parquet_api_uri(&self) -> String {
        format!(
            "https://huggingface.co/api/{BUCKET}/{REPOSITORY}/parquet",
            BUCKET = self.bucket,
            REPOSITORY = self.repository,
        )
    }
}

pub struct HFSource {
    http_source: HttpSource,
}

impl From<HttpSource> for HFSource {
    fn from(http_source: HttpSource) -> Self {
        Self { http_source }
    }
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
    pub async fn get_client(config: &HTTPConfig) -> super::Result<Arc<Self>> {
        let http_source = HttpSource::get_client(config).await?;
        let http_source = Arc::try_unwrap(http_source).expect("Could not unwrap Arc<HttpSource>");
        let client = http_source.client;
        Ok(Self {
            http_source: HttpSource { client },
        }
        .into())
    }
}

#[async_trait]
impl ObjectSource for HFSource {
    async fn get(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        fn make_request(
            uri: &str,
            cache_bust: bool,
            client: &reqwest_middleware::ClientWithMiddleware,
            range: Option<&GetRange>,
        ) -> super::Result<reqwest_middleware::RequestBuilder> {
            let path_parts = uri.parse::<HFPathParts>()?;
            let uri = &path_parts.get_file_uri(cache_bust);

            let req = client.get(uri);
            Ok(match range {
                None => req,
                Some(range) => {
                    range.validate().context(InvalidRangeRequestSnafu)?;
                    req.header(RANGE, range.to_string())
                }
            })
        }
        let request = make_request(uri, false, &self.http_source.client, range.as_ref())?;

        let response = request
            .send()
            .await
            .context(UnableToConnectSnafu::<String> {
                path: uri.to_string(),
            })?;

        let response = match response.error_for_status() {
            Err(e) => {
                let status_code = e.status();
                match status_code {
                    Some(StatusCode::UNAUTHORIZED) => Err(Error::Unauthorized),
                    // HTTP 416 (Range Not Satisfiable) occurs due to Hugging Face's buggy caching that incorrectly serves the data.
                    // Retry once with cache busting to bypass the improperly cached response and get correct file metadata.
                    // If it fails again, we'll return the error.
                    Some(StatusCode::RANGE_NOT_SATISFIABLE) => {
                        let request =
                            make_request(uri, true, &self.http_source.client, range.as_ref())?;

                        let response = request.send().await.context(UnableToConnectSnafu::<
                            String,
                        > {
                            path: uri.into(),
                        })?;
                        let res = response.error_for_status().map_err(|e| {
                            if e.status() == Some(StatusCode::UNAUTHORIZED) {
                                Error::Unauthorized
                            } else {
                                Error::UnableToOpenFile {
                                    path: uri.to_string(),
                                    source: e,
                                }
                            }
                        })?;
                        Ok(res)
                    }
                    _ => Err(Error::UnableToOpenFile {
                        path: uri.to_string(),
                        source: e,
                    }),
                }
            }
            Ok(res) => Ok(res),
        }?;

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
        Ok(GetResult::Stream(
            io_stats_on_bytestream(stream, io_stats),
            size_bytes,
            None,
            None,
        ))
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
        let path_parts = uri.parse::<HFPathParts>()?;
        let uri = &path_parts.get_file_uri(false);

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
                try_parquet_api(glob_path, limit, io_stats.clone(), &self.http_source.client).await;
            match res {
                Ok(Some(stream)) => return Ok(stream),
                Err(e) => return Err(e.into()),
                Ok(None) => {
                    // INTENTIONALLY EMPTY
                    // If we can't determine if the dataset is private, we'll fall back to the default globbing
                }
            }
        }

        glob(self, glob_path, None, None, limit, io_stats).await
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        _continuation_token: Option<&str>,
        _page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult> {
        if !posix {
            unimplemented!("Prefix-listing is not implemented for HTTP listing");
        }
        let path_parts = path.parse::<HFPathParts>()?;

        let api_uri = path_parts.get_api_uri();

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
    glob_path: &str,
    limit: Option<usize>,
    io_stats: Option<IOStatsRef>,
    client: &ClientWithMiddleware,
) -> Result<Option<BoxStream<'static, super::Result<FileMetadata>>>, Error> {
    let hf_glob_path = glob_path.parse::<HFPathParts>()?;
    if hf_glob_path.path.is_empty() {
        let api_path = hf_glob_path.get_parquet_api_uri();

        let response = client
            .get(api_path.clone())
            .send()
            .await
            .with_context(|_| UnableToConnectSnafu {
                path: api_path.to_string(),
            })?;

        if response.status() == 400 {
            if let Some(error_message) = response
                .headers()
                .get("x-error-message")
                .and_then(|v| v.to_str().ok())
            {
                const PRIVATE_DATASET_ERROR: &str =
                    "Private datasets are only supported for PRO users and Enterprise Hub organizations.";
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
                path: api_path.to_string(),
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
    use common_io_config::HTTPConfig;

    use crate::{
        huggingface::{HFPathParts, HFSource},
        integrations::test_full_get,
        object_io::ObjectSource,
    };

    #[tokio::test]
    async fn test_full_get_from_hf() -> crate::Result<()> {
        let test_file_path = "hf://datasets/google/FACTS-grounding-public/README.md";
        let expected_md5 = "46df309e52cf88f458a4e3e2fb692fc1";

        let client = HFSource::get_client(&HTTPConfig::default()).await?;
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
