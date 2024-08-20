use std::{num::ParseIntError, ops::Range, str::FromStr, string::FromUtf8Error, sync::Arc};

use async_trait::async_trait;
use common_io_config::HTTPConfig;
use futures::{stream::BoxStream, TryStreamExt};

use hyper::header;
use reqwest::header::{CONTENT_LENGTH, RANGE};
use snafu::{IntoError, ResultExt, Snafu};

use crate::{
    http::HttpSource,
    object_io::{FileMetadata, FileType, LSResult},
    stats::IOStatsRef,
    stream_utils::io_stats_on_bytestream,
};
use serde::{Deserialize, Serialize};

use super::object_io::{GetResult, ObjectSource};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to connect to {}: {}", path, source))]
    UnableToConnect {
        path: String,
        source: reqwest::Error,
    },

    #[snafu(display("Unable to open {}: {}", path, source))]
    UnableToOpenFile {
        path: String,
        source: reqwest::Error,
    },

    #[snafu(display("Unable to determine size of {}", path))]
    UnableToDetermineSize { path: String },

    #[snafu(display("Unable to read data from {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: reqwest::Error,
    },

    #[snafu(display("Unable to create Http Client {}", source))]
    UnableToCreateClient { source: reqwest::Error },

    #[snafu(display(
        "Unable to parse data as Utf8 while reading header for file: {path}. {source}"
    ))]
    UnableToParseUtf8Header { path: String, source: FromUtf8Error },

    #[snafu(display(
        "Unable to parse data as Integer while reading header for file: {path}. {source}"
    ))]
    UnableToParseInteger { path: String, source: ParseIntError },

    #[snafu(display("Unable to create HTTP header: {source}"))]
    UnableToCreateHeader { source: header::InvalidHeaderValue },
    #[snafu(display("Invalid path: {}", path))]
    InvalidPath { path: String },
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
    type Err = super::Error;
    /// Extracts path components from a hugging face path:
    /// `hf:// [datasets | spaces] / {username} / {reponame} @ {revision} / {path from root}`
    fn from_str(uri: &str) -> super::Result<Self, Self::Err> {
        // hf:// [datasets] / {username} / {reponame} @ {revision} / {path from root}
        //       !>
        if !uri.starts_with("hf://") {
            return Err(Error::InvalidPath {
                path: uri.to_string(),
            }
            .into());
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
                return Some(HFPathParts {
                    bucket: bucket.to_string(),
                    repository: format!("{}/{}", username, uri),
                    revision: "main".to_string(),
                    path: "".to_string(),
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
            let repository = format!("{}/{}", username, repository);
            // {path from root}
            // ^--------------^
            let path = uri.to_string();

            Some(HFPathParts {
                bucket: bucket.to_string(),
                repository,
                revision,
                path,
            })
        })()
        .ok_or_else(|| {
            Error::InvalidPath {
                path: uri.to_string(),
            }
            .into()
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
    fn get_file_uri(&self) -> String {
        format!(
            "https://huggingface.co/{BUCKET}/{REPOSITORY}/resolve/{REVISION}/{PATH}",
            BUCKET = self.bucket,
            REPOSITORY = self.repository,
            REVISION = self.revision,
            PATH = self.path
        )
    }

    fn get_api_uri(&self) -> String {
        // "https://huggingface.co/api/ [datasets] / {username} / {reponame} / tree / {revision} / {path from root}"
        format!(
            "https://huggingface.co/api/{BUCKET}/{REPOSITORY}/tree/{REVISION}/{PATH}",
            BUCKET = self.bucket,
            REPOSITORY = self.repository,
            REVISION = self.revision,
            PATH = self.path
        )
    }
}

pub(crate) struct HFSource {
    http_source: HttpSource,
}
impl From<HttpSource> for HFSource {
    fn from(http_source: HttpSource) -> Self {
        Self { http_source }
    }
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::*;
        match error {
            UnableToOpenFile { path, source } => match source.status().map(|v| v.as_u16()) {
                Some(404) | Some(410) => super::Error::NotFound {
                    path,
                    source: source.into(),
                },
                None | Some(_) => super::Error::UnableToOpenFile {
                    path,
                    source: source.into(),
                },
            },
            UnableToDetermineSize { path } => super::Error::UnableToDetermineSize { path },
            _ => super::Error::Generic {
                store: super::SourceType::Http,
                source: error.into(),
            },
        }
    }
}

impl HFSource {
    pub async fn get_client(config: &HTTPConfig) -> super::Result<Arc<Self>> {
        let mut default_headers = header::HeaderMap::new();
        default_headers.append(
            "user-agent",
            header::HeaderValue::from_str(config.user_agent.as_str())
                .context(UnableToCreateHeaderSnafu)?,
        );
        if let Some(token) = &config.bearer_token {
            default_headers.append(
                "Authorization",
                header::HeaderValue::from_str(&format!("Bearer {}", token))
                    .context(UnableToCreateHeaderSnafu)?,
            );
        }

        Ok(HFSource {
            http_source: HttpSource {
                client: reqwest::ClientBuilder::default()
                    .pool_max_idle_per_host(70)
                    .default_headers(default_headers)
                    .build()
                    .context(UnableToCreateClientSnafu)?,
            },
        }
        .into())
    }
}

#[async_trait]
impl ObjectSource for HFSource {
    async fn get(
        &self,
        uri: &str,
        range: Option<Range<usize>>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        let path_parts = uri.parse::<HFPathParts>()?;
        let uri = &path_parts.get_file_uri();
        let request = self.http_source.client.get(uri);
        let request = match range {
            None => request,
            Some(range) => request.header(
                RANGE,
                format!("bytes={}-{}", range.start, range.end.saturating_sub(1)),
            ),
        };

        let response = request
            .send()
            .await
            .context(UnableToConnectSnafu::<String> { path: uri.into() })?;
        let response = response
            .error_for_status()
            .context(UnableToOpenFileSnafu::<String> { path: uri.into() })?;
        if let Some(is) = io_stats.as_ref() {
            is.mark_get_requests(1)
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
        let uri = &path_parts.get_file_uri();

        let request = self.http_source.client.head(uri);
        let response = request
            .send()
            .await
            .context(UnableToConnectSnafu::<String> { path: uri.into() })?;
        let response = response
            .error_for_status()
            .context(UnableToOpenFileSnafu::<String> { path: uri.into() })?;

        if let Some(is) = io_stats.as_ref() {
            is.mark_head_requests(1)
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
    ) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>> {
        use crate::object_store_glob::glob;
        // const DEFAULT_GLOB_FANOUT_LIMIT: usize = 1024;
        // const DEFAULT_GLOB_PAGE_SIZE: i32 = 1000;

        // let fanout_limit = fanout_limit.or(Some(DEFAULT_GLOB_FANOUT_LIMIT));
        // let page_size = page_size.or(Some(DEFAULT_GLOB_PAGE_SIZE));

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
            })?
            .error_for_status()
            .with_context(|_| UnableToOpenFileSnafu {
                path: api_uri.clone(),
            })?;
        if let Some(is) = io_stats.as_ref() {
            is.mark_list_requests(1)
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
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use crate::huggingface::HFPathParts;

    #[test]
    fn test_parse_hf_parts() -> DaftResult<()> {
        let uri = "hf://datasets/wikimedia/wikipedia/20231101.ab/*.parquet";
        let parts = uri.parse::<HFPathParts>()?;
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
        let parts = uri.parse::<HFPathParts>()?;
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
        let parts = uri.parse::<HFPathParts>()?;
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
        let parts = uri.parse::<HFPathParts>()?;
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
