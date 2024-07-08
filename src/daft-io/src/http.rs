use std::{num::ParseIntError, ops::Range, string::FromUtf8Error, sync::Arc};

use async_trait::async_trait;
use common_io_config::HTTPConfig;
use futures::{stream::BoxStream, TryStreamExt};

use hyper::header;
use lazy_static::lazy_static;
use regex::Regex;
use reqwest::header::{CONTENT_LENGTH, RANGE};
use snafu::{IntoError, ResultExt, Snafu};
use url::Position;

use crate::{
    object_io::{FileMetadata, FileType, LSResult},
    stats::IOStatsRef,
    stream_utils::io_stats_on_bytestream,
};

use super::object_io::{GetResult, ObjectSource};

const HTTP_DELIMITER: &str = "/";

lazy_static! {
    // Taken from: https://stackoverflow.com/a/15926317/3821154
    static ref HTML_A_TAG_HREF_RE: Regex =
        Regex::new(r#"<(a|A)\s+(?:[^>]*?\s+)?(href|HREF)=["'](?P<url>[^"']+)"#).unwrap();
}

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

    #[snafu(display("Unable to parse URL: \"{}\"", path))]
    InvalidUrl {
        path: String,
        source: url::ParseError,
    },

    #[snafu(display(
        "Unable to parse data as Utf8 while reading header for file: {path}. {source}"
    ))]
    UnableToParseUtf8Header { path: String, source: FromUtf8Error },

    #[snafu(display(
        "Unable to parse data as Utf8 while reading body for file: {path}. {source}"
    ))]
    UnableToParseUtf8Body {
        path: String,
        source: reqwest::Error,
    },

    #[snafu(display(
        "Unable to parse data as Integer while reading header for file: {path}. {source}"
    ))]
    UnableToParseInteger { path: String, source: ParseIntError },

    #[snafu(display("Unable to create HTTP header: {source}"))]
    UnableToCreateHeader { source: header::InvalidHeaderValue },
}

/// Finds and retrieves FileMetadata from HTML text
///
/// This function will look for `<a href=***>` tags and return all the links that it finds as
/// absolute URLs
fn _get_file_metadata_from_html(path: &str, text: &str) -> super::Result<Vec<FileMetadata>> {
    let path_url = url::Url::parse(path).with_context(|_| InvalidUrlSnafu { path })?;
    let metas = HTML_A_TAG_HREF_RE
        .captures_iter(text)
        .map(|captures| {
            // Parse the matched URL into an absolute URL
            let matched_url = captures.name("url").unwrap().as_str();
            let absolute_path = if let Ok(parsed_matched_url) = url::Url::parse(matched_url) {
                // matched_url is already an absolute path
                parsed_matched_url
            } else if matched_url.starts_with(HTTP_DELIMITER) {
                // matched_url is a path relative to the origin of `path`
                let base = url::Url::parse(&path_url[..Position::BeforePath]).unwrap();
                base.join(matched_url)
                    .with_context(|_| InvalidUrlSnafu { path: matched_url })?
            } else {
                // matched_url is a path relative to `path` and needs to be joined
                path_url
                    .join(matched_url)
                    .with_context(|_| InvalidUrlSnafu { path: matched_url })?
            };

            // Ignore any links that are not descendants of `path` to avoid cycles
            let relative = path_url.make_relative(&absolute_path);
            match relative {
                None => {
                    return Ok(None);
                }
                Some(relative_path)
                    if relative_path.is_empty() || relative_path.starts_with("..") =>
                {
                    return Ok(None);
                }
                _ => (),
            };

            let filetype = if matched_url.ends_with(HTTP_DELIMITER) {
                FileType::Directory
            } else {
                FileType::File
            };
            Ok(Some(FileMetadata {
                filepath: absolute_path.to_string(),
                // NOTE: This is consistent with fsspec behavior, but we may choose to HEAD the files to grab Content-Length
                // for populating `size` if necessary
                size: None,
                filetype,
            }))
        })
        .collect::<super::Result<Vec<_>>>()?;

    Ok(metas.into_iter().flatten().collect())
}

pub(crate) struct HttpSource {
    client: reqwest::Client,
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

impl HttpSource {
    pub async fn get_client(config: &HTTPConfig) -> super::Result<Arc<Self>> {
        let mut default_headers = header::HeaderMap::new();
        default_headers.append(
            "user-agent",
            header::HeaderValue::from_str(config.user_agent.as_str())
                .context(UnableToCreateHeaderSnafu)?,
        );

        Ok(HttpSource {
            client: reqwest::ClientBuilder::default()
                .pool_max_idle_per_host(70)
                .default_headers(default_headers)
                .build()
                .context(UnableToCreateClientSnafu)?,
        }
        .into())
    }
}

#[async_trait]
impl ObjectSource for HttpSource {
    async fn get(
        &self,
        uri: &str,
        range: Option<Range<usize>>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        let request = self.client.get(uri);
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
        let request = self.client.head(uri);
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

        // Ensure fanout_limit is None because HTTP ObjectSource does not support prefix listing
        let fanout_limit = None;
        let page_size = None;

        glob(self, glob_path, fanout_limit, page_size, limit, io_stats).await
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

        let request = self.client.get(path);
        let response = request
            .send()
            .await
            .context(UnableToConnectSnafu::<String> { path: path.into() })?
            .error_for_status()
            .with_context(|_| UnableToOpenFileSnafu { path })?;
        if let Some(is) = io_stats.as_ref() {
            is.mark_list_requests(1)
        }

        // Reconstruct the actual path of the request, which may have been redirected via a 301
        // This is important because downstream URL joining logic relies on proper trailing-slashes/index.html
        let path = response.url().to_string();
        let path = if path.ends_with(HTTP_DELIMITER) {
            format!("{}/", path.trim_end_matches(HTTP_DELIMITER))
        } else {
            path
        };

        match response.headers().get("content-type") {
            // If the content-type is text/html, we treat the data on this path as a traversable "directory"
            Some(header_value) if header_value.to_str().map_or(false, |v| v == "text/html") => {
                let text = response
                    .text()
                    .await
                    .with_context(|_| UnableToParseUtf8BodySnafu {
                        path: path.to_string(),
                    })?;
                let file_metadatas = _get_file_metadata_from_html(path.as_str(), text.as_str())?;
                Ok(LSResult {
                    files: file_metadatas,
                    continuation_token: None,
                })
            }
            // All other forms of content-type is treated as a raw file
            _ => Ok(LSResult {
                files: vec![FileMetadata {
                    filepath: path.to_string(),
                    filetype: FileType::File,
                    size: response.content_length(),
                }],
                continuation_token: None,
            }),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::default;

    use crate::object_io::ObjectSource;
    use crate::HttpSource;
    use crate::Result;

    #[tokio::test]
    async fn test_full_get_from_http() -> Result<()> {
        let parquet_file_path = "https://daft-public-data.s3.us-west-2.amazonaws.com/test_fixtures/parquet_small/0dad4c3f-da0d-49db-90d8-98684571391b-0.parquet";
        let parquet_expected_md5 = "929674747af64a98aceaa6d895863bd3";

        let client = HttpSource::get_client(&default::Default::default()).await?;
        let parquet_file = client.get(parquet_file_path, None, None).await?;
        let bytes = parquet_file.bytes().await?;
        let all_bytes = bytes.as_ref();
        let checksum = format!("{:x}", md5::compute(all_bytes));
        assert_eq!(checksum, parquet_expected_md5);

        let first_bytes = client
            .get(parquet_file_path, Some(0..10), None)
            .await?
            .bytes()
            .await?;
        assert_eq!(first_bytes.len(), 10);
        assert_eq!(first_bytes.as_ref(), &all_bytes[..10]);

        let first_bytes = client
            .get(parquet_file_path, Some(10..100), None)
            .await?
            .bytes()
            .await?;
        assert_eq!(first_bytes.len(), 90);
        assert_eq!(first_bytes.as_ref(), &all_bytes[10..100]);

        let last_bytes = client
            .get(
                parquet_file_path,
                Some((all_bytes.len() - 10)..(all_bytes.len() + 10)),
                None,
            )
            .await?
            .bytes()
            .await?;
        assert_eq!(last_bytes.len(), 10);
        assert_eq!(last_bytes.as_ref(), &all_bytes[(all_bytes.len() - 10)..]);

        let size_from_get_size = client.get_size(parquet_file_path, None).await?;
        assert_eq!(size_from_get_size, all_bytes.len());
        Ok(())
    }
}
