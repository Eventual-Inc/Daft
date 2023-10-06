use std::ops::Range;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::StreamExt;
use futures::TryStreamExt;
use google_cloud_storage::client::ClientConfig;

use async_trait::async_trait;
use google_cloud_storage::client::Client;
use google_cloud_storage::http::objects::get::GetObjectRequest;

use google_cloud_storage::http::objects::list::ListObjectsRequest;
use google_cloud_storage::http::Error as GError;
use snafu::IntoError;
use snafu::ResultExt;
use snafu::Snafu;

use crate::object_io::FileMetadata;
use crate::object_io::FileType;
use crate::object_io::LSResult;
use crate::object_io::ObjectSource;
use crate::s3_like;
use crate::GetResult;
use common_io_config::GCSConfig;

const GCS_DELIMITER: &str = "/";
const GCS_SCHEME: &str = "gs";
const DEFAULT_GLOB_FANOUT_LIMIT: usize = 1024;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to open {}: {}", path, source))]
    UnableToOpenFile { path: String, source: GError },

    #[snafu(display("Unable to list objects: \"{}\"", path))]
    UnableToListObjects { path: String, source: GError },

    #[snafu(display("Unable to read data from {}: {}", path, source))]
    UnableToReadBytes { path: String, source: GError },

    #[snafu(display("Unable to parse URL: \"{}\"", path))]
    InvalidUrl {
        path: String,
        source: url::ParseError,
    },
    #[snafu(display("Unable to load Credentials: {}", source))]
    UnableToLoadCredentials {
        source: google_cloud_storage::client::google_cloud_auth::error::Error,
    },

    #[snafu(display("Not a File: \"{}\"", path))]
    NotFound { path: String },
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::*;
        match error {
            UnableToReadBytes { path, source }
            | UnableToOpenFile { path, source }
            | UnableToListObjects { path, source } => match source {
                GError::HttpClient(err) => match err.status().map(|s| s.as_u16()) {
                    Some(404) | Some(410) => super::Error::NotFound {
                        path,
                        source: err.into(),
                    },
                    Some(401) => super::Error::Unauthorized {
                        store: super::SourceType::GCS,
                        path,
                        source: err.into(),
                    },
                    _ => super::Error::UnableToOpenFile {
                        path,
                        source: err.into(),
                    },
                },
                GError::Response(err) => match err.code {
                    404 | 410 => super::Error::NotFound {
                        path,
                        source: err.into(),
                    },
                    401 => super::Error::Unauthorized {
                        store: super::SourceType::GCS,
                        path,
                        source: err.into(),
                    },
                    _ => super::Error::UnableToOpenFile {
                        path,
                        source: err.into(),
                    },
                },
                GError::TokenSource(err) => super::Error::UnableToLoadCredentials {
                    store: super::SourceType::GCS,
                    source: err,
                },
            },
            NotFound { ref path } => super::Error::NotFound {
                path: path.into(),
                source: error.into(),
            },
            InvalidUrl { path, source } => super::Error::InvalidUrl { path, source },
            UnableToLoadCredentials { source } => super::Error::UnableToLoadCredentials {
                store: super::SourceType::GCS,
                source: source.into(),
            },
        }
    }
}

enum GCSClientWrapper {
    Native(Client),
    S3Compat(Arc<s3_like::S3LikeSource>),
}

fn parse_uri(uri: &url::Url) -> super::Result<(&str, &str)> {
    let bucket = match uri.host_str() {
        Some(s) => Ok(s),
        None => Err(Error::InvalidUrl {
            path: uri.to_string(),
            source: url::ParseError::EmptyHost,
        }),
    }?;
    let key = uri.path();
    let key = key.strip_prefix(GCS_DELIMITER).unwrap_or(key);
    Ok((bucket, key))
}

impl GCSClientWrapper {
    async fn get(&self, uri: &str, range: Option<Range<usize>>) -> super::Result<GetResult> {
        let uri = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { path: uri })?;
        let (bucket, key) = parse_uri(&uri)?;
        match self {
            GCSClientWrapper::Native(client) => {
                let req = GetObjectRequest {
                    bucket: bucket.into(),
                    object: key.into(),
                    ..Default::default()
                };
                use google_cloud_storage::http::objects::download::Range as GRange;
                let (grange, size) = if let Some(range) = range {
                    (
                        GRange(Some(range.start as u64), Some(range.end as u64)),
                        Some(range.len()),
                    )
                } else {
                    (GRange::default(), None)
                };
                let owned_uri = uri.to_string();
                let response = client
                    .download_streamed_object(&req, &grange)
                    .await
                    .context(UnableToOpenFileSnafu {
                        path: uri.to_string(),
                    })?;
                let response = response.map_err(move |e| {
                    UnableToReadBytesSnafu::<String> {
                        path: owned_uri.clone(),
                    }
                    .into_error(e)
                    .into()
                });
                Ok(GetResult::Stream(response.boxed(), size, None))
            }
            GCSClientWrapper::S3Compat(client) => {
                let uri = format!("s3://{}/{}", bucket, key);
                client.get(&uri, range).await
            }
        }
    }

    async fn get_size(&self, uri: &str) -> super::Result<usize> {
        let uri = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { path: uri })?;
        let (bucket, key) = parse_uri(&uri)?;
        match self {
            GCSClientWrapper::Native(client) => {
                let req = GetObjectRequest {
                    bucket: bucket.into(),
                    object: key.into(),
                    ..Default::default()
                };

                let response = client
                    .get_object(&req)
                    .await
                    .context(UnableToOpenFileSnafu {
                        path: uri.to_string(),
                    })?;
                Ok(response.size as usize)
            }
            GCSClientWrapper::S3Compat(client) => {
                let uri = format!("s3://{}/{}", bucket, key);
                client.get_size(&uri).await
            }
        }
    }

    async fn _ls_impl(
        &self,
        client: &Client,
        bucket: &str,
        key: &str,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
    ) -> super::Result<LSResult> {
        let req = ListObjectsRequest {
            bucket: bucket.to_string(),
            prefix: Some(key.to_string()),
            end_offset: None,
            start_offset: None,
            page_token: continuation_token.map(|s| s.to_string()),
            delimiter: delimiter.map(|d| d.to_string()), // returns results in "directory mode" if delimiter is provided
            max_results: page_size,
            include_trailing_delimiter: Some(false), // This will not populate "directories" in the response's .item[]
            projection: None,
            versions: None,
        };
        let ls_response = client
            .list_objects(&req)
            .await
            .context(UnableToListObjectsSnafu {
                path: format!("{GCS_SCHEME}://{}/{}", bucket, key),
            })?;
        let response_items = ls_response.items.unwrap_or_default();
        let response_prefixes = ls_response.prefixes.unwrap_or_default();
        let files = response_items.iter().map(|obj| FileMetadata {
            filepath: format!("{GCS_SCHEME}://{}/{}", bucket, obj.name),
            size: Some(obj.size as u64),
            filetype: FileType::File,
        });
        let dirs = response_prefixes.iter().map(|pref| FileMetadata {
            filepath: format!("{GCS_SCHEME}://{}/{}", bucket, pref),
            size: None,
            filetype: FileType::Directory,
        });
        Ok(LSResult {
            files: files.chain(dirs).collect(),
            continuation_token: ls_response.next_page_token,
        })
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
    ) -> super::Result<LSResult> {
        let uri = url::Url::parse(path).with_context(|_| InvalidUrlSnafu { path })?;
        let (bucket, key) = parse_uri(&uri)?;
        match self {
            GCSClientWrapper::Native(client) => {
                if posix {
                    // Attempt to forcefully ls the key as a directory (by ensuring a "/" suffix)
                    let forced_directory_key = if key.is_empty() {
                        "".to_string()
                    } else {
                        format!("{}{GCS_DELIMITER}", key.trim_end_matches(GCS_DELIMITER))
                    };
                    let forced_directory_ls_result = self
                        ._ls_impl(
                            client,
                            bucket,
                            forced_directory_key.as_str(),
                            Some(GCS_DELIMITER),
                            continuation_token,
                            page_size,
                        )
                        .await?;

                    // If no items were obtained, then this is actually a file and we perform a second ls to obtain just the file's
                    // details as the one-and-only-one entry
                    if forced_directory_ls_result.files.is_empty() {
                        let mut file_result = self
                            ._ls_impl(
                                client,
                                bucket,
                                key,
                                Some(GCS_DELIMITER),
                                continuation_token,
                                page_size,
                            )
                            .await?;

                        // Only retain exact matches (since the API does prefix lists by default)
                        let target_path = format!("{GCS_SCHEME}://{bucket}/{key}");
                        file_result.files.retain(|fm| fm.filepath == target_path);

                        // Not dir and not file, so it is missing
                        if file_result.files.is_empty() {
                            return Err(Error::NotFound {
                                path: path.to_string(),
                            }
                            .into());
                        }

                        Ok(file_result)
                    } else {
                        Ok(forced_directory_ls_result)
                    }
                } else {
                    self._ls_impl(
                        client,
                        bucket,
                        key,
                        None, // Force a prefix-listing
                        continuation_token,
                        page_size,
                    )
                    .await
                }
            }
            GCSClientWrapper::S3Compat(client) => {
                client.ls(path, posix, continuation_token, page_size).await
            }
        }
    }
}

pub(crate) struct GCSSource {
    client: GCSClientWrapper,
}

impl GCSSource {
    async fn build_s3_compat_client() -> super::Result<Arc<Self>> {
        let s3_config = common_io_config::S3Config {
            anonymous: true,
            endpoint_url: Some("https://storage.googleapis.com".to_string()),
            ..Default::default()
        };
        let s3_client = s3_like::S3LikeSource::get_client(&s3_config).await?;
        Ok(GCSSource {
            client: GCSClientWrapper::S3Compat(s3_client),
        }
        .into())
    }
    pub async fn get_client(config: &GCSConfig) -> super::Result<Arc<Self>> {
        if config.anonymous {
            GCSSource::build_s3_compat_client().await
        } else {
            let config = ClientConfig::default()
                .with_auth()
                .await
                .context(UnableToLoadCredentialsSnafu {});
            match config {
                Ok(config) => {
                    let client = Client::new(config);
                    Ok(GCSSource {
                        client: GCSClientWrapper::Native(client),
                    }
                    .into())
                }
                Err(err) => {
                    log::warn!("Google Cloud Storage Credentials not provided or found when making client. Reverting to Anonymous mode.\nDetails\n{err}");
                    GCSSource::build_s3_compat_client().await
                }
            }
        }
    }
}

#[async_trait]
impl ObjectSource for GCSSource {
    async fn get(&self, uri: &str, range: Option<Range<usize>>) -> super::Result<GetResult> {
        self.client.get(uri, range).await
    }

    async fn get_size(&self, uri: &str) -> super::Result<usize> {
        self.client.get_size(uri).await
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        use crate::object_store_glob::glob;

        // Ensure fanout_limit is not None to prevent runaway concurrency
        let fanout_limit = fanout_limit.or(Some(DEFAULT_GLOB_FANOUT_LIMIT));

        glob(self, glob_path, fanout_limit, page_size.or(Some(1000))).await
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        page_size: Option<i32>,
    ) -> super::Result<LSResult> {
        self.client
            .ls(path, posix, continuation_token, page_size)
            .await
    }
}
