use std::ops::Range;
use std::sync::Arc;

use futures::StreamExt;
use futures::TryStreamExt;
use google_cloud_storage::client::ClientConfig;

use async_trait::async_trait;
use google_cloud_storage::client::Client;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::Error as GError;
use snafu::IntoError;
use snafu::ResultExt;
use snafu::Snafu;

use crate::config;
use crate::config::GCSConfig;
use crate::object_io::ObjectSource;
use crate::s3_like;
use crate::GetResult;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to open {}: {}", path, source))]
    UnableToOpenFile { path: String, source: GError },

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
    NotAFile { path: String },
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::*;
        match error {
            UnableToReadBytes { path, source } | UnableToOpenFile { path, source } => {
                match source {
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
                }
            }
            NotAFile { path } => super::Error::NotAFile { path },
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

impl GCSClientWrapper {
    async fn get(&self, uri: &str, range: Option<Range<usize>>) -> super::Result<GetResult> {
        let parsed = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { path: uri })?;
        let bucket = match parsed.host_str() {
            Some(s) => Ok(s),
            None => Err(Error::InvalidUrl {
                path: uri.into(),
                source: url::ParseError::EmptyHost,
            }),
        }?;
        let key = parsed.path();
        let key = if let Some(key) = key.strip_prefix('/') {
            key
        } else {
            return Err(Error::NotAFile { path: uri.into() }.into());
        };

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
                Ok(GetResult::Stream(response.boxed(), size))
            }
            GCSClientWrapper::S3Compat(client) => {
                let uri = format!("s3://{}/{}", bucket, key);
                client.get(&uri, range).await
            }
        }
    }

    async fn get_size(&self, uri: &str) -> super::Result<usize> {
        let parsed = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { path: uri })?;
        let bucket = match parsed.host_str() {
            Some(s) => Ok(s),
            None => Err(Error::InvalidUrl {
                path: uri.into(),
                source: url::ParseError::EmptyHost,
            }),
        }?;
        let key = parsed.path();
        let key = if let Some(key) = key.strip_prefix('/') {
            key
        } else {
            return Err(Error::NotAFile { path: uri.into() }.into());
        };
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
}

pub(crate) struct GCSSource {
    client: GCSClientWrapper,
}

impl GCSSource {
    async fn build_s3_compat_client() -> super::Result<Arc<Self>> {
        let s3_config = config::S3Config {
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
}
