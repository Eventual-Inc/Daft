use std::ops::Range;
use std::sync::Arc;

use futures::StreamExt;
use futures::TryStreamExt;
use google_cloud_storage::client::ClientConfig;

use google_cloud_storage::client::Client;
use google_cloud_storage::http::objects::get::GetObjectRequest;

use async_trait::async_trait;
use snafu::IntoError;
use snafu::ResultExt;
use snafu::Snafu;

use crate::config;
use crate::object_io::ObjectSource;
use crate::s3_like;
use crate::GetResult;

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
        source: azure_storage::Error,
    },

    #[snafu(display("Unable to determine size of {}", path))]
    UnableToDetermineSize { path: String },

    #[snafu(display("Unable to read data from {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: google_cloud_storage::http::Error,
    },

    // #[snafu(display("Unable to create Http Client {}", source))]
    // UnableToCreateClient { source: reqwest::Error },
    #[snafu(display("Unable to parse URL: \"{}\"", path))]
    InvalidUrl {
        path: String,
        source: url::ParseError,
    },
    // #[snafu(display("Azure Storage Account not set and is required.\n Set either `AzureConfig.storage_account` or the `AZURE_STORAGE_ACCOUNT` environment variable."))]
    // StorageAccountNotSet,

    // #[snafu(display(
    //     "Unable to parse data as Utf8 while reading header for file: {path}. {source}"
    // ))]
    // UnableToParseUtf8 { path: String, source: FromUtf8Error },

    // #[snafu(display(
    //     "Unable to parse data as Integer while reading header for file: {path}. {source}"
    // ))]
    // UnableToParseInteger { path: String, source: ParseIntError },
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::*;
        match error {
            // UnableToReadBytes { path, source } | UnableToOpenFile { path, source } => {
            //     match source.as_http_error().map(|v| v.status().into()) {
            //         Some(404) | Some(410) => super::Error::NotFound {
            //             path,
            //             source: source.into(),
            //         },
            //         Some(401) => super::Error::Unauthorized {
            //             store: super::SourceType::AzureBlob,
            //             path,
            //             source: source.into(),
            //         },
            //         None | Some(_) => super::Error::UnableToOpenFile {
            //             path,
            //             source: source.into(),
            //         },
            //     }
            // }
            _ => super::Error::Generic {
                store: super::SourceType::GCS,
                source: error.into(),
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
                    .unwrap();
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
                // TODO Add not a file error here
                let uri = format!("s3://{}{}", bucket, key);
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

        match self {
            GCSClientWrapper::Native(client) => {
                let req = GetObjectRequest {
                    bucket: bucket.into(),
                    object: key.into(),
                    ..Default::default()
                };

                let response = client.get_object(&req).await.unwrap();
                Ok(response.size as usize)
            }
            GCSClientWrapper::S3Compat(client) => {
                let uri = format!("s3://{}{}", bucket, key);
                client.get_size(&uri).await
            }
        }
    }
}

pub(crate) struct GCSSource {
    client: GCSClientWrapper,
}

impl GCSSource {
    pub async fn get_client() -> super::Result<Arc<Self>> {
        let anon = true;
        if anon {
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
        } else {
            let config = ClientConfig::default();
            log::warn!("config: {config:?}");
            let client = Client::new(config);
            Ok(GCSSource {
                client: GCSClientWrapper::Native(client),
            }
            .into())
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
