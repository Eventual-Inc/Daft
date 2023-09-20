use async_trait::async_trait;
use azure_storage::prelude::*;
use azure_storage_blobs::{
    container::{operations::BlobItem, Container},
    prelude::*,
};
use futures::{StreamExt, TryStreamExt};
use snafu::{IntoError, ResultExt, Snafu};
use std::{num::ParseIntError, ops::Range, string::FromUtf8Error, sync::Arc};

use crate::{
    object_io::{FileMetadata, FileType, LSResult, ObjectSource},
    GetResult,
};
use common_io_config::AzureConfig;

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
        source: azure_storage::Error,
    },

    #[snafu(display("Unable to create Http Client {}", source))]
    UnableToCreateClient { source: reqwest::Error },

    #[snafu(display("Unable to parse URL: \"{}\"", path))]
    InvalidUrl {
        path: String,
        source: url::ParseError,
    },

    #[snafu(display("Azure Storage Account not set and is required.\n Set either `AzureConfig.storage_account` or the `AZURE_STORAGE_ACCOUNT` environment variable."))]
    StorageAccountNotSet,

    #[snafu(display(
        "Unable to parse data as Utf8 while reading header for file: {path}. {source}"
    ))]
    UnableToParseUtf8 { path: String, source: FromUtf8Error },

    #[snafu(display(
        "Unable to parse data as Integer while reading header for file: {path}. {source}"
    ))]
    UnableToParseInteger { path: String, source: ParseIntError },
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::*;
        match error {
            UnableToReadBytes { path, source } | UnableToOpenFile { path, source } => {
                match source.as_http_error().map(|v| v.status().into()) {
                    Some(404) | Some(410) => super::Error::NotFound {
                        path,
                        source: source.into(),
                    },
                    Some(401) => super::Error::Unauthorized {
                        store: super::SourceType::AzureBlob,
                        path,
                        source: source.into(),
                    },
                    None | Some(_) => super::Error::UnableToOpenFile {
                        path,
                        source: source.into(),
                    },
                }
            }
            _ => super::Error::Generic {
                store: super::SourceType::AzureBlob,
                source: error.into(),
            },
        }
    }
}

fn container_to_file_metadata(container: &Container) -> FileMetadata {
    FileMetadata {
        filepath: format!("https://{}", container.name),
        size: None,
        filetype: FileType::Directory,
    }
}

fn blob_item_to_file_metadata(prefix: &str, blob_item: &BlobItem) -> FileMetadata {
    match blob_item {
        BlobItem::Blob(blob) => {
            todo!()
        }
        BlobItem::BlobPrefix(prefix) => {
            todo!()
        }
    }
}

pub(crate) struct AzureBlobSource {
    blob_client: Arc<BlobServiceClient>,
}

impl AzureBlobSource {
    pub async fn get_client(config: &AzureConfig) -> super::Result<Arc<Self>> {
        let storage_account = if let Some(storage_account) = &config.storage_account {
            storage_account.clone()
        } else if let Ok(storage_account) = std::env::var("AZURE_STORAGE_ACCOUNT") {
            storage_account
        } else {
            return Err(Error::StorageAccountNotSet.into());
        };

        let storage_credentials = if config.anonymous {
            StorageCredentials::anonymous()
        } else if let Some(access_key) = &config.access_key {
            StorageCredentials::access_key(&storage_account, access_key)
        } else if let Ok(access_key) = std::env::var("AZURE_STORAGE_KEY") {
            StorageCredentials::access_key(&storage_account, access_key)
        } else {
            log::warn!("Azure access key not found, Set either `AzureConfig.access_key` or the `AZURE_STORAGE_KEY` environment variable. Defaulting to anonymous mode.");
            StorageCredentials::anonymous()
        };
        let blob_client = BlobServiceClient::new(storage_account, storage_credentials);

        Ok(AzureBlobSource {
            blob_client: blob_client.into(),
        }
        .into())
    }

    async fn _list_containers(&self) -> super::Result<LSResult> {
        let responses_stream = self
            .blob_client
            .clone()
            .list_containers()
            .include_metadata(true)
            .into_stream();

        // It looks like the azure rust library API
        // does not currently allow using the continuation token:
        // https://docs.rs/azure_storage_blobs/0.15.0/azure_storage_blobs/service/operations/struct.ListContainersBuilder.html
        // https://docs.rs/azure_core/0.15.0/azure_core/struct.Pageable.html
        // For now, collect the entire result.
        let responses = responses_stream.try_collect::<Vec<_>>().await;

        match responses {
            Ok(responses) => {
                let containers = responses
                    .iter()
                    .flat_map(|resp| &resp.containers)
                    .map(container_to_file_metadata)
                    .collect::<Vec<_>>();

                let result = LSResult {
                    files: containers,
                    continuation_token: None,
                };

                Ok(result)
            }
            Err(e) => todo!(),
        }
    }

    async fn _list_directory(
        &self,
        container_name: &str,
        delimiter: &str,
        prefix: &str,
    ) -> super::Result<LSResult> {
        let responses_stream = self
            .blob_client
            .container_client(container_name)
            .list_blobs()
            .delimiter(delimiter.to_string())
            .into_stream();

        // It looks like the azure rust library API
        // does not currently allow using the continuation token:
        // https://docs.rs/azure_storage_blobs/0.15.0/azure_storage_blobs/container/operations/list_blobs/struct.ListBlobsBuilder.html
        // https://docs.rs/azure_core/0.15.0/azure_core/struct.Pageable.html
        // For now, collect the entire result.
        let responses = responses_stream.try_collect::<Vec<_>>().await;

        match responses {
            Ok(responses) => {
                let blob_items = responses
                    .iter()
                    .flat_map(|resp| &resp.blobs.items)
                    .map(|blob_item| blob_item_to_file_metadata(prefix, blob_item))
                    .collect::<Vec<_>>();

                todo!()
            }
            Err(e) => {
                todo!()
            }
        }
    }
}

#[async_trait]
impl ObjectSource for AzureBlobSource {
    async fn get(&self, uri: &str, range: Option<Range<usize>>) -> super::Result<GetResult> {
        let parsed = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { path: uri })?;
        let container = match parsed.host_str() {
            Some(s) => Ok(s),
            None => Err(Error::InvalidUrl {
                path: uri.into(),
                source: url::ParseError::EmptyHost,
            }),
        }?;
        let key = parsed.path();

        let container_client = self.blob_client.container_client(container);
        let blob_client = container_client.blob_client(key);
        let request_builder = blob_client.get();
        let request_builder = if let Some(range) = range {
            request_builder.range(range)
        } else {
            request_builder
        };
        let blob_stream = request_builder.into_stream();

        let owned_string = uri.to_string();
        let stream = blob_stream
            .and_then(async move |v| v.data.collect().await)
            .map_err(move |e| {
                UnableToReadBytesSnafu::<String> {
                    path: owned_string.clone(),
                }
                .into_error(e)
                .into()
            });
        Ok(GetResult::Stream(stream.boxed(), None, None))
    }

    async fn get_size(&self, uri: &str) -> super::Result<usize> {
        let parsed = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { path: uri })?;
        let container = match parsed.host_str() {
            Some(s) => Ok(s),
            None => Err(Error::InvalidUrl {
                path: uri.into(),
                source: url::ParseError::EmptyHost,
            }),
        }?;
        let key = parsed.path();

        let container_client = self.blob_client.container_client(container);
        let blob_client = container_client.blob_client(key);
        let metadata = blob_client
            .get_properties()
            .await
            .context(UnableToOpenFileSnafu::<String> { path: uri.into() })?;
        Ok(metadata.blob.properties.content_length as usize)
    }

    // path can be root (buckets) or path within a bucket.
    async fn ls(
        &self,
        path: &str,
        delimiter: Option<&str>,
        _continuation_token: Option<&str>,
    ) -> super::Result<LSResult> {
        let parsed = url::Url::parse(path).with_context(|_| InvalidUrlSnafu { path })?;
        let delimiter = delimiter.unwrap_or("/");

        // "Container" is Azure's name for Bucket.
        let container = parsed.host_str();

        match container {
            // List containers.
            None => self._list_containers().await,
            // List a path within a container.
            Some(s) => todo!(),
        }
    }
}
