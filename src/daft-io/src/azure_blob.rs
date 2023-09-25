use async_trait::async_trait;
use azure_storage::prelude::*;
use azure_storage_blobs::{
    container::{operations::BlobItem, Container},
    prelude::*,
};
use futures::{StreamExt, TryStreamExt};
use snafu::{IntoError, ResultExt, Snafu};
use std::{collections::HashSet, num::ParseIntError, ops::Range, string::FromUtf8Error, sync::Arc};

use crate::{
    object_io::{FileMetadata, FileType, LSResult, ObjectSource},
    GetResult,
};
use common_io_config::AzureConfig;

#[derive(Debug, Snafu)]
enum Error {
    // Input parsing errors.
    #[snafu(display("Unable to parse URL: \"{}\"", path))]
    InvalidUrl {
        path: String,
        source: url::ParseError,
    },
    #[snafu(display(
        "Unable to parse data as Utf8 while reading header for file: {path}. {source}"
    ))]
    UnableToParseUtf8 { path: String, source: FromUtf8Error },

    #[snafu(display(
        "Unable to parse data as Integer while reading header for file: {path}. {source}"
    ))]
    UnableToParseInteger { path: String, source: ParseIntError },

    // Generic client errors.
    #[snafu(display("Azure Storage Account not set and is required.\n Set either `AzureConfig.storage_account` or the `AZURE_STORAGE_ACCOUNT` environment variable."))]
    StorageAccountNotSet,
    #[snafu(display("Unable to create Azure Client {}", source))]
    UnableToCreateClient { source: azure_storage::Error },
    #[snafu(display("Azure client generic error: {}", source))]
    AzureGenericError { source: azure_storage::Error },

    // Parameterized client errors.\
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

    #[snafu(display("Unable to read data from {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: azure_storage::Error,
    },

    #[snafu(display("Unable to read metadata about {}: {}", path, source))]
    RequestFailedForPath {
        path: String,
        source: azure_storage::Error,
    },

    #[snafu(display("Not Found: \"{}\"", path))]
    NotFound { path: String },

    #[snafu(display("Unable to access container: {}: {}", path, source))]
    ContainerAccessError {
        path: String,
        source: azure_storage::Error,
    },
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
            NotFound { ref path } => super::Error::NotFound {
                path: path.into(),
                source: error.into(),
            },
            _ => super::Error::Generic {
                store: super::SourceType::AzureBlob,
                source: error.into(),
            },
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

    async fn _list_containers(&self, protocol: &str) -> super::Result<LSResult> {
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
        let containers = responses_stream
            .try_collect::<Vec<_>>()
            .await
            .with_context(|_| AzureGenericSnafu {})?
            .iter()
            .flat_map(|resp| &resp.containers)
            .map(|container| self._container_to_file_metadata(protocol, container))
            .collect::<Vec<_>>();

        Ok(LSResult {
            files: containers,
            continuation_token: None,
        })
    }

    async fn _list_directory(
        &self,
        protocol: &str,
        container_name: &str,
        prefix: &str,
        delimiter: &str,
    ) -> super::Result<LSResult> {
        let container_client = self.blob_client.container_client(container_name);

        // Blob stores expose listing by prefix and delimiter,
        // but this is not the exact same as a unix-like LS behaviour
        // (e.g. /somef is a prefix of /somefile, but you cannot ls /somef)
        // To use prefix listing as LS, we need to ensure the path given is exactly a directory or a file, not a prefix.

        // It turns out Azure list_blobs("path/") will match both a file at "path" and a folder at "path/", which is exactly what we need.
        let prefix_with_delimiter = format!("{}{delimiter}", prefix.trim_end_matches(delimiter));
        let full_path = format!("{}://{}{}", protocol, container_name, prefix);
        let full_path_with_trailing_delimiter = format!(
            "{}://{}{}",
            protocol, container_name, &prefix_with_delimiter
        );

        let results = container_client
            .list_blobs()
            .delimiter(delimiter.to_string())
            .prefix(prefix_with_delimiter.clone())
            .into_stream()
            .try_collect::<Vec<_>>()
            .await
            .with_context(|_| RequestFailedForPathSnafu {
                path: &full_path_with_trailing_delimiter,
            })?
            .iter()
            .flat_map(|resp| &resp.blobs.items)
            .map(|blob_item| self._blob_item_to_file_metadata(protocol, container_name, blob_item))
            .collect::<Vec<_>>();

        match &results[..] {
            [] => {
                // If an empty list is returned, we need to check whether the prefix actually exists and has nothing after it
                // or if it is a nonexistent prefix.
                // (Azure does not return marker files for empty directories.)

                let prefix_exists = match prefix {
                    "" | "/" => true,
                    _ => {
                        // To check whether the prefix actually exists, check whether it exists as a result one directory above.
                        let upper_dir = prefix // "/upper/blah/"
                            .trim_end_matches(delimiter) // "/upper/blah"
                            .trim_end_matches(|c: char| c.to_string() != delimiter); // "/upper/"

                        let upper_results = container_client
                            .list_blobs()
                            .delimiter(delimiter.to_string())
                            .prefix(upper_dir.to_string())
                            .into_stream()
                            .try_collect::<Vec<_>>()
                            .await
                            .with_context(|_| RequestFailedForPathSnafu {
                                path: format!("{}://{}{}", protocol, container_name, upper_dir),
                            })?
                            .iter()
                            .flat_map(|resp| &resp.blobs.items)
                            .map(|blob_item| {
                                self._blob_item_to_file_metadata(
                                    protocol,
                                    container_name,
                                    blob_item,
                                )
                                .filepath
                            })
                            .collect::<HashSet<_>>();
                        upper_results.contains(&full_path_with_trailing_delimiter)
                    }
                };

                if prefix_exists {
                    Ok(LSResult {
                        files: vec![],
                        continuation_token: None,
                    })
                } else {
                    Err(Error::NotFound { path: full_path }.into())
                }
            }
            [result] => {
                // Azure prefixing does not differentiate between directories and files even if the trailing slash is provided.
                // This returns incorrect results when we asked for a directory and got a file.
                // (The other way around is okay - we can ask for a path without a trailing slash and get a directory with a trailing slash.)
                // If we get a single result, we need to check whether we asked for a directory but got a file, in which case the requested directory does not exist.

                if full_path.len() > result.filepath.len() {
                    Err(Error::NotFound { path: full_path }.into())
                } else {
                    Ok(LSResult {
                        files: results,
                        continuation_token: None,
                    })
                }
            }
            _ => Ok(LSResult {
                files: results,
                continuation_token: None,
            }),
        }
    }

    fn _container_to_file_metadata(&self, protocol: &str, container: &Container) -> FileMetadata {
        // NB: Cannot pass through to Azure client's .url() methods here
        // because they return URIs of the form https://.../container/path.
        FileMetadata {
            filepath: format!("{protocol}://{}", &container.name),
            size: None,
            filetype: FileType::Directory,
        }
    }

    fn _blob_item_to_file_metadata(
        &self,
        protocol: &str,
        container_name: &str,
        blob_item: &BlobItem,
    ) -> FileMetadata {
        match blob_item {
            BlobItem::Blob(blob) => FileMetadata {
                filepath: format!("{protocol}://{}/{}", container_name, &blob.name),
                size: Some(blob.properties.content_length),
                filetype: FileType::File,
            },
            BlobItem::BlobPrefix(prefix) => FileMetadata {
                filepath: format!("{protocol}://{}/{}", container_name, &prefix.name),
                size: None,
                filetype: FileType::Directory,
            },
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

    // path can be root (buckets) or path prefix within a bucket.
    async fn ls(
        &self,
        path: &str,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
    ) -> super::Result<LSResult> {
        let parsed = url::Url::parse(path).with_context(|_| InvalidUrlSnafu { path })?;
        let delimiter = delimiter.unwrap_or("/");

        // It looks like the azure rust library API
        // does not currently allow using the continuation token:
        // https://docs.rs/azure_storage_blobs/0.15.0/azure_storage_blobs/container/operations/list_blobs/struct.ListBlobsBuilder.html
        // https://docs.rs/azure_storage_blobs/0.15.0/azure_storage_blobs/service/operations/struct.ListContainersBuilder.html
        // https://docs.rs/azure_core/0.15.0/azure_core/struct.Pageable.html
        assert!(
            continuation_token.is_none(),
            "unexpected Azure continuation_token {:?} received",
            continuation_token
        );

        // "Container" is Azure's name for Bucket.
        let container = {
            // fsspec supports two URI formats are supported; for compatibility, we will support both as well.
            // PROTOCOL://container/path-part/file
            // PROTOCOL://container@account.dfs.core.windows.net/path-part/file
            // See https://github.com/fsspec/adlfs/ for more details
            let username = parsed.username();
            match username {
                "" => parsed.host_str(),
                _ => Some(username),
            }
        };

        // fsspec supports multiple URI protocol strings for Azure: az:// and abfs://.
        // NB: It's unclear if there is a semantic difference between the protocols
        // or if there is a standard for the behaviour either;
        // here, we will treat them both the same, but persist whichever protocol string was used.
        let protocol = parsed.scheme();

        match container {
            // List containers.
            None => self._list_containers(protocol).await,
            // List a path within a container.
            Some(container_name) => {
                let prefix = parsed.path();
                self._list_directory(protocol, container_name, prefix, delimiter)
                    .await
            }
        }
    }
}
