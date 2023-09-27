use async_trait::async_trait;
use azure_storage::prelude::*;
use azure_storage_blobs::{
    container::{operations::BlobItem, Container},
    prelude::*,
};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
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

    // Parameterized client errors.
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

    async fn list_containers_stream(
        &self,
        protocol: String,
    ) -> BoxStream<super::Result<FileMetadata>> {
        // Paginated stream of results from Azure API call.
        let responses_stream = self
            .blob_client
            .clone()
            .list_containers()
            .include_metadata(true)
            .into_stream();

        // Flatmap each page of results to a single stream of our standardized FileMetadata.
        responses_stream
            .flat_map(move |response| match response {
                Ok(response) => {
                    let containers = response
                        .containers
                        .iter()
                        .map(|container| {
                            Ok(self._container_to_file_metadata(protocol.clone(), container))
                        })
                        .collect::<Vec<_>>();
                    futures::stream::iter(containers)
                }
                Err(error) => {
                    let error = Err(Error::AzureGenericError { source: error }.into());
                    futures::stream::iter(vec![error])
                }
            })
            .boxed()
    }

    async fn list_directory_stream(
        &self,
        protocol: &str,
        container_name: &str,
        prefix: &str,
        delimiter: &str,
    ) -> BoxStream<super::Result<FileMetadata>> {
        let container_client = self.blob_client.container_client(container_name);

        // Clone and own some references that we need for the lifetime of the stream.
        let protocol = protocol.to_string();
        let container_name = container_name.to_string();
        let prefix = prefix.to_string();
        let delimiter = delimiter.to_string();

        // Blob stores expose listing by prefix and delimiter,
        // but this is not the exact same as a unix-like LS behaviour
        // (e.g. /somef is a prefix of /somefile, but you cannot ls /somef)
        // To use prefix listing as LS, we need to ensure the path given is exactly a directory or a file, not a prefix.

        // It turns out Azure list_blobs("path/") will match both a file at "path" and a folder at "path/", which is exactly what we need.
        let prefix_with_delimiter = format!("{}{delimiter}", prefix.trim_end_matches(&delimiter));
        let full_path = format!("{}://{}{}", protocol, container_name, prefix);
        let full_path_with_trailing_delimiter = format!(
            "{}://{}{}",
            protocol, container_name, &prefix_with_delimiter
        );

        let mut unchecked_results = self
            ._list_directory_delimiter_stream(
                &container_client,
                &protocol,
                &container_name,
                &prefix_with_delimiter,
                &delimiter,
            )
            .await;

        // The Azure API call result is almost identical to the desired list directory result
        // with a couple of exceptions:
        // 1. Nonexistent paths return an empty list instead of 404.
        // 2. A file "path" can be returned for a lookup "path/", which is not the desired behaviour.
        //
        // To check for and deal with these cases,
        // manually process the first two items of the stream.

        let mut maybe_first_two_items = vec![];
        let mut stream_exhausted = false;
        for _ in 0..2 {
            let item = unchecked_results.next().await;
            if let Some(item) = item {
                maybe_first_two_items.push(item);
            } else {
                stream_exhausted = true;
                break;
            }
        }

        // Make sure the stream is pollable even if empty,
        // since we will chain it later with the two items we already popped.
        let unchecked_results = if !stream_exhausted {
            unchecked_results
        } else {
            futures::stream::iter(vec![]).boxed()
        };

        match &maybe_first_two_items[..] {
            [] => {
                // Check whether the path actually exists.
                let s = async_stream::stream! {
                    let prefix_exists = match prefix.as_str() {
                        "" | "/" => true,
                        _ => {
                            // To check whether the prefix actually exists, check whether it exists as a result one directory above.
                            // (Azure does not return marker files for empty directories.)
                            let upper_dir = prefix // "/upper/blah/"
                                .trim_end_matches(&delimiter) // "/upper/blah"
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
                                        &protocol,
                                        &container_name,
                                        blob_item,
                                    )
                                    .filepath
                                })
                                .collect::<HashSet<_>>();

                            upper_results.contains(&full_path_with_trailing_delimiter)
                        }
                    };
                    // If the prefix does not exist, the stream needs to yield a single NotFound error.
                    // Otherwise, it is a truly empty directory and we return an empty stream.
                    if !prefix_exists {
                        yield Err(Error::NotFound { path: full_path }.into())
                    }
                };
                s.boxed()
            }
            [Ok(item)] => {
                // If we get a single result, we need to check whether we asked for a directory but got a file,
                // in which case the requested directory does not actually exist.

                if full_path.len() > item.filepath.len() {
                    let error = Err(Error::NotFound { path: full_path }.into());
                    futures::stream::iter(vec![error]).boxed()
                } else {
                    futures::stream::iter(maybe_first_two_items)
                        .chain(unchecked_results)
                        .boxed()
                }
            }
            _ => futures::stream::iter(maybe_first_two_items)
                .chain(unchecked_results)
                .boxed(),
        }
    }

    async fn _list_directory_delimiter_stream(
        &self,
        container_client: &ContainerClient,
        protocol: &str,
        container_name: &str,
        prefix: &str,
        delimiter: &str,
    ) -> BoxStream<super::Result<FileMetadata>> {
        // Calls Azure list_blobs with the prefix
        // and returns the result flattened and standardized into FileMetadata.

        // Clone and own some references that we need for the lifetime of the stream.
        let protocol = protocol.to_string();
        let container_name = container_name.to_string();
        let prefix = prefix.to_string();

        // Paginated response stream from Azure API.
        let responses_stream = container_client
            .list_blobs()
            .delimiter(delimiter.to_string())
            .prefix(prefix.clone())
            .into_stream();

        // Map each page of results to a page of standardized FileMetadata.
        responses_stream
            .flat_map(move |response| match response {
                Ok(response) => {
                    let paths_data = response
                        .blobs
                        .items
                        .iter()
                        .map(|blob_item| {
                            Ok(self._blob_item_to_file_metadata(
                                &protocol,
                                &container_name,
                                blob_item,
                            ))
                        })
                        .collect::<Vec<_>>();
                    futures::stream::iter(paths_data)
                }
                Err(error) => {
                    let error = Err(Error::RequestFailedForPath {
                        path: format!("{}://{}{}", &protocol, &container_name, &prefix),
                        source: error,
                    }
                    .into());
                    futures::stream::iter(vec![error])
                }
            })
            .boxed()
    }

    fn _container_to_file_metadata(&self, protocol: String, container: &Container) -> FileMetadata {
        // NB: Cannot pass through to Azure client's .url() methods here
        // because they return URIs of a very different format (https://.../container/path).
        FileMetadata {
            filepath: format!("{protocol}://{}/", &container.name),
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

    async fn iter_dir(
        &self,
        uri: &str,
        delimiter: Option<&str>,
        _limit: Option<usize>,
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        let uri = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { path: uri })?;
        let delimiter = delimiter.unwrap_or("/");

        // path can be root (buckets) or path prefix within a bucket.
        let container = {
            // "Container" is Azure's name for Bucket.
            //
            // fsspec supports two URI formats; for compatibility, we will support both as well.
            // PROTOCOL://container/path-part/file
            // PROTOCOL://container@account.dfs.core.windows.net/path-part/file
            // See https://github.com/fsspec/adlfs/ for more details
            let username = uri.username();
            match username {
                "" => uri.host_str(),
                _ => Some(username),
            }
        };

        // fsspec supports multiple URI protocol strings for Azure: az:// and abfs://.
        // NB: It's unclear if there is a semantic difference between the protocols
        // or if there is a standard for the behaviour either;
        // here, we will treat them both the same, but persist whichever protocol string was used.
        let protocol = uri.scheme();

        match container {
            // List containers.
            None => Ok(self.list_containers_stream(protocol.to_string()).await),
            // List a path within a container.
            Some(container_name) => {
                let prefix = uri.path();
                Ok(self
                    .list_directory_stream(protocol, container_name, prefix, delimiter)
                    .await)
            }
        }
    }

    async fn ls(
        &self,
        path: &str,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
    ) -> super::Result<LSResult> {
        // It looks like the azure rust library API
        // does not currently allow using the continuation token:
        // https://docs.rs/azure_storage_blobs/0.15.0/azure_storage_blobs/container/operations/list_blobs/struct.ListBlobsBuilder.html
        // https://docs.rs/azure_storage_blobs/0.15.0/azure_storage_blobs/service/operations/struct.ListContainersBuilder.html
        // https://docs.rs/azure_core/0.15.0/azure_core/struct.Pageable.html
        assert!(
            continuation_token.is_none(),
            "Azure continuation_token {:?} received, which we cannot use",
            continuation_token
        );

        let files = self
            .iter_dir(path, delimiter, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        Ok(LSResult {
            files,
            continuation_token: None,
        })
    }
}
