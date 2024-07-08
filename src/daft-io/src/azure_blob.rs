use async_trait::async_trait;
use azure_core::{auth::TokenCredential, new_http_client};
use azure_identity::{ClientSecretCredential, DefaultAzureCredential};
use azure_storage::{prelude::*, CloudLocation};
use azure_storage_blobs::{
    blob::operations::GetBlobResponse,
    container::{operations::BlobItem, Container},
    prelude::*,
};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use snafu::{IntoError, ResultExt, Snafu};
use std::{ops::Range, sync::Arc};

use crate::{
    object_io::{FileMetadata, FileType, LSResult, ObjectSource},
    stats::IOStatsRef,
    stream_utils::io_stats_on_bytestream,
    GetResult,
};
use common_io_config::AzureConfig;

const AZURE_DELIMITER: &str = "/";
const DEFAULT_GLOB_FANOUT_LIMIT: usize = 1024;
const AZURE_STORAGE_RESOURCE: &str = "https://storage.azure.com";
const AZURE_STORE_SUFFIX: &str = ".dfs.core.windows.net";

#[derive(Debug, Snafu)]
enum Error {
    // Input errors.
    #[snafu(display("Unable to parse URL: \"{}\"", path))]
    InvalidUrl {
        path: String,
        source: url::ParseError,
    },
    #[snafu(display("Continuation tokens are not supported: \"{}\"", token))]
    ContinuationToken { token: String },

    // Generic client errors.
    #[snafu(display("Azure Storage Account not set and is required.\n Set either `AzureConfig.storage_account` or the `AZURE_STORAGE_ACCOUNT` environment variable."))]
    StorageAccountNotSet,
    #[snafu(display("Azure client generic error: {}", source))]
    AzureGeneric { source: azure_storage::Error },
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

    #[snafu(display("Not a File: \"{}\"", path))]
    NotAFile { path: String },
}

/// Parse an Azure URI into its components.
/// Returns (protocol, (container, key) if exists).
fn parse_azure_uri(uri: &str) -> super::Result<(String, Option<(String, String)>)> {
    let uri = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { path: uri })?;

    // "Container" is Azure's name for Bucket.
    //
    // fsspec supports two URI formats; for compatibility, we will support both as well.
    // PROTOCOL://container/path-part/file
    // PROTOCOL://container@account.dfs.core.windows.net/path-part/file
    // See https://github.com/fsspec/adlfs/ for more details
    //
    // It also supports PROTOCOL://account.dfs.core.windows.net/container/path-part/file
    // but it is not documented
    // https://github.com/fsspec/adlfs/blob/5c24b2e886fc8e068a313819ce3db9b7077c27e3/adlfs/spec.py#L364
    let username = uri.username();
    let container_and_key = if username.is_empty() {
        match uri.host_str() {
            Some(host) if host.ends_with(AZURE_STORE_SUFFIX) => uri
                .path()
                .split_once('/')
                .map(|(c, k)| (c.into(), k.into())),
            Some(host) => Some((host.into(), uri.path().into())),
            None => None,
        }
    } else {
        Some((username.into(), uri.path().into()))
    };

    // fsspec supports multiple URI protocol strings for Azure: az:// and abfs://.
    // NB: It's unclear if there is a semantic difference between the protocols
    // or if there is a standard for the behaviour either;
    // here, we will treat them both the same, but persist whichever protocol string was used.
    let protocol = uri.scheme();

    Ok((protocol.into(), container_and_key))
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
            NotAFile { path } => super::Error::NotAFile { path },
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
            // TODO(Clark): Allow no storage account + anonymous access + custom endpoint.
            return Err(Error::StorageAccountNotSet.into());
        };

        let access_key = config
            .access_key
            .clone()
            .or_else(|| std::env::var("AZURE_STORAGE_KEY").ok());
        let sas_token = config
            .sas_token
            .clone()
            .or_else(|| std::env::var("AZURE_STORAGE_SAS_TOKEN").ok());
        let bearer_token = config
            .bearer_token
            .clone()
            .or_else(|| std::env::var("AZURE_STORAGE_TOKEN").ok());

        let storage_credentials = if config.anonymous {
            StorageCredentials::anonymous()
        } else if let Some(access_key) = access_key {
            StorageCredentials::access_key(&storage_account, access_key)
        } else if let Some(sas_token) = sas_token {
            StorageCredentials::sas_token(sas_token)
                .map_err(|e| Error::AzureGeneric { source: e })?
        } else if let Some(bearer_token) = bearer_token {
            StorageCredentials::bearer_token(bearer_token)
        } else if let Some(tenant_id) = &config.tenant_id
            && let Some(client_id) = &config.client_id
            && let Some(client_secret) = &config.client_secret
        {
            StorageCredentials::token_credential(Arc::new(ClientSecretCredential::new(
                new_http_client(),
                tenant_id.clone(),
                client_id.clone(),
                client_secret.clone(),
                Default::default(),
            )))
        } else {
            let default_creds = Arc::new(DefaultAzureCredential::default());

            if default_creds
                .get_token(AZURE_STORAGE_RESOURCE)
                .await
                .is_ok()
            {
                StorageCredentials::token_credential(default_creds)
            } else {
                log::warn!("Azure credentials resolution failed. Defaulting to anonymous mode.");
                StorageCredentials::anonymous()
            }
        };
        let endpoint_url = if let Some(endpoint_url) = &config.endpoint_url {
            Some(endpoint_url.clone())
        } else if let Ok(endpoint_url) = std::env::var("AZURE_ENDPOINT_URL") {
            Some(endpoint_url)
        } else {
            None
        };
        let blob_client = if let Some(endpoint_url) = endpoint_url {
            ClientBuilder::with_location(
                CloudLocation::Custom { uri: endpoint_url },
                storage_credentials,
            )
            .blob_service_client()
        } else if config.use_fabric_endpoint {
            ClientBuilder::with_location(
                CloudLocation::Custom {
                    uri: format!("https://{}.blob.fabric.microsoft.com", storage_account),
                },
                storage_credentials,
            )
            .blob_service_client()
        } else {
            BlobServiceClient::new(storage_account, storage_credentials)
        };

        Ok(AzureBlobSource {
            blob_client: blob_client.into(),
        }
        .into())
    }

    async fn list_containers_stream(
        &self,
        protocol: &str,
        io_stats: Option<IOStatsRef>,
    ) -> BoxStream<super::Result<FileMetadata>> {
        let protocol = protocol.to_string();

        // Paginated stream of results from Azure API call.
        let responses_stream = self
            .blob_client
            .clone()
            .list_containers()
            .include_metadata(true)
            .into_stream();

        // Flatmap each page of results to a single stream of our standardized FileMetadata.
        responses_stream
            .map(move |response| {
                if let Some(is) = io_stats.clone() {
                    is.mark_list_requests(1)
                }
                (response, protocol.clone())
            })
            .flat_map(move |(response, protocol)| match response {
                Ok(response) => {
                    let containers = response.containers.into_iter().map(move |container| {
                        Ok(self._container_to_file_metadata(protocol.as_str(), &container))
                    });
                    futures::stream::iter(containers).boxed()
                }
                Err(error) => {
                    let error = Err(Error::AzureGeneric { source: error }.into());
                    futures::stream::iter(vec![error]).boxed()
                }
            })
            .boxed()
    }

    async fn list_directory_stream(
        &self,
        protocol: &str,
        container_name: &str,
        prefix: &str,
        posix: bool,
        io_stats: Option<IOStatsRef>,
    ) -> BoxStream<super::Result<FileMetadata>> {
        let container_client = self.blob_client.container_client(container_name);

        // Clone and own some references that we need for the lifetime of the stream.
        let protocol = protocol.to_string();
        let container_name = container_name.to_string();
        let prefix = prefix.to_string();

        // Blob stores expose listing by prefix and delimiter,
        // but this is not the exact same as a unix-like LS behaviour
        // (e.g. /somef is a prefix of /somefile, but you cannot ls /somef)
        // To use prefix listing as LS, we need to ensure the path given is exactly a directory or a file, not a prefix.

        // It turns out Azure list_blobs("path/") will match both a file at "path" and a folder at "path/", which is exactly what we need.
        let prefix_with_delimiter = format!(
            "{}{AZURE_DELIMITER}",
            prefix.trim_end_matches(&AZURE_DELIMITER)
        );
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
                &posix,
                io_stats.clone(),
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
                                .trim_end_matches(&AZURE_DELIMITER) // "/upper/blah"
                                .trim_end_matches(|c: char| c.to_string() != AZURE_DELIMITER); // "/upper/"

                            let upper_results_stream = self._list_directory_delimiter_stream(
                                &container_client,
                                &protocol,
                                &container_name,
                                upper_dir,
                                &posix,
                                io_stats.clone()
                            ).await;

                            // At this point, we have a stream of Result<FileMetadata>.
                            // We would like to stop as soon as there is a file match,
                            // or if there is an error.
                            upper_results_stream
                                .map_ok(|file_info| (file_info.filepath == full_path_with_trailing_delimiter))
                                .try_skip_while(|is_match| futures::future::ready(Ok(!is_match)))
                                .try_next()
                                .await?
                                .is_some()
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
        posix: &bool,
        io_stats: Option<IOStatsRef>,
    ) -> BoxStream<super::Result<FileMetadata>> {
        // Calls Azure list_blobs with the prefix
        // and returns the result flattened and standardized into FileMetadata.

        // Clone and own some references that we need for the lifetime of the stream.
        let protocol = protocol.to_string();
        let container_name = container_name.to_string();
        let prefix = prefix.to_string();

        // Paginated response stream from Azure API.
        let mut responses_stream = container_client.list_blobs().prefix(prefix.clone());

        // Setting delimiter will trigger "directory-mode" which is a posix-like ls for the current directory
        if *posix {
            responses_stream = responses_stream.delimiter(AZURE_DELIMITER.to_string());
        }

        let responses_stream = responses_stream.into_stream();

        // Map each page of results to a page of standardized FileMetadata.
        responses_stream
            .map(move |response| {
                if let Some(is) = io_stats.clone() {
                    is.mark_list_requests(1)
                }
                (response, protocol.clone(), container_name.clone())
            })
            .flat_map(move |(response, protocol, container_name)| match response {
                Ok(response) => {
                    let paths_data =
                        response.blobs.items.into_iter().map(move |blob_item| {
                            Ok(self._blob_item_to_file_metadata(
                                &protocol,
                                &container_name,
                                &blob_item,
                            ))
                        });
                    futures::stream::iter(paths_data).boxed()
                }
                Err(error) => {
                    let error = Err(Error::RequestFailedForPath {
                        path: format!("{}://{}{}", &protocol, &container_name, &prefix),
                        source: error,
                    }
                    .into());
                    futures::stream::iter(vec![error]).boxed()
                }
            })
            .boxed()
    }

    fn _container_to_file_metadata(&self, protocol: &str, container: &Container) -> FileMetadata {
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
    async fn get(
        &self,
        uri: &str,
        range: Option<Range<usize>>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        let (_, container_and_key) = parse_azure_uri(uri)?;
        let (container, key) = container_and_key.ok_or_else(|| Error::InvalidUrl {
            path: uri.into(),
            source: url::ParseError::EmptyHost,
        })?;

        if key.is_empty() {
            return Err(Error::NotAFile { path: uri.into() }.into());
        }

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
            .and_then(async move |v: GetBlobResponse| v.data.collect().await)
            .map_err(move |e| {
                UnableToReadBytesSnafu::<String> {
                    path: owned_string.clone(),
                }
                .into_error(e)
                .into()
            });
        if let Some(is) = io_stats.as_ref() {
            is.mark_get_requests(1)
        }
        Ok(GetResult::Stream(
            io_stats_on_bytestream(Box::pin(stream), io_stats),
            None,
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
        todo!("PUTs to Azure blob store are not yet supported! Please file an issue.");
    }

    async fn get_size(&self, uri: &str, io_stats: Option<IOStatsRef>) -> super::Result<usize> {
        let (_, container_and_key) = parse_azure_uri(uri)?;
        let (container, key) = container_and_key.ok_or_else(|| Error::InvalidUrl {
            path: uri.into(),
            source: url::ParseError::EmptyHost,
        })?;

        if key.is_empty() {
            return Err(Error::NotAFile { path: uri.into() }.into());
        }

        let container_client = self.blob_client.container_client(container);
        let blob_client = container_client.blob_client(key);
        let metadata = blob_client
            .get_properties()
            .await
            .context(UnableToOpenFileSnafu::<String> { path: uri.into() })?;
        if let Some(is) = io_stats.as_ref() {
            is.mark_head_requests(1)
        }

        Ok(metadata.blob.properties.content_length as usize)
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<BoxStream<'static, super::Result<FileMetadata>>> {
        use crate::object_store_glob::glob;

        // Ensure fanout_limit is not None to prevent runaway concurrency
        let fanout_limit = fanout_limit.or(Some(DEFAULT_GLOB_FANOUT_LIMIT));

        glob(
            self,
            glob_path,
            fanout_limit,
            page_size.or(Some(1000)),
            limit,
            io_stats,
        )
        .await
    }

    async fn iter_dir(
        &self,
        uri: &str,
        posix: bool,
        _page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<BoxStream<super::Result<FileMetadata>>> {
        let (protocol, container_and_key) = parse_azure_uri(uri)?;

        match container_and_key {
            // List containers.
            None => Ok(self
                .list_containers_stream(protocol.as_str(), io_stats)
                .await),
            // List a path within a container.
            Some((container_name, key)) => Ok(self
                .list_directory_stream(
                    protocol.as_str(),
                    container_name.as_str(),
                    key.as_str(),
                    posix,
                    io_stats,
                )
                .await),
        }
    }

    async fn ls(
        &self,
        path: &str,
        posix: bool,
        continuation_token: Option<&str>,
        _page_size: Option<i32>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<LSResult> {
        // It looks like the azure rust library API
        // does not currently allow using the continuation token:
        // https://docs.rs/azure_storage_blobs/0.15.0/azure_storage_blobs/container/operations/list_blobs/struct.ListBlobsBuilder.html
        // https://docs.rs/azure_storage_blobs/0.15.0/azure_storage_blobs/service/operations/struct.ListContainersBuilder.html
        // https://docs.rs/azure_core/0.15.0/azure_core/struct.Pageable.html
        match continuation_token {
            None => Ok(()),
            Some(token) => Err(Error::ContinuationToken {
                token: token.to_string(),
            }),
        }?;

        let files = self
            .iter_dir(path, posix, None, io_stats)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        Ok(LSResult {
            files,
            continuation_token: None,
        })
    }
}
