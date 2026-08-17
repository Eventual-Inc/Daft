use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use azure_core::{
    credentials::{Secret, TokenCredential},
    http::{ClientOptions, StatusCode, Url, policies::Policy},
};
use azure_identity::{
    ClientSecretCredential, DeveloperToolsCredential, ManagedIdentityCredential,
    WorkloadIdentityCredential,
};
use azure_storage_blob::{
    BlobClient, BlobClientOptions, BlobServiceClient, BlobServiceClientOptions,
    models::{
        BlobClientDownloadOptions, BlobClientGetPropertiesResultHeaders,
        BlobContainerClientListBlobsHierarchicalOptions, BlobContainerClientListBlobsOptions,
        BlobItem, ContainerItem, HttpRange,
    },
};
use common_io_config::AzureConfig;
use common_runtime::get_io_pool_num_threads;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use snafu::{IntoError, ResultExt, Snafu};

use crate::{
    FileFormat, GetResult, InvalidRangeRequestSnafu,
    azure_auth::{SasTokenPolicy, SharedKeyAuthorizationPolicy, StaticBearerCredential},
    object_io::{FileMetadata, FileType, LSResult, ObjectSource},
    range::GetRange,
    stats::IOStatsRef,
    stream_utils::io_stats_on_bytestream,
};

const AZURE_DELIMITER: &str = "/";
const DEFAULT_GLOB_FANOUT_LIMIT: usize = 1024;
const AZURE_STORAGE_RESOURCE: &str = "https://storage.azure.com/.default";
const AZURE_STORE_SUFFIX: &str = ".dfs.core.windows.net";
/// Pin below the GA crate default (`2026-04-06`). Azurite rejects newer
/// `x-ms-version` values with HTTP 400 (`InvalidHeaderValue`). Azure Storage
/// still accepts this version, and delimiter listing does not need the latest.
const AZURE_REST_API_VERSION: &str = "2025-01-05";

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
    #[snafu(display(
        "Azure Storage Account not set and is required.\n Set either `AzureConfig.storage_account` or the `AZURE_STORAGE_ACCOUNT` environment variable."
    ))]
    StorageAccountNotSet,
    #[snafu(display("Azure client generic error: {}", source))]
    AzureGeneric { source: azure_core::Error },
    #[snafu(display("Unable to open {}: {}", path, source))]
    UnableToOpenFile {
        path: String,
        source: azure_core::Error,
    },

    #[snafu(display("Unable to read data from {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: azure_core::Error,
    },

    #[snafu(display("Unable to read metadata about {}: {}", path, source))]
    RequestFailedForPath {
        path: String,
        source: azure_core::Error,
    },

    #[snafu(display("Unable to parse Azure list-blobs XML for {}: {}", path, reason))]
    InvalidListBlobsXml { path: String, reason: String },

    #[snafu(display("Not Found: \"{}\"", path))]
    NotFound { path: String },

    #[snafu(display("Not a File: \"{}\"", path))]
    NotAFile { path: String },

    #[snafu(display("Unable to grab semaphore. {}", source))]
    UnableToGrabSemaphore { source: tokio::sync::AcquireError },
}

struct ParsedAzureUri {
    pub protocol: String,
    pub account_name: Option<String>,
    pub container_and_key: Option<(String, String)>,
}

impl ParsedAzureUri {
    fn new(protocol: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
            account_name: None,
            container_and_key: None,
        }
    }
}

/// Parse an Azure URI into its components.
/// Returns (protocol, (container, key) if exists, storage account if exists).
fn parse_azure_uri(uri: &str) -> super::Result<ParsedAzureUri> {
    let uri = url::Url::parse(uri).with_context(|_| InvalidUrlSnafu { path: uri })?;

    let mut parsed = ParsedAzureUri::new(uri.scheme());

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
    if let Some(host) = uri.host_str() {
        if host.ends_with(AZURE_STORE_SUFFIX) {
            match uri.username() {
                "" => {
                    if let Some((container, key)) = uri.path().split_once('/') {
                        parsed.container_and_key = Some((container.into(), key.into()));
                    }
                }
                username => {
                    parsed.container_and_key = Some((username.into(), uri.path().into()));
                }
            }

            let account_name_len = host.len() - AZURE_STORE_SUFFIX.len();
            parsed.account_name = Some(host[..account_name_len].into());
        } else {
            parsed.container_and_key = Some((host.into(), uri.path().into()));
        }
    }

    Ok(parsed)
}

/// Extract the HTTP status code from an `azure_core::Error`, if the error was
/// an HTTP error response.
fn core_error_status(error: &azure_core::Error) -> Option<StatusCode> {
    match error.kind() {
        azure_core::error::ErrorKind::HttpResponse { status, .. } => Some(*status),
        _ => None,
    }
}

impl From<Error> for super::Error {
    fn from(error: Error) -> Self {
        use Error::{
            NotAFile, NotFound, RequestFailedForPath, UnableToOpenFile, UnableToReadBytes,
        };
        match error {
            UnableToOpenFile { path, source }
            | UnableToReadBytes { path, source }
            | RequestFailedForPath { path, source } => {
                match core_error_status(&source).map(u16::from) {
                    Some(404 | 410) => Self::NotFound {
                        path,
                        source: source.into(),
                    },
                    Some(401) => Self::Unauthorized {
                        store: super::SourceType::AzureBlob,
                        path,
                        source: source.into(),
                    },
                    None | Some(_) => Self::UnableToOpenFile {
                        path,
                        source: source.into(),
                    },
                }
            }
            NotFound { ref path } => Self::NotFound {
                path: path.into(),
                source: error.into(),
            },
            NotAFile { path } => Self::NotAFile { path },
            _ => Self::Generic {
                store: super::SourceType::AzureBlob,
                source: error.into(),
            },
        }
    }
}

/// Resolve a default token credential the same way the old
/// `DefaultAzureCredential` did: try each candidate credential source and use
/// the first one that can produce a storage token.
async fn default_credential_chain() -> Option<Arc<dyn TokenCredential>> {
    let mut candidates: Vec<(&str, Arc<dyn TokenCredential>)> = Vec::new();
    match WorkloadIdentityCredential::new(None) {
        Ok(cred) => candidates.push(("workload identity", cred as _)),
        Err(e) => log::debug!("Azure workload identity credential unavailable: {e}"),
    }
    match ManagedIdentityCredential::new(None) {
        Ok(cred) => candidates.push(("managed identity", cred as _)),
        Err(e) => log::debug!("Azure managed identity credential unavailable: {e}"),
    }
    match DeveloperToolsCredential::new(None) {
        Ok(cred) => candidates.push(("developer tools", cred as _)),
        Err(e) => log::debug!("Azure developer tools credential unavailable: {e}"),
    }

    for (name, cred) in candidates {
        match cred
            .get_token(std::slice::from_ref(&AZURE_STORAGE_RESOURCE), None)
            .await
        {
            Ok(_) => {
                log::debug!("Using Azure {name} credential");
                return Some(cred);
            }
            Err(e) => log::debug!("Azure {name} credential failed to get a token: {e}"),
        }
    }
    None
}

fn container_to_file_metadata(protocol: &str, container: &ContainerItem) -> Option<FileMetadata> {
    container.name.as_ref().map(|name| FileMetadata {
        filepath: format!("{protocol}://{name}/"),
        size: None,
        filetype: FileType::Directory,
    })
}

fn blob_to_file_metadata(
    protocol: &str,
    container_name: &str,
    blob: &BlobItem,
) -> Option<FileMetadata> {
    blob.name.as_ref().map(|name| {
        file_metadata_for_blob(
            protocol,
            container_name,
            name,
            blob.properties.as_ref().and_then(|p| p.content_length),
        )
    })
}

fn file_metadata_for_blob(
    protocol: &str,
    container_name: &str,
    name: &str,
    size: Option<u64>,
) -> FileMetadata {
    FileMetadata {
        filepath: format!("{protocol}://{container_name}/{name}"),
        size,
        filetype: FileType::File,
    }
}

fn file_metadata_for_prefix(protocol: &str, container_name: &str, name: &str) -> FileMetadata {
    FileMetadata {
        filepath: format!("{protocol}://{container_name}/{name}"),
        size: None,
        filetype: FileType::Directory,
    }
}

struct ParsedBlobList {
    blobs: Vec<(String, Option<u64>)>,
    prefixes: Vec<String>,
}

/// Parse a List Blobs (hierarchical / delimiter) XML body.
///
/// `azure_storage_blob` 1.1.0-beta.1 deserializes `blob_prefixes` as
/// `Option<Vec<BlobPrefix>>`, which treats a second `<BlobPrefix>` sibling as a
/// duplicate field. HNS accounts also attach `<Properties>` to prefixes. Parse
/// the XML ourselves so posix listing keeps working.
fn parse_hierarchical_listing_xml(xml: &[u8]) -> Result<ParsedBlobList, String> {
    let text = std::str::from_utf8(xml).map_err(|e| format!("invalid utf-8: {e}"))?;
    let text = text.trim_start_matches('\u{feff}');

    let mut prefixes = Vec::new();
    for block in xml_blocks(text, "<BlobPrefix>", "</BlobPrefix>") {
        if let Some(name) = xml_tag_text(block, "Name") {
            prefixes.push(name);
        }
    }

    let mut blobs = Vec::new();
    for block in xml_blocks(text, "<Blob>", "</Blob>") {
        if let Some(name) = xml_tag_text(block, "Name") {
            let size = xml_tag_text(block, "Content-Length")
                .filter(|s| !s.is_empty())
                .and_then(|s| s.parse().ok());
            blobs.push((name, size));
        }
    }

    Ok(ParsedBlobList { blobs, prefixes })
}

fn xml_blocks<'a>(xml: &'a str, open: &'a str, close: &'a str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(open) {
        let after = &rest[start + open.len()..];
        match after.find(close) {
            Some(end) => {
                out.push(&after[..end]);
                rest = &after[end + close.len()..];
            }
            None => break,
        }
    }
    out
}

fn xml_tag_text(block: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let start = block.find(&open)?;
    let after_name = &block[start + open.len()..];
    let inner_start = after_name.find('>')? + 1;
    let inner = &after_name[inner_start..];
    let end = inner.find(&close)?;
    Some(unescape_xml(&inner[..end]))
}

fn unescape_xml(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

fn ensure_trailing_slash(mut url: Url) -> Url {
    let mut path = url.path().to_string();
    if !path.ends_with('/') {
        path.push('/');
        url.set_path(&path);
    }
    url
}

/// Join container + blob name onto the service URL without encoding `/` in the
/// blob name. The GA helper `BlobServiceClient::blob_client` treats the whole
/// blob name as one path segment, turning `dir/file` into `dir%2Ffile`, which
/// Azure treats as a different blob.
fn blob_url(service: &Url, container: &str, key: &str) -> Url {
    let mut url = service.clone();
    {
        let mut segments = url
            .path_segments_mut()
            .expect("Azure service URL must be a base URL");
        segments.pop_if_empty();
        if !container.is_empty() {
            segments.push(container);
        }
        for segment in key.split('/') {
            if !segment.is_empty() {
                segments.push(segment);
            }
        }
    }
    url
}

pub struct AzureBlobSource {
    blob_client: Arc<BlobServiceClient>,
    token_credential: Option<Arc<dyn TokenCredential>>,
    blob_client_options: BlobClientOptions,
    connection_pool_sema: Arc<tokio::sync::Semaphore>,
}

impl AzureBlobSource {
    pub async fn get_client(config: &AzureConfig, uri: &str) -> super::Result<Arc<Self>> {
        let parsed_uri = parse_azure_uri(uri)?;

        let storage_account = if let Some(account_name) = parsed_uri.account_name {
            account_name
        } else if let Some(storage_account) = &config.storage_account {
            storage_account.clone()
        } else if let Ok(storage_account) = std::env::var("AZURE_STORAGE_ACCOUNT") {
            storage_account
        } else {
            // TODO(Clark): Allow no storage account + anonymous access + custom endpoint.
            return Err(Error::StorageAccountNotSet.into());
        };

        let access_key = config.access_key.clone().or_else(|| {
            std::env::var("AZURE_STORAGE_KEY")
                .ok()
                .map(std::convert::Into::into)
        });
        let sas_token = config
            .sas_token
            .clone()
            .or_else(|| std::env::var("AZURE_STORAGE_SAS_TOKEN").ok());
        let bearer_token = config
            .bearer_token
            .clone()
            .or_else(|| std::env::var("AZURE_STORAGE_TOKEN").ok());

        // The GA `azure_storage_blob` SDK natively supports Entra ID token
        // credentials only. The other Daft credential paths are implemented as
        // custom pipeline policies (see `azure_auth`):
        // - access key -> `SharedKeyAuthorizationPolicy` (per-try request signing)
        // - SAS token -> `SasTokenPolicy` (per-try query param injection)
        // - static bearer token -> `StaticBearerCredential`
        let mut token_credential: Option<Arc<dyn TokenCredential>> = None;
        let mut per_try_policies: Vec<Arc<dyn Policy>> = Vec::new();

        if config.anonymous {
            // No credential.
        } else if let Some(access_key) = access_key {
            per_try_policies.push(SharedKeyAuthorizationPolicy::new(
                storage_account.clone(),
                access_key.as_string().clone(),
            ) as _);
        } else if let Some(sas_token) = sas_token {
            per_try_policies.push(SasTokenPolicy::new(&sas_token) as _);
        } else if let Some(bearer_token) = bearer_token {
            token_credential = Some(StaticBearerCredential::new(bearer_token) as _);
        } else if let Some(tenant_id) = &config.tenant_id
            && let Some(client_id) = &config.client_id
            && let Some(client_secret) = &config.client_secret
        {
            let cred = ClientSecretCredential::new(
                tenant_id,
                client_id.clone(),
                Secret::new(client_secret.as_string().clone()),
                None,
            )
            .context(AzureGenericSnafu {})?;
            token_credential = Some(cred as _);
        } else if let Some(cred) = default_credential_chain().await {
            token_credential = Some(cred);
        } else {
            log::warn!("Azure credentials resolution failed. Defaulting to anonymous mode.");
        }

        let endpoint_url = if let Some(endpoint_url) = &config.endpoint_url {
            Some(endpoint_url.clone())
        } else {
            std::env::var("AZURE_ENDPOINT_URL").ok()
        };
        let endpoint = if let Some(endpoint_url) = endpoint_url {
            endpoint_url
        } else if config.use_fabric_endpoint {
            format!("https://{storage_account}.blob.fabric.microsoft.com")
        } else {
            format!("https://{storage_account}.blob.core.windows.net")
        };
        let endpoint = Url::parse(&endpoint).with_context(|_| InvalidUrlSnafu {
            path: endpoint.clone(),
        })?;
        let endpoint = ensure_trailing_slash(endpoint);

        let client_options = ClientOptions {
            per_try_policies,
            ..Default::default()
        };
        let options = BlobServiceClientOptions {
            client_options: client_options.clone(),
            version: AZURE_REST_API_VERSION.to_string(),
        };
        let blob_client_options = BlobClientOptions {
            client_options,
            version: AZURE_REST_API_VERSION.to_string(),
        };

        let blob_client = BlobServiceClient::new(endpoint, token_credential.clone(), Some(options))
            .context(AzureGenericSnafu {})?;

        let connection_pool_sema = Arc::new(tokio::sync::Semaphore::new(
            (config.max_connections_per_io_thread.max(1) as usize)
                * get_io_pool_num_threads().expect("Should be running in tokio pool"),
        ));

        Ok(Self {
            blob_client: blob_client.into(),
            token_credential,
            blob_client_options,
            connection_pool_sema,
        }
        .into())
    }

    fn blob_client_for(&self, container: &str, key: &str) -> super::Result<BlobClient> {
        Ok(BlobClient::new(
            blob_url(self.blob_client.url(), container, key),
            self.token_credential.clone(),
            Some(self.blob_client_options.clone()),
        )
        .context(AzureGenericSnafu {})?)
    }

    async fn list_containers_stream(
        &self,
        protocol: &str,
        io_stats: Option<IOStatsRef>,
    ) -> BoxStream<'_, super::Result<FileMetadata>> {
        let protocol = protocol.to_string();
        let blob_client = self.blob_client.clone();

        let s = async_stream::stream! {
            let mut pager = match blob_client.list_containers(None) {
                Ok(pager) => pager.into_pages(),
                Err(error) => {
                    yield Err(Error::AzureGeneric { source: error }.into());
                    return;
                }
            };
            while let Some(page) = pager.next().await {
                if let Some(is) = io_stats.clone() {
                    is.mark_list_requests(1);
                }
                match page {
                    Ok(response) => match response.into_model() {
                        Ok(body) => {
                            for container in &body.container_items {
                                if let Some(file_metadata) =
                                    container_to_file_metadata(&protocol, container)
                                {
                                    yield Ok(file_metadata);
                                }
                            }
                        }
                        Err(error) => {
                            yield Err(Error::AzureGeneric { source: error }.into());
                        }
                    },
                    Err(error) => {
                        yield Err(Error::AzureGeneric { source: error }.into());
                    }
                }
            }
        };
        s.boxed()
    }

    async fn list_directory_stream(
        &self,
        protocol: &str,
        container_name: &str,
        prefix: &str,
        posix: bool,
        io_stats: Option<IOStatsRef>,
    ) -> BoxStream<'_, super::Result<FileMetadata>> {
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
        let full_path = format!("{protocol}://{container_name}{prefix}");
        let full_path_with_trailing_delimiter = format!(
            "{}://{}{}",
            protocol, container_name, &prefix_with_delimiter
        );

        let mut unchecked_results = self
            .list_directory_delimiter_stream(
                &protocol,
                &container_name,
                &prefix_with_delimiter,
                posix,
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
        let unchecked_results = if stream_exhausted {
            futures::stream::iter(vec![]).boxed()
        } else {
            unchecked_results
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

                            let upper_results_stream = self.list_directory_delimiter_stream(
                                &protocol,
                                &container_name,
                                upper_dir,
                                posix,
                                io_stats.clone()
                            ).await;

                            // At this point, we have a stream of Result<FileMetadata>.
                            // We would like to stop as soon as there is a file match,
                            // or if there is an error.
                            upper_results_stream
                                .map_ok(|file_info| file_info.filepath == full_path_with_trailing_delimiter)
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

    async fn list_directory_delimiter_stream(
        &self,
        protocol: &str,
        container_name: &str,
        prefix: &str,
        posix: bool,
        io_stats: Option<IOStatsRef>,
    ) -> BoxStream<'_, super::Result<FileMetadata>> {
        // Calls Azure list_blobs with the prefix
        // and returns the result flattened and standardized into FileMetadata.

        // Clone and own some references that we need for the lifetime of the stream.
        let protocol = protocol.to_string();
        let container_name = container_name.to_string();
        let prefix = prefix.to_string();
        let container_client = self.blob_client.blob_container_client(&container_name);

        let s = async_stream::stream! {
            let request_path = format!("{}://{}{}", &protocol, &container_name, &prefix);
            if posix {
                // Setting a delimiter triggers "directory-mode" which is a posix-like ls for the current directory.
                let options = BlobContainerClientListBlobsHierarchicalOptions {
                    prefix: Some(prefix.clone()),
                    ..Default::default()
                };
                let mut pager =
                    match container_client.list_blobs_hierarchical(AZURE_DELIMITER, Some(options)) {
                        Ok(pager) => pager,
                        Err(error) => {
                            yield Err(Error::RequestFailedForPath {
                                path: request_path,
                                source: error,
                            }
                            .into());
                            return;
                        }
                    };
                while let Some(page) = pager.next().await {
                    if let Some(is) = io_stats.clone() {
                        is.mark_list_requests(1);
                    }
                    match page {
                        Ok(response) => {
                            let body = response.into_body();
                            match parse_hierarchical_listing_xml(&body) {
                                Ok(listing) => {
                                    for name in listing.prefixes {
                                        yield Ok(file_metadata_for_prefix(
                                            &protocol,
                                            &container_name,
                                            &name,
                                        ));
                                    }
                                    for (name, size) in listing.blobs {
                                        yield Ok(file_metadata_for_blob(
                                            &protocol,
                                            &container_name,
                                            &name,
                                            size,
                                        ));
                                    }
                                }
                                Err(reason) => {
                                    yield Err(Error::InvalidListBlobsXml {
                                        path: request_path.clone(),
                                        reason,
                                    }
                                    .into());
                                }
                            }
                        }
                        Err(error) => {
                            yield Err(Error::RequestFailedForPath {
                                path: request_path.clone(),
                                source: error,
                            }
                            .into());
                        }
                    }
                }
            } else {
                let options = BlobContainerClientListBlobsOptions {
                    prefix: Some(prefix.clone()),
                    ..Default::default()
                };
                let mut pager = match container_client.list_blobs(Some(options)) {
                    Ok(pager) => pager.into_pages(),
                    Err(error) => {
                        yield Err(Error::RequestFailedForPath {
                            path: request_path,
                            source: error,
                        }
                        .into());
                        return;
                    }
                };
                while let Some(page) = pager.next().await {
                    if let Some(is) = io_stats.clone() {
                        is.mark_list_requests(1);
                    }
                    match page {
                        Ok(response) => match response.into_model() {
                            Ok(body) => {
                                for blob in body.blob_items {
                                    if let Some(file_metadata) =
                                        blob_to_file_metadata(&protocol, &container_name, &blob)
                                    {
                                        yield Ok(file_metadata);
                                    }
                                }
                            }
                            Err(error) => {
                                yield Err(Error::RequestFailedForPath {
                                    path: request_path.clone(),
                                    source: error,
                                }
                                .into());
                            }
                        },
                        Err(error) => {
                            yield Err(Error::RequestFailedForPath {
                                path: request_path.clone(),
                                source: error,
                            }
                            .into());
                        }
                    }
                }
            }
        };
        s.boxed()
    }

    /// Internal get_size that does NOT acquire the semaphore.
    /// Used by `get()` which already holds a permit (avoids deadlock for GetRange::Suffix).
    async fn get_size_internal(
        &self,
        uri: &str,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<usize> {
        let parsed_uri = parse_azure_uri(uri)?;
        let (container, key) = parsed_uri
            .container_and_key
            .ok_or_else(|| Error::InvalidUrl {
                path: uri.into(),
                source: url::ParseError::EmptyHost,
            })?;

        if key.is_empty() {
            return Err(Error::NotAFile { path: uri.into() }.into());
        }

        let blob_client = self.blob_client_for(&container, &key)?;
        let response = blob_client
            .get_properties(None)
            .await
            .context(UnableToOpenFileSnafu::<String> { path: uri.into() })?;
        if let Some(is) = io_stats.as_ref() {
            is.mark_head_requests(1);
        }

        let content_length = response
            .content_length()
            .context(RequestFailedForPathSnafu::<String> { path: uri.into() })?
            .ok_or_else(|| Error::NotAFile { path: uri.into() })?;

        Ok(content_length as usize)
    }
}

#[async_trait]
impl ObjectSource for AzureBlobSource {
    async fn supports_range(&self, _: &str) -> super::Result<bool> {
        Ok(true)
    }

    async fn get(
        &self,
        uri: &str,
        range: Option<GetRange>,
        io_stats: Option<IOStatsRef>,
    ) -> super::Result<GetResult> {
        let permit = self
            .connection_pool_sema
            .clone()
            .acquire_owned()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;

        let parsed_uri = parse_azure_uri(uri)?;
        let (container, key) = parsed_uri
            .container_and_key
            .ok_or_else(|| Error::InvalidUrl {
                path: uri.into(),
                source: url::ParseError::EmptyHost,
            })?;

        if key.is_empty() {
            return Err(Error::NotAFile { path: uri.into() }.into());
        }

        let blob_client = self.blob_client_for(&container, &key)?;
        let mut options = BlobClientDownloadOptions::default();
        if let Some(range) = range {
            range.validate().context(InvalidRangeRequestSnafu)?;
            options.range = Some(match range {
                GetRange::Bounded(u) => HttpRange::from(u),
                // Note: if n is greater than file size, Azure will return the whole content.
                GetRange::Offset(n) => HttpRange::from_offset(n as u64),
                GetRange::Suffix(n) => {
                    // Use get_size_internal to avoid deadlock (we already hold a permit)
                    let size = self.get_size_internal(uri, io_stats.clone()).await?;
                    HttpRange::from_offset(size.saturating_sub(n) as u64)
                }
            });
        }

        let response = blob_client
            .download(Some(options))
            .await
            .context(UnableToOpenFileSnafu::<String> { path: uri.into() })?;

        let owned_string = uri.to_string();
        let stream = response.body.map_err(move |e| {
            UnableToReadBytesSnafu::<String> {
                path: owned_string.clone(),
            }
            .into_error(e)
            .into()
        });
        if let Some(is) = io_stats.as_ref() {
            is.mark_get_requests(1);
        }
        Ok(GetResult::Stream(
            io_stats_on_bytestream(Box::pin(stream), io_stats),
            None,
            Some(permit),
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
        let _permit = self
            .connection_pool_sema
            .acquire()
            .await
            .context(UnableToGrabSemaphoreSnafu)?;
        self.get_size_internal(uri, io_stats).await
    }

    async fn glob(
        self: Arc<Self>,
        glob_path: &str,
        fanout_limit: Option<usize>,
        page_size: Option<i32>,
        limit: Option<usize>,
        io_stats: Option<IOStatsRef>,
        _file_format: Option<FileFormat>,
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
        let parsed_uri = parse_azure_uri(uri)?;

        match parsed_uri.container_and_key {
            // List containers.
            None => Ok(self
                .list_containers_stream(parsed_uri.protocol.as_str(), io_stats)
                .await),
            // List a path within a container.
            Some((container_name, key)) => Ok(self
                .list_directory_stream(
                    parsed_uri.protocol.as_str(),
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
        // Continuation tokens are handled internally by the SDK's page
        // iterators; resuming from an external token is not supported.
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

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

#[cfg(test)]
mod tests {
    use azure_core::http::Url;

    use super::{blob_url, ensure_trailing_slash, parse_hierarchical_listing_xml, unescape_xml};

    #[test]
    fn parses_repeated_hns_blob_prefixes() {
        // HNS accounts emit multiple <BlobPrefix> siblings, each with <Properties>.
        // The GA SDK's Option<Vec<BlobPrefix>> deserializer rejects this XML with
        // "duplicate field BlobPrefix".
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="https://dafttestdata.blob.core.windows.net/" ContainerName="public-anonymous">
  <Prefix>/</Prefix>
  <Delimiter>/</Delimiter>
  <Blobs>
    <BlobPrefix>
      <Name>iceberg/</Name>
      <Properties>
        <Creation-Time>Thu, 06 Jun 2024 21:52:18 GMT</Creation-Time>
        <ResourceType>directory</ResourceType>
        <Content-Length>0</Content-Length>
        <BlobType>BlockBlob</BlobType>
      </Properties>
    </BlobPrefix>
    <Blob>
      <Name>mvp.parquet</Name>
      <Properties>
        <Content-Length>1234</Content-Length>
        <BlobType>BlockBlob</BlobType>
      </Properties>
    </Blob>
    <BlobPrefix>
      <Name>test_ls/</Name>
      <Properties>
        <ResourceType>directory</ResourceType>
        <Content-Length>0</Content-Length>
      </Properties>
    </BlobPrefix>
  </Blobs>
  <NextMarker />
</EnumerationResults>"#;

        let listing = parse_hierarchical_listing_xml(xml.as_bytes()).unwrap();
        assert_eq!(listing.prefixes, vec!["iceberg/", "test_ls/"]);
        assert_eq!(listing.blobs, vec![("mvp.parquet".to_string(), Some(1234))]);
    }

    #[test]
    fn strips_bom_and_unescapes_names() {
        let xml = "\u{feff}<Blobs><BlobPrefix><Name>a&amp;b/</Name></BlobPrefix><Blob><Name>c&lt;d</Name><Properties><Content-Length></Content-Length></Properties></Blob></Blobs>";
        let listing = parse_hierarchical_listing_xml(xml.as_bytes()).unwrap();
        assert_eq!(listing.prefixes, vec!["a&b/"]);
        assert_eq!(listing.blobs, vec![("c<d".to_string(), None)]);
        assert_eq!(unescape_xml("&quot;x&apos;"), "\"x'");
    }

    #[test]
    fn blob_url_keeps_virtual_directory_slashes() {
        let service = Url::parse("http://127.0.0.1:10000/devstoreaccount1/").unwrap();
        let url = blob_url(&service, "test-data", "/part_idx=0/file.parquet");
        assert_eq!(
            url.as_str(),
            "http://127.0.0.1:10000/devstoreaccount1/test-data/part_idx=0/file.parquet"
        );
    }

    #[test]
    fn blob_url_production_account() {
        let service = Url::parse("https://myaccount.blob.core.windows.net").unwrap();
        let url = blob_url(
            &ensure_trailing_slash(service),
            "container",
            "dir/blob.parquet",
        );
        assert_eq!(
            url.as_str(),
            "https://myaccount.blob.core.windows.net/container/dir/blob.parquet"
        );
    }
}
