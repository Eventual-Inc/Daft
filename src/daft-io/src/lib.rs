#![feature(async_closure)]
#![feature(let_chains)]
mod azure_blob;
mod google_cloud;
mod http;
mod local;
mod object_io;
mod object_store_glob;
mod s3_like;
use azure_blob::AzureBlobSource;
use google_cloud::GCSSource;
use lazy_static::lazy_static;
#[cfg(feature = "python")]
pub mod python;

pub use common_io_config::{AzureConfig, IOConfig, S3Config};
pub use object_io::GetResult;
#[cfg(feature = "python")]
pub use python::register_modules;

use std::{borrow::Cow, collections::HashMap, hash::Hash, ops::Range, sync::Arc};

use futures::{StreamExt, TryStreamExt};

use snafu::Snafu;
use url::ParseError;

use snafu::prelude::*;

use daft_core::{
    array::ops::as_arrow::AsArrow,
    datatypes::{BinaryArray, Utf8Array},
    DataType, IntoSeries, Series,
};

use common_error::{DaftError, DaftResult};
use s3_like::S3LikeSource;

use self::{http::HttpSource, local::LocalSource, object_io::ObjectSource};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Generic {} error: {}", store, source))]
    Generic { store: SourceType, source: DynError },
    #[snafu(display("Object at location {} not found\nDetails:\n{}", path, source))]
    NotFound { path: String, source: DynError },

    #[snafu(display("Invalid Argument: {:?}", msg))]
    InvalidArgument { msg: String },

    #[snafu(display("Unable to open file {}: {:?}", path, source))]
    UnableToOpenFile { path: String, source: DynError },

    #[snafu(display("Unable to read data from file {}: {}", path, source))]
    UnableToReadBytes {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to convert URL \"{}\" to path", path))]
    InvalidUrl {
        path: String,
        source: url::ParseError,
    },

    #[snafu(display("Not a File: \"{}\"", path))]
    NotAFile { path: String },

    #[snafu(display("Unable to load Credentials for store: {store}\nDetails:\n{source:?}"))]
    UnableToLoadCredentials { store: SourceType, source: DynError },

    #[snafu(display("Failed to load Credentials for store: {store}\nDetails:\n{source:?}"))]
    UnableToCreateClient { store: SourceType, source: DynError },

    #[snafu(display("Unauthorized to access store: {store} for file: {path}\nYou may need to set valid Credentials\n{source}"))]
    Unauthorized {
        store: SourceType,
        path: String,
        source: DynError,
    },

    #[snafu(display("Source not yet implemented: {}", store))]
    NotImplementedSource { store: String },

    #[snafu(display("Unhandled Error for path: {}\nDetails:\n{}", path, msg))]
    Unhandled { path: String, msg: String },

    #[snafu(
        display("Error sending data over a tokio channel: {}", source),
        context(false)
    )]
    UnableToSendDataOverChannel { source: DynError },

    #[snafu(display("Error joining spawned task: {}", source), context(false))]
    JoinError { source: tokio::task::JoinError },
}

impl From<Error> for DaftError {
    fn from(err: Error) -> DaftError {
        use Error::*;
        match err {
            NotFound { path, source } => DaftError::FileNotFound { path, source },
            _ => DaftError::External(err.into()),
        }
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, err)
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Default)]
pub struct IOClient {
    source_type_to_store: tokio::sync::RwLock<HashMap<SourceType, Arc<dyn ObjectSource>>>,
    config: Arc<IOConfig>,
}

impl IOClient {
    pub fn new(config: Arc<IOConfig>) -> Result<Self> {
        Ok(IOClient {
            source_type_to_store: tokio::sync::RwLock::new(HashMap::new()),
            config,
        })
    }

    async fn get_source(&self, source_type: &SourceType) -> Result<Arc<dyn ObjectSource>> {
        {
            if let Some(client) = self.source_type_to_store.read().await.get(source_type) {
                return Ok(client.clone());
            }
        }
        let mut w_handle = self.source_type_to_store.write().await;

        if let Some(client) = w_handle.get(source_type) {
            return Ok(client.clone());
        }

        let new_source = match source_type {
            SourceType::File => LocalSource::get_client().await? as Arc<dyn ObjectSource>,
            SourceType::Http => HttpSource::get_client().await? as Arc<dyn ObjectSource>,
            SourceType::S3 => {
                S3LikeSource::get_client(&self.config.s3).await? as Arc<dyn ObjectSource>
            }
            SourceType::AzureBlob => {
                AzureBlobSource::get_client(&self.config.azure).await? as Arc<dyn ObjectSource>
            }

            SourceType::GCS => {
                GCSSource::get_client(&self.config.gcs).await? as Arc<dyn ObjectSource>
            }
        };

        if w_handle.get(source_type).is_none() {
            w_handle.insert(*source_type, new_source.clone());
        }
        Ok(new_source)
    }

    pub async fn single_url_get(
        &self,
        input: String,
        range: Option<Range<usize>>,
    ) -> Result<GetResult> {
        let (scheme, path) = parse_url(&input)?;
        let source = self.get_source(&scheme).await?;
        source.get(path.as_ref(), range).await
    }

    pub async fn single_url_get_size(&self, input: String) -> Result<usize> {
        let (scheme, path) = parse_url(&input)?;
        let source = self.get_source(&scheme).await?;
        source.get_size(path.as_ref()).await
    }

    async fn single_url_download(
        &self,
        index: usize,
        input: Option<String>,
        raise_error_on_failure: bool,
    ) -> Result<Option<bytes::Bytes>> {
        let value = if let Some(input) = input {
            let response = self.single_url_get(input, None).await;
            let res = match response {
                Ok(res) => res.bytes().await,
                Err(err) => Err(err),
            };
            Some(res)
        } else {
            None
        };

        match value {
            Some(Ok(bytes)) => Ok(Some(bytes)),
            Some(Err(err)) => match raise_error_on_failure {
                true => Err(err),
                false => {
                    log::warn!(
                        "Error occurred during url_download at index: {index} {} (falling back to Null)",
                        err
                    );
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    }
}

#[derive(Debug, Hash, PartialEq, std::cmp::Eq, Clone, Copy)]
pub enum SourceType {
    File,
    Http,
    S3,
    AzureBlob,
    GCS,
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SourceType::File => write!(f, "file"),
            SourceType::Http => write!(f, "http"),
            SourceType::S3 => write!(f, "s3"),
            SourceType::AzureBlob => write!(f, "AzureBlob"),
            SourceType::GCS => write!(f, "gcs"),
        }
    }
}

pub fn parse_url(input: &str) -> Result<(SourceType, Cow<'_, str>)> {
    let mut fixed_input = Cow::Borrowed(input);

    let url = match url::Url::parse(input) {
        Ok(url) => Ok(url),
        Err(ParseError::RelativeUrlWithoutBase) => {
            fixed_input = Cow::Owned(format!("file://{input}"));
            url::Url::parse(fixed_input.as_ref())
        }
        Err(err) => Err(err),
    }
    .context(InvalidUrlSnafu { path: input })?;

    let scheme = url.scheme().to_lowercase();
    match scheme.as_ref() {
        "file" => Ok((SourceType::File, fixed_input)),
        "http" | "https" => Ok((SourceType::Http, fixed_input)),
        "s3" => Ok((SourceType::S3, fixed_input)),
        "az" | "abfs" => Ok((SourceType::AzureBlob, fixed_input)),
        "gcs" | "gs" => Ok((SourceType::GCS, fixed_input)),
        #[cfg(target_env = "msvc")]
        _ if scheme.len() == 1 && ("a" <= scheme.as_str() && (scheme.as_str() <= "z")) => {
            Ok((SourceType::File, Cow::Owned(format!("file://{input}"))))
        }
        _ => Err(Error::NotImplementedSource { store: scheme }),
    }
}
type CacheKey = (bool, Arc<IOConfig>);
lazy_static! {
    static ref NUM_CPUS: usize = std::thread::available_parallelism().unwrap().get();
    static ref THREADED_RUNTIME: tokio::sync::RwLock<(Arc<tokio::runtime::Runtime>, usize)> =
        tokio::sync::RwLock::new((
            Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(8.min(*NUM_CPUS))
                    .enable_all()
                    .build()
                    .unwrap()
            ),
            8.min(*NUM_CPUS)
        ));
    static ref CLIENT_CACHE: tokio::sync::RwLock<HashMap<CacheKey, Arc<IOClient>>> =
        tokio::sync::RwLock::new(HashMap::new());
}

pub fn get_io_client(multi_thread: bool, config: Arc<IOConfig>) -> DaftResult<Arc<IOClient>> {
    let read_handle = CLIENT_CACHE.blocking_read();
    let key = (multi_thread, config.clone());
    if let Some(client) = read_handle.get(&key) {
        Ok(client.clone())
    } else {
        drop(read_handle);

        let mut w_handle = CLIENT_CACHE.blocking_write();
        if let Some(client) = w_handle.get(&key) {
            Ok(client.clone())
        } else {
            let client = Arc::new(IOClient::new(config.clone())?);
            w_handle.insert(key, client.clone());
            Ok(client)
        }
    }
}

pub fn get_runtime(multi_thread: bool) -> DaftResult<Arc<tokio::runtime::Runtime>> {
    match multi_thread {
        false => Ok(Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        )),
        true => {
            let guard = THREADED_RUNTIME.blocking_read();
            Ok(guard.clone().0)
        }
    }
}

pub fn set_io_pool_num_threads(num_threads: usize) -> bool {
    {
        let guard = THREADED_RUNTIME.blocking_read();
        if guard.1 == num_threads {
            return false;
        }
    }
    let mut client_guard = CLIENT_CACHE.blocking_write();
    let mut guard = THREADED_RUNTIME.blocking_write();

    client_guard.clear();

    guard.1 = num_threads;
    guard.0 = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_threads)
            .enable_all()
            .build()
            .unwrap(),
    );
    true
}

pub fn _url_download(
    array: &Utf8Array,
    max_connections: usize,
    raise_error_on_failure: bool,
    multi_thread: bool,
    config: Arc<IOConfig>,
) -> DaftResult<BinaryArray> {
    let urls = array.as_arrow().iter();
    let name = array.name();
    ensure!(
        max_connections > 0,
        InvalidArgumentSnafu {
            msg: "max_connections for url_download must be non-zero".to_owned()
        }
    );

    let runtime_handle = get_runtime(multi_thread)?;
    let _rt_guard = runtime_handle.enter();
    let max_connections = match multi_thread {
        false => max_connections,
        true => max_connections * usize::from(std::thread::available_parallelism()?),
    };
    let io_client = get_io_client(multi_thread, config)?;

    let fetches = futures::stream::iter(urls.enumerate().map(|(i, url)| {
        let owned_url = url.map(|s| s.to_string());
        let owned_client = io_client.clone();
        tokio::spawn(async move {
            (
                i,
                owned_client
                    .single_url_download(i, owned_url, raise_error_on_failure)
                    .await,
            )
        })
    }))
    .buffer_unordered(max_connections)
    .then(async move |r| match r {
        Ok((i, Ok(v))) => Ok((i, v)),
        Ok((_i, Err(error))) => Err(error),
        Err(error) => Err(Error::JoinError { source: error }),
    });

    let collect_future = fetches.try_collect::<Vec<_>>();
    let mut results = runtime_handle.block_on(collect_future)?;

    results.sort_by_key(|k| k.0);
    let mut offsets: Vec<i64> = Vec::with_capacity(results.len() + 1);
    offsets.push(0);
    let mut valid = Vec::with_capacity(results.len());
    valid.reserve(results.len());

    let cap_needed: usize = results
        .iter()
        .filter_map(|f| f.1.as_ref().map(|f| f.len()))
        .sum();
    let mut data = Vec::with_capacity(cap_needed);
    for (_, b) in results.into_iter() {
        match b {
            Some(b) => {
                data.extend(b.as_ref());
                offsets.push(b.len() as i64 + offsets.last().unwrap());
                valid.push(true);
            }
            None => {
                offsets.push(*offsets.last().unwrap());
                valid.push(false);
            }
        }
    }
    Ok(BinaryArray::try_from((name, data, offsets))?
        .with_validity(valid.as_slice())
        .unwrap())
}

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub fn url_download(
    series: &Series,
    max_connections: usize,
    raise_error_on_failure: bool,
    multi_thread: bool,
    config: Arc<IOConfig>,
) -> DaftResult<Series> {
    match series.data_type() {
        DataType::Utf8 => Ok(_url_download(
            series.utf8()?,
            max_connections,
            raise_error_on_failure,
            multi_thread,
            config,
        )?
        .into_series()),
        dt => Err(DaftError::TypeError(format!(
            "url download not implemented for type {dt}"
        ))),
    }
}
