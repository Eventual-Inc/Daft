#![feature(async_closure)]

pub mod config;
mod http;
mod local;
mod object_io;
mod s3_like;

#[cfg(feature = "python")]
pub mod python;

use config::IOConfig;
#[cfg(feature = "python")]
pub use python::register_modules;

use std::{borrow::Cow, hash::Hash, ops::Range, sync::Arc};

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

use self::{
    http::HttpSource,
    local::LocalSource,
    object_io::{GetResult, ObjectSource},
};

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display("Generic {} error: {:?}", store, source))]
    Generic { store: SourceType, source: DynError },

    #[snafu(display("Object at location {} not found: {:?}", path, source))]
    NotFound { path: String, source: DynError },

    #[snafu(display("Invalid Argument: {:?}", msg))]
    InvalidArgument { msg: String },

    #[snafu(display("Unable to open file {}: {}", path, source))]
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

    #[snafu(display("Unable to load Credentials for store: {store} {source}"))]
    UnableToLoadCredentials { store: SourceType, source: DynError },

    #[snafu(display("Failed to load Credentials for store: {store} {source}"))]
    UnableToCreateClient { store: SourceType, source: DynError },

    #[snafu(display("Source not yet implemented: {}", store))]
    NotImplementedSource { store: String },

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

type Result<T, E = Error> = std::result::Result<T, E>;

async fn get_source(source_type: SourceType, config: &IOConfig) -> Result<Arc<dyn ObjectSource>> {
    Ok(match source_type {
        SourceType::File => LocalSource::get_client().await? as Arc<dyn ObjectSource>,
        SourceType::Http => HttpSource::get_client().await? as Arc<dyn ObjectSource>,
        SourceType::S3 => S3LikeSource::get_client(&config.s3).await? as Arc<dyn ObjectSource>,
    })
}

#[derive(Debug, Hash, PartialEq, std::cmp::Eq, Clone, Copy)]
pub(crate) enum SourceType {
    File,
    Http,
    S3,
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SourceType::File => write!(f, "file"),
            SourceType::Http => write!(f, "http"),
            SourceType::S3 => write!(f, "s3"),
        }
    }
}

fn parse_url(input: &str) -> Result<(SourceType, Cow<'_, str>)> {
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
        _ => Err(Error::NotImplementedSource { store: scheme }),
    }
}

async fn single_url_get(
    input: String,
    range: Option<Range<usize>>,
    config: &IOConfig,
) -> Result<GetResult> {
    let (scheme, path) = parse_url(&input)?;
    let source = get_source(scheme, config).await?;
    source.get(path.as_ref(), range).await
}

async fn single_url_download(
    index: usize,
    input: Option<String>,
    raise_error_on_failure: bool,
    config: Arc<IOConfig>,
) -> Result<Option<bytes::Bytes>> {
    let value = if let Some(input) = input {
        let response = single_url_get(input, None, config.as_ref()).await;
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
    let rt = match multi_thread {
        false => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build(),
        true => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build(),
    }?;

    let max_connections = match multi_thread {
        false => max_connections,
        true => max_connections * usize::from(std::thread::available_parallelism()?),
    };
    // let thread_max_connections =
    let fetches = futures::stream::iter(urls.enumerate().map(|(i, url)| {
        let owned_url = url.map(|s| s.to_string());
        let owned_config = config.clone();
        tokio::spawn(async move {
            (
                i,
                single_url_download(i, owned_url, raise_error_on_failure, owned_config).await,
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
    let mut results = rt.block_on(collect_future)?;

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
