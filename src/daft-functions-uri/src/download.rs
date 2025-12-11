use std::sync::Arc;

use common_error::{DaftError, DaftResult, ensure};
use daft_core::prelude::*;
use daft_dsl::{
    ExprRef,
    functions::{FunctionArgs, scalar::AsyncScalarUDF},
};
use daft_io::{Error, IOConfig, IOStatsContext, IOStatsRef, get_io_client};
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;

/// Container for the keyword arguments of `url_download`
/// ex:
/// ```text
/// url_download(input)
/// url_download(input, max_connections=32)
/// url_download(input, on_error='raise')
/// url_download(input, on_error='null')
/// url_download(input, max_connections=32, on_error='raise')
/// ```
#[derive(Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct UrlDownload;
impl UrlDownload {
    pub const CONNECTION_BATCH_FACTOR: usize = 4;
    pub const DEFAULT_URL_MAX_CONNECTIONS: usize = 32;
}
#[derive(FunctionArgs)]
pub struct UrlDownloadArgs<T> {
    pub input: T,
    #[arg(optional)]
    pub multi_thread: Option<bool>,
    #[arg(optional)]
    pub io_config: Option<IOConfig>,
    #[arg(optional)]
    pub max_connections: Option<usize>,
    #[arg(optional)]
    pub on_error: Option<String>,
}
#[typetag::serde]
#[async_trait::async_trait]
impl AsyncScalarUDF for UrlDownload {
    fn preferred_batch_size(&self, inputs: FunctionArgs<ExprRef>) -> DaftResult<Option<usize>> {
        let UrlDownloadArgs {
            max_connections, ..
        } = inputs.try_into()?;
        let max_connections = max_connections.unwrap_or(Self::DEFAULT_URL_MAX_CONNECTIONS);
        Ok(Some(max_connections * Self::CONNECTION_BATCH_FACTOR))
    }

    fn name(&self) -> &'static str {
        "url_download"
    }
    async fn call(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let UrlDownloadArgs {
            input,
            multi_thread,
            io_config,
            max_connections,
            on_error,
        } = inputs.try_into()?;

        let max_connections = max_connections.unwrap_or(Self::DEFAULT_URL_MAX_CONNECTIONS);
        let on_error = on_error.unwrap_or_else(|| "raise".to_string());
        let multi_thread = multi_thread.unwrap_or(true);
        let io_config = io_config.unwrap_or_default();

        let raise_error_on_failure = match on_error.as_str() {
            "raise" => true,
            "null" => false,
            _ => {
                return Err(DaftError::ValueError(format!(
                    "Invalid value for 'on_error': {}",
                    on_error
                )));
            }
        };

        let array = input.utf8()?;
        let io_stats = IOStatsContext::new("download");
        let result = url_download(
            array,
            max_connections,
            raise_error_on_failure,
            multi_thread,
            Arc::new(io_config),
            Some(io_stats),
        )
        .await?;
        Ok(result.into_series())
    }

    fn get_return_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let UrlDownloadArgs { input, .. } = inputs.try_into()?;
        let field = input.to_field(schema)?;
        ensure!(field.dtype.is_string(), TypeError: "Input must be a string");
        Ok(Field::new(field.name, DataType::Binary))
    }
}

async fn url_download(
    array: &Utf8Array,
    max_connections: usize,
    raise_error_on_failure: bool,
    multi_thread: bool,
    config: Arc<IOConfig>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<BinaryArray> {
    let name = array.name();
    ensure!(
        max_connections > 0,
        ValueError: "max_connections for url_download must be non-zero"
    );

    let max_connections = match multi_thread {
        false => max_connections,
        true => max_connections * usize::from(std::thread::available_parallelism()?),
    };
    let io_client = get_io_client(multi_thread, config)?;

    let owned_array = array.clone();

    #[expect(
        clippy::needless_collect,
        reason = "This actually might be needed, but need to double check TODO:(andrewgazelka)"
    )]
    let urls = owned_array
        .into_iter()
        .map(|s| s.map(std::string::ToString::to_string))
        .collect::<Vec<_>>();

    let stream = futures::stream::iter(urls.into_iter().enumerate().map(move |(i, url)| {
        let owned_client = io_client.clone();
        let owned_io_stats = io_stats.clone();
        tokio::spawn(async move {
            (
                i,
                owned_client
                    .single_url_download(url, raise_error_on_failure, owned_io_stats)
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
    let mut results = stream.try_collect::<Vec<_>>().await?;

    results.sort_by_key(|k| k.0);
    let mut offsets: Vec<i64> = Vec::with_capacity(results.len() + 1);
    offsets.push(0);
    let mut valid = Vec::with_capacity(results.len());
    valid.reserve(results.len());

    let cap_needed: usize = results
        .iter()
        .filter_map(|f| f.1.as_ref().map(bytes::Bytes::len))
        .sum();
    let mut data = Vec::with_capacity(cap_needed);
    for (_, b) in results {
        if let Some(b) = b {
            data.extend(b.as_ref());
            offsets.push(b.len() as i64 + offsets.last().unwrap());
            valid.push(true);
        } else {
            offsets.push(*offsets.last().unwrap());
            valid.push(false);
        }
    }
    Ok(BinaryArray::try_from((name, data, offsets))?
        .with_validity_slice(valid.as_slice())
        .unwrap())
}
