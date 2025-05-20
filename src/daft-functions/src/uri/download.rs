use std::sync::Arc;

use common_error::{ensure, DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_core::prelude::*;
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use daft_io::{get_io_client, Error, IOConfig, IOStatsContext, IOStatsRef};
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
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct UrlDownload;

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct UrlDownloadArgs {
    pub max_connections: usize,
    pub raise_error_on_failure: bool,
    pub multi_thread: bool,
    pub io_config: Arc<IOConfig>,
}

impl UrlDownloadArgs {
    pub fn new(
        max_connections: usize,
        raise_error_on_failure: bool,
        multi_thread: bool,
        io_config: Option<IOConfig>,
    ) -> Self {
        Self {
            max_connections,
            raise_error_on_failure,
            multi_thread,
            io_config: io_config.unwrap_or_default().into(),
        }
    }
}

impl Default for UrlDownloadArgs {
    fn default() -> Self {
        Self {
            max_connections: 32,
            raise_error_on_failure: true,
            multi_thread: true,
            io_config: IOConfig::default().into(),
        }
    }
}

#[typetag::serde]
impl ScalarUDF for UrlDownload {
    fn name(&self) -> &'static str {
        "url_download"
    }
    fn evaluate(&self, inputs: daft_dsl::functions::FunctionArgs<Series>) -> DaftResult<Series> {
        let input = inputs.required((0, "input"))?;
        let max_connections = inputs
            .extract_optional::<usize, _>("max_connections")?
            .unwrap_or(32);
        let on_error = inputs
            .extract_optional::<String, _>("on_error")?
            .unwrap_or_else(|| "raise".to_string());

        let raise_error_on_failure = match on_error.as_str() {
            "raise" => true,
            "null" => false,
            _ => {
                return Err(DaftError::ValueError(format!(
                    "Invalid value for 'on_error': {}",
                    on_error
                )))
            }
        };

        let multi_thread = inputs
            .extract_optional::<bool, _>("multi_thread")?
            .unwrap_or(true);
        let io_config = inputs
            .extract_optional::<IOConfig, _>("io_config")?
            .unwrap_or_default();

        let array = input.utf8()?;
        let io_stats = IOStatsContext::new("download");
        let result = url_download(
            array,
            max_connections,
            raise_error_on_failure,
            multi_thread,
            Arc::new(io_config),
            Some(io_stats),
        )?;
        Ok(result.into_series())
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &Schema,
    ) -> DaftResult<Field> {
        let field = inputs.required((0, "input"))?.to_field(schema)?;
        let _ = inputs.extract_optional::<bool, _>("multi_thread")?;
        let _ = inputs.extract_optional::<IOConfig, _>("io_config")?;
        let _ = inputs.extract_optional::<usize, _>("max_connections")?;
        let _ = inputs.extract_optional::<String, _>("on_error")?;
        ensure!(field.dtype.is_string(), TypeError: "Input must be a string");
        Ok(Field::new(field.name, DataType::Binary))
    }
}

fn url_download(
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

    let runtime_handle = get_io_runtime(true);
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
    let fetches = async move {
        let urls = owned_array
            .as_arrow()
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
                        .single_url_download(i, url, raise_error_on_failure, owned_io_stats)
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
        stream.try_collect::<Vec<_>>().await
    };

    let mut results = runtime_handle.block_on(fetches)??;

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
