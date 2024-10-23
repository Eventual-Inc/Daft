use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_core::prelude::*;
use daft_dsl::{functions::ScalarUDF, ExprRef};
use daft_io::{get_io_client, Error, IOConfig, IOStatsContext, IOStatsRef};
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;
use snafu::prelude::*;

use crate::InvalidArgumentSnafu;

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub(super) struct DownloadFunction {
    pub(super) max_connections: usize,
    pub(super) raise_error_on_failure: bool,
    pub(super) multi_thread: bool,
    pub(super) config: Arc<IOConfig>,
}

#[typetag::serde]
impl ScalarUDF for DownloadFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "download"
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        let Self {
            max_connections,
            raise_error_on_failure,
            multi_thread,
            config,
        } = self;

        match inputs {
            [input] => match input.data_type() {
                DataType::Utf8 => {
                    let array = input.utf8()?;
                    let io_stats = IOStatsContext::new("download");
                    let result = url_download(
                        array,
                        *max_connections,
                        *raise_error_on_failure,
                        *multi_thread,
                        config.clone(),
                        Some(io_stats),
                    )?;
                    Ok(result.into_series())
                }
                _ => Err(DaftError::TypeError(format!(
                    "Download can only download uris from Utf8Array, got {input}"
                ))),
            },
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [input] => {
                let field = input.to_field(schema)?;

                match &field.dtype {
                    DataType::Utf8 => Ok(Field::new(field.name, DataType::Binary)),
                    _ => Err(DaftError::TypeError(format!(
                        "Download can only download uris from Utf8Array, got {field}"
                    ))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
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
        InvalidArgumentSnafu {
            msg: "max_connections for url_download must be non-zero".to_owned()
        }
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
