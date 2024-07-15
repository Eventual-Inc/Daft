use std::sync::Arc;

use daft_core::array::ops::as_arrow::AsArrow;
use daft_core::datatypes::{BinaryArray, Field, Utf8Array};
use daft_core::{DataType, IntoSeries};
use daft_dsl::functions::ScalarUDF;
use daft_dsl::ExprRef;
use daft_io::{get_io_client, get_runtime, Error, IOConfig, IOStatsContext, IOStatsRef};
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;
use snafu::prelude::*;

use common_error::{DaftError, DaftResult};
use daft_core::schema::Schema;
use daft_core::series::Series;

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
        let DownloadFunction {
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
                    "Download can only download uris from Utf8Array, got {}",
                    input
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
                        "Download can only download uris from Utf8Array, got {}",
                        field
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
        let owned_io_stats = io_stats.clone();
        tokio::spawn(async move {
            (
                i,
                owned_client
                    .single_url_download(i, owned_url, raise_error_on_failure, owned_io_stats)
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
        .with_validity_slice(valid.as_slice())
        .unwrap())
}
