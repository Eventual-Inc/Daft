use std::sync::Arc;

use daft_core::array::ops::as_arrow::AsArrow;
use daft_core::datatypes::{Field, Utf8Array};
use daft_core::{DataType, IntoSeries};
use daft_dsl::functions::ScalarUDF;
use daft_dsl::ExprRef;
use daft_io::{get_io_client, get_runtime, IOConfig, IOStatsRef, SourceType};
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;

use common_error::{DaftError, DaftResult};
use daft_core::schema::Schema;
use daft_core::series::Series;

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub(super) struct UploadFunction {
    pub(super) location: String,
    pub(super) max_connections: usize,
    pub(super) multi_thread: bool,
    pub(super) config: Arc<IOConfig>,
}

#[typetag::serde]
impl ScalarUDF for UploadFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "upload"
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        let UploadFunction {
            location,
            config,
            max_connections,
            multi_thread,
        } = self;

        match inputs {
            [data] => url_upload(
                data,
                location,
                *max_connections,
                *multi_thread,
                config.clone(),
                None,
            ),
            _ => Err(DaftError::ValueError(format!(
                "Expected 1 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data] => {
                let data_field = data.to_field(schema)?;
                match data_field.dtype {
                    DataType::Binary | DataType::FixedSizeBinary(..) | DataType::Utf8 => Ok(Field::new(data_field.name, DataType::Utf8)),
                    _ => Err(DaftError::TypeError(format!("Expects input to url_upload to be Binary, FixedSizeBinary or String, but received {}", data_field))),
                }
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 1 input arg, got {}",
                inputs.len()
            ))),
        }
    }
}

/// Uploads data from a Binary/FixedSizeBinary/Utf8 Series to the provided folder_path
///
/// This performs an async upload of each row, and creates in-memory copies of the data that is currently in-flight.
/// Memory consumption should be tunable by configuring `max_connections`, which tunes the number of in-flight tokio tasks.
pub fn url_upload(
    series: &Series,
    folder_path: &str,
    max_connections: usize,
    multi_thread: bool,
    config: Arc<IOConfig>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<Series> {
    fn _upload_bytes_to_folder(
        folder_path: &str,
        // TODO: We can further optimize this for larger rows by using instead an Iterator<Item = bytes::Bytes>
        // This would allow us to iteratively copy smaller chunks of data and feed it to the AWS SDKs, instead
        // of materializing the entire row at once as a single bytes::Bytes.
        //
        // Alternatively, we can find a way of creating a `bytes::Bytes` that just references the underlying
        // arrow2 buffer, without making a copy. This would be the ideal case.
        bytes_iter: impl Iterator<Item = Option<bytes::Bytes>>,
        max_connections: usize,
        multi_thread: bool,
        config: Arc<IOConfig>,
        io_stats: Option<IOStatsRef>,
    ) -> DaftResult<Vec<Option<String>>> {
        // HACK: Creates folders if running locally. This is a bit of a hack to do it here because we'd rather delegate this to
        // the appropriate source. However, most sources such as the object stores don't have the concept of "folders".
        let (source, folder_path) = daft_io::parse_url(folder_path)?;
        if matches!(source, SourceType::File) {
            let local_prefixless_folder_path = match folder_path.strip_prefix("file://") {
                Some(p) => p,
                None => folder_path.as_ref(),
            };

            std::fs::create_dir_all(local_prefixless_folder_path).map_err(|e| {
                daft_io::Error::UnableToCreateDir {
                    path: folder_path.as_ref().to_string(),
                    source: e,
                }
            })?;
        }

        let runtime_handle = get_runtime(multi_thread)?;
        let _rt_guard = runtime_handle.enter();
        let max_connections = match multi_thread {
            false => max_connections,
            true => max_connections * usize::from(std::thread::available_parallelism()?),
        };
        let io_client = get_io_client(multi_thread, config)?;
        let folder_path = folder_path.as_ref().trim_end_matches('/');

        let uploads = futures::stream::iter(bytes_iter.enumerate().map(|(i, data)| {
            let owned_client = io_client.clone();
            let owned_io_stats = io_stats.clone();

            // TODO: Allow configuration of this path (e.g. providing a file extension, or a corresponding Series with matching length with filenames)
            let path = format!("{}/{}", folder_path, uuid::Uuid::new_v4());

            tokio::spawn(async move {
                (
                    i,
                    owned_client
                        .single_url_upload(i, path, data, owned_io_stats)
                        .await,
                )
            })
        }))
        .buffer_unordered(max_connections)
        .then(async move |r| match r {
            Ok((i, Ok(v))) => Ok((i, v)),
            Ok((_i, Err(error))) => Err(error),
            Err(error) => Err(daft_io::Error::JoinError { source: error }),
        });

        let collect_future = uploads.try_collect::<Vec<_>>();
        let mut results = runtime_handle.block_on(collect_future)?;
        results.sort_by_key(|k| k.0);

        Ok(results.into_iter().map(|(_, path)| path).collect())
    }

    let results = match series.data_type() {
        DataType::Binary => {
            let bytes_iter = series
                .binary()
                .unwrap()
                .as_arrow()
                .iter()
                .map(|bytes_slice| bytes_slice.map(|b| bytes::Bytes::from(b.to_vec())));
            _upload_bytes_to_folder(
                folder_path,
                bytes_iter,
                max_connections,
                multi_thread,
                config,
                io_stats,
            )
        }
        DataType::FixedSizeBinary(..) => {
            let bytes_iter = series
                .fixed_size_binary()
                .unwrap()
                .as_arrow()
                .iter()
                .map(|bytes_slice| bytes_slice.map(|b| bytes::Bytes::from(b.to_vec())));
            _upload_bytes_to_folder(
                folder_path,
                bytes_iter,
                max_connections,
                multi_thread,
                config,
                io_stats,
            )
        }
        DataType::Utf8 => {
            let bytes_iter =
                series.utf8().unwrap().as_arrow().iter().map(|utf8_slice| {
                    utf8_slice.map(|s| bytes::Bytes::from(s.as_bytes().to_vec()))
                });
            _upload_bytes_to_folder(
                folder_path,
                bytes_iter,
                max_connections,
                multi_thread,
                config,
                io_stats,
            )
        }
        dt => Err(DaftError::TypeError(format!(
            "url_upload not implemented for type {dt}"
        ))),
    }?;

    Ok(Utf8Array::from_iter(series.name(), results.into_iter()).into_series())
}
