use std::{collections::HashSet, iter::repeat, path::Path, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_runtime::get_io_runtime;
use daft_core::prelude::*;
use daft_dsl::{functions::ScalarUDF, ExprRef};
use daft_io::{get_io_client, IOConfig, IOStatsRef, SourceType};
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub(super) struct UploadFunction {
    pub(super) max_connections: usize,
    pub(super) raise_error_on_failure: bool,
    pub(super) multi_thread: bool,
    pub(super) is_single_folder: bool,
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
        let Self {
            config,
            max_connections,
            raise_error_on_failure,
            multi_thread,
            is_single_folder,
        } = self;

        match inputs {
            [data, location] => url_upload(
                data,
                location,
                *max_connections,
                *raise_error_on_failure,
                *multi_thread,
                *is_single_folder,
                config.clone(),
                None,
            ),
            _ => Err(DaftError::ValueError(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        match inputs {
            [data, location] => {
                let data_field = data.to_field(schema)?;
                let location_field = location.to_field(schema)?;
                match data_field.dtype {
                    DataType::Binary | DataType::FixedSizeBinary(..) | DataType::Utf8 => (),
                    _ => return Err(DaftError::TypeError(format!("Expects input to url_upload to be Binary, FixedSizeBinary or String, but received {data_field}"))),
                }
                if !location_field.dtype.is_string() {
                    return Err(DaftError::TypeError(format!(
                        "Expected location to be string, received: {}",
                        location_field.dtype
                    )));
                }
                Ok(Field::new(data_field.name, DataType::Utf8))
            }
            _ => Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input args, got {}",
                inputs.len()
            ))),
        }
    }
}

/// Helper function that takes a given folder path, a boolean `is_single_folder` that indicates if we were given a single folder
/// for uploading files or if we were given a row-specific path, and a set of instantiated folder paths, then creates the folder
/// if they has not yet been instantiated.
///
/// This function also parses the given folder path and trims '/' from the end of the path.
fn instantiate_and_trim_path(
    folder_path: &str,
    is_single_folder: bool,
    instantiated_folder_paths: &mut HashSet<String>,
) -> DaftResult<String> {
    // HACK: Creates folders if running locally. This is a bit of a hack to do it here because we'd rather delegate this to
    // the appropriate source. However, most sources such as the object stores don't have the concept of "folders".
    let (source, folder_path) = daft_io::parse_url(folder_path)?;
    if matches!(source, SourceType::File) {
        let local_prefixless_folder_path = match folder_path.strip_prefix("file://") {
            Some(p) => p,
            None => folder_path.as_ref(),
        };
        if is_single_folder {
            // If we were given a single folder, create a directory at the given path.
            if instantiated_folder_paths.insert(local_prefixless_folder_path.to_string()) {
                std::fs::create_dir_all(local_prefixless_folder_path).map_err(|e| {
                    daft_io::Error::UnableToCreateDir {
                        path: local_prefixless_folder_path.to_string(),
                        source: e,
                    }
                })?;
            }
        } else {
            // If we were given row-specific paths, then we create directories at the parents of the given paths.
            let path = Path::new(local_prefixless_folder_path);
            if let Some(parent_dir) = path.parent() {
                if let Some(parent_dir) = parent_dir.to_str() {
                    if instantiated_folder_paths.insert(parent_dir.to_string()) {
                        std::fs::create_dir_all(parent_dir).map_err(|e| {
                            daft_io::Error::UnableToCreateDir {
                                path: parent_dir.to_string(),
                                source: e,
                            }
                        })?;
                    }
                }
            }
        }
    }
    Ok(folder_path.trim_end_matches('/').to_string())
}

/// Helper function that takes an utf8 array of folder paths that may have either 1 or `len` paths,
/// and returns a Vec of `len` folder paths. This function will also instantiate and trim the given
/// folder paths as needed.
///
/// If `is_single_folder` is set, the prepared path is repeated `len` times.
/// Otherwise, we return the array of prepared paths.
fn prepare_folder_paths(
    arr: &Utf8Array,
    len: usize,
    is_single_folder: bool,
) -> DaftResult<Vec<String>> {
    let mut instantiated_folder_paths = HashSet::new();
    if is_single_folder {
        let folder_path = arr.get(0).unwrap();
        let folder_path =
            instantiate_and_trim_path(folder_path, true, &mut instantiated_folder_paths)?;
        Ok(repeat(folder_path).take(len).collect())
    } else {
        debug_assert_eq!(arr.len(), len);
        Ok(arr
            .as_arrow()
            .iter()
            .map(|folder_path| {
                instantiate_and_trim_path(
                    folder_path.unwrap(),
                    false,
                    &mut instantiated_folder_paths,
                )
            })
            .collect::<DaftResult<Vec<String>>>()?)
    }
}

/// Uploads data from a Binary/FixedSizeBinary/Utf8 Series to the provided folder_path
///
/// This performs an async upload of each row, and creates in-memory copies of the data that is currently in-flight.
/// Memory consumption should be tunable by configuring `max_connections`, which tunes the number of in-flight tokio tasks.
#[allow(clippy::too_many_arguments)]
pub fn url_upload(
    series: &Series,
    folder_paths: &Series,
    max_connections: usize,
    raise_error_on_failure: bool,
    multi_thread: bool,
    is_single_folder: bool,
    config: Arc<IOConfig>,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<Series> {
    #[allow(clippy::too_many_arguments)]
    fn _upload_bytes_to_folder(
        folder_path_iter: Vec<String>,
        // TODO: We can further optimize this for larger rows by using instead an Iterator<Item = bytes::Bytes>
        // This would allow us to iteratively copy smaller chunks of data and feed it to the AWS SDKs, instead
        // of materializing the entire row at once as a single bytes::Bytes.
        //
        // Alternatively, we can find a way of creating a `bytes::Bytes` that just references the underlying
        // arrow2 buffer, without making a copy. This would be the ideal case.
        to_upload: Vec<Option<bytes::Bytes>>,
        max_connections: usize,
        raise_error_on_failure: bool,
        multi_thread: bool,
        is_single_folder: bool,
        config: Arc<IOConfig>,
        io_stats: Option<IOStatsRef>,
    ) -> DaftResult<Vec<Option<String>>> {
        let runtime_handle = get_io_runtime(multi_thread);
        let max_connections = match multi_thread {
            false => max_connections,
            true => max_connections * usize::from(std::thread::available_parallelism()?),
        };
        let io_client = get_io_client(multi_thread, config)?;

        let uploads = async move {
            futures::stream::iter(to_upload.into_iter().zip(folder_path_iter).enumerate().map(
                |(i, (data, folder_path))| {
                    let owned_client = io_client.clone();
                    let owned_io_stats = io_stats.clone();

                    // TODO: Allow configuration of the folder path (e.g. providing a file extension, or a corresponding Series with matching length with filenames)

                    // If the user specifies a single location via a string, we should upload to a single folder by appending a UUID to each path. Otherwise,
                    // if the user gave an expression, we assume that each row has a specific url to upload to.
                    let path = if is_single_folder {
                        format!("{}/{}", folder_path, uuid::Uuid::new_v4())
                    } else {
                        folder_path
                    };
                    tokio::spawn(async move {
                        (
                            i,
                            owned_client
                                .single_url_upload(
                                    i,
                                    path,
                                    data,
                                    raise_error_on_failure,
                                    owned_io_stats,
                                )
                                .await,
                        )
                    })
                },
            ))
            .buffer_unordered(max_connections)
            .then(async move |r| match r {
                Ok((i, Ok(v))) => Ok((i, v)),
                Ok((_i, Err(error))) => Err(error),
                Err(error) => Err(daft_io::Error::JoinError { source: error }),
            })
            .try_collect::<Vec<_>>()
            .await
        };

        let mut results = runtime_handle.block_on(uploads)??;
        results.sort_by_key(|k| k.0);

        Ok(results.into_iter().map(|(_, path)| path).collect())
    }

    let folder_path_series = folder_paths.cast(&DataType::Utf8)?;
    let folder_path_arr = folder_path_series.utf8().unwrap();
    let folder_path_arr = prepare_folder_paths(folder_path_arr, series.len(), is_single_folder)?;
    let results = match series.data_type() {
        DataType::Binary => {
            let bytes_array = series
                .binary()
                .unwrap()
                .as_arrow()
                .into_iter()
                .map(|v| v.map(|b| bytes::Bytes::from(b.to_vec())))
                .collect();
            _upload_bytes_to_folder(
                folder_path_arr,
                bytes_array,
                max_connections,
                raise_error_on_failure,
                multi_thread,
                is_single_folder,
                config,
                io_stats,
            )
        }
        DataType::FixedSizeBinary(..) => {
            let bytes_array = series
                .fixed_size_binary()
                .unwrap()
                .as_arrow()
                .into_iter()
                .map(|v| v.map(|b| bytes::Bytes::from(b.to_vec())))
                .collect();
            _upload_bytes_to_folder(
                folder_path_arr,
                bytes_array,
                max_connections,
                raise_error_on_failure,
                multi_thread,
                is_single_folder,
                config,
                io_stats,
            )
        }
        DataType::Utf8 => {
            let bytes_array = series
                .utf8()
                .unwrap()
                .as_arrow()
                .into_iter()
                .map(|utf8_slice| utf8_slice.map(|s| bytes::Bytes::from(s.as_bytes().to_vec())))
                .collect();
            _upload_bytes_to_folder(
                folder_path_arr,
                bytes_array,
                max_connections,
                raise_error_on_failure,
                multi_thread,
                is_single_folder,
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
