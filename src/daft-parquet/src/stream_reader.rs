use std::{
    collections::HashSet,
    fs::File,
    io::{Read, Seek},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use arrow2::{bitmap::Bitmap, io::parquet::read};
use common_error::DaftResult;
use common_runtime::get_compute_runtime;
use daft_core::{prelude::*, utils::arrow::cast_array_for_daft_if_needed};
use daft_dsl::ExprRef;
use daft_io::IOStatsRef;
use daft_table::Table;
use futures::{stream::BoxStream, StreamExt};
use itertools::Itertools;
use rayon::{
    iter::{IntoParallelRefMutIterator, ParallelIterator},
    prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelBridge},
};
use snafu::ResultExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    file::{build_row_ranges, RowGroupRange},
    read::{ArrowChunk, ArrowChunkIters, ParquetSchemaInferenceOptions},
    semaphore::DynamicParquetReadingSemaphore,
    stream_reader::read::schema::infer_schema_with_options,
    UnableToConvertSchemaToDaftSnafu,
};

fn prune_fields_from_schema(
    schema: arrow2::datatypes::Schema,
    columns: Option<&[String]>,
) -> super::Result<arrow2::datatypes::Schema> {
    if let Some(columns) = columns {
        let avail_names = schema
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<HashSet<_>>();
        let mut names_to_keep = HashSet::new();
        for col_name in columns {
            if avail_names.contains(col_name.as_str()) {
                names_to_keep.insert(col_name.to_string());
            }
        }
        Ok(schema.filter(|_, field| names_to_keep.contains(&field.name)))
    } else {
        Ok(schema)
    }
}

#[allow(clippy::too_many_arguments)]
pub fn arrow_column_iters_to_table_iter(
    arr_iters: ArrowChunkIters,
    row_range_start: usize,
    schema_ref: SchemaRef,
    uri: String,
    predicate: Option<ExprRef>,
    original_columns: Option<Vec<String>>,
    original_num_rows: Option<usize>,
    delete_rows: Option<Vec<i64>>,
) -> Option<impl Iterator<Item = DaftResult<Table>>> {
    if arr_iters.is_empty() {
        return None;
    }
    pub struct ParallelLockStepIter {
        pub iters: ArrowChunkIters,
    }
    impl Iterator for ParallelLockStepIter {
        type Item = arrow2::error::Result<ArrowChunk>;

        fn next(&mut self) -> Option<Self::Item> {
            self.iters
                .par_iter_mut()
                .map(std::iter::Iterator::next)
                .collect()
        }
    }
    let par_lock_step_iter = ParallelLockStepIter { iters: arr_iters };

    let mut curr_delete_row_idx = 0;
    // Keep track of the current index in the row group so we can throw away arrays that are not needed
    // and slice arrays that are partially needed.
    let mut index_so_far = 0;
    let owned_schema_ref = schema_ref;
    let table_iter = par_lock_step_iter.into_iter().map(move |chunk| {
        let chunk = chunk.with_context(|_| {
            super::UnableToCreateChunkFromStreamingFileReaderSnafu { path: uri.clone() }
        })?;
        arrow_chunk_to_table(
            chunk,
            &owned_schema_ref,
            &uri,
            row_range_start,
            &mut index_so_far,
            delete_rows.as_deref(),
            &mut curr_delete_row_idx,
            predicate.clone(),
            original_columns.as_deref(),
            original_num_rows,
        )
    });
    Some(table_iter)
}

#[allow(clippy::too_many_arguments)]
fn arrow_chunk_to_table(
    arrow_chunk: ArrowChunk,
    schema_ref: &SchemaRef,
    uri: &str,
    row_range_start: usize,
    index_so_far: &mut usize,
    delete_rows: Option<&[i64]>,
    curr_delete_row_idx: &mut usize,
    predicate: Option<ExprRef>,
    original_columns: Option<&[String]>,
    original_num_rows: Option<usize>,
) -> DaftResult<Table> {
    let all_series = arrow_chunk
        .into_iter()
        .zip(schema_ref.fields.iter())
        .filter_map(|(mut arr, (f_name, _))| {
            if (*index_so_far + arr.len()) < row_range_start {
                // No need to process arrays that are less than the start offset
                return None;
            }
            if *index_so_far < row_range_start {
                // Slice arrays that are partially needed
                let offset = row_range_start.saturating_sub(*index_so_far);
                arr = arr.sliced(offset, arr.len() - offset);
            }
            let series_result =
                Series::try_from((f_name.as_str(), cast_array_for_daft_if_needed(arr)));
            Some(series_result)
        })
        .collect::<DaftResult<Vec<_>>>()?;

    let len = all_series
        .first()
        .map(daft_core::series::Series::len)
        .expect("All series should not be empty when creating table from parquet chunks");
    if all_series.iter().any(|s| s.len() != len) {
        return Err(super::Error::ParquetColumnsDontHaveEqualRows {
            path: uri.to_string(),
        }
        .into());
    }

    let mut table = Table::new_with_size(
        Schema::new(all_series.iter().map(|s| s.field().clone()).collect())?,
        all_series,
        len,
    )
    .with_context(|_| super::UnableToCreateTableFromChunkSnafu {
        path: uri.to_string(),
    })?;

    // Apply delete rows if needed
    if let Some(delete_rows) = &delete_rows
        && !delete_rows.is_empty()
    {
        let mut selection_mask = Bitmap::new_trued(table.len()).make_mut();
        while *curr_delete_row_idx < delete_rows.len()
            && delete_rows[*curr_delete_row_idx] < *index_so_far as i64 + len as i64
        {
            let table_row = delete_rows[*curr_delete_row_idx] as usize - *index_so_far;
            unsafe {
                selection_mask.set_unchecked(table_row, false);
            }
            *curr_delete_row_idx += 1;
        }
        let selection_mask: BooleanArray = ("selection_mask", Bitmap::from(selection_mask)).into();
        table = table.mask_filter(&selection_mask.into_series())?;
    }
    *index_so_far += len;

    // Apply pushdowns if needed
    if let Some(predicate) = predicate {
        table = table.filter(&[predicate])?;
        if let Some(oc) = &original_columns {
            table = table.get_columns(oc)?;
        }
        if let Some(nr) = original_num_rows {
            table = table.head(nr)?;
        }
    }
    Ok(table)
}

/// Spawns a task that reads the column iterators and converts them into a table.
#[allow(clippy::too_many_arguments)]
pub fn spawn_column_iters_to_table_task(
    arr_iters: ArrowChunkIters,
    rg_range: RowGroupRange,
    schema_ref: SchemaRef,
    uri: String,
    predicate: Option<ExprRef>,
    original_columns: Option<Vec<String>>,
    original_num_rows: Option<usize>,
    delete_rows: Option<Vec<i64>>,
    output_sender: tokio::sync::mpsc::Sender<DaftResult<Table>>,
    permit: tokio::sync::OwnedSemaphorePermit,
    record_compute_times_fn: impl Fn(Duration, Duration) + Send + Sync + 'static,
) {
    let (arrow_chunk_senders, mut arrow_chunk_receivers): (Vec<_>, Vec<_>) = arr_iters
        .iter()
        .map(|_| tokio::sync::mpsc::channel(1))
        .unzip();

    let compute_runtime = get_compute_runtime();

    let mut deserializer_handles = Vec::with_capacity(arr_iters.len());
    for (sender, arr_iter) in arrow_chunk_senders.into_iter().zip(arr_iters.into_iter()) {
        let deserialization_task = async move {
            let mut total_deserialization_time = std::time::Duration::ZERO;
            let mut deserialization_iteration_time = std::time::Instant::now();
            for arr in arr_iter {
                let elapsed = deserialization_iteration_time.elapsed();
                total_deserialization_time += elapsed;
                if sender.send(arr).await.is_err() {
                    break;
                }
                deserialization_iteration_time = std::time::Instant::now();
            }
            total_deserialization_time
        };
        deserializer_handles.push(compute_runtime.spawn(deserialization_task));
    }

    compute_runtime.spawn_detached(async move {
        if deserializer_handles.is_empty() {
            let empty = Table::new_with_size(schema_ref.clone(), vec![], rg_range.num_rows);
            let _ = output_sender.send(empty).await;
            return;
        }

        let mut curr_delete_row_idx = 0;
        // Keep track of the current index in the row group so we can throw away arrays that are not needed
        // and slice arrays that are partially needed.
        let mut index_so_far = 0;
        let mut total_compute_time = std::time::Duration::ZERO;
        let mut total_waiting_time = std::time::Duration::ZERO;
        loop {
            let arr_results =
                futures::future::join_all(arrow_chunk_receivers.iter_mut().map(|s| s.recv())).await;
            if arr_results.iter().any(|r| r.is_none()) {
                break;
            }

            let table_creation_start_time = std::time::Instant::now();
            let table = (|| {
                let chunk = arr_results
                    .into_iter()
                    .flatten()
                    .collect::<Result<Vec<_>, _>>()
                    .with_context(|_| super::UnableToCreateChunkFromStreamingFileReaderSnafu {
                        path: uri.clone(),
                    })?;
                arrow_chunk_to_table(
                    chunk,
                    &schema_ref,
                    &uri,
                    rg_range.start,
                    &mut index_so_far,
                    delete_rows.as_deref(),
                    &mut curr_delete_row_idx,
                    predicate.clone(),
                    original_columns.as_deref(),
                    original_num_rows,
                )
            })();
            total_compute_time += table_creation_start_time.elapsed();

            let waiting_start_time = std::time::Instant::now();
            if output_sender.send(table).await.is_err() {
                break;
            }
            let waiting_elapsed = waiting_start_time.elapsed();
            total_waiting_time += waiting_elapsed;
        }
        let average_deserialization_time = futures::future::join_all(deserializer_handles)
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .sum::<Duration>()
            .div_f32(arrow_chunk_receivers.len() as f32);
        total_compute_time += average_deserialization_time;
        record_compute_times_fn(total_compute_time, total_waiting_time);
        drop(permit);
    });
}

struct CountingReader<R> {
    reader: R,
    count: usize,
    io_stats: Option<IOStatsRef>,
}

impl<R> CountingReader<R> {
    fn update_count(&mut self) {
        if let Some(ios) = &self.io_stats {
            ios.mark_bytes_read(self.count);
            self.count = 0;
        }
    }
}

impl<R> Read for CountingReader<R>
where
    R: Read + Seek,
{
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.reader.read(buf)?;
        self.count += read;
        Ok(read)
    }
    #[inline]
    fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> std::io::Result<usize> {
        let read = self.reader.read_vectored(bufs)?;
        self.count += read;
        Ok(read)
    }
    #[inline]
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        let read = self.reader.read_to_end(buf)?;
        self.count += read;
        Ok(read)
    }
}

impl<R> Seek for CountingReader<R>
where
    R: Read + Seek,
{
    #[inline]
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.reader.seek(pos)
    }
}

impl<R> Drop for CountingReader<R> {
    fn drop(&mut self) {
        self.update_count();
    }
}

#[allow(clippy::too_many_arguments)]
pub fn local_parquet_read_into_column_iters(
    uri: &str,
    columns: Option<&[String]>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<parquet2::metadata::FileMetaData>>,
    chunk_size: usize,
    io_stats: Option<IOStatsRef>,
    semaphore: Arc<DynamicParquetReadingSemaphore>,
) -> super::Result<(
    Arc<parquet2::metadata::FileMetaData>,
    SchemaRef,
    Vec<RowGroupRange>,
    impl Iterator<Item = super::Result<ArrowChunkIters>>,
)> {
    const LOCAL_PROTOCOL: &str = "file://";
    let uri = uri
        .strip_prefix(LOCAL_PROTOCOL)
        .map(std::string::ToString::to_string)
        .unwrap_or_else(|| uri.to_string());

    let reader = File::open(uri.clone()).with_context(|_| super::InternalIOSnafu {
        path: uri.to_string(),
    })?;
    io_stats.as_ref().inspect(|ios| ios.mark_get_requests(1));
    let size = reader
        .metadata()
        .with_context(|_| super::InternalIOSnafu {
            path: uri.to_string(),
        })?
        .len();

    if size < 12 {
        return Err(super::Error::FileTooSmall {
            path: uri,
            file_size: size as usize,
        });
    }
    let mut reader = CountingReader {
        reader,
        count: 0,
        io_stats,
    };

    let metadata = match metadata {
        Some(m) => m,
        None => read::read_metadata(&mut reader)
            .map(Arc::new)
            .with_context(|_| super::UnableToParseMetadataFromLocalFileSnafu {
                path: uri.to_string(),
            })?,
    };

    let schema = infer_schema_with_options(&metadata, Some(schema_infer_options.into()))
        .with_context(|_| super::UnableToParseSchemaFromMetadataSnafu {
            path: uri.to_string(),
        })?;
    let schema = prune_fields_from_schema(schema, columns)?;
    let daft_schema =
        Schema::try_from(&schema).with_context(|_| UnableToConvertSchemaToDaftSnafu {
            path: uri.to_string(),
        })?;

    let row_ranges = build_row_ranges(
        num_rows,
        0,
        row_groups,
        predicate,
        &daft_schema,
        &metadata,
        &uri,
    )?;

    let all_row_groups = metadata.row_groups.clone();

    // Read all the required row groups into memory sequentially
    let column_iters_per_rg = row_ranges.clone().into_iter().map(move |rg_range| {
        let rg_metadata = all_row_groups.get(&rg_range.row_group_index).unwrap();
        let read_start_time = std::time::Instant::now();

        // This operation is IO-bounded O(C) where C is the number of columns in the row group.
        // It reads all the columns to memory from the row group associated to the requested fields,
        // and returns a Vec of iterators that perform decompression and deserialization for each column.
        let single_rg_column_iter = read::read_columns_many(
            &mut reader,
            rg_metadata,
            schema.fields.clone(),
            Some(chunk_size),
            Some(rg_range.num_rows),
            None,
        )
        .with_context(|_| super::UnableToReadParquetRowGroupSnafu { path: uri.clone() })?;
        reader.update_count();
        semaphore.record_io_time(read_start_time.elapsed());
        Ok(single_rg_column_iter)
    });
    Ok((
        metadata,
        Arc::new(daft_schema),
        row_ranges,
        column_iters_per_rg,
    ))
}

#[allow(clippy::too_many_arguments)]
pub fn local_parquet_read_into_arrow(
    uri: &str,
    columns: Option<&[String]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<parquet2::metadata::FileMetaData>>,
    chunk_size: Option<usize>,
) -> super::Result<(
    Arc<parquet2::metadata::FileMetaData>,
    arrow2::datatypes::Schema,
    Vec<ArrowChunk>,
    usize,
)> {
    const LOCAL_PROTOCOL: &str = "file://";

    let uri = uri.strip_prefix(LOCAL_PROTOCOL).unwrap_or(uri);

    let mut reader = File::open(uri).with_context(|_| super::InternalIOSnafu {
        path: uri.to_string(),
    })?;
    let size = reader
        .metadata()
        .with_context(|_| super::InternalIOSnafu {
            path: uri.to_string(),
        })?
        .len();

    if size < 12 {
        return Err(super::Error::FileTooSmall {
            path: uri.into(),
            file_size: size as usize,
        });
    }
    let metadata = match metadata {
        Some(m) => m,
        None => read::read_metadata(&mut reader)
            .with_context(|_| super::UnableToParseMetadataFromLocalFileSnafu {
                path: uri.to_string(),
            })
            .map(Arc::new)?,
    };

    // and infer a [`Schema`] from the `metadata`.
    let schema = infer_schema_with_options(&metadata, Some(schema_infer_options.into()))
        .with_context(|_| super::UnableToParseSchemaFromMetadataSnafu {
            path: uri.to_string(),
        })?;
    let schema = prune_fields_from_schema(schema, columns)?;
    let daft_schema =
        Schema::try_from(&schema).with_context(|_| UnableToConvertSchemaToDaftSnafu {
            path: uri.to_string(),
        })?;
    let chunk_size = chunk_size.unwrap_or(128 * 1024);
    let max_rows = metadata.num_rows.min(num_rows.unwrap_or(metadata.num_rows));

    let num_expected_arrays = f32::ceil(max_rows as f32 / chunk_size as f32) as usize;
    let row_ranges = build_row_ranges(
        num_rows,
        start_offset.unwrap_or(0),
        row_groups,
        predicate,
        &daft_schema,
        &metadata,
        uri,
    )?;

    let columns_iters_per_rg = row_ranges
        .iter()
        .enumerate()
        .map(|(req_idx, rg_range)| {
            let rg = metadata.row_groups.get(&rg_range.row_group_index).unwrap();
            let single_rg_column_iter = read::read_columns_many(
                &mut reader,
                rg,
                schema.fields.clone(),
                Some(chunk_size),
                Some(rg_range.num_rows),
                None,
            );
            let single_rg_column_iter = single_rg_column_iter?;
            arrow2::error::Result::Ok(
                single_rg_column_iter
                    .into_iter()
                    .enumerate()
                    .map(move |(idx, iter)| (req_idx, rg_range, idx, iter)),
            )
        })
        .flatten_ok();

    let columns_iters_per_rg = columns_iters_per_rg.par_bridge();
    let collected_columns = columns_iters_per_rg.map(|payload: Result<_, _>| {
        let (req_idx, row_range, col_idx, arr_iter) = payload?;

        let mut arrays_so_far = vec![];
        let mut curr_index = 0;

        for arr in arr_iter {
            let arr = arr?;
            if (curr_index + arr.len()) < row_range.start {
                // throw arrays less than what we need
                curr_index += arr.len();
                continue;
            } else if curr_index < row_range.start {
                let offset = row_range.start.saturating_sub(curr_index);
                arrays_so_far.push(arr.sliced(offset, arr.len() - offset));
                curr_index += arr.len();
            } else {
                curr_index += arr.len();
                arrays_so_far.push(arr);
            }
        }
        Ok((req_idx, col_idx, arrays_so_far))
    });
    let mut all_columns = vec![Vec::with_capacity(num_expected_arrays); schema.fields.len()];
    let mut all_computed = collected_columns
        .collect::<Result<Vec<_>, _>>()
        .with_context(|_| super::UnableToCreateChunkFromStreamingFileReaderSnafu {
            path: uri.to_string(),
        })?;

    // sort by row groups that were requested
    all_computed.sort_by_key(|(req_idx, _, _)| *req_idx);
    for (_, col_idx, v) in all_computed {
        all_columns
            .get_mut(col_idx)
            .expect("array index during scatter out of index")
            .extend(v);
    }
    Ok((
        metadata,
        schema,
        all_columns,
        row_ranges.iter().map(|rr| rr.num_rows).sum(),
    ))
}

#[allow(clippy::too_many_arguments)]
pub async fn local_parquet_read_async(
    uri: &str,
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<parquet2::metadata::FileMetaData>>,
    chunk_size: Option<usize>,
) -> DaftResult<(Arc<parquet2::metadata::FileMetaData>, Table)> {
    let (send, recv) = tokio::sync::oneshot::channel();
    let uri = uri.to_string();
    rayon::spawn(move || {
        let result = (move || {
            let v = local_parquet_read_into_arrow(
                &uri,
                columns.as_deref(),
                start_offset,
                num_rows,
                row_groups.as_deref(),
                predicate,
                schema_infer_options,
                metadata,
                chunk_size,
            );
            let (metadata, schema, arrays, num_rows_read) = v?;

            let converted_arrays = arrays
                .into_par_iter()
                .zip(schema.fields)
                .map(|(v, f)| {
                    let f_name = f.name.as_str();
                    if v.is_empty() {
                        Ok(Series::empty(f_name, &f.data_type().into()))
                    } else {
                        let casted_arrays = v
                            .into_iter()
                            .map(move |a| {
                                Series::try_from((f_name, cast_array_for_daft_if_needed(a)))
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        Series::concat(casted_arrays.iter().collect::<Vec<_>>().as_slice())
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((
                metadata,
                Table::new_with_size(
                    Schema::new(converted_arrays.iter().map(|s| s.field().clone()).collect())?,
                    converted_arrays,
                    num_rows_read,
                )?,
            ))
        })();
        let _ = send.send(result);
    });

    recv.await.context(super::OneShotRecvSnafu {})?
}

#[allow(clippy::too_many_arguments)]
pub async fn local_parquet_stream(
    uri: &str,
    original_columns: Option<Vec<String>>,
    columns: Option<Vec<String>>,
    original_num_rows: Option<usize>,
    num_rows: Option<usize>,
    delete_rows: Option<Vec<i64>>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<parquet2::metadata::FileMetaData>>,
    maintain_order: bool,
    io_stats: Option<IOStatsRef>,
) -> DaftResult<(
    Arc<parquet2::metadata::FileMetaData>,
    BoxStream<'static, DaftResult<Table>>,
)> {
    let chunk_size = 128 * 1024;
    // We use a semaphore to limit the number of concurrent row group deserialization tasks.
    // Set the maximum number of concurrent tasks to 2 * number of available threads.
    let semaphore = DynamicParquetReadingSemaphore::new(
        std::thread::available_parallelism()
            .unwrap_or(NonZeroUsize::new(2).unwrap())
            .checked_mul(2.try_into().unwrap())
            .unwrap()
            .into(),
    );
    let (metadata, schema_ref, row_ranges, column_iters) = local_parquet_read_into_column_iters(
        uri,
        columns.as_deref(),
        num_rows,
        row_groups.as_deref(),
        predicate.clone(),
        schema_infer_options,
        metadata,
        chunk_size,
        io_stats,
        semaphore.clone(),
    )?;

    let (output_senders, output_receivers) = row_ranges
        .iter()
        .map(|_| tokio::sync::mpsc::channel(1))
        .unzip::<_, _, Vec<_>, Vec<_>>();

    let owned_uri = uri.to_string();
    let compute_runtime = get_compute_runtime();
    compute_runtime.spawn_detached(async move {
        for ((column_iters, sender), rg_range) in column_iters.zip(output_senders).zip(row_ranges) {
            if let Err(e) = column_iters {
                let _ = sender.send(Err(e.into())).await;
                break;
            }

            let permit = semaphore.acquire().await;
            let semaphore_ref = semaphore.clone();
            spawn_column_iters_to_table_task(
                column_iters.unwrap(),
                rg_range,
                schema_ref.clone(),
                owned_uri.clone(),
                predicate.clone(),
                original_columns.clone(),
                original_num_rows,
                delete_rows.clone(),
                sender,
                permit,
                move |compute_time, waiting_time| {
                    semaphore_ref.record_compute_times(compute_time, waiting_time);
                },
            );
        }
    });

    let result_stream =
        futures::stream::iter(output_receivers.into_iter().map(ReceiverStream::new));

    match maintain_order {
        true => Ok((metadata, Box::pin(result_stream.flatten()))),
        false => Ok((metadata, Box::pin(result_stream.flatten_unordered(None)))),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn local_parquet_read_into_arrow_async(
    uri: &str,
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<parquet2::metadata::FileMetaData>>,
    chunk_size: Option<usize>,
) -> super::Result<(
    Arc<parquet2::metadata::FileMetaData>,
    arrow2::datatypes::Schema,
    Vec<ArrowChunk>,
    usize,
)> {
    let (send, recv) = tokio::sync::oneshot::channel();
    let uri = uri.to_string();
    rayon::spawn(move || {
        let v = local_parquet_read_into_arrow(
            &uri,
            columns.as_deref(),
            start_offset,
            num_rows,
            row_groups.as_deref(),
            predicate,
            schema_infer_options,
            metadata,
            chunk_size,
        );
        let _ = send.send(v);
    });

    recv.await.context(super::OneShotRecvSnafu {})?
}
