use std::{cmp::max, collections::HashSet, fs::File, sync::Arc};

use common_error::DaftResult;
use common_runtime::{RuntimeTask, combine_stream, get_compute_runtime};
use daft_arrow::{bitmap::Bitmap, io::parquet::read};
use daft_core::prelude::*;
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr};
use daft_io::{CountingReader, IOStatsRef};
use daft_recordbatch::RecordBatch;
use futures::{FutureExt, StreamExt, stream::BoxStream};
use itertools::Itertools;
use rayon::{
    iter::ParallelIterator,
    prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelBridge},
};
use snafu::ResultExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    PARQUET_MORSEL_SIZE, determine_parquet_parallelism,
    file::{RowGroupRange, build_row_ranges},
    infer_arrow_schema_from_metadata,
    read::{ArrowChunk, ArrowChunkIters, ParquetSchemaInferenceOptions},
};

fn prune_fields_from_schema(
    schema: daft_arrow::datatypes::Schema,
    columns: Option<&[String]>,
) -> super::Result<daft_arrow::datatypes::Schema> {
    if let Some(columns) = columns {
        let avail_names = schema
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<HashSet<_>>();
        let mut names_to_keep = HashSet::new();
        for col_name in columns {
            if avail_names.contains(col_name.as_str()) {
                names_to_keep.insert(col_name.clone());
            }
        }
        Ok(schema.filter(|_, field| names_to_keep.contains(&field.name)))
    } else {
        Ok(schema)
    }
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
) -> DaftResult<RecordBatch> {
    let all_series = arrow_chunk
        .into_iter()
        .zip(schema_ref.field_names())
        .filter_map(|(mut arr, f_name)| {
            if (*index_so_far + arr.len()) < row_range_start {
                // No need to process arrays that are less than the start offset
                return None;
            }
            if *index_so_far < row_range_start {
                // Slice arrays that are partially needed
                let offset = row_range_start.saturating_sub(*index_so_far);
                arr = arr.sliced(offset, arr.len() - offset);
            }
            let series_result = Series::try_from((f_name, arr));
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

    let mut table = RecordBatch::new_with_size(
        Schema::new(all_series.iter().map(|s| s.field().clone())),
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
        let predicate = BoundExpr::try_new(predicate, &table.schema)?;
        table = table.filter(&[predicate])?;
        if let Some(oc) = &original_columns {
            let oc_indices = oc
                .iter()
                .map(|name| table.schema.get_index(name))
                .collect::<DaftResult<Vec<_>>>()?;

            table = table.get_columns(&oc_indices);
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
    output_sender: tokio::sync::mpsc::Sender<DaftResult<RecordBatch>>,
    permit: tokio::sync::OwnedSemaphorePermit,
    channel_size: usize,
) -> RuntimeTask<DaftResult<()>> {
    let (arrow_chunk_senders, mut arrow_chunk_receivers): (Vec<_>, Vec<_>) = arr_iters
        .iter()
        .map(|_| tokio::sync::mpsc::channel(channel_size))
        .unzip();

    let compute_runtime = get_compute_runtime();

    let deserializer_handles = arrow_chunk_senders
        .into_iter()
        .zip(arr_iters)
        .map(|(sender, arr_iter)| {
            let deserialization_task = async move {
                for arr in arr_iter {
                    if sender.send(arr?).await.is_err() {
                        break;
                    }
                }
                DaftResult::Ok(())
            };
            compute_runtime.spawn(deserialization_task)
        })
        .collect::<Vec<_>>();

    compute_runtime.spawn(async move {
        if deserializer_handles.is_empty() {
            let empty = RecordBatch::new_with_size(schema_ref.clone(), vec![], rg_range.num_rows);
            let _ = output_sender.send(empty).await;
            return Ok(());
        }

        let mut curr_delete_row_idx = 0;
        // Keep track of the current index in the row group so we can throw away arrays that are not needed
        // and slice arrays that are partially needed.
        let mut index_so_far = 0;
        loop {
            let chunk =
                futures::future::join_all(arrow_chunk_receivers.iter_mut().map(|s| s.recv()))
                    .await
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();
            if chunk.len() != deserializer_handles.len() {
                break;
            }
            let table = arrow_chunk_to_table(
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
            )?;

            if output_sender.send(Ok(table)).await.is_err() {
                break;
            }
        }

        futures::future::try_join_all(deserializer_handles)
            .await?
            .into_iter()
            .collect::<DaftResult<()>>()?;

        drop(permit);
        DaftResult::Ok(())
    })
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
pub fn local_parquet_read_into_column_iters(
    uri: String,
    columns: Option<Vec<String>>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<parquet2::metadata::FileMetaData>>,
    chunk_size: usize,
    io_stats: Option<IOStatsRef>,
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
        .unwrap_or_else(|| uri.clone());

    let reader =
        File::open(uri.clone()).with_context(|_| super::InternalIOSnafu { path: uri.clone() })?;
    io_stats.as_ref().inspect(|ios| ios.mark_get_requests(1));
    let size = reader
        .metadata()
        .with_context(|_| super::InternalIOSnafu { path: uri.clone() })?
        .len();

    if size < 12 {
        return Err(super::Error::FileTooSmall {
            path: uri,
            file_size: size as usize,
        });
    }
    let mut reader = CountingReader::new(reader, io_stats);

    let metadata = match metadata {
        Some(m) => m,
        None => read::read_metadata(&mut reader)
            .map(Arc::new)
            .with_context(|_| super::UnableToParseMetadataFromLocalFileSnafu {
                path: uri.clone(),
            })?,
    };

    let inferred_schema =
        infer_arrow_schema_from_metadata(&metadata, Some(schema_infer_options.into()))
            .with_context(|_| super::UnableToParseSchemaFromMetadataSnafu { path: uri.clone() })?;
    let schema = prune_fields_from_schema(inferred_schema.into(), columns.as_deref())?;
    let daft_schema = Schema::from(&schema);

    let row_ranges = build_row_ranges(
        num_rows,
        0,
        row_groups.as_deref(),
        predicate,
        &daft_schema,
        &metadata,
        &uri,
    )?;

    let all_row_groups = metadata.row_groups.clone();

    // Read all the required row groups into memory sequentially
    let column_iters_per_rg = row_ranges.clone().into_iter().map(move |rg_range| {
        let rg_metadata = all_row_groups.get(&rg_range.row_group_index).unwrap();

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
    daft_arrow::datatypes::Schema,
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
    let inferred_schema =
        infer_arrow_schema_from_metadata(&metadata, Some(schema_infer_options.into()))
            .with_context(|_| super::UnableToParseSchemaFromMetadataSnafu {
                path: uri.to_string(),
            })?;
    let schema = prune_fields_from_schema(inferred_schema, columns)?;
    let daft_schema = Schema::from(&schema);
    let chunk_size = chunk_size.unwrap_or(PARQUET_MORSEL_SIZE);
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
            daft_arrow::error::Result::Ok(
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
) -> DaftResult<(Arc<parquet2::metadata::FileMetaData>, RecordBatch)> {
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
                            .map(move |a| Series::try_from((f_name, a)))
                            .collect::<Result<Vec<_>, _>>()?;
                        Series::concat(casted_arrays.iter().collect::<Vec<_>>().as_slice())
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((
                metadata,
                RecordBatch::new_with_size(
                    Schema::new(converted_arrays.iter().map(|s| s.field().clone())),
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
    uri: String,
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
    chunk_size: Option<usize>,
) -> DaftResult<(
    Arc<parquet2::metadata::FileMetaData>,
    BoxStream<'static, DaftResult<RecordBatch>>,
)> {
    let chunk_size = chunk_size.unwrap_or(PARQUET_MORSEL_SIZE);
    let (metadata, schema_ref, row_ranges, column_iters) = local_parquet_read_into_column_iters(
        uri.clone(),
        columns,
        num_rows,
        row_groups,
        predicate.clone(),
        schema_infer_options,
        metadata,
        chunk_size,
        io_stats,
    )?;

    // We use a semaphore to limit the number of concurrent row group deserialization tasks.
    // Set the maximum number of concurrent tasks to ceil(number of available threads / columns).
    let num_parallel_tasks = determine_parquet_parallelism(&schema_ref);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(num_parallel_tasks));

    let compute_runtime = get_compute_runtime();

    let (output_senders, output_receivers): (Vec<_>, Vec<_>) = row_ranges
        .iter()
        .map(|_| tokio::sync::mpsc::channel(1))
        .unzip();

    let parquet_task = compute_runtime.spawn(async move {
        let mut table_tasks = Vec::with_capacity(row_ranges.len());
        for ((column_iters, sender), rg_range) in column_iters.zip(output_senders).zip(row_ranges) {
            if let Err(e) = column_iters {
                let _ = sender.send(Err(e.into())).await;
                break;
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let table_task = spawn_column_iters_to_table_task(
                column_iters.unwrap(),
                rg_range,
                schema_ref.clone(),
                uri.clone(),
                predicate.clone(),
                original_columns.clone(),
                original_num_rows,
                delete_rows.clone(),
                sender,
                permit,
                max(PARQUET_MORSEL_SIZE / chunk_size, 1),
            );
            table_tasks.push(table_task);
        }

        futures::future::try_join_all(table_tasks)
            .await?
            .into_iter()
            .collect::<DaftResult<()>>()?;

        DaftResult::Ok(())
    });

    let stream_of_streams =
        futures::stream::iter(output_receivers.into_iter().map(ReceiverStream::new));
    let parquet_task = parquet_task.map(|x| x?);
    let combined = match maintain_order {
        true => combine_stream(stream_of_streams.flatten(), parquet_task).boxed(),
        false => combine_stream(stream_of_streams.flatten_unordered(None), parquet_task).boxed(),
    };
    Ok((metadata, combined))
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
    daft_arrow::datatypes::Schema,
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
