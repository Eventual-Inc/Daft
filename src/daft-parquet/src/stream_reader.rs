use std::{
    collections::HashSet,
    fs::File,
    io::{Read, Seek},
    sync::Arc,
};

use arrow2::{bitmap::Bitmap, io::parquet::read};
use common_error::DaftResult;
use common_runtime::get_compute_runtime;
use daft_core::{prelude::*, utils::arrow::cast_array_for_daft_if_needed};
use daft_dsl::ExprRef;
use daft_io::IOStatsRef;
use daft_table::Table;
use futures::{
    future::{join_all, try_join_all},
    stream::BoxStream,
    Stream, StreamExt,
};
use itertools::Itertools;
use snafu::ResultExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    file::{build_row_ranges, RowGroupRange},
    read::{ArrowChunk, ArrowChunkIter, ParquetSchemaInferenceOptions},
    stream_reader::read::schema::infer_schema_with_options,
    JoinSnafu, UnableToConvertSchemaToDaftSnafu,
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

type ArrowArrayReceiver =
    tokio::sync::mpsc::Receiver<DaftResult<arrow2::error::Result<Box<dyn arrow2::array::Array>>>>;
#[allow(clippy::too_many_arguments)]
pub fn arrow_array_receivers_to_table_stream(
    mut arrow_array_receivers: Vec<ArrowArrayReceiver>,
    row_range: RowGroupRange,
    schema_ref: SchemaRef,
    uri: String,
    predicate: Option<ExprRef>,
    original_columns: Option<Vec<String>>,
    original_num_rows: Option<usize>,
    delete_rows: Option<Vec<i64>>,
) -> DaftResult<impl Stream<Item = DaftResult<Table>>> {
    let (output_sender, output_receiver) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        if arrow_array_receivers.is_empty() {
            let empty_table = Table::new_with_size(schema_ref.clone(), vec![], row_range.num_rows);
            let _ = output_sender.send(empty_table).await;
            return;
        }

        let mut curr_delete_row_idx = 0;
        // Keep track of the current index in the row group so we can throw away arrays that are not needed
        // and slice arrays that are partially needed.
        let mut index_so_far = 0;
        let owned_schema_ref = schema_ref;
        loop {
            let arr_results = join_all(arrow_array_receivers.iter_mut().map(|s| s.recv())).await;
            if arr_results.iter().any(|r| r.is_none()) {
                break;
            }
            let table = (|| {
                let all_series = arr_results
                    .into_iter()
                    .zip(owned_schema_ref.as_ref().fields.keys())
                    .filter_map(|(arr_result, f_name)| {
                        let series_result = (|| {
                            let mut arr = arr_result.unwrap()?.with_context(|_| {
                                super::UnableToCreateChunkFromStreamingFileReaderSnafu {
                                    path: uri.to_string(),
                                }
                            })?;

                            if (index_so_far + arr.len()) < row_range.start {
                                return Ok(None);
                            }

                            if index_so_far < row_range.start {
                                let offset = row_range.start.saturating_sub(index_so_far);
                                arr = arr.sliced(offset, arr.len() - offset);
                            }

                            let series_result = Series::try_from((
                                f_name.as_str(),
                                cast_array_for_daft_if_needed(arr),
                            ))?;
                            Ok(Some(series_result))
                        })();
                        series_result.transpose()
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                let len = all_series.first().map(Series::len).expect(
                    "All series should not be empty when creating table from parquet chunks",
                );
                if all_series.iter().any(|s| s.len() != len) {
                    return Err(super::Error::ParquetColumnsDontHaveEqualRows {
                        path: uri.clone(),
                    }
                    .into());
                }

                let mut table = Table::new_with_size(
                    Schema::new(all_series.iter().map(|s| s.field().clone()).collect())?,
                    all_series,
                    len,
                )
                .with_context(|_| super::UnableToCreateTableFromChunkSnafu { path: uri.clone() })?;

                // Apply delete rows if needed
                if let Some(delete_rows) = &delete_rows
                    && !delete_rows.is_empty()
                {
                    let mut selection_mask = Bitmap::new_trued(table.len()).make_mut();
                    while curr_delete_row_idx < delete_rows.len()
                        && delete_rows[curr_delete_row_idx] < index_so_far as i64 + len as i64
                    {
                        let table_row = delete_rows[curr_delete_row_idx] as usize - index_so_far;
                        unsafe {
                            selection_mask.set_unchecked(table_row, false);
                        }
                        curr_delete_row_idx += 1;
                    }
                    let selection_mask: BooleanArray =
                        ("selection_mask", Bitmap::from(selection_mask)).into();
                    table = table.mask_filter(&selection_mask.into_series())?;
                }
                index_so_far += len;

                // Apply pushdowns if needed
                if let Some(predicate) = &predicate {
                    table = table.filter(&[predicate.clone()])?;
                    if let Some(oc) = &original_columns {
                        table = table.get_columns(oc)?;
                    }
                    if let Some(nr) = original_num_rows {
                        table = table.head(nr)?;
                    }
                }
                Ok(table)
            })();
            let _ = output_sender.send(table).await;
        }
    });
    Ok(Box::pin(ReceiverStream::new(output_receiver)))
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
) -> super::Result<(
    Arc<parquet2::metadata::FileMetaData>,
    SchemaRef,
    Vec<RowGroupRange>,
    impl Iterator<Item = super::Result<Vec<ArrowChunkIter>>>,
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
pub async fn local_parquet_read_into_arrow(
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
    let total_rows = row_ranges.iter().map(|rr| rr.num_rows).sum();

    let columns_iters_per_rg = row_ranges
        .into_iter()
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

    let collected_column_handles = columns_iters_per_rg.map(|payload: Result<_, _>| {
        tokio::spawn(async move {
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
        })
    });
    let mut all_computed = try_join_all(collected_column_handles)
        .await
        .context(JoinSnafu { path: uri })?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .with_context(|_| super::UnableToCreateChunkFromStreamingFileReaderSnafu {
            path: uri.to_string(),
        })?;
    let mut all_columns = vec![Vec::with_capacity(num_expected_arrays); schema.fields.len()];

    // sort by row groups that were requested
    all_computed.sort_by_key(|(req_idx, _, _)| *req_idx);
    for (_, col_idx, v) in all_computed {
        all_columns
            .get_mut(col_idx)
            .expect("array index during scatter out of index")
            .extend(v);
    }
    Ok((metadata, schema, all_columns, total_rows))
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
    let compute_runtime = get_compute_runtime();
    let uri = uri.to_string();
    compute_runtime
        .await_on(async move {
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
            )
            .await;
            let (metadata, schema, arrays, num_rows_read) = v?;

            let converted_array_handles = arrays.into_iter().zip(schema.fields).map(|(v, f)| {
                tokio::spawn(async move {
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
            });

            let converted_arrays = try_join_all(converted_array_handles)
                .await
                .context(JoinSnafu { path: uri.clone() })?
                .into_iter()
                .collect::<DaftResult<Vec<_>>>()?;
            Ok((
                metadata,
                Table::new_with_size(
                    Schema::new(converted_arrays.iter().map(|s| s.field().clone()).collect())?,
                    converted_arrays,
                    num_rows_read,
                )?,
            ))
        })
        .await?
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
    let runtime = get_compute_runtime();
    let uri = uri.to_string();
    let (metadata, streams) = runtime
        .await_on(async move {
            let (metadata, schema_ref, row_ranges, column_iters) =
                local_parquet_read_into_column_iters(
                    &uri,
                    columns.as_deref(),
                    num_rows,
                    row_groups.as_deref(),
                    predicate.clone(),
                    schema_infer_options,
                    metadata,
                    chunk_size,
                    io_stats,
                )?;

            let streams = column_iters
                .zip(row_ranges)
                .map(|(column_iter_result, rg_range)| {
                    // For each vec of column iters, iterate through them in parallel lock step such that each iteration
                    // produces a chunk of the row group that can be converted into a table.
                    let column_iters = column_iter_result?;
                    let (senders, receivers): (Vec<_>, Vec<_>) = (0..column_iters.len())
                        .map(|_| tokio::sync::mpsc::channel(1))
                        .unzip();
                    for (column_iter, sender) in column_iters.into_iter().zip(senders) {
                        tokio::spawn(async move {
                            for chunk in column_iter {
                                let _ = sender.send(Ok(chunk)).await;
                            }
                        });
                    }
                    arrow_array_receivers_to_table_stream(
                        receivers,
                        rg_range,
                        schema_ref.clone(),
                        uri.clone(),
                        predicate.clone(),
                        original_columns.clone(),
                        original_num_rows,
                        delete_rows.clone(),
                    )
                })
                .collect::<DaftResult<Vec<_>>>()?;
            DaftResult::Ok((metadata, streams))
        })
        .await??;

    let result_stream = futures::stream::iter(streams);

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
) -> DaftResult<(
    Arc<parquet2::metadata::FileMetaData>,
    arrow2::datatypes::Schema,
    Vec<ArrowChunk>,
    usize,
)> {
    let compute_runtime = get_compute_runtime();
    let uri = uri.to_string();
    Ok(compute_runtime
        .await_on(async move {
            local_parquet_read_into_arrow(
                &uri,
                columns.as_deref(),
                start_offset,
                num_rows,
                row_groups.as_deref(),
                predicate,
                schema_infer_options,
                metadata,
                chunk_size,
            )
            .await
        })
        .await??)
}
