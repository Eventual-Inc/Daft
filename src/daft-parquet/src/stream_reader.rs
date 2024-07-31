use std::{collections::HashSet, fs::File, sync::Arc};

use arrow2::io::parquet::read;
use common_error::DaftResult;
use daft_core::{
    schema::{Schema, SchemaRef},
    utils::arrow::cast_array_for_daft_if_needed,
    Series,
};
use daft_dsl::ExprRef;
use daft_table::Table;
use futures::{stream::BoxStream, StreamExt};
use itertools::Itertools;
use rayon::{
    iter::IntoParallelRefMutIterator,
    prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelBridge},
};
use snafu::ResultExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    file::{build_row_ranges, RowGroupRange},
    read::{ArrowChunk, ArrowChunkIters, ParquetSchemaInferenceOptions},
    UnableToConvertSchemaToDaftSnafu,
};

use crate::stream_reader::read::schema::infer_schema_with_options;
use rayon::iter::ParallelIterator;

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
pub(crate) fn local_parquet_read_into_column_iters(
    uri: &str,
    columns: Option<&[String]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<parquet2::metadata::FileMetaData>>,
    chunk_size: usize,
) -> super::Result<(
    Arc<parquet2::metadata::FileMetaData>,
    SchemaRef,
    Vec<RowGroupRange>,
    impl Iterator<Item = super::Result<ArrowChunkIters>>,
)> {
    const LOCAL_PROTOCOL: &str = "file://";
    let uri = uri
        .strip_prefix(LOCAL_PROTOCOL)
        .map(|s| s.to_string())
        .unwrap_or(uri.to_string());

    let mut reader = File::open(uri.clone()).with_context(|_| super::InternalIOSnafu {
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
            path: uri,
            file_size: size as usize,
        });
    }

    // Use block in place to read metadata as the current function is in an asynchronous context.
    let metadata = match metadata {
        Some(m) => m,
        None => tokio::task::block_in_place(|| read::read_metadata(&mut reader).map(Arc::new))
            .with_context(|_| super::UnableToParseMetadataFromLocalFileSnafu {
                path: uri.to_string(),
            })?,
    };

    let schema = infer_schema_with_options(&metadata, &Some(schema_infer_options.into()))
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
        start_offset.unwrap_or(0),
        row_groups,
        predicate.clone(),
        &daft_schema,
        &metadata,
        &uri,
    )?;

    let required_row_group_idxs = row_ranges
        .iter()
        .map(|rg_range| rg_range.row_group_index)
        .collect::<Vec<_>>();
    let all_row_groups = metadata.row_groups.clone();

    // Read all the required row groups into memory sequentially
    let column_iters_per_rg = required_row_group_idxs.into_iter().map(move |idx| {
        let rg_metadata = all_row_groups.get(idx).unwrap();

        // This operation is IO-bounded O(C) where C is the number of columns in the row group.
        // It reads all the columns to memory from the row group associated to the requested fields,
        // and returns a Vec of iterators that perform decompression and deserialization for each column.
        let single_rg_column_iter = read::read_columns_many(
            &mut reader,
            rg_metadata,
            schema.fields.clone(),
            Some(chunk_size),
            num_rows,
            None,
        )
        .with_context(|_| super::UnableToReadParquetRowGroupSnafu { path: uri.clone() })?;
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
pub(crate) fn local_parquet_read_into_arrow(
    uri: &str,
    columns: Option<&[String]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<parquet2::metadata::FileMetaData>>,
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
    let schema = infer_schema_with_options(&metadata, &Some(schema_infer_options.into()))
        .with_context(|_| super::UnableToParseSchemaFromMetadataSnafu {
            path: uri.to_string(),
        })?;
    let schema = prune_fields_from_schema(schema, columns)?;
    let daft_schema =
        Schema::try_from(&schema).with_context(|_| UnableToConvertSchemaToDaftSnafu {
            path: uri.to_string(),
        })?;
    let chunk_size = 128 * 1024;
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
            let rg = metadata.row_groups.get(rg_range.row_group_index).unwrap();
            let single_rg_column_iter = read::read_columns_many(
                &mut reader,
                rg,
                schema.fields.clone(),
                Some(chunk_size),
                num_rows,
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
pub(crate) async fn local_parquet_read_async(
    uri: &str,
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<parquet2::metadata::FileMetaData>>,
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
pub(crate) fn local_parquet_stream(
    uri: &str,
    original_columns: Option<Vec<String>>,
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    original_num_rows: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<parquet2::metadata::FileMetaData>>,
) -> DaftResult<(
    Arc<parquet2::metadata::FileMetaData>,
    BoxStream<'static, DaftResult<Table>>,
)> {
    let chunk_size = 128 * 1024;
    let (metadata, schema_ref, row_ranges, column_iters) = local_parquet_read_into_column_iters(
        uri,
        columns.as_deref(),
        start_offset,
        num_rows,
        row_groups.as_deref(),
        predicate.clone(),
        schema_infer_options,
        metadata,
        chunk_size,
    )?;

    // Create a channel for each row group to send the processed tables to the stream
    // Each channel is expected to have a number of chunks equal to the number of chunks in the row group
    let (senders, receivers): (Vec<_>, Vec<_>) = row_ranges
        .iter()
        .map(|rg_range| {
            let expected_num_chunks =
                f32::ceil(rg_range.num_rows as f32 / chunk_size as f32) as usize;
            tokio::sync::mpsc::channel(expected_num_chunks)
        })
        .unzip();
    // Create a channel to send errors to the stream
    let (error_tx, error_rx) = tokio::sync::mpsc::channel(1);

    let uri = uri.to_string();
    rayon::spawn(move || {
        // Once a row group has been read into memory and we have the column iterators,
        // we can start processing them in parallel.
        let par_column_iters = column_iters.zip(senders).zip(row_ranges).par_bridge();

        // For each vec of column iters, process them in parallel lock step such that each iteration
        // produces a chunk of the row group that can be processed into a table.
        let result = par_column_iters.try_for_each(move |((rg_col_iter_result, tx), rg_range)| {
            let rg_col_iter = rg_col_iter_result?;
            let owned_schema_ref = schema_ref.clone();
            let owned_predicate = predicate.clone();
            let owned_original_columns = original_columns.clone();
            let owned_uri = uri.clone();

            struct ParallelLockStepIter {
                iters: ArrowChunkIters,
            }
            impl Iterator for ParallelLockStepIter {
                type Item = arrow2::error::Result<ArrowChunk>;

                fn next(&mut self) -> Option<Self::Item> {
                    self.iters.par_iter_mut().map(|iter| iter.next()).collect()
                }
            }
            let par_lock_step_iter = ParallelLockStepIter { iters: rg_col_iter };

            // Keep track of the current index in the row group so we can throw away arrays that are not needed
            // and slice arrays that are partially needed.
            let mut index_so_far = 0;
            let table_iter = par_lock_step_iter.into_iter().map(move |chunk| {
                let chunk = chunk.with_context(|_| {
                    super::UnableToCreateChunkFromStreamingFileReaderSnafu {
                        path: owned_uri.clone(),
                    }
                })?;
                let all_series = chunk
                    .into_iter()
                    .zip(owned_schema_ref.fields.clone())
                    .filter_map(|(mut arr, (f_name, _))| {
                        if (index_so_far + arr.len()) < rg_range.start {
                            // No need to process arrays that are less than the start offset
                            return None;
                        }
                        if index_so_far < rg_range.start {
                            // Slice arrays that are partially needed
                            let offset = rg_range.start.saturating_sub(index_so_far);
                            arr = arr.sliced(offset, arr.len() - offset);
                        }
                        let series_result =
                            Series::try_from((f_name.as_str(), cast_array_for_daft_if_needed(arr)));
                        Some(series_result)
                    })
                    .collect::<DaftResult<Vec<_>>>()?;

                let len = all_series[0].len();
                if all_series.iter().any(|s| s.len() != len) {
                    return Err(super::Error::ParquetColumnsDontHaveEqualRows {
                        path: owned_uri.clone(),
                    }
                    .into());
                }
                index_so_far += len;

                let mut table = Table::new_with_size(owned_schema_ref.clone(), all_series, len)
                    .with_context(|_| super::UnableToCreateTableFromChunkSnafu {
                        path: owned_uri.clone(),
                    })?;

                // Apply pushdowns if needed
                if let Some(predicate) = &owned_predicate {
                    table = table.filter(&[predicate.clone()])?;
                    if let Some(oc) = &owned_original_columns {
                        table = table.get_columns(oc)?;
                    }
                    if let Some(nr) = original_num_rows {
                        table = table.head(nr)?;
                    }
                }
                DaftResult::Ok(table)
            });
            for table in table_iter {
                let _ = tx.blocking_send(table);
            }
            DaftResult::Ok(())
        });
        if let Err(e) = result {
            let _ = error_tx.blocking_send(Err(e));
        }
    });

    let result_stream = futures::stream::iter(
        receivers
            .into_iter()
            .chain(std::iter::once(error_rx))
            .map(ReceiverStream::new),
    )
    .flatten();
    Ok((metadata, result_stream.boxed()))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn local_parquet_read_into_arrow_async(
    uri: &str,
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<parquet2::metadata::FileMetaData>>,
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
        );
        let _ = send.send(v);
    });

    recv.await.context(super::OneShotRecvSnafu {})?
}
