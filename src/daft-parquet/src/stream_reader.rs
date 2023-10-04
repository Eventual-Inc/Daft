use std::{collections::HashSet, fs::File};

use arrow2::io::parquet::read;
use common_error::DaftResult;
use daft_core::{utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_table::Table;
use itertools::Itertools;
use rayon::prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelBridge};
use snafu::ResultExt;

use crate::{
    file::build_row_ranges,
    read::{ArrowChunk, ParquetSchemaInferenceOptions},
};

use crate::stream_reader::read::schema::infer_schema_with_options;
use rayon::iter::ParallelIterator;

fn prune_fields_from_schema(
    schema: arrow2::datatypes::Schema,
    columns: Option<&[String]>,
    uri: &str,
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
            } else {
                return Err(super::Error::FieldNotFound {
                    field: col_name.to_string(),
                    available_fields: avail_names.iter().map(|v| v.to_string()).collect(),
                    path: uri.to_string(),
                });
            }
        }
        Ok(schema.filter(|_, field| names_to_keep.contains(&field.name)))
    } else {
        Ok(schema)
    }
}

pub(crate) fn local_parquet_read_into_arrow(
    uri: &str,
    columns: Option<&[String]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> super::Result<(
    parquet2::metadata::FileMetaData,
    arrow2::datatypes::Schema,
    Vec<ArrowChunk>,
)> {
    const LOCAL_PROTOCOL: &str = "file://";

    let uri = uri.strip_prefix(LOCAL_PROTOCOL).unwrap_or(uri);

    let mut reader = File::open(uri).with_context(|_| super::InternalIOSnafu {
        path: uri.to_string(),
    })?;
    let metadata = read::read_metadata(&mut reader).with_context(|_| {
        super::UnableToParseMetadataFromLocalFileSnafu {
            path: uri.to_string(),
        }
    })?;

    // and infer a [`Schema`] from the `metadata`.
    let schema = infer_schema_with_options(&metadata, &Some(schema_infer_options.into()))
        .with_context(|_| super::UnableToParseSchemaFromMetadataSnafu {
            path: uri.to_string(),
        })?;
    let schema = prune_fields_from_schema(schema, columns, uri)?;
    let chunk_size = 128 * 1024;
    let expected_rows = metadata.num_rows.min(num_rows.unwrap_or(metadata.num_rows));

    let num_expected_arrays = f32::ceil(expected_rows as f32 / chunk_size as f32) as usize;
    let row_ranges = build_row_ranges(
        expected_rows,
        start_offset.unwrap_or(0),
        row_groups,
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
    Ok((metadata, schema, all_columns))
}

pub(crate) async fn local_parquet_read_async(
    uri: &str,
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> DaftResult<(parquet2::metadata::FileMetaData, Table)> {
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
                schema_infer_options,
            );
            let (metadata, schema, arrays) = v?;

            let converted_arrays = arrays
                .into_par_iter()
                .zip(schema.fields)
                .map(|(v, f)| {
                    let casted_arrays = v
                        .into_iter()
                        .map(move |a| {
                            Series::try_from((f.name.as_str(), cast_array_for_daft_if_needed(a)))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Series::concat(casted_arrays.iter().collect::<Vec<_>>().as_slice())
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((metadata, Table::from_columns(converted_arrays)?))
        })();
        let _ = send.send(result);
    });

    recv.await.context(super::OneShotRecvSnafu {})?
}

pub(crate) async fn local_parquet_read_into_arrow_async(
    uri: &str,
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> super::Result<(
    parquet2::metadata::FileMetaData,
    arrow2::datatypes::Schema,
    Vec<ArrowChunk>,
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
            schema_infer_options,
        );
        let _ = send.send(v);
    });

    recv.await.context(super::OneShotRecvSnafu {})?
}
