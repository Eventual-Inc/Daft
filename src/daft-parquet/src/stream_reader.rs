use std::{collections::HashSet, fs::File, sync::Arc};

use daft_arrow::io::parquet::read;
use daft_core::prelude::*;
use daft_dsl::ExprRef;
use itertools::Itertools;
use rayon::{iter::ParallelIterator, prelude::ParallelBridge};
use snafu::ResultExt;

use crate::{
    PARQUET_MORSEL_SIZE,
    file::build_row_ranges,
    infer_arrow_schema_from_metadata,
    read::{ArrowChunk, ParquetSchemaInferenceOptions},
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
