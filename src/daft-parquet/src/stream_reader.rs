use std::{collections::HashSet, fs::File, sync::Arc, time::SystemTime};

use arrow2::io::parquet::read;
use common_error::DaftResult;
use daft_core::{utils::arrow::cast_array_for_daft_if_needed, Series};
use daft_table::Table;
use itertools::Itertools;
use rayon::prelude::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefMutIterator, ParallelBridge,
};
use snafu::ResultExt;

use crate::{
    file,
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
        return Ok(schema);
    }
}

pub(crate) fn local_parquet_read_into_arrow(
    uri: &str,
    columns: Option<&[String]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> super::Result<(arrow2::datatypes::Schema, Vec<ArrowChunk>)> {
    assert!(start_offset.is_none());
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

    let row_groups_to_iter = if let Some(row_groups) = row_groups {
        row_groups.to_vec()
    } else {
        (0..metadata.row_groups.len() as i64).collect::<Vec<_>>()
    };
    let columns_iters_per_rg = row_groups_to_iter
        .iter()
        .map(|idx| metadata.row_groups.get(*idx as usize).unwrap())
        .map(|rg| {
            let single_rg_column_iter = read::read_columns_many(
                &mut reader,
                rg,
                schema.fields.clone(),
                Some(chunk_size),
                num_rows,
                None,
            );
            let single_rg_column_iter = single_rg_column_iter?;
            arrow2::error::Result::Ok(single_rg_column_iter.into_iter().enumerate())
        })
        .flatten_ok();

    let columns_iters_per_rg = columns_iters_per_rg.par_bridge();
    let collected_columns = columns_iters_per_rg.map(|payload: Result<_, _>| {
        let (idx, v) = payload?;
        Ok((idx, v.collect::<Result<Vec<_>, _>>()?))
    });
    let mut all_columns = vec![Vec::with_capacity(num_expected_arrays); schema.fields.len()];
    let all_computed = collected_columns
        .collect::<Result<Vec<_>, _>>()
        .with_context(|_| super::UnableToCreateChunkFromStreamingFileReaderSnafu {
            path: uri.to_string(),
        })?;
    for (idx, v) in all_computed {
        all_columns
            .get_mut(idx)
            .expect("array index during scatter out of index")
            .extend(v);
    }
    Ok((schema, all_columns))
}

pub(crate) async fn local_parquet_read_async(
    uri: &str,
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> DaftResult<Table> {
    let (send, recv) = tokio::sync::oneshot::channel();
    let uri = uri.to_string();
    rayon::spawn(move || {
        let final_table = (move || {
            let v = local_parquet_read_into_arrow(
                &uri,
                columns.as_deref(),
                start_offset,
                num_rows,
                row_groups.as_deref(),
                schema_infer_options,
            );
            let (schema, arrays) = v?;

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
            Table::from_columns(converted_arrays)
        })();
        let _ = send.send(final_table);
    });
    let v = recv.await.context(super::OneShotRecvSnafu {})?;
    v
}

pub(crate) async fn local_parquet_read_into_arrow_async(
    uri: &str,
    columns: Option<Vec<String>>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<Vec<i64>>,
    schema_infer_options: ParquetSchemaInferenceOptions,
) -> super::Result<(arrow2::datatypes::Schema, Vec<ArrowChunk>)> {
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
    let v = recv.await.context(super::OneShotRecvSnafu {})?;
    v
}
