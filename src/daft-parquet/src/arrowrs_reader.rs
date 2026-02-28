//! Arrow-rs based parquet reader.
//!
//! This module provides a parquet reader built on the arrow-rs `parquet` crate,
//! replacing the parquet2/arrow2 decode pipeline. It uses [`DaftAsyncFileReader`]
//! as the IO bridge for remote reads, and the sync `ParquetRecordBatchReaderBuilder`
//! with `std::fs::File` for local reads (avoiding IOClient overhead).

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::prelude::*;
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr, optimization::get_required_columns};
use daft_io::{IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use daft_stats::TruthValue;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{
            ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions,
            ParquetRecordBatchReaderBuilder, RowFilter, RowSelection, RowSelector,
        },
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    file::metadata::ParquetMetaData,
    schema::types::SchemaDescriptor,
};
use rayon::prelude::*;

use crate::{
    async_reader::DaftAsyncFileReader,
    metadata::apply_field_ids_to_arrowrs_parquet_metadata,
    read::ParquetSchemaInferenceOptions,
    schema_inference::{arrow_schema_to_daft_schema, infer_schema_from_parquet_metadata_arrowrs},
    statistics::arrowrs_row_group_metadata_to_table_stats,
};

/// Default batch size for the arrow-rs reader (number of rows per batch).
const DEFAULT_BATCH_SIZE: usize = 8192;

/// Convert a parquet error to a DaftError.
fn parquet_err(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> common_error::DaftError {
    common_error::DaftError::External(e.into())
}

/// Build a `ProjectionMask` from the given column names and arrow schema.
fn build_projection_mask(
    col_set: &std::collections::HashSet<&str>,
    arrow_schema: &arrow::datatypes::Schema,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> ProjectionMask {
    ProjectionMask::roots(
        parquet_schema,
        arrow_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| col_set.contains(f.name().as_str()))
            .map(|(i, _)| i),
    )
}

/// Project a Daft schema to only the columns in `col_set`.
fn project_daft_schema(schema: &Schema, col_set: &std::collections::HashSet<&str>) -> Schema {
    Schema::new(
        schema
            .into_iter()
            .filter(|f| col_set.contains(f.name.as_str()))
            .cloned(),
    )
}

/// Infer the arrow and Daft schemas from parquet metadata with Daft options.
fn infer_schemas(
    parquet_metadata: &ParquetMetaData,
    schema_infer_options: &ParquetSchemaInferenceOptions,
) -> DaftResult<(arrow::datatypes::Schema, Schema)> {
    let arrow_schema = infer_schema_from_parquet_metadata_arrowrs(
        parquet_metadata,
        Some(schema_infer_options.coerce_int96_timestamp_unit),
        schema_infer_options.string_encoding
            == daft_arrow::io::parquet::read::schema::StringEncoding::Raw,
    )
    .map_err(parquet_err)?;
    let daft_schema = arrow_schema_to_daft_schema(&arrow_schema)?;
    Ok((arrow_schema, daft_schema))
}

/// Try to build an arrow-rs `RowFilter` from a Daft predicate expression.
///
/// Returns `None` if any predicate column is missing from the schema (e.g. computed
/// columns that don't exist in the parquet file). On success, returns the RowFilter
/// and the set of column names required by the predicate.
fn build_row_filter(
    predicate: &ExprRef,
    daft_schema: &Schema,
    arrow_schema: &arrow::datatypes::Schema,
    parquet_schema: &SchemaDescriptor,
) -> Option<(RowFilter, HashSet<String>)> {
    let filter_columns: Vec<String> = get_required_columns(predicate);
    if filter_columns.is_empty() {
        return None;
    }

    // Verify all filter columns exist in the file schema.
    for col in &filter_columns {
        if daft_schema.get_field(col).is_err() {
            return None;
        }
    }

    let filter_col_set: HashSet<&str> = filter_columns.iter().map(|s| s.as_str()).collect();

    // Build projection mask for filter columns only.
    let filter_projection = build_projection_mask(&filter_col_set, arrow_schema, parquet_schema);

    // Build the Daft schema for filter columns only (preserving file column order).
    let filter_daft_schema = Arc::new(project_daft_schema(daft_schema, &filter_col_set));

    let pred = predicate.clone();
    let predicate_fn = ArrowPredicateFn::new(filter_projection, move |batch| {
        // Convert arrow-rs RecordBatch (filter columns only) to Daft RecordBatch.
        let daft_batch =
            crate::arrow_bridge::arrowrs_to_daft_recordbatch(&batch, &filter_daft_schema)
                .map_err(|e| arrow::error::ArrowError::ExternalError(e.into()))?;

        // Bind and evaluate the predicate.
        let bound = BoundExpr::try_new(pred.clone(), &daft_batch.schema)
            .map_err(|e| arrow::error::ArrowError::ExternalError(e.into()))?;
        let result = daft_batch
            .eval_expression(&bound)
            .map_err(|e| arrow::error::ArrowError::ExternalError(e.into()))?;

        // Extract the arrow-rs BooleanArray.
        let bool_arr = result
            .bool()
            .map_err(|e| arrow::error::ArrowError::ExternalError(e.into()))?;
        let arrow_bool = bool_arr
            .as_arrow()
            .map_err(|e| arrow::error::ArrowError::ExternalError(e.into()))?;
        Ok(arrow_bool.clone())
    });

    let filter_col_names: HashSet<String> = filter_columns.into_iter().collect();
    Some((
        RowFilter::new(vec![Box::new(predicate_fn)]),
        filter_col_names,
    ))
}

/// Build a `RowSelection` that skips the first `offset` rows.
///
/// This is used to implement file-level offset when a RowFilter (predicate pushdown)
/// is active, because arrow-rs's `with_offset` is applied *after* RowFilter, but
/// Daft's `start_offset` should be applied *before* the filter (it's a file-level
/// row skip). RowSelection is applied before RowFilter, so converting offset to a
/// RowSelection gives the correct semantics.
fn build_offset_row_selection(offset: usize, total_rows: usize) -> RowSelection {
    if offset >= total_rows {
        RowSelection::from(vec![RowSelector::skip(total_rows)])
    } else {
        RowSelection::from(vec![
            RowSelector::skip(offset),
            RowSelector::select(total_rows - offset),
        ])
    }
}

/// Build a `RowSelection` from Iceberg positional delete indices.
///
/// Converts absolute file-level row indices into a selection relative to the
/// concatenated stream of selected row groups, where deleted rows are skipped.
fn build_delete_row_selection(
    delete_rows: &[i64],
    rg_indices: &[usize],
    parquet_metadata: &ParquetMetaData,
) -> RowSelection {
    // Compute the global row start for each row group in the file.
    let mut rg_global_starts = Vec::with_capacity(parquet_metadata.num_row_groups());
    let mut cumulative = 0usize;
    for i in 0..parquet_metadata.num_row_groups() {
        rg_global_starts.push(cumulative);
        cumulative += parquet_metadata.row_group(i).num_rows() as usize;
    }

    // Collect local delete indices within the concatenated selected row groups.
    let mut local_deletes = Vec::new();
    let mut stream_offset = 0usize;

    for &rg_idx in rg_indices {
        let rg_start = rg_global_starts[rg_idx];
        let rg_rows = parquet_metadata.row_group(rg_idx).num_rows() as usize;
        let rg_end = rg_start + rg_rows;

        // Binary search for delete indices within this row group.
        let lo = delete_rows.partition_point(|&r| (r as usize) < rg_start);
        let hi = delete_rows.partition_point(|&r| (r as usize) < rg_end);

        for &global_row in &delete_rows[lo..hi] {
            let local_in_rg = global_row as usize - rg_start;
            local_deletes.push(stream_offset + local_in_rg);
        }

        stream_offset += rg_rows;
    }

    let total_rows = stream_offset;
    deletes_to_row_selection(&local_deletes, total_rows)
}

/// Build a `RowSelection` for a single row group from delete indices.
fn build_single_rg_delete_selection(
    delete_rows: &[i64],
    rg_global_start: usize,
    rg_rows: usize,
) -> RowSelection {
    let rg_end = rg_global_start + rg_rows;
    let lo = delete_rows.partition_point(|&r| (r as usize) < rg_global_start);
    let hi = delete_rows.partition_point(|&r| (r as usize) < rg_end);

    let local_deletes: Vec<usize> = delete_rows[lo..hi]
        .iter()
        .map(|&r| r as usize - rg_global_start)
        .collect();

    deletes_to_row_selection(&local_deletes, rg_rows)
}

/// Convert sorted local delete indices to alternating select/skip RowSelectors.
fn deletes_to_row_selection(local_deletes: &[usize], total_rows: usize) -> RowSelection {
    if local_deletes.is_empty() {
        return vec![RowSelector::select(total_rows)].into();
    }

    let mut selectors = Vec::with_capacity(local_deletes.len() * 2 + 1);
    let mut pos = 0usize;

    for &del in local_deletes {
        if del > pos {
            selectors.push(RowSelector::select(del - pos));
        }
        selectors.push(RowSelector::skip(1));
        pos = del + 1;
    }
    if pos < total_rows {
        selectors.push(RowSelector::select(total_rows - pos));
    }

    selectors.into()
}

/// Read a single parquet file into a Daft [`RecordBatch`] using the arrow-rs reader.
///
/// This is the arrow-rs equivalent of the parquet2-based `read_parquet_single`.
/// When `predicate` and/or `delete_rows` are provided, the reader handles them
/// internally using arrow-rs `RowFilter` and `RowSelection` for late materialization.
///
/// # `start_offset` semantics
///
/// `start_offset` is a file-level row skip: skip the first N rows before applying
/// predicates or limits. The intended order of operations is:
///
///   offset (skip file rows) → predicate filter → limit
///
/// Note: `start_offset > 0` is rejected by the micropartition reader and never used
/// in production (the streaming scan path doesn't even accept the parameter). The
/// parquet2 reader has latent bugs for this case — both its local and remote paths
/// produce RecordBatch size mismatches when `start_offset > 0`. Our implementation
/// follows the intended semantics based on the code structure and the `apply_delete_rows`
/// docstring in `read.rs`, but there is no working reference implementation to compare
/// against.
#[allow(clippy::too_many_arguments)]
pub async fn read_parquet_single_arrowrs(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<ParquetMetaData>>,
    batch_size: Option<usize>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    delete_rows: Option<&[i64]>,
) -> DaftResult<RecordBatch> {
    // 1. Create the async file reader and fetch metadata.
    let mut reader = DaftAsyncFileReader::new(uri.to_string(), io_client, io_stats, metadata, None);
    let mut parquet_metadata = reader.get_metadata(None).await.map_err(parquet_err)?;

    // 1b. Apply field ID mapping (Iceberg schema evolution) if provided.
    if let Some(ref mapping) = field_id_mapping {
        parquet_metadata = apply_field_ids_to_arrowrs_parquet_metadata(parquet_metadata, mapping)?;
    }

    // 2. Infer schema with Daft options (INT96 coercion, string encoding).
    let (arrow_schema, daft_schema) = infer_schemas(&parquet_metadata, &schema_infer_options)?;

    // 3. Determine user-requested columns and expand for predicate if needed.
    let user_col_set: Option<HashSet<&str>> = columns.map(|cols| cols.iter().copied().collect());
    let mut read_col_set: Option<HashSet<&str>> = user_col_set.clone();

    // Try to build a RowFilter from the predicate.
    let row_filter_result = predicate.as_ref().and_then(|pred| {
        build_row_filter(
            pred,
            &daft_schema,
            &arrow_schema,
            parquet_metadata.file_metadata().schema_descr(),
        )
    });
    let predicate_pushed = row_filter_result.is_some();

    // Expand read columns to include predicate columns.
    if let Some((_, ref filter_col_names)) = row_filter_result
        && let Some(ref mut col_set) = read_col_set
    {
        for name in filter_col_names {
            col_set.insert(name.as_str());
        }
    }

    // 4. Build the stream builder with our custom schema.
    let options = ArrowReaderOptions::new().with_schema(Arc::new(arrow_schema.clone()));
    let arrow_reader_metadata =
        ArrowReaderMetadata::try_new(parquet_metadata.clone(), options).map_err(parquet_err)?;
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_reader_metadata);

    // 5. Apply column projection (expanded to include predicate columns).
    let (read_daft_schema, builder) = if let Some(ref col_set) = read_col_set {
        let mask = build_projection_mask(col_set, &arrow_schema, builder.parquet_schema());
        (
            project_daft_schema(&daft_schema, col_set),
            builder.with_projection(mask),
        )
    } else {
        (daft_schema.clone(), builder)
    };

    // The schema to return to the caller (user-requested columns only).
    let return_daft_schema = if let Some(ref col_set) = user_col_set {
        project_daft_schema(&daft_schema, col_set)
    } else {
        daft_schema
    };

    // 6. Determine which row groups to read (with predicate-based pruning).
    let rg_indices = prune_row_groups(
        &parquet_metadata,
        row_groups,
        predicate.as_ref(),
        &read_daft_schema,
        uri,
    )?;

    if rg_indices.is_empty() {
        return Ok(RecordBatch::empty(Some(Arc::new(return_daft_schema))));
    }

    // 7. Apply row groups, offset, batch size, RowFilter, RowSelection, and limit.
    let batch_size = batch_size.unwrap_or_else(|| {
        rg_indices
            .iter()
            .map(|&idx| parquet_metadata.row_group(idx).num_rows() as usize)
            .max()
            .unwrap_or(DEFAULT_BATCH_SIZE)
    });
    let mut builder = builder.with_row_groups(rg_indices.clone());

    // Offset handling: arrow-rs `with_offset` is applied post-RowFilter, but Daft's
    // start_offset is a file-level skip (pre-filter). When a predicate is pushed,
    // convert offset to a RowSelection (applied pre-RowFilter) instead.
    let use_offset_selection = predicate_pushed && start_offset.is_some_and(|o| o > 0);
    if !use_offset_selection && let Some(offset) = start_offset {
        builder = builder.with_offset(offset);
    }

    // Wire RowSelection for offset and/or delete_rows (applied before RowFilter).
    {
        let total_selected_rows: usize = rg_indices
            .iter()
            .map(|&idx| parquet_metadata.row_group(idx).num_rows() as usize)
            .sum();

        let offset_selection = if use_offset_selection {
            Some(build_offset_row_selection(
                start_offset.unwrap(),
                total_selected_rows,
            ))
        } else {
            None
        };

        let delete_selection = if let Some(delete_rows) = delete_rows
            && !delete_rows.is_empty()
        {
            Some(build_delete_row_selection(
                delete_rows,
                &rg_indices,
                &parquet_metadata,
            ))
        } else {
            None
        };

        let combined = match (offset_selection, delete_selection) {
            (Some(a), Some(b)) => Some(a.intersection(&b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        if let Some(selection) = combined {
            builder = builder.with_row_selection(selection);
        }
    }

    // Wire RowFilter for predicate pushdown (late materialization).
    if let Some((row_filter, _)) = row_filter_result {
        builder = builder.with_row_filter(row_filter);
    }

    // Limit: if predicate was pushed down, arrow-rs applies limit post-filter.
    // If no predicate at all, apply limit at decoder level.
    // If predicate was NOT pushed down, limit is applied post-read.
    if (predicate_pushed || predicate.is_none())
        && let Some(limit) = num_rows
    {
        builder = builder.with_limit(limit);
    }

    builder = builder.with_batch_size(batch_size);

    // 8. Build the stream and collect all batches.
    let stream = builder.build().map_err(parquet_err)?;
    let arrow_batches: Vec<arrow::array::RecordBatch> =
        stream.try_collect().await.map_err(parquet_err)?;

    // 9. Convert to Daft RecordBatch.
    let mut table = crate::arrow_bridge::arrowrs_batches_to_daft_recordbatch(
        &arrow_batches,
        &read_daft_schema,
    )?;

    // 10. Post-read predicate fallback (if RowFilter couldn't be built).
    if let Some(predicate) = predicate
        && !predicate_pushed
    {
        let bound = BoundExpr::try_new(predicate, &table.schema)?;
        table = table.filter(&[bound])?;
        if let Some(limit) = num_rows {
            table = table.head(limit)?;
        }
    }

    // 11. Strip predicate-only columns from output.
    if user_col_set.is_some() && read_daft_schema.len() != return_daft_schema.len() {
        let return_indices: Vec<usize> = return_daft_schema
            .names()
            .iter()
            .map(|name| table.schema.get_index(name))
            .collect::<DaftResult<Vec<_>>>()?;
        table = table.get_columns(&return_indices);
    }

    Ok(table)
}

/// Read a local parquet file using the sync arrow-rs reader with parallel row group decode.
///
/// This avoids the overhead of `DaftAsyncFileReader` + `IOClient` for local files
/// by using `std::fs::File` directly with `ParquetRecordBatchReaderBuilder`.
/// Row groups are decoded in parallel using rayon, matching the parquet2 reader's
/// parallelism strategy. Supports late materialization via `RowFilter` and
/// positional delete skipping via `RowSelection`.
///
/// See [`read_parquet_single_arrowrs`] for `start_offset` semantics.
#[allow(clippy::too_many_arguments)]
pub fn local_parquet_read_arrowrs(
    path: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    batch_size: Option<usize>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    delete_rows: Option<&[i64]>,
) -> DaftResult<RecordBatch> {
    // 1. Open the file and read metadata once.
    let file = std::fs::File::open(path).map_err(|e| {
        parquet_err(format!(
            "Failed to open local parquet file '{}': {}",
            path, e
        ))
    })?;

    let arrow_reader_metadata =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true))
            .map_err(parquet_err)?;
    let mut parquet_metadata = arrow_reader_metadata.metadata().clone();

    // 1b. Apply field ID mapping (Iceberg schema evolution) if provided.
    if let Some(ref mapping) = field_id_mapping {
        parquet_metadata = apply_field_ids_to_arrowrs_parquet_metadata(parquet_metadata, mapping)?;
    }

    // 2. Infer schema with Daft options.
    let (arrow_schema, daft_schema) = infer_schemas(&parquet_metadata, &schema_infer_options)?;

    // 3. Determine user-requested columns and expand for predicate if needed.
    let user_col_set: Option<HashSet<&str>> = columns.map(|cols| cols.iter().copied().collect());
    let mut read_col_set: Option<HashSet<&str>> = user_col_set.clone();

    // Try to build a RowFilter from the predicate.
    let row_filter_data = predicate.as_ref().and_then(|pred| {
        let parquet_schema = parquet_metadata.file_metadata().schema_descr();
        build_row_filter(pred, &daft_schema, &arrow_schema, parquet_schema)
            .map(|(_, filter_col_names)| filter_col_names)
    });
    let predicate_pushed = row_filter_data.is_some();

    // Expand read columns to include predicate columns.
    if let Some(ref filter_col_names) = row_filter_data
        && let Some(ref mut col_set) = read_col_set
    {
        for name in filter_col_names {
            col_set.insert(name.as_str());
        }
    }

    // 4. Rebuild metadata with our custom schema.
    let options = ArrowReaderOptions::new().with_schema(Arc::new(arrow_schema.clone()));
    let arrow_reader_metadata =
        ArrowReaderMetadata::try_new(parquet_metadata.clone(), options).map_err(parquet_err)?;

    // 5. Compute schemas.
    // Keep full schema for RowFilter construction (needs all file columns).
    let daft_schema_for_filter = Arc::new(daft_schema.clone());
    let read_daft_schema = if let Some(ref col_set) = read_col_set {
        project_daft_schema(&daft_schema, col_set)
    } else {
        daft_schema.clone()
    };
    let return_daft_schema = if let Some(ref col_set) = user_col_set {
        project_daft_schema(&daft_schema, col_set)
    } else {
        daft_schema
    };

    // 6. Row group pruning.
    let rg_indices = prune_row_groups(
        &parquet_metadata,
        row_groups,
        predicate.as_ref(),
        &read_daft_schema,
        path,
    )?;

    if rg_indices.is_empty() {
        return Ok(RecordBatch::empty(Some(Arc::new(return_daft_schema))));
    }

    // 7. Compute per-row-group local offset/limit so the decoder only
    //    materializes the rows we actually need.
    //    Offset is always applied at decode time (it's a file-level row offset).
    //    Limit is only applied at decode time when there's no predicate;
    //    with a predicate, we read all rows post-offset, filter, then limit.
    let has_predicate = predicate.is_some();
    let global_start = start_offset.unwrap_or(0);
    let global_limit = if has_predicate { None } else { num_rows };
    let global_end = global_start + global_limit.unwrap_or(usize::MAX - global_start);

    struct RgTask {
        rg_idx: usize,
        local_offset: usize,
        local_limit: Option<usize>,
    }

    // Compute global row starts for each RG (needed for delete_rows).
    let mut rg_global_starts = Vec::with_capacity(parquet_metadata.num_row_groups());
    let mut cumulative_global = 0usize;
    for i in 0..parquet_metadata.num_row_groups() {
        rg_global_starts.push(cumulative_global);
        cumulative_global += parquet_metadata.row_group(i).num_rows() as usize;
    }

    let mut rg_tasks = Vec::new();
    let mut cumulative_rows = 0usize;
    for &rg_idx in &rg_indices {
        let rg_rows = parquet_metadata.row_group(rg_idx).num_rows() as usize;
        let rg_start = cumulative_rows;
        let rg_end = cumulative_rows + rg_rows;
        cumulative_rows = rg_end;

        // Skip row groups entirely outside the requested range.
        if rg_end <= global_start || rg_start >= global_end {
            continue;
        }

        let local_offset = global_start.saturating_sub(rg_start);
        let local_end = global_end.min(rg_end) - rg_start;
        let local_limit = if global_limit.is_some() || local_offset > 0 {
            Some(local_end - local_offset)
        } else {
            None
        };

        rg_tasks.push(RgTask {
            rg_idx,
            local_offset,
            local_limit,
        });
    }

    if rg_tasks.is_empty() {
        return Ok(RecordBatch::empty(Some(Arc::new(return_daft_schema))));
    }

    // Use a large batch size so each row group produces a single batch,
    // matching parquet2's behavior and avoiding per-batch concat overhead.
    let batch_size = batch_size.unwrap_or_else(|| {
        rg_tasks
            .iter()
            .map(|t| parquet_metadata.row_group(t.rg_idx).num_rows() as usize)
            .max()
            .unwrap_or(256 * 1024)
    });

    // Shared data for building RowFilter per rayon thread.
    let arrow_schema_arc = Arc::new(arrow_schema.clone());
    let parquet_schema_arc = Arc::new(parquet_metadata.file_metadata().schema_descr().clone());
    let delete_rows_arc: Option<Arc<[i64]>> = delete_rows.map(|d| d.into());

    /// Build a RowFilter + RowSelection for a single RG builder.
    ///
    /// When `predicate_pushed` and `offset > 0`, the offset is converted to a
    /// RowSelection (pre-filter) rather than using `with_offset` (post-filter).
    fn apply_rg_filter_and_deletes(
        builder: ParquetRecordBatchReaderBuilder<std::fs::File>,
        predicate: Option<&ExprRef>,
        predicate_pushed: bool,
        daft_schema: &Schema,
        arrow_schema: &arrow::datatypes::Schema,
        parquet_schema: &SchemaDescriptor,
        delete_rows: Option<&[i64]>,
        rg_global_start: usize,
        rg_rows: usize,
        offset: usize,
        limit: Option<usize>,
    ) -> DaftResult<ParquetRecordBatchReaderBuilder<std::fs::File>> {
        let mut builder = builder;

        // Build RowSelections: offset (when predicate pushed) and delete_rows.
        let offset_selection = if predicate_pushed && offset > 0 {
            Some(build_offset_row_selection(offset, rg_rows))
        } else {
            None
        };

        let delete_selection = if let Some(deletes) = delete_rows
            && !deletes.is_empty()
        {
            Some(build_single_rg_delete_selection(
                deletes,
                rg_global_start,
                rg_rows,
            ))
        } else {
            None
        };

        let combined = match (offset_selection, delete_selection) {
            (Some(a), Some(b)) => Some(a.intersection(&b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        if let Some(selection) = combined {
            builder = builder.with_row_selection(selection);
        }

        // Wire RowFilter for predicate pushdown.
        if predicate_pushed
            && let Some(pred) = predicate
            && let Some((row_filter, _)) =
                build_row_filter(pred, daft_schema, arrow_schema, parquet_schema)
        {
            builder = builder.with_row_filter(row_filter);
        }
        if predicate_pushed && let Some(lim) = limit {
            builder = builder.with_limit(lim);
        }

        Ok(builder)
    }

    // 8a. Fast path for single row group: skip rayon dispatch.
    if rg_tasks.len() == 1 {
        let task = &rg_tasks[0];
        let rg_rows = parquet_metadata.row_group(task.rg_idx).num_rows() as usize;
        let mut rg_builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, arrow_reader_metadata);
        if let Some(ref col_set) = read_col_set {
            let mask = build_projection_mask(col_set, &arrow_schema, rg_builder.parquet_schema());
            rg_builder = rg_builder.with_projection(mask);
        }
        rg_builder = rg_builder
            .with_row_groups(vec![task.rg_idx])
            .with_batch_size(batch_size);
        // Only use with_offset when predicate is NOT pushed (offset is pre-filter
        // via RowSelection when pushed).
        if !predicate_pushed {
            rg_builder = rg_builder.with_offset(task.local_offset);
            if let Some(lim) = task.local_limit {
                rg_builder = rg_builder.with_limit(lim);
            }
        }
        rg_builder = apply_rg_filter_and_deletes(
            rg_builder,
            predicate.as_ref(),
            predicate_pushed,
            &daft_schema_for_filter,
            &arrow_schema_arc,
            &parquet_schema_arc,
            delete_rows_arc.as_deref(),
            rg_global_starts[task.rg_idx],
            rg_rows,
            task.local_offset,
            num_rows,
        )?;
        let reader = rg_builder.build().map_err(parquet_err)?;
        let arrow_batches: Vec<arrow::array::RecordBatch> =
            reader.collect::<Result<Vec<_>, _>>().map_err(parquet_err)?;
        let mut table = crate::arrow_bridge::arrowrs_batches_to_daft_recordbatch(
            &arrow_batches,
            &read_daft_schema,
        )?;

        // Post-read predicate fallback.
        if let Some(ref pred) = predicate
            && !predicate_pushed
        {
            let bound = BoundExpr::try_new(pred.clone(), &table.schema)?;
            table = table.filter(&[bound])?;
            if let Some(limit) = num_rows {
                table = table.head(limit)?;
            }
        }

        // Strip predicate-only columns.
        if user_col_set.is_some() && read_daft_schema.len() != return_daft_schema.len() {
            let return_indices: Vec<usize> = return_daft_schema
                .names()
                .iter()
                .map(|name| table.schema.get_index(name))
                .collect::<DaftResult<Vec<_>>>()?;
            table = table.get_columns(&return_indices);
        }

        return Ok(table);
    }

    // 8b. Decode row groups in parallel using rayon.
    let path_owned = path.to_string();
    let arrow_reader_metadata = Arc::new(arrow_reader_metadata);

    let rg_batches: Vec<DaftResult<Vec<arrow::array::RecordBatch>>> = rg_tasks
        .par_iter()
        .map(|task| {
            let rg_rows = parquet_metadata.row_group(task.rg_idx).num_rows() as usize;
            let file = std::fs::File::open(&path_owned)
                .map_err(|e| parquet_err(format!("Failed to open '{}': {}", path_owned, e)))?;
            let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                file,
                (*arrow_reader_metadata).clone(),
            );
            if let Some(ref col_set) = read_col_set {
                let mask = build_projection_mask(col_set, &arrow_schema, builder.parquet_schema());
                builder = builder.with_projection(mask);
            }
            builder = builder
                .with_row_groups(vec![task.rg_idx])
                .with_batch_size(batch_size);
            if !predicate_pushed {
                builder = builder.with_offset(task.local_offset);
                if let Some(lim) = task.local_limit {
                    builder = builder.with_limit(lim);
                }
            }
            builder = apply_rg_filter_and_deletes(
                builder,
                predicate.as_ref(),
                predicate_pushed,
                &daft_schema_for_filter,
                &arrow_schema_arc,
                &parquet_schema_arc,
                delete_rows_arc.as_deref(),
                rg_global_starts[task.rg_idx],
                rg_rows,
                task.local_offset,
                None, // limit applied post-concat
            )?;
            let reader = builder.build().map_err(parquet_err)?;
            reader.collect::<Result<Vec<_>, _>>().map_err(parquet_err)
        })
        .collect();

    // 9. Flatten batches.
    let all_batches: Vec<arrow::array::RecordBatch> = rg_batches
        .into_iter()
        .collect::<DaftResult<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();

    // 10. Convert to Daft RecordBatch.
    let mut table =
        crate::arrow_bridge::arrowrs_batches_to_daft_recordbatch(&all_batches, &read_daft_schema)?;

    // 11. Post-read predicate fallback.
    if let Some(ref pred) = predicate
        && !predicate_pushed
    {
        let bound = BoundExpr::try_new(pred.clone(), &table.schema)?;
        table = table.filter(&[bound])?;
    }

    // 12. Apply limit post-filter for predicate path.
    //     Offset was already applied at decode time (file-level row skip).
    if has_predicate && let Some(limit) = num_rows {
        table = table.head(limit)?;
    }

    // 13. Strip predicate-only columns.
    if user_col_set.is_some() && read_daft_schema.len() != return_daft_schema.len() {
        let return_indices: Vec<usize> = return_daft_schema
            .names()
            .iter()
            .map(|name| table.schema.get_index(name))
            .collect::<DaftResult<Vec<_>>>()?;
        table = table.get_columns(&return_indices);
    }

    Ok(table)
}

/// Stream a single parquet file as Daft [`RecordBatch`]es using the arrow-rs reader.
///
/// This is the arrow-rs equivalent of the parquet2-based `stream_parquet_single`.
/// Supports late materialization via `RowFilter` and positional delete skipping
/// via `RowSelection`.
#[allow(clippy::too_many_arguments)]
pub async fn stream_parquet_single_arrowrs(
    uri: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<ExprRef>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    metadata: Option<Arc<ParquetMetaData>>,
    batch_size: Option<usize>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    delete_rows: Option<&[i64]>,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    // 1. Create the async file reader and fetch metadata.
    let mut reader = DaftAsyncFileReader::new(uri.to_string(), io_client, io_stats, metadata, None);
    let mut parquet_metadata = reader.get_metadata(None).await.map_err(parquet_err)?;

    // 1b. Apply field ID mapping (Iceberg schema evolution) if provided.
    if let Some(ref mapping) = field_id_mapping {
        parquet_metadata = apply_field_ids_to_arrowrs_parquet_metadata(parquet_metadata, mapping)?;
    }

    // 2. Infer schema with Daft options.
    let (arrow_schema, daft_schema) = infer_schemas(&parquet_metadata, &schema_infer_options)?;

    // 3. Determine user-requested columns and expand for predicate if needed.
    let user_col_set: Option<HashSet<&str>> = columns.map(|cols| cols.iter().copied().collect());
    let mut read_col_set: Option<HashSet<&str>> = user_col_set.clone();

    let row_filter_result = predicate.as_ref().and_then(|pred| {
        build_row_filter(
            pred,
            &daft_schema,
            &arrow_schema,
            parquet_metadata.file_metadata().schema_descr(),
        )
    });
    let predicate_pushed = row_filter_result.is_some();

    if let Some((_, ref filter_col_names)) = row_filter_result
        && let Some(ref mut col_set) = read_col_set
    {
        for name in filter_col_names {
            col_set.insert(name.as_str());
        }
    }

    // 4. Build the stream builder with our custom schema.
    let options = ArrowReaderOptions::new().with_schema(Arc::new(arrow_schema.clone()));
    let arrow_reader_metadata =
        ArrowReaderMetadata::try_new(parquet_metadata.clone(), options).map_err(parquet_err)?;
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_reader_metadata);

    // 5. Apply column projection (expanded for predicate columns).
    let (read_daft_schema, builder) = if let Some(ref col_set) = read_col_set {
        let mask = build_projection_mask(col_set, &arrow_schema, builder.parquet_schema());
        (
            project_daft_schema(&daft_schema, col_set),
            builder.with_projection(mask),
        )
    } else {
        (daft_schema.clone(), builder)
    };

    let return_daft_schema = if let Some(ref col_set) = user_col_set {
        project_daft_schema(&daft_schema, col_set)
    } else {
        daft_schema
    };
    let needs_column_strip =
        user_col_set.is_some() && read_daft_schema.len() != return_daft_schema.len();

    // 6. Row group pruning.
    let rg_indices = prune_row_groups(
        &parquet_metadata,
        row_groups,
        predicate.as_ref(),
        &read_daft_schema,
        uri,
    )?;

    if rg_indices.is_empty() {
        return Ok(futures::stream::empty().boxed());
    }

    // 7. Apply row groups, offset, batch size, RowFilter, RowSelection, and limit.
    let mut builder = builder.with_row_groups(rg_indices.clone());

    // Offset: arrow-rs with_offset is post-RowFilter, but Daft offset is file-level.
    let use_offset_selection = predicate_pushed && start_offset.is_some_and(|o| o > 0);
    if !use_offset_selection && let Some(offset) = start_offset {
        builder = builder.with_offset(offset);
    }

    // Wire RowSelection for offset and/or delete_rows (applied before RowFilter).
    {
        let total_selected_rows: usize = rg_indices
            .iter()
            .map(|&idx| parquet_metadata.row_group(idx).num_rows() as usize)
            .sum();

        let offset_selection = if use_offset_selection {
            Some(build_offset_row_selection(
                start_offset.unwrap(),
                total_selected_rows,
            ))
        } else {
            None
        };

        let delete_selection = if let Some(delete_rows) = delete_rows
            && !delete_rows.is_empty()
        {
            Some(build_delete_row_selection(
                delete_rows,
                &rg_indices,
                &parquet_metadata,
            ))
        } else {
            None
        };

        let combined = match (offset_selection, delete_selection) {
            (Some(a), Some(b)) => Some(a.intersection(&b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        if let Some(selection) = combined {
            builder = builder.with_row_selection(selection);
        }
    }

    // Wire RowFilter for predicate pushdown.
    if let Some((row_filter, _)) = row_filter_result {
        builder = builder.with_row_filter(row_filter);
    }

    // Limit: if predicate was pushed down, arrow-rs applies limit post-filter.
    if (predicate_pushed || predicate.is_none())
        && let Some(limit) = num_rows
    {
        builder = builder.with_limit(limit);
    }

    builder = builder.with_batch_size(batch_size.unwrap_or(DEFAULT_BATCH_SIZE));

    // 8. Build the stream.
    let stream = builder.build().map_err(parquet_err)?;
    let read_schema = Arc::new(read_daft_schema);
    let return_schema = Arc::new(return_daft_schema);

    let mapped = stream.map(move |result| {
        let arrow_batch = result.map_err(parquet_err)?;
        let mut table =
            crate::arrow_bridge::arrowrs_to_daft_recordbatch(&arrow_batch, &read_schema)?;

        // Post-read predicate fallback.
        if let Some(ref pred) = predicate
            && !predicate_pushed
        {
            let bound = BoundExpr::try_new(pred.clone(), &table.schema)?;
            table = table.filter(&[bound])?;
        }

        // Strip predicate-only columns.
        if needs_column_strip {
            let return_indices: Vec<usize> = return_schema
                .names()
                .iter()
                .map(|name| table.schema.get_index(name))
                .collect::<DaftResult<Vec<_>>>()?;
            table = table.get_columns(&return_indices);
        }

        Ok(table)
    });

    Ok(mapped.boxed())
}

/// Determine which row groups to read, applying predicate-based pruning.
///
/// Returns the list of row group indices to read. If `requested_row_groups` is Some,
/// only those are considered; otherwise all row groups are candidates.
fn prune_row_groups(
    metadata: &ParquetMetaData,
    requested_row_groups: Option<&[i64]>,
    predicate: Option<&ExprRef>,
    schema: &Schema,
    uri: &str,
) -> DaftResult<Vec<usize>> {
    let num_row_groups = metadata.num_row_groups();

    // Determine candidate row groups.
    let candidates: Vec<usize> = if let Some(rgs) = requested_row_groups {
        rgs.iter()
            .map(|&i| {
                let idx = i as usize;
                if idx >= num_row_groups {
                    Err(common_error::DaftError::ValueError(format!(
                        "Row group index {} out of bounds for '{}' (has {} row groups)",
                        i, uri, num_row_groups
                    )))
                } else {
                    Ok(idx)
                }
            })
            .collect::<DaftResult<Vec<_>>>()?
    } else {
        (0..num_row_groups).collect()
    };

    // If no predicate, return all candidates.
    let predicate = match predicate {
        Some(p) => p,
        None => return Ok(candidates),
    };

    // Bind the predicate expression to the schema.
    let bound_pred = BoundExpr::try_new((*predicate).clone(), schema).map_err(|e| {
        common_error::DaftError::ValueError(format!(
            "Failed to bind predicate for row group pruning on '{}': {}",
            uri, e
        ))
    })?;

    // Evaluate the predicate against each row group's statistics.
    let mut result = Vec::with_capacity(candidates.len());
    for rg_idx in candidates {
        let rg_meta = metadata.row_group(rg_idx);
        match arrowrs_row_group_metadata_to_table_stats(rg_meta, schema) {
            Ok(stats) => {
                let evaled = stats.eval_expression(&bound_pred)?;
                if evaled.to_truth_value() != TruthValue::False {
                    result.push(rg_idx);
                }
                // else: predicate definitively false for this RG, skip it
            }
            Err(_) => {
                // If stats are unavailable/unparsable, include the RG (be conservative).
                result.push(rg_idx);
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::Int32Array,
        datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    };
    use parquet::{arrow::ArrowWriter, file::reader::FileReader};

    use super::*;

    /// Create parquet metadata from an in-memory parquet file.
    fn create_test_parquet_metadata() -> Arc<parquet::file::metadata::ParquetMetaData> {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", ArrowDataType::Int32, true),
            ArrowField::new("b", ArrowDataType::Int32, true),
        ]));

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();

        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read back the metadata from the in-memory buffer.
        let reader =
            parquet::file::serialized_reader::SerializedFileReader::new(bytes::Bytes::from(buf))
                .unwrap();
        Arc::new(reader.metadata().clone())
    }

    #[test]
    fn test_prune_row_groups_no_predicate() {
        let metadata = create_test_parquet_metadata();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32),
            Field::new("b", DataType::Int32),
        ]);

        let result = prune_row_groups(&metadata, None, None, &schema, "test.parquet").unwrap();
        // Should return all row groups.
        assert_eq!(result.len(), metadata.num_row_groups());
    }

    #[test]
    fn test_prune_row_groups_with_selection() {
        let metadata = create_test_parquet_metadata();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32),
            Field::new("b", DataType::Int32),
        ]);

        // Request specific row group 0.
        let result =
            prune_row_groups(&metadata, Some(&[0]), None, &schema, "test.parquet").unwrap();
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_prune_row_groups_out_of_bounds() {
        let metadata = create_test_parquet_metadata();
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32),
            Field::new("b", DataType::Int32),
        ]);

        // Request out-of-bounds row group.
        let result = prune_row_groups(&metadata, Some(&[999]), None, &schema, "test.parquet");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_parquet_single_arrowrs_from_local_file() {
        use daft_io::IOConfig;

        // Write a test parquet file to a temp directory.
        let dir = std::env::temp_dir().join("daft_parquet_test_arrowrs");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("x", ArrowDataType::Int32, true),
            ArrowField::new("y", ArrowDataType::Utf8, true),
        ]));

        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read through the new reader.
        let uri = file_path.to_str().unwrap();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());

        let result = read_parquet_single_arrowrs(
            uri,
            None, // all columns
            None, // no offset
            None, // no limit
            None, // all row groups
            None, // no predicate
            io_client,
            None, // no io_stats
            ParquetSchemaInferenceOptions::default(),
            None, // no cached metadata
            None, // default batch size
            None, // no field_id_mapping
            None, // no delete_rows
        )
        .await
        .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.num_columns(), 2);

        // Verify column names.
        assert_eq!(result.schema.fields()[0].name, "x");
        assert_eq!(result.schema.fields()[1].name, "y");

        // Clean up.
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_read_parquet_single_arrowrs_with_column_projection() {
        use daft_io::IOConfig;

        let dir = std::env::temp_dir().join("daft_parquet_test_arrowrs_proj");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("x", ArrowDataType::Int32, true),
            ArrowField::new("y", ArrowDataType::Utf8, true),
            ArrowField::new("z", ArrowDataType::Float64, true),
        ]));

        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "b", "c", "d", "e",
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    1.0, 2.0, 3.0, 4.0, 5.0,
                ])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uri = file_path.to_str().unwrap();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());

        // Read only columns "x" and "z".
        let result = read_parquet_single_arrowrs(
            uri,
            Some(&["x", "z"]),
            None,
            None,
            None,
            None,
            io_client,
            None,
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.len(), 5);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.schema.fields()[0].name, "x");
        assert_eq!(result.schema.fields()[1].name, "z");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_read_parquet_single_arrowrs_with_limit() {
        use daft_io::IOConfig;

        let dir = std::env::temp_dir().join("daft_parquet_test_arrowrs_limit");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "val",
            ArrowDataType::Int32,
            true,
        )]));

        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
            ]))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uri = file_path.to_str().unwrap();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());

        // Read with limit of 3.
        let result = read_parquet_single_arrowrs(
            uri,
            None,
            None,
            Some(3), // limit
            None,
            None,
            io_client,
            None,
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.len(), 3);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_stream_parquet_single_arrowrs() {
        use daft_io::IOConfig;

        let dir = std::env::temp_dir().join("daft_parquet_test_arrowrs_stream");
        std::fs::create_dir_all(&dir).unwrap();
        let file_path = dir.join("test.parquet");

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));

        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uri = file_path.to_str().unwrap();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());

        let mut stream = stream_parquet_single_arrowrs(
            uri,
            None,
            None,
            None,
            None,
            None,
            io_client,
            None,
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let mut total_rows = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            total_rows += batch.len();
        }
        assert_eq!(total_rows, 5);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_read_existing_mvp_parquet() {
        use daft_io::IOConfig;

        // Read the existing MVP test fixture through the new reader.
        let mvp_path = path_macro::path!(
            env!("CARGO_MANIFEST_DIR")
                / ".."
                / ".."
                / "tests"
                / "assets"
                / "parquet-data"
                / "mvp.parquet"
        );
        let uri = mvp_path.to_str().unwrap();
        let io_client = Arc::new(IOClient::new(IOConfig::default().into()).unwrap());

        let result = read_parquet_single_arrowrs(
            uri,
            None,
            None,
            None,
            None,
            None,
            io_client,
            None,
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        // The MVP parquet file should have some rows and columns.
        assert!(result.len() > 0, "MVP parquet should have rows");
        assert!(result.num_columns() > 0, "MVP parquet should have columns");
    }

    /// Helper: write a parquet file with columns "val" (Int32: 0..n) and "label" (Utf8).
    /// Returns the file path.
    fn write_test_parquet(dir: &std::path::Path, n: i32) -> String {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("val", ArrowDataType::Int32, false),
            ArrowField::new("label", ArrowDataType::Utf8, false),
        ]));
        let file_path = dir.join("test.parquet");
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
        let vals: Vec<i32> = (0..n).collect();
        let labels: Vec<String> = (0..n).map(|i| format!("row_{i}")).collect();
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vals)),
                Arc::new(arrow::array::StringArray::from(labels)),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        file_path.to_str().unwrap().to_string()
    }

    /// Helper: read via parquet2 path (read_parquet_single, bypassing arrowrs env var).
    async fn read_via_parquet2(
        uri: &str,
        columns: Option<Vec<String>>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        predicate: Option<ExprRef>,
    ) -> RecordBatch {
        let io_client =
            Arc::new(daft_io::IOClient::new(daft_io::IOConfig::default().into()).unwrap());
        // Call read_parquet_single directly (async) to avoid runtime nesting.
        // This always goes through the parquet2 path since we don't set the env var.
        // SAFETY: tests are single-threaded for this module.
        unsafe { std::env::remove_var("DAFT_PARQUET_READER") };
        crate::read::read_parquet_single_for_test(
            uri,
            columns,
            start_offset,
            num_rows,
            None,
            predicate,
            io_client,
            None,
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap()
    }

    /// Helper: read via arrowrs path directly.
    async fn read_via_arrowrs(
        uri: &str,
        columns: Option<&[&str]>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        predicate: Option<ExprRef>,
    ) -> RecordBatch {
        let io_client =
            Arc::new(daft_io::IOClient::new(daft_io::IOConfig::default().into()).unwrap());
        read_parquet_single_arrowrs(
            uri,
            columns,
            start_offset,
            num_rows,
            None,
            predicate,
            io_client,
            None,
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap()
    }

    /// Helper: read via arrowrs local path directly.
    fn read_via_arrowrs_local(
        uri: &str,
        columns: Option<&[&str]>,
        start_offset: Option<usize>,
        num_rows: Option<usize>,
        predicate: Option<ExprRef>,
    ) -> RecordBatch {
        local_parquet_read_arrowrs(
            uri,
            columns,
            start_offset,
            num_rows,
            None,
            predicate,
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
            None,
        )
        .unwrap()
    }

    /// Build a predicate: val > 3 (keep rows with val 4,5,6,7,8,9).
    fn val_gt_3_predicate() -> ExprRef {
        use daft_dsl::{lit, resolved_col};
        resolved_col("val").gt(lit(3i32))
    }

    /// Extract the "val" column as a Vec<i32> for easy comparison.
    fn extract_val_column(batch: &RecordBatch) -> Vec<i32> {
        let idx = batch.schema.get_index("val").unwrap();
        let series = &batch.columns()[idx];
        let arr = series.i32().unwrap();
        (0..arr.len()).map(|i| arr.get(i).unwrap()).collect()
    }

    // ---- Scenario 1: offset + predicate (no limit) ----
    // File has val=0..10. Predicate: val > 3. Offset=3.
    // Offset skips first 3 file rows → decode [3,4,5,6,7,8,9]
    // → filter val>3 → [4,5,6,7,8,9]

    #[test]
    fn test_offset_with_predicate() {
        let dir = std::env::temp_dir().join("daft_test_offset_pred");
        std::fs::create_dir_all(&dir).unwrap();
        let uri = write_test_parquet(&dir, 10);

        let result = read_via_arrowrs_local(
            &uri,
            Some(&["val"]),
            Some(3),
            None,
            Some(val_gt_3_predicate()),
        );

        let vals = extract_val_column(&result);
        assert_eq!(vals, vec![4, 5, 6, 7, 8, 9]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Scenario 2: offset + limit + predicate ----
    // File has val=0..10. Predicate: val > 3. Offset=3, limit=2.
    // Offset skips first 3 file rows → decode [3,4,5,6,7,8,9]
    // → filter val>3 → [4,5,6,7,8,9] → limit 2 → [4,5]

    #[test]
    fn test_offset_limit_predicate() {
        let dir = std::env::temp_dir().join("daft_test_offset_limit_pred");
        std::fs::create_dir_all(&dir).unwrap();
        let uri = write_test_parquet(&dir, 10);

        let result = read_via_arrowrs_local(
            &uri,
            Some(&["val"]),
            Some(3),
            Some(2),
            Some(val_gt_3_predicate()),
        );

        let vals = extract_val_column(&result);
        assert_eq!(vals, vec![4, 5]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Scenario 3: predicate + limit (no offset) ----
    // Verify limit applies post-filter, matching parquet2 behavior.
    // File has val=0..10. Predicate: val > 3. Limit=3.
    // → decode all [0..9] → filter val>3 → [4,5,6,7,8,9] → limit 3 → [4,5,6]

    #[tokio::test]
    async fn test_predicate_with_limit_matches_parquet2() {
        let dir = std::env::temp_dir().join("daft_test_pred_limit");
        std::fs::create_dir_all(&dir).unwrap();
        let uri = write_test_parquet(&dir, 10);

        let pq2 = read_via_parquet2(
            &uri,
            Some(vec!["val".into()]),
            None,
            Some(3),
            Some(val_gt_3_predicate()),
        )
        .await;
        let arrowrs = read_via_arrowrs_local(
            &uri,
            Some(&["val"]),
            None,
            Some(3),
            Some(val_gt_3_predicate()),
        );

        let pq2_vals = extract_val_column(&pq2);
        let arrowrs_vals = extract_val_column(&arrowrs);
        assert_eq!(
            arrowrs_vals, pq2_vals,
            "predicate+limit: arrowrs should match parquet2"
        );
        assert_eq!(pq2_vals, vec![4, 5, 6]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Scenario 4: predicate + column expansion ----
    // Request only "label" but predicate references "val". Reader must expand
    // columns to include "val" for the predicate, then strip it from output.
    // File has val=0..10. Predicate: val > 3. Columns: ["label"].
    // → decode all → filter val>3 → rows 4..9 → strip "val" → only "label"

    #[tokio::test]
    async fn test_predicate_column_expansion_matches_parquet2() {
        let dir = std::env::temp_dir().join("daft_test_pred_col_expand");
        std::fs::create_dir_all(&dir).unwrap();
        let uri = write_test_parquet(&dir, 10);

        let pq2 = read_via_parquet2(
            &uri,
            Some(vec!["label".into()]),
            None,
            None,
            Some(val_gt_3_predicate()),
        )
        .await;
        let arrowrs = read_via_arrowrs_local(
            &uri,
            Some(&["label"]),
            None,
            None,
            Some(val_gt_3_predicate()),
        );

        // Both should return only the "label" column (not "val").
        assert_eq!(arrowrs.num_columns(), 1);
        assert_eq!(arrowrs.schema.fields()[0].name, "label");
        assert_eq!(pq2.num_columns(), 1);
        assert_eq!(pq2.schema.fields()[0].name, "label");
        // Both should have 6 rows (val 4..9 pass val > 3).
        assert_eq!(arrowrs.len(), pq2.len());
        assert_eq!(pq2.len(), 6);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Scenario 5: offset + predicate, arrowrs async matches local ----
    // NOTE: parquet2 has a pre-existing bug where offset + predicate on local reads
    // causes a RecordBatch size mismatch (num_rows_read doesn't subtract start offset).
    // So we compare arrowrs async against arrowrs local (already verified in scenarios 1-2).

    #[tokio::test]
    async fn test_offset_with_predicate_async_matches_local() {
        let dir = std::env::temp_dir().join("daft_test_offset_pred_async");
        std::fs::create_dir_all(&dir).unwrap();
        let uri = write_test_parquet(&dir, 10);

        let local = read_via_arrowrs_local(
            &uri,
            Some(&["val"]),
            Some(3),
            None,
            Some(val_gt_3_predicate()),
        );
        let async_result = read_via_arrowrs(
            &uri,
            Some(&["val"]),
            Some(3),
            None,
            Some(val_gt_3_predicate()),
        )
        .await;

        let local_vals = extract_val_column(&local);
        let async_vals = extract_val_column(&async_result);
        assert_eq!(
            async_vals, local_vals,
            "offset+predicate: async should match local"
        );
        assert_eq!(local_vals, vec![4, 5, 6, 7, 8, 9]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Scenario 6: offset + limit + predicate, arrowrs async matches local ----

    #[tokio::test]
    async fn test_offset_limit_predicate_async_matches_local() {
        let dir = std::env::temp_dir().join("daft_test_offset_limit_pred_async");
        std::fs::create_dir_all(&dir).unwrap();
        let uri = write_test_parquet(&dir, 10);

        let local = read_via_arrowrs_local(
            &uri,
            Some(&["val"]),
            Some(3),
            Some(2),
            Some(val_gt_3_predicate()),
        );
        let async_result = read_via_arrowrs(
            &uri,
            Some(&["val"]),
            Some(3),
            Some(2),
            Some(val_gt_3_predicate()),
        )
        .await;

        let local_vals = extract_val_column(&local);
        let async_vals = extract_val_column(&async_result);
        assert_eq!(
            async_vals, local_vals,
            "offset+limit+predicate: async should match local"
        );
        assert_eq!(local_vals, vec![4, 5]);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
