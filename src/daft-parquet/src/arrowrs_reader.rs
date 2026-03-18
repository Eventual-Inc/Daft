//! Parquet reader built on the arrow-rs `parquet` crate.
//!
//! Uses [`DaftAsyncFileReader`] as the IO bridge for remote reads, and the sync
//! `ParquetRecordBatchReaderBuilder` with `std::fs::File` for local reads.

use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashSet},
    hash::Hash,
    sync::Arc,
};

use common_error::DaftResult;
use common_runtime::{combine_stream, get_compute_runtime};
use daft_core::prelude::*;
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr, optimization::get_required_columns};
use daft_io::{IOClient, IOStatsRef};
use daft_recordbatch::RecordBatch;
use daft_stats::TruthValue;
use futures::{FutureExt, StreamExt, TryStreamExt, stream::BoxStream};
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
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    async_reader::{
        DaftAsyncFileReader, PrefetchedAsyncFileReader, build_read_planner_and_collect,
    },
    metadata::{
        apply_field_ids_to_arrowrs_parquet_metadata, strip_string_types_from_parquet_metadata,
    },
    read::{ParquetSchemaInferenceOptions, StringEncoding},
    read_planner::RangesContainer,
    schema_inference::{arrow_schema_to_daft_schema, infer_schema_from_parquet_metadata_arrowrs},
    statistics::row_group_metadata_to_table_stats,
};

/// Default batch size for the arrow-rs reader (number of rows per batch).
const DEFAULT_BATCH_SIZE: usize = 8192;

/// Minimum uncompressed row group byte size to enable intra-RG column parallelism.
/// Below this threshold, the overhead of per-column readers (metadata clones, buffer
/// setup, hconcat) exceeds the benefit of parallel decode.
const MIN_RG_BYTES_FOR_COL_PARALLELISM: i64 = 16 * 1024 * 1024; // 16 MiB

/// Minimum number of read columns to enable intra-RG column parallelism.
/// With fewer columns the per-column reader overhead dominates the parallel benefit.
const MIN_COLS_FOR_COL_PARALLELISM: usize = 3;

/// Convert a parquet error to a DaftError.
fn parquet_err(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> common_error::DaftError {
    common_error::DaftError::External(e.into())
}

/// Build a `ProjectionMask` from the given column names and arrow schema.
fn build_projection_mask<K: Borrow<str> + Eq + Hash>(
    col_set: &HashSet<K>,
    arrow_schema: &arrow::datatypes::Schema,
    parquet_schema: &SchemaDescriptor,
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
fn project_daft_schema<K: Borrow<str> + Eq + Hash>(
    schema: &Schema,
    col_set: &HashSet<K>,
) -> Schema {
    Schema::new(
        schema
            .into_iter()
            .filter(|f| col_set.contains(f.name.as_ref()))
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
        schema_infer_options.string_encoding == StringEncoding::Raw,
    )
    .map_err(parquet_err)?;
    let daft_schema = arrow_schema_to_daft_schema(&arrow_schema)?;
    Ok((arrow_schema, daft_schema))
}

/// Try to build an arrow-rs `RowFilter` from a Daft predicate expression.
///
/// Returns `None` if the predicate has no required columns, or if any predicate
/// column is missing from the schema (e.g. computed columns that don't exist in
/// the parquet file). On success, returns the RowFilter and the set of column
/// names required by the predicate.
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

    let pred = predicate.clone();
    let predicate_fn = ArrowPredicateFn::new(filter_projection, move |batch| {
        // Convert arrow-rs RecordBatch (filter columns only) to Daft RecordBatch.
        let daft_batch = RecordBatch::try_from(&batch)
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

/// Sort and deduplicate delete row positions.
///
/// Multiple Iceberg delete files may produce unsorted or duplicate positions.
/// Returns the input unchanged (borrowed) if already sorted and unique.
fn normalize_delete_rows(delete_rows: &[i64]) -> std::borrow::Cow<'_, [i64]> {
    debug_assert!(
        delete_rows.iter().all(|&r| r >= 0),
        "delete_rows contains negative values"
    );
    if delete_rows.windows(2).any(|w| w[0] >= w[1]) {
        let mut sorted = delete_rows.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        std::borrow::Cow::Owned(sorted)
    } else {
        std::borrow::Cow::Borrowed(delete_rows)
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
    let delete_rows = normalize_delete_rows(delete_rows);
    let delete_rows = &*delete_rows;

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
    let delete_rows = normalize_delete_rows(delete_rows);
    let delete_rows = &*delete_rows;

    let rg_end = rg_global_start + rg_rows;
    let lo = delete_rows.partition_point(|&r| (r as usize) < rg_global_start);
    let hi = delete_rows.partition_point(|&r| (r as usize) < rg_end);

    let local_deletes: Vec<usize> = delete_rows[lo..hi]
        .iter()
        .map(|&r| r as usize - rg_global_start)
        .collect();

    deletes_to_row_selection(&local_deletes, rg_rows)
}

/// Combine two optional `RowSelection`s via intersection.
fn combine_selections(a: Option<RowSelection>, b: Option<RowSelection>) -> Option<RowSelection> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.intersection(&b)),
        (a @ Some(_), None) | (None, a @ Some(_)) => a,
        (None, None) => None,
    }
}

/// Apply post-read predicate fallback and strip predicate-only columns.
///
/// When the predicate couldn't be pushed into the reader as a RowFilter, apply it
/// now. Then remove any columns that were only needed for the predicate.
fn finalize_batch(
    mut table: RecordBatch,
    predicate: Option<&ExprRef>,
    predicate_pushed: bool,
    read_schema: &Schema,
    return_schema: &Schema,
) -> DaftResult<RecordBatch> {
    if let Some(pred) = predicate
        && !predicate_pushed
    {
        let bound = BoundExpr::try_new(pred.clone(), &table.schema)?;
        table = table.filter(&[bound])?;
    }

    if read_schema.len() != return_schema.len() {
        let return_indices: Vec<usize> = return_schema
            .names()
            .iter()
            .map(|name| table.schema.get_index(name))
            .collect::<DaftResult<Vec<_>>>()?;
        table = table.get_columns(&return_indices);
    }

    Ok(table)
}

/// Convert sorted local delete indices to alternating select/skip RowSelectors.
fn deletes_to_row_selection(local_deletes: &[usize], total_rows: usize) -> RowSelection {
    if local_deletes.is_empty() {
        return vec![RowSelector::select(total_rows)].into();
    }

    let mut selectors = Vec::with_capacity(local_deletes.len() * 2 + 1);
    let mut pos = 0usize;

    for &del in local_deletes {
        // Skip duplicate positions (can happen with overlapping Iceberg delete files).
        if del < pos {
            continue;
        }
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

// ---------------------------------------------------------------------------
// Shared helpers for intra-RG column parallelism
// ---------------------------------------------------------------------------

/// RLE-encode a boolean mask into a `RowSelection`.
fn bool_array_to_row_selection(mask: &arrow::array::BooleanArray) -> RowSelection {
    use arrow::array::Array;
    let mut selectors = Vec::new();
    let mut current_select = false;
    let mut current_count = 0usize;

    for i in 0..mask.len() {
        let val = mask.is_valid(i) && mask.value(i);
        if val == current_select {
            current_count += 1;
        } else {
            if current_count > 0 {
                selectors.push(if current_select {
                    RowSelector::select(current_count)
                } else {
                    RowSelector::skip(current_count)
                });
            }
            current_select = val;
            current_count = 1;
        }
    }
    if current_count > 0 {
        selectors.push(if current_select {
            RowSelector::select(current_count)
        } else {
            RowSelector::skip(current_count)
        });
    }

    selectors.into()
}

/// Compose a base selection (relative to full RG) with a predicate selection
/// (relative to base-selected rows) into a final selection (relative to full RG).
///
/// For each base selector: if skip(n), emit skip(n). If select(n), consume n rows
/// from predicate_sel, mapping their select/skip back to base coordinates.
fn refine_selection(base: &RowSelection, predicate_sel: &RowSelection) -> RowSelection {
    let base_selectors: Vec<RowSelector> = base.iter().copied().collect();
    let pred_selectors: Vec<RowSelector> = predicate_sel.iter().copied().collect();

    let mut result = Vec::new();
    let mut pred_idx = 0usize;
    let mut pred_remaining = if !pred_selectors.is_empty() {
        pred_selectors[0].row_count
    } else {
        0
    };

    for base_sel in &base_selectors {
        if base_sel.skip {
            result.push(RowSelector::skip(base_sel.row_count));
        } else {
            // This base selector selects `base_sel.row_count` rows.
            // Consume that many rows from predicate_sel.
            let mut remaining = base_sel.row_count;
            while remaining > 0 && pred_idx < pred_selectors.len() {
                let consume = remaining.min(pred_remaining);
                if pred_selectors[pred_idx].skip {
                    result.push(RowSelector::skip(consume));
                } else {
                    result.push(RowSelector::select(consume));
                }
                remaining -= consume;
                pred_remaining -= consume;
                if pred_remaining == 0 {
                    pred_idx += 1;
                    if pred_idx < pred_selectors.len() {
                        pred_remaining = pred_selectors[pred_idx].row_count;
                    }
                }
            }
            // If predicate_sel is exhausted but base still has rows, skip them.
            if remaining > 0 {
                result.push(RowSelector::skip(remaining));
            }
        }
    }

    result.into()
}

/// Merge columns from multiple arrow RecordBatches into one.
/// All batches must have the same row count.
fn hconcat_record_batches(
    batches: &[arrow::array::RecordBatch],
) -> DaftResult<arrow::array::RecordBatch> {
    if batches.is_empty() {
        return Err(common_error::DaftError::ValueError(
            "hconcat_record_batches: empty input".to_string(),
        ));
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }

    let num_rows = batches[0].num_rows();
    let mut fields = Vec::new();
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

    for batch in batches {
        debug_assert_eq!(
            batch.num_rows(),
            num_rows,
            "hconcat: row count mismatch ({} vs {})",
            batch.num_rows(),
            num_rows
        );
        for (i, field) in batch.schema().fields().iter().enumerate() {
            fields.push(field.clone());
            columns.push(batch.column(i).clone());
        }
    }

    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    arrow::array::RecordBatch::try_new(schema, columns).map_err(|e| parquet_err(e).into())
}

/// Build the base RowSelection (offset + delete) for a single RG.
/// Returns None if no selection is needed.
fn build_base_row_selection(
    setup: &LocalParquetSetup,
    task: &RgTask,
    rg_rows: usize,
) -> Option<RowSelection> {
    let offset_selection = if setup.predicate_pushed && task.local_offset > 0 {
        Some(build_offset_row_selection(task.local_offset, rg_rows))
    } else {
        None
    };

    let delete_selection = if let Some(ref deletes) = setup.delete_rows
        && !deletes.is_empty()
    {
        Some(build_single_rg_delete_selection(
            deletes,
            setup.rg_global_starts[task.rg_idx],
            rg_rows,
        ))
    } else {
        None
    };

    combine_selections(offset_selection, delete_selection)
}

/// Compute root column indices from the arrow schema, optionally filtering
/// to `read_col_set` and excluding `exclude` columns.
fn compute_root_indices(
    arrow_schema: &arrow::datatypes::Schema,
    read_col_set: Option<&HashSet<String>>,
    exclude: Option<&HashSet<String>>,
) -> Vec<usize> {
    arrow_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| {
            let name = f.name().as_str();
            let in_read = read_col_set.is_none() || read_col_set.unwrap().contains(name);
            let not_excluded = exclude.is_none() || !exclude.unwrap().contains(name);
            in_read && not_excluded
        })
        .map(|(i, _)| i)
        .collect()
}

// ---------------------------------------------------------------------------
// Sync column-parallel decode helpers (Paths 1, 2)
// ---------------------------------------------------------------------------

/// Decode predicate columns for a single RG, evaluate predicate, return filtered
/// predicate batch and the refined RowSelection for data columns.
fn decode_rg_predicate_phase(
    path: &str,
    setup: &LocalParquetSetup,
    task: &RgTask,
    predicate: &ExprRef,
    pred_col_indices: &[usize],
) -> DaftResult<(arrow::array::RecordBatch, RowSelection)> {
    let rg_rows = setup.parquet_metadata.row_group(task.rg_idx).num_rows() as usize;

    let file = std::fs::File::open(path)
        .map_err(|e| parquet_err(format!("Failed to open '{}': {}", path, e)))?;
    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        file,
        (*setup.arrow_reader_metadata).clone(),
    );

    // Project to predicate columns only.
    let mask = ProjectionMask::roots(builder.parquet_schema(), pred_col_indices.iter().copied());
    builder = builder
        .with_projection(mask)
        .with_row_groups(vec![task.rg_idx])
        .with_batch_size(setup.batch_size);

    // Apply base row selection (offset + deletes).
    let base_selection = build_base_row_selection(setup, task, rg_rows);
    if let Some(ref sel) = base_selection {
        builder = builder.with_row_selection(sel.clone());
    }

    // Non-pushed offset for predicate phase.
    if !setup.predicate_pushed {
        builder = builder.with_offset(task.local_offset);
    }

    let reader = builder.build().map_err(parquet_err)?;
    let arrow_batches: Vec<arrow::array::RecordBatch> =
        reader.collect::<Result<Vec<_>, _>>().map_err(parquet_err)?;

    // Concat predicate batches into one.
    let pred_batch = if arrow_batches.is_empty() {
        // Empty RG: return empty batch with predicate schema.
        let pred_col_names: HashSet<&str> = setup
            .predicate_columns
            .as_ref()
            .unwrap()
            .iter()
            .map(|s| s.as_str())
            .collect();
        let fields: Vec<arrow::datatypes::FieldRef> = setup
            .arrow_schema
            .fields()
            .iter()
            .filter(|f| pred_col_names.contains(f.name().as_str()))
            .cloned()
            .collect();
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        arrow::array::RecordBatch::new_empty(schema)
    } else if arrow_batches.len() == 1 {
        arrow_batches.into_iter().next().unwrap()
    } else {
        arrow::compute::concat_batches(&arrow_batches[0].schema(), &arrow_batches)
            .map_err(parquet_err)?
    };

    // Evaluate predicate on pred_batch.
    let daft_pred_batch = RecordBatch::try_from(&pred_batch)?;
    let bound = BoundExpr::try_new(predicate.clone(), &daft_pred_batch.schema)
        .map_err(|e| parquet_err(format!("Failed to bind predicate: {}", e)))?;
    let result = daft_pred_batch
        .eval_expression(&bound)
        .map_err(|e| parquet_err(format!("Failed to eval predicate: {}", e)))?;
    let bool_arr = result
        .bool()
        .map_err(|e| parquet_err(format!("Predicate did not produce boolean: {}", e)))?;
    let arrow_bool = bool_arr
        .as_arrow()
        .map_err(|e| parquet_err(format!("Failed to convert to arrow bool: {}", e)))?;

    // Convert predicate result to RowSelection.
    let predicate_sel = bool_array_to_row_selection(arrow_bool);

    // Compute final selection = refine(base, predicate).
    let final_selection = if let Some(ref base) = base_selection {
        refine_selection(base, &predicate_sel)
    } else {
        // No base selection: predicate_sel is relative to full RG.
        predicate_sel
    };

    // Filter the predicate batch to only keep matching rows.
    let filter_arr = arrow_bool;
    let filtered_pred_batch =
        arrow::compute::filter_record_batch(&pred_batch, filter_arr).map_err(parquet_err)?;

    Ok((filtered_pred_batch, final_selection))
}

/// Decode a single column from a single RG with the given RowSelection.
/// Opens its own file handle (independent seek position, ~microsecond syscall).
/// The OS page cache ensures file data is served from memory after first access.
fn decode_rg_column(
    path: &str,
    setup: &LocalParquetSetup,
    task: &RgTask,
    col_root_index: usize,
    row_selection: Option<&RowSelection>,
) -> DaftResult<arrow::array::RecordBatch> {
    let file = std::fs::File::open(path)
        .map_err(|e| parquet_err(format!("Failed to open '{}': {}", path, e)))?;
    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        file,
        (*setup.arrow_reader_metadata).clone(),
    );

    let mask = ProjectionMask::roots(builder.parquet_schema(), std::iter::once(col_root_index));
    builder = builder
        .with_projection(mask)
        .with_row_groups(vec![task.rg_idx])
        .with_batch_size(setup.batch_size);

    if let Some(sel) = row_selection {
        builder = builder.with_row_selection(sel.clone());
    }

    let reader = builder.build().map_err(parquet_err)?;
    let arrow_batches: Vec<arrow::array::RecordBatch> =
        reader.collect::<Result<Vec<_>, _>>().map_err(parquet_err)?;

    if arrow_batches.is_empty() {
        let field = setup.arrow_schema.field(col_root_index).clone();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        Ok(arrow::array::RecordBatch::new_empty(schema))
    } else if arrow_batches.len() == 1 {
        Ok(arrow_batches.into_iter().next().unwrap())
    } else {
        arrow::compute::concat_batches(&arrow_batches[0].schema(), &arrow_batches)
            .map_err(|e| parquet_err(e).into())
    }
}

/// Decode a single RG with intra-RG column parallelism (rayon).
/// Used by the streaming local path (Path 2).
fn decode_single_rg_col_parallel(
    path: &str,
    setup: &LocalParquetSetup,
    task_idx: usize,
    predicate: Option<&ExprRef>,
) -> DaftResult<RecordBatch> {
    let task = &setup.rg_tasks[task_idx];
    let rg_rows = setup.parquet_metadata.row_group(task.rg_idx).num_rows() as usize;

    let all_col_indices =
        compute_root_indices(&setup.arrow_schema, setup.read_col_set.as_ref(), None);

    // Fallback: single column or small row group where parallel overhead exceeds benefit.
    // This is called per-RG from the streaming path, so even 2 columns benefit from splitting.
    let rg_byte_size = setup
        .parquet_metadata
        .row_group(task.rg_idx)
        .total_byte_size();
    if all_col_indices.len() < MIN_COLS_FOR_COL_PARALLELISM
        || rg_byte_size < MIN_RG_BYTES_FOR_COL_PARALLELISM
    {
        return decode_single_rg(path, setup, task, predicate, None);
    }

    if setup.predicate_pushed {
        let pred = predicate.unwrap();
        let pred_cols = setup.predicate_columns.as_ref().unwrap();
        let pred_col_indices = compute_root_indices(&setup.arrow_schema, Some(pred_cols), None);
        let data_col_indices = compute_root_indices(
            &setup.arrow_schema,
            setup.read_col_set.as_ref(),
            Some(pred_cols),
        );

        // Phase 1: decode predicate columns.
        let (pred_batch, final_selection) =
            decode_rg_predicate_phase(path, setup, task, pred, &pred_col_indices)?;

        if data_col_indices.is_empty() {
            let daft_batch = RecordBatch::try_from(&pred_batch)?;
            return finalize_batch(
                daft_batch,
                None,
                true,
                &setup.read_daft_schema,
                &setup.return_daft_schema,
            );
        }

        // Phase 2: decode data columns in parallel (each task opens its own file handle).
        let col_results: Vec<DaftResult<arrow::array::RecordBatch>> = data_col_indices
            .par_iter()
            .map(|&col_idx| decode_rg_column(path, setup, task, col_idx, Some(&final_selection)))
            .collect();
        let col_batches: Vec<arrow::array::RecordBatch> =
            col_results.into_iter().collect::<DaftResult<Vec<_>>>()?;

        let mut all_batches = vec![pred_batch];
        all_batches.extend(col_batches);
        let merged = hconcat_record_batches(&all_batches)?;
        let daft_batch = RecordBatch::try_from(&merged)?;
        finalize_batch(
            daft_batch,
            None,
            true,
            &setup.read_daft_schema,
            &setup.return_daft_schema,
        )
    } else {
        // No predicate: decode all columns in parallel (each task opens its own file handle).
        let mut base_sel = build_base_row_selection(setup, task, rg_rows);
        if task.local_offset > 0 {
            let offset_sel = build_offset_row_selection(task.local_offset, rg_rows);
            base_sel = combine_selections(base_sel, Some(offset_sel));
        }

        let col_results: Vec<DaftResult<arrow::array::RecordBatch>> = all_col_indices
            .par_iter()
            .map(|&col_idx| decode_rg_column(path, setup, task, col_idx, base_sel.as_ref()))
            .collect();
        let col_batches: Vec<arrow::array::RecordBatch> =
            col_results.into_iter().collect::<DaftResult<Vec<_>>>()?;

        let merged = hconcat_record_batches(&col_batches)?;
        let daft_batch = RecordBatch::try_from(&merged)?;
        finalize_batch(
            daft_batch,
            predicate,
            false,
            &setup.read_daft_schema,
            &setup.return_daft_schema,
        )
    }
}

// ---------------------------------------------------------------------------
// Pre-fetched async column decode helpers (S3 byte range coalescing)
// ---------------------------------------------------------------------------

/// Map root (top-level) column indices to parquet leaf column indices.
///
/// Parquet files can have nested schemas where a single root column maps to
/// multiple leaf columns. The `SchemaDescriptor` tracks this mapping.
fn root_to_leaf_columns(schema_descr: &SchemaDescriptor, root_indices: &[usize]) -> Vec<usize> {
    let root_set: HashSet<usize> = root_indices.iter().copied().collect();
    (0..schema_descr.num_columns())
        .filter(|&leaf| root_set.contains(&schema_descr.get_column_root_idx(leaf)))
        .collect()
}

/// Pre-fetch all byte ranges for the given (rg, col) pairs using a single
/// `ReadPlanner`, enabling cross-column and cross-RG coalescing into fewer
/// large HTTP requests.
#[allow(clippy::ref_option)]
async fn prefetch_column_ranges(
    uri: &str,
    io_client: &Arc<IOClient>,
    io_stats: &Option<IOStatsRef>,
    parquet_metadata: &Arc<ParquetMetaData>,
    rg_indices: &[usize],
    root_col_indices: &[usize],
) -> DaftResult<Arc<RangesContainer>> {
    let schema_descr = parquet_metadata.file_metadata().schema_descr();
    let leaf_cols = root_to_leaf_columns(schema_descr, root_col_indices);

    let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
    for &rg_idx in rg_indices {
        let rg_meta = parquet_metadata.row_group(rg_idx);
        for &leaf_col in &leaf_cols {
            let col_meta = rg_meta.column(leaf_col);
            let (offset, length) = col_meta.byte_range();
            ranges.push(offset as usize..(offset + length) as usize);
        }
    }

    let container =
        build_read_planner_and_collect(uri, &ranges, io_client.clone(), io_stats.clone())
            .map_err(|e| common_error::DaftError::External(Box::new(e)))?;

    Ok(container)
}

/// Async decode of predicate columns for a single RG using a pre-fetched cache.
#[allow(clippy::too_many_arguments)]
async fn decode_rg_predicate_phase_async_prefetched(
    ranges_container: &Arc<RangesContainer>,
    parquet_metadata: &Arc<ParquetMetaData>,
    arrow_schema: &Arc<arrow::datatypes::Schema>,
    predicate: &ExprRef,
    pred_col_indices: &[usize],
    rg_idx: usize,
    base_selection: Option<RowSelection>,
    predicate_columns: &HashSet<String>,
    batch_size: usize,
) -> DaftResult<(arrow::array::RecordBatch, RowSelection)> {
    let reader = PrefetchedAsyncFileReader::new(ranges_container.clone(), parquet_metadata.clone());
    let options = ArrowReaderOptions::new().with_schema(arrow_schema.clone());
    let arrow_reader_metadata =
        ArrowReaderMetadata::try_new(parquet_metadata.clone(), options).map_err(parquet_err)?;
    let mut builder =
        ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_reader_metadata);

    let mask = ProjectionMask::roots(builder.parquet_schema(), pred_col_indices.iter().copied());
    builder = builder
        .with_projection(mask)
        .with_row_groups(vec![rg_idx])
        .with_batch_size(batch_size);

    if let Some(ref sel) = base_selection {
        builder = builder.with_row_selection(sel.clone());
    }

    let stream = builder.build().map_err(parquet_err)?;
    let arrow_batches: Vec<arrow::array::RecordBatch> =
        stream.try_collect().await.map_err(parquet_err)?;

    let pred_batch = if arrow_batches.is_empty() {
        let pred_col_names: HashSet<&str> = predicate_columns.iter().map(|s| s.as_str()).collect();
        let fields: Vec<arrow::datatypes::FieldRef> = arrow_schema
            .fields()
            .iter()
            .filter(|f| pred_col_names.contains(f.name().as_str()))
            .cloned()
            .collect();
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        arrow::array::RecordBatch::new_empty(schema)
    } else if arrow_batches.len() == 1 {
        arrow_batches.into_iter().next().unwrap()
    } else {
        arrow::compute::concat_batches(&arrow_batches[0].schema(), &arrow_batches)
            .map_err(parquet_err)?
    };

    // Evaluate predicate.
    let daft_pred_batch = RecordBatch::try_from(&pred_batch)?;
    let bound = BoundExpr::try_new(predicate.clone(), &daft_pred_batch.schema)
        .map_err(|e| parquet_err(format!("Failed to bind predicate: {}", e)))?;
    let result = daft_pred_batch
        .eval_expression(&bound)
        .map_err(|e| parquet_err(format!("Failed to eval predicate: {}", e)))?;
    let bool_arr = result
        .bool()
        .map_err(|e| parquet_err(format!("Predicate did not produce boolean: {}", e)))?;
    let arrow_bool = bool_arr
        .as_arrow()
        .map_err(|e| parquet_err(format!("Failed to convert to arrow bool: {}", e)))?;

    let predicate_sel = bool_array_to_row_selection(arrow_bool);
    let final_selection = if let Some(ref base) = base_selection {
        refine_selection(base, &predicate_sel)
    } else {
        predicate_sel
    };

    let filtered_pred_batch =
        arrow::compute::filter_record_batch(&pred_batch, arrow_bool).map_err(parquet_err)?;

    Ok((filtered_pred_batch, final_selection))
}

/// Async decode of a single column from a single RG using a pre-fetched cache.
#[allow(clippy::too_many_arguments)]
async fn decode_rg_column_async_prefetched(
    ranges_container: &Arc<RangesContainer>,
    parquet_metadata: &Arc<ParquetMetaData>,
    arrow_schema: &Arc<arrow::datatypes::Schema>,
    col_root_index: usize,
    rg_idx: usize,
    row_selection: Option<RowSelection>,
    batch_size: usize,
) -> DaftResult<arrow::array::RecordBatch> {
    let reader = PrefetchedAsyncFileReader::new(ranges_container.clone(), parquet_metadata.clone());
    let options = ArrowReaderOptions::new().with_schema(arrow_schema.clone());
    let arrow_reader_metadata =
        ArrowReaderMetadata::try_new(parquet_metadata.clone(), options).map_err(parquet_err)?;
    let mut builder =
        ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_reader_metadata);

    let mask = ProjectionMask::roots(builder.parquet_schema(), std::iter::once(col_root_index));
    builder = builder
        .with_projection(mask)
        .with_row_groups(vec![rg_idx])
        .with_batch_size(batch_size);

    if let Some(sel) = row_selection {
        builder = builder.with_row_selection(sel);
    }

    let stream = builder.build().map_err(parquet_err)?;
    let arrow_batches: Vec<arrow::array::RecordBatch> =
        stream.try_collect().await.map_err(parquet_err)?;

    if arrow_batches.is_empty() {
        let field = arrow_schema.field(col_root_index).clone();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        Ok(arrow::array::RecordBatch::new_empty(schema))
    } else if arrow_batches.len() == 1 {
        Ok(arrow_batches.into_iter().next().unwrap())
    } else {
        arrow::compute::concat_batches(&arrow_batches[0].schema(), &arrow_batches)
            .map_err(|e| parquet_err(e).into())
    }
}

/// Read a single parquet file into a Daft [`RecordBatch`].
///
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
/// in production (the streaming scan path doesn't even accept the parameter). Our
/// implementation follows the intended semantics based on the code structure and the
/// `apply_delete_rows`
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
    let io_client_saved = io_client.clone();
    let io_stats_saved = io_stats.clone();
    let mut reader = DaftAsyncFileReader::new(uri.to_string(), io_client, io_stats, metadata, None);
    let mut parquet_metadata = reader.get_metadata(None).await.map_err(parquet_err)?;

    // 1b. Apply field ID mapping (Iceberg schema evolution) if provided.
    if let Some(ref mapping) = field_id_mapping {
        parquet_metadata = apply_field_ids_to_arrowrs_parquet_metadata(parquet_metadata, mapping)?;
    }

    // 1c. For StringEncoding::Raw, strip STRING/UTF8 logical types from the parquet
    // metadata so arrow-rs infers Binary instead of Utf8. This avoids UTF-8
    // validation during decode, allowing files with invalid UTF-8 to be read.
    if schema_infer_options.string_encoding == StringEncoding::Raw {
        parquet_metadata = strip_string_types_from_parquet_metadata(parquet_metadata)?;
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

    // 4. Compute schemas.
    let read_daft_schema = if let Some(ref col_set) = read_col_set {
        project_daft_schema(&daft_schema, col_set)
    } else {
        daft_schema.clone()
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

    // Zero-column read (e.g. metadata-only query): no decoding needed.
    if return_daft_schema.is_empty() {
        let total: usize = rg_indices
            .iter()
            .map(|&i| parquet_metadata.row_group(i).num_rows() as usize)
            .sum();
        return Ok(row_count_batch(return_daft_schema, total, num_rows));
    }

    // 7. Compute column indices and per-RG selections for column-parallel decode.
    let predicate_columns: Option<HashSet<String>> =
        row_filter_result.as_ref().map(|(_, cols)| cols.clone());
    let read_col_set_owned: Option<HashSet<String>> = read_col_set
        .as_ref()
        .map(|s| s.iter().map(|x| (*x).to_string()).collect());
    let arrow_schema_arc = Arc::new(arrow_schema.clone());
    let all_col_indices = compute_root_indices(&arrow_schema, read_col_set_owned.as_ref(), None);

    // Compute per-RG base selections (offset + deletes).
    let use_offset_selection = predicate_pushed && start_offset.is_some_and(|o| o > 0);
    let mut rg_global_starts = Vec::with_capacity(parquet_metadata.num_row_groups());
    let mut cumulative = 0usize;
    for i in 0..parquet_metadata.num_row_groups() {
        rg_global_starts.push(cumulative);
        cumulative += parquet_metadata.row_group(i).num_rows() as usize;
    }

    // Compute per-RG offset within the concatenated selected RGs (for start_offset).
    let global_start = start_offset.unwrap_or(0);
    let per_rg_selections: Vec<Option<RowSelection>> = {
        let mut cumulative_rows = 0usize;
        rg_indices
            .iter()
            .map(|&rg_idx| {
                let rg_rows = parquet_metadata.row_group(rg_idx).num_rows() as usize;
                let rg_start_in_stream = cumulative_rows;
                cumulative_rows += rg_rows;

                let offset_sel = if use_offset_selection || (!predicate_pushed && global_start > 0)
                {
                    let local_offset = global_start.saturating_sub(rg_start_in_stream);
                    if local_offset > 0 {
                        Some(build_offset_row_selection(local_offset, rg_rows))
                    } else {
                        None
                    }
                } else {
                    None
                };

                let delete_sel = if let Some(deletes) = delete_rows
                    && !deletes.is_empty()
                {
                    Some(build_single_rg_delete_selection(
                        deletes,
                        rg_global_starts[rg_idx],
                        rg_rows,
                    ))
                } else {
                    None
                };

                combine_selections(offset_sel, delete_sel)
            })
            .collect()
    };

    // Fallback: few columns -> single-stream with prefetched I/O for cross-RG coalescing.
    if all_col_indices.len() < MIN_COLS_FOR_COL_PARALLELISM {
        // Pre-fetch all RG ranges upfront for cross-RG coalescing.
        let prefetch_container = prefetch_column_ranges(
            uri,
            &io_client_saved,
            &io_stats_saved,
            &parquet_metadata,
            &rg_indices,
            &all_col_indices,
        )
        .await?;

        let reader2 = PrefetchedAsyncFileReader::new(prefetch_container, parquet_metadata.clone());
        let options2 = ArrowReaderOptions::new().with_schema(arrow_schema_arc.clone());
        let arm2 = ArrowReaderMetadata::try_new(parquet_metadata.clone(), options2)
            .map_err(parquet_err)?;
        let mut builder2 = ParquetRecordBatchStreamBuilder::new_with_metadata(reader2, arm2);
        if let Some(ref col_set) = read_col_set {
            let mask = build_projection_mask(col_set, &arrow_schema, builder2.parquet_schema());
            builder2 = builder2.with_projection(mask);
        }
        builder2 = builder2.with_row_groups(rg_indices.clone());
        if !use_offset_selection && let Some(offset) = start_offset {
            builder2 = builder2.with_offset(offset);
        }
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
            let delete_selection = if let Some(deletes) = delete_rows
                && !deletes.is_empty()
            {
                Some(build_delete_row_selection(
                    deletes,
                    &rg_indices,
                    &parquet_metadata,
                ))
            } else {
                None
            };
            if let Some(selection) = combine_selections(offset_selection, delete_selection) {
                builder2 = builder2.with_row_selection(selection);
            }
        }
        if let Some((row_filter, _)) = row_filter_result {
            builder2 = builder2.with_row_filter(row_filter);
        }
        if (predicate_pushed || predicate.is_none())
            && let Some(limit) = num_rows
        {
            builder2 = builder2.with_limit(limit);
        }
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        builder2 = builder2.with_batch_size(batch_size);
        let stream = builder2.build().map_err(parquet_err)?;
        let arrow_batches: Vec<arrow::array::RecordBatch> =
            stream.try_collect().await.map_err(parquet_err)?;
        let daft_batches: Vec<RecordBatch> = arrow_batches
            .iter()
            .map(RecordBatch::try_from)
            .collect::<DaftResult<Vec<_>>>()?;
        let daft_refs: Vec<&RecordBatch> = daft_batches.iter().collect();
        let mut table =
            RecordBatch::concat_or_empty(&daft_refs, Some(Arc::new(read_daft_schema.clone())))?;
        table = finalize_batch(
            table,
            predicate.as_ref(),
            predicate_pushed,
            &read_daft_schema,
            &return_daft_schema,
        )?;
        if predicate.is_some()
            && !predicate_pushed
            && let Some(limit) = num_rows
        {
            table = table.head(limit)?;
        }
        return Ok(table);
    }

    // Column-parallel async decode with prefetched I/O.
    // One stream per column across ALL RGs (mirrors parquet2's architecture),
    // eliminating the expensive final RecordBatch::concat across RGs.
    let total_selected_rows: usize = rg_indices
        .iter()
        .map(|&idx| parquet_metadata.row_group(idx).num_rows() as usize)
        .sum();

    if predicate_pushed {
        let pred_cols = predicate_columns.as_ref().unwrap();
        let pred_col_indices = compute_root_indices(&arrow_schema, Some(pred_cols), None);
        let data_col_indices =
            compute_root_indices(&arrow_schema, read_col_set_owned.as_ref(), Some(pred_cols));

        // Prefetch ALL columns (pred + data) upfront for maximum coalescing.
        let prefetch_container = prefetch_column_ranges(
            uri,
            &io_client_saved,
            &io_stats_saved,
            &parquet_metadata,
            &rg_indices,
            &all_col_indices,
        )
        .await?;

        // Phase 1: decode predicate columns per RG to compute RowSelections.
        let phase1_handles: Vec<_> = rg_indices
            .iter()
            .enumerate()
            .map(|(ri, &rg_idx)| {
                let container = prefetch_container.clone();
                let pm = parquet_metadata.clone();
                let as_arc = arrow_schema_arc.clone();
                let pred = predicate.clone().unwrap();
                let pci = pred_col_indices.clone();
                let base_sel = per_rg_selections[ri].clone();
                let pc = pred_cols.clone();
                let rg_rows = parquet_metadata.row_group(rg_idx).num_rows() as usize;
                tokio::spawn(async move {
                    decode_rg_predicate_phase_async_prefetched(
                        &container, &pm, &as_arc, &pred, &pci, rg_idx, base_sel, &pc, rg_rows,
                    )
                    .await
                })
            })
            .collect::<Vec<_>>();
        let phase1_results = futures::future::join_all(phase1_handles).await;
        let phase1: Vec<(arrow::array::RecordBatch, RowSelection)> = phase1_results
            .into_iter()
            .map(|r| r.map_err(|e| common_error::DaftError::External(e.into()))?)
            .collect::<DaftResult<Vec<_>>>()?;

        // Concat pred batches across RGs into one.
        let pred_batches: Vec<arrow::array::RecordBatch> =
            phase1.iter().map(|(b, _)| b.clone()).collect();
        let combined_pred_batch = if pred_batches.len() == 1 {
            pred_batches.into_iter().next().unwrap()
        } else {
            arrow::compute::concat_batches(&pred_batches[0].schema(), &pred_batches)
                .map_err(parquet_err)?
        };

        if data_col_indices.is_empty() {
            let daft_batch = RecordBatch::try_from(&combined_pred_batch)?;
            let mut table = finalize_batch(
                daft_batch,
                None,
                true,
                &read_daft_schema,
                &return_daft_schema,
            )?;
            if let Some(limit) = num_rows {
                table = table.head(limit)?;
            }
            return Ok(table);
        }

        // Concatenate per-RG selections into one combined selection for all RGs.
        let combined_selection: RowSelection = {
            let mut all_selectors = Vec::new();
            for (_, sel) in &phase1 {
                for selector in sel.iter() {
                    all_selectors.push(*selector);
                }
            }
            all_selectors.into()
        };

        // Phase 2: one spawned task per data column across ALL RGs.
        // tokio::spawn creates independent tasks that run on different worker
        // threads, unlike try_join_all which polls all futures from one task.
        let col_handles: Vec<_> = data_col_indices
            .iter()
            .map(|&col_idx| {
                let container = prefetch_container.clone();
                let pm = parquet_metadata.clone();
                let as_arc = arrow_schema_arc.clone();
                let sel = combined_selection.clone();
                let rg_indices = rg_indices.clone();
                tokio::spawn(async move {
                    let reader = PrefetchedAsyncFileReader::new(container, pm.clone());
                    let options = ArrowReaderOptions::new().with_schema(as_arc.clone());
                    let arm =
                        ArrowReaderMetadata::try_new(pm.clone(), options).map_err(parquet_err)?;
                    let mut builder =
                        ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arm);
                    let mask =
                        ProjectionMask::roots(builder.parquet_schema(), std::iter::once(col_idx));
                    builder = builder
                        .with_projection(mask)
                        .with_row_groups(rg_indices)
                        .with_row_selection(sel)
                        .with_batch_size(total_selected_rows);
                    let stream = builder.build().map_err(parquet_err)?;
                    let batches: Vec<arrow::array::RecordBatch> =
                        stream.try_collect().await.map_err(parquet_err)?;
                    if batches.is_empty() {
                        let field = as_arc.field(col_idx).clone();
                        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
                        Ok::<_, common_error::DaftError>(arrow::array::RecordBatch::new_empty(
                            schema,
                        ))
                    } else if batches.len() == 1 {
                        Ok(batches.into_iter().next().unwrap())
                    } else {
                        arrow::compute::concat_batches(&batches[0].schema(), &batches)
                            .map_err(|e| parquet_err(e).into())
                    }
                })
            })
            .collect::<Vec<_>>();
        let data_col_results = futures::future::join_all(col_handles).await;
        let data_col_batches: Vec<arrow::array::RecordBatch> = data_col_results
            .into_iter()
            .map(|r| r.map_err(|e| common_error::DaftError::External(e.into()))?)
            .collect::<DaftResult<Vec<_>>>()?;

        // hconcat pred + data columns into one RecordBatch.
        let mut all_batches = vec![combined_pred_batch];
        all_batches.extend(data_col_batches);
        let merged = hconcat_record_batches(&all_batches)?;
        let daft_batch = RecordBatch::try_from(&merged)?;
        let mut table = finalize_batch(
            daft_batch,
            None,
            true,
            &read_daft_schema,
            &return_daft_schema,
        )?;
        if let Some(limit) = num_rows {
            table = table.head(limit)?;
        }
        Ok(table)
    } else {
        // No predicate: one stream per column across ALL RGs.
        let prefetch_container = prefetch_column_ranges(
            uri,
            &io_client_saved,
            &io_stats_saved,
            &parquet_metadata,
            &rg_indices,
            &all_col_indices,
        )
        .await?;

        // Compute combined selection across all RGs (offset + deletes).
        let combined_selection = {
            let offset_selection =
                if use_offset_selection || (!predicate_pushed && global_start > 0) {
                    Some(build_offset_row_selection(
                        global_start,
                        total_selected_rows,
                    ))
                } else {
                    None
                };
            let delete_selection = if let Some(deletes) = delete_rows
                && !deletes.is_empty()
            {
                Some(build_delete_row_selection(
                    deletes,
                    &rg_indices,
                    &parquet_metadata,
                ))
            } else {
                None
            };
            combine_selections(offset_selection, delete_selection)
        };

        // Spawn each column decode as an independent tokio task for true
        // multi-thread parallelism (try_join_all polls from a single task).
        let col_handles: Vec<_> = all_col_indices
            .iter()
            .map(|&col_idx| {
                let container = prefetch_container.clone();
                let pm = parquet_metadata.clone();
                let as_arc = arrow_schema_arc.clone();
                let sel = combined_selection.clone();
                let rg_indices = rg_indices.clone();
                tokio::spawn(async move {
                    let reader = PrefetchedAsyncFileReader::new(container, pm.clone());
                    let options = ArrowReaderOptions::new().with_schema(as_arc.clone());
                    let arm =
                        ArrowReaderMetadata::try_new(pm.clone(), options).map_err(parquet_err)?;
                    let mut builder =
                        ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arm);
                    let mask =
                        ProjectionMask::roots(builder.parquet_schema(), std::iter::once(col_idx));
                    builder = builder
                        .with_projection(mask)
                        .with_row_groups(rg_indices)
                        .with_batch_size(total_selected_rows);
                    if let Some(sel) = sel {
                        builder = builder.with_row_selection(sel);
                    }
                    if let Some(limit) = num_rows {
                        builder = builder.with_limit(limit);
                    }
                    let stream = builder.build().map_err(parquet_err)?;
                    let batches: Vec<arrow::array::RecordBatch> =
                        stream.try_collect().await.map_err(parquet_err)?;
                    if batches.is_empty() {
                        let field = as_arc.field(col_idx).clone();
                        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
                        Ok::<_, common_error::DaftError>(arrow::array::RecordBatch::new_empty(
                            schema,
                        ))
                    } else if batches.len() == 1 {
                        Ok(batches.into_iter().next().unwrap())
                    } else {
                        arrow::compute::concat_batches(&batches[0].schema(), &batches)
                            .map_err(|e| parquet_err(e).into())
                    }
                })
            })
            .collect::<Vec<_>>();
        let col_results = futures::future::join_all(col_handles).await;
        let col_batches: Vec<arrow::array::RecordBatch> = col_results
            .into_iter()
            .map(|r| r.map_err(|e| common_error::DaftError::External(e.into()))?)
            .collect::<DaftResult<Vec<_>>>()?;

        let merged = hconcat_record_batches(&col_batches)?;
        let daft_batch = RecordBatch::try_from(&merged)?;
        let mut table = finalize_batch(
            daft_batch,
            predicate.as_ref(),
            false,
            &read_daft_schema,
            &return_daft_schema,
        )?;
        if predicate.is_some()
            && let Some(limit) = num_rows
        {
            table = table.head(limit)?;
        }
        Ok(table)
    }
}

// ---------------------------------------------------------------------------
// Extracted types and functions for local parquet reading
// ---------------------------------------------------------------------------

/// Per-row-group decode task with local offset/limit.
pub(crate) struct RgTask {
    pub rg_idx: usize,
    pub local_offset: usize,
    pub local_limit: Option<usize>,
}

/// Build a row-count-only RecordBatch (zero columns) from metadata.
///
/// Used for metadata-only queries (e.g. `file_path_column`) where no data columns
/// need to be decoded from the parquet file.
fn row_count_batch(schema: Schema, total_rows: usize, num_rows: Option<usize>) -> RecordBatch {
    let capped = num_rows.map_or(total_rows, |limit| limit.min(total_rows));
    RecordBatch::new_unchecked(Arc::new(schema), vec![], capped)
}

/// Setup state for local parquet reading, shared across row group decode tasks.
pub(crate) struct LocalParquetSetup {
    pub arrow_reader_metadata: Arc<ArrowReaderMetadata>,
    pub arrow_schema: Arc<arrow::datatypes::Schema>,
    pub parquet_schema: Arc<SchemaDescriptor>,
    pub parquet_metadata: Arc<ParquetMetaData>,
    pub daft_schema_for_filter: Arc<Schema>,
    pub read_daft_schema: Schema,
    pub return_daft_schema: Schema,
    pub read_col_set: Option<HashSet<String>>,
    pub predicate_columns: Option<HashSet<String>>,
    pub rg_tasks: Vec<RgTask>,
    pub rg_global_starts: Vec<usize>,
    pub batch_size: usize,
    pub predicate_pushed: bool,
    pub delete_rows: Option<Arc<[i64]>>,
}

/// Apply RowFilter, RowSelection (offset + deletes), and limit to a single-RG builder.
///
/// When `predicate_pushed` and `offset > 0`, the offset is converted to a
/// RowSelection (pre-filter) rather than using `with_offset` (post-filter).
fn apply_rg_filter_and_deletes(
    builder: ParquetRecordBatchReaderBuilder<std::fs::File>,
    setup: &LocalParquetSetup,
    task: &RgTask,
    predicate: Option<&ExprRef>,
    rg_rows: usize,
    decoder_limit: Option<usize>,
) -> DaftResult<ParquetRecordBatchReaderBuilder<std::fs::File>> {
    let mut builder = builder;

    // Build RowSelections: offset (when predicate pushed) and delete_rows.
    let offset_selection = if setup.predicate_pushed && task.local_offset > 0 {
        Some(build_offset_row_selection(task.local_offset, rg_rows))
    } else {
        None
    };

    let delete_selection = if let Some(ref deletes) = setup.delete_rows
        && !deletes.is_empty()
    {
        Some(build_single_rg_delete_selection(
            deletes,
            setup.rg_global_starts[task.rg_idx],
            rg_rows,
        ))
    } else {
        None
    };

    if let Some(selection) = combine_selections(offset_selection, delete_selection) {
        builder = builder.with_row_selection(selection);
    }

    // Wire RowFilter for predicate pushdown.
    if setup.predicate_pushed
        && let Some(pred) = predicate
        && let Some((row_filter, _)) = build_row_filter(
            pred,
            &setup.daft_schema_for_filter,
            &setup.arrow_schema,
            &setup.parquet_schema,
        )
    {
        builder = builder.with_row_filter(row_filter);
    }
    if setup.predicate_pushed
        && let Some(lim) = decoder_limit
    {
        builder = builder.with_limit(lim);
    }

    Ok(builder)
}

/// Perform the setup phase for local parquet reading: open file, read metadata,
/// infer schemas, prune row groups, compute per-RG tasks.
#[allow(clippy::too_many_arguments)]
pub(crate) fn local_parquet_setup(
    path: &str,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<&ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    batch_size: Option<usize>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    delete_rows: Option<&[i64]>,
) -> DaftResult<LocalParquetSetup> {
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

    // 1c. For StringEncoding::Raw, strip STRING/UTF8 logical types so arrow-rs
    // reads BYTE_ARRAY as Binary (no UTF-8 validation).
    if schema_infer_options.string_encoding == StringEncoding::Raw {
        parquet_metadata = strip_string_types_from_parquet_metadata(parquet_metadata)?;
    }

    // 2. Infer schema with Daft options.
    let (arrow_schema, daft_schema) = infer_schemas(&parquet_metadata, &schema_infer_options)?;

    // 3. Determine user-requested columns and expand for predicate if needed.
    let user_col_set: Option<HashSet<String>> =
        columns.map(|cols| cols.iter().map(|s| (*s).to_string()).collect());
    let mut read_col_set: Option<HashSet<String>> = user_col_set.clone();

    // Try to build a RowFilter from the predicate.
    let row_filter_data = predicate.and_then(|pred| {
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
            col_set.insert(name.clone());
        }
    }

    // 4. Rebuild metadata with our custom schema.
    let options = ArrowReaderOptions::new().with_schema(Arc::new(arrow_schema.clone()));
    let arrow_reader_metadata =
        ArrowReaderMetadata::try_new(parquet_metadata.clone(), options).map_err(parquet_err)?;

    // 5. Compute schemas.
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
        predicate,
        &read_daft_schema,
        path,
    )?;

    if rg_indices.is_empty() {
        return Ok(LocalParquetSetup {
            arrow_reader_metadata: Arc::new(arrow_reader_metadata),
            arrow_schema: Arc::new(arrow_schema),
            parquet_schema: Arc::new(parquet_metadata.file_metadata().schema_descr().clone()),
            parquet_metadata: parquet_metadata.clone(),
            daft_schema_for_filter,
            read_daft_schema,
            return_daft_schema,
            read_col_set,
            predicate_columns: row_filter_data,
            rg_tasks: Vec::new(),
            rg_global_starts: Vec::new(),
            batch_size: batch_size.unwrap_or(256 * 1024),
            predicate_pushed,
            delete_rows: delete_rows.map(|d| d.into()),
        });
    }

    // 7. Compute per-row-group local offset/limit.
    let has_predicate = predicate.is_some();
    let global_start = start_offset.unwrap_or(0);
    let global_limit = if has_predicate { None } else { num_rows };
    let global_end = global_start + global_limit.unwrap_or(usize::MAX - global_start);

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

    let batch_size = batch_size.unwrap_or_else(|| {
        rg_tasks
            .iter()
            .map(|t| parquet_metadata.row_group(t.rg_idx).num_rows() as usize)
            .max()
            .unwrap_or(256 * 1024)
    });

    let arrow_schema_arc = Arc::new(arrow_schema);
    let parquet_schema_arc = Arc::new(parquet_metadata.file_metadata().schema_descr().clone());
    let delete_rows_arc: Option<Arc<[i64]>> = delete_rows.map(|d| d.into());

    Ok(LocalParquetSetup {
        arrow_reader_metadata: Arc::new(arrow_reader_metadata),
        arrow_schema: arrow_schema_arc,
        parquet_schema: parquet_schema_arc,
        parquet_metadata,
        daft_schema_for_filter,
        read_daft_schema,
        return_daft_schema,
        read_col_set,
        predicate_columns: row_filter_data,
        rg_tasks,
        rg_global_starts,
        batch_size,
        predicate_pushed,
        delete_rows: delete_rows_arc,
    })
}

/// Decode a single row group from a local parquet file, returning a Daft RecordBatch.
///
/// Opens its own file handle, builds the arrow-rs reader with the given setup state,
/// decodes, converts to Daft, applies post-read predicate fallback if needed, and
/// strips predicate-only columns.
///
/// `decoder_limit`: passed to `with_limit()` when predicate is pushed down.
/// For single-RG reads this can be the user's num_rows; for multi-RG it should be None
/// (limit applied after concatenation).
pub(crate) fn decode_single_rg(
    path: &str,
    setup: &LocalParquetSetup,
    task: &RgTask,
    predicate: Option<&ExprRef>,
    decoder_limit: Option<usize>,
) -> DaftResult<RecordBatch> {
    let rg_rows = setup.parquet_metadata.row_group(task.rg_idx).num_rows() as usize;
    let file = std::fs::File::open(path)
        .map_err(|e| parquet_err(format!("Failed to open '{}': {}", path, e)))?;
    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        file,
        (*setup.arrow_reader_metadata).clone(),
    );
    if let Some(ref col_set) = setup.read_col_set {
        let mask = build_projection_mask(col_set, &setup.arrow_schema, builder.parquet_schema());
        builder = builder.with_projection(mask);
    }
    builder = builder
        .with_row_groups(vec![task.rg_idx])
        .with_batch_size(setup.batch_size);
    if !setup.predicate_pushed {
        builder = builder.with_offset(task.local_offset);
        if let Some(lim) = task.local_limit {
            builder = builder.with_limit(lim);
        }
    }
    builder = apply_rg_filter_and_deletes(builder, setup, task, predicate, rg_rows, decoder_limit)?;
    let reader = builder.build().map_err(parquet_err)?;
    let arrow_batches: Vec<arrow::array::RecordBatch> =
        reader.collect::<Result<Vec<_>, _>>().map_err(parquet_err)?;
    let daft_batches: Vec<RecordBatch> = arrow_batches
        .iter()
        .map(RecordBatch::try_from)
        .collect::<DaftResult<Vec<_>>>()?;
    let daft_refs: Vec<&RecordBatch> = daft_batches.iter().collect();
    let table =
        RecordBatch::concat_or_empty(&daft_refs, Some(Arc::new(setup.read_daft_schema.clone())))?;

    // Post-read finalize: predicate fallback + column strip.
    // No limit here — caller handles cross-RG limit.
    finalize_batch(
        table,
        predicate,
        setup.predicate_pushed,
        &setup.read_daft_schema,
        &setup.return_daft_schema,
    )
}

/// Read a local parquet file using the sync arrow-rs reader with parallel row group decode.
///
/// This avoids the overhead of `DaftAsyncFileReader` + `IOClient` for local files
/// by using `std::fs::File` directly with `ParquetRecordBatchReaderBuilder`.
/// Row groups are decoded in parallel using rayon. Supports late materialization via `RowFilter` and
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
    let setup = local_parquet_setup(
        path,
        columns,
        start_offset,
        num_rows,
        row_groups,
        predicate.as_ref(),
        schema_infer_options,
        batch_size,
        field_id_mapping,
        delete_rows,
    )?;

    if setup.rg_tasks.is_empty() {
        return Ok(RecordBatch::empty(Some(Arc::new(setup.return_daft_schema))));
    }

    // Zero-column read (e.g. metadata-only query): no decoding needed.
    if setup.return_daft_schema.is_empty() {
        let total: usize = setup
            .rg_tasks
            .iter()
            .map(|t| setup.parquet_metadata.row_group(t.rg_idx).num_rows() as usize)
            .sum();
        return Ok(row_count_batch(setup.return_daft_schema, total, num_rows));
    }

    // Compute column indices for parallel decode.
    let all_col_indices =
        compute_root_indices(&setup.arrow_schema, setup.read_col_set.as_ref(), None);

    // Column parallelism decision:
    // - For single-RG reads, column splitting is the only way to parallelize, so use it
    //   whenever we have >= 2 columns and the RG is large enough.
    // - For multi-RG reads, per-RG decode provides parallelism on its own. Only add column
    //   splitting when we have enough columns (>= 4) AND RGs don't already saturate cores.
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2);
    let has_large_rg = setup.rg_tasks.iter().any(|t| {
        setup.parquet_metadata.row_group(t.rg_idx).total_byte_size()
            >= MIN_RG_BYTES_FOR_COL_PARALLELISM
    });
    let use_col_parallelism = has_large_rg
        && all_col_indices.len() >= MIN_COLS_FOR_COL_PARALLELISM
        && (setup.rg_tasks.len() == 1 || setup.rg_tasks.len() < num_cpus * 2);

    // Fallback: use decode_single_rg per RG with rayon over RGs.
    if !use_col_parallelism {
        // Single-column: no benefit from column splitting.
        if setup.rg_tasks.len() == 1 {
            let mut table = decode_single_rg(
                path,
                &setup,
                &setup.rg_tasks[0],
                predicate.as_ref(),
                num_rows,
            )?;
            if predicate.is_some()
                && !setup.predicate_pushed
                && let Some(limit) = num_rows
            {
                table = table.head(limit)?;
            }
            return Ok(table);
        }

        let has_predicate = predicate.is_some();
        let rg_results: Vec<DaftResult<RecordBatch>> = setup
            .rg_tasks
            .par_iter()
            .map(|task| decode_single_rg(path, &setup, task, predicate.as_ref(), num_rows))
            .collect();
        let batches: Vec<RecordBatch> = rg_results.into_iter().collect::<DaftResult<Vec<_>>>()?;
        let mut table = RecordBatch::concat(batches.as_slice())?;
        if has_predicate && let Some(limit) = num_rows {
            table = table.head(limit)?;
        }
        return Ok(table);
    }

    let has_predicate = predicate.is_some();

    // Each column task opens its own file handle (independent seek position, ~microsecond
    // syscall). This avoids the cost of reading the entire file into memory upfront
    // (e.g., 728MB -> ~380ms), and the OS page cache serves subsequent reads from memory.

    if setup.predicate_pushed {
        // Two-phase decode: predicate columns first (serial per RG), then data columns parallel.
        let pred_cols = setup.predicate_columns.as_ref().unwrap();
        let pred_col_indices = compute_root_indices(&setup.arrow_schema, Some(pred_cols), None);
        let data_col_indices = compute_root_indices(
            &setup.arrow_schema,
            setup.read_col_set.as_ref(),
            Some(pred_cols),
        );

        // Phase 1: decode predicate columns per RG (parallel over RGs).
        let phase1_results: Vec<DaftResult<(arrow::array::RecordBatch, RowSelection)>> = setup
            .rg_tasks
            .par_iter()
            .map(|task| {
                decode_rg_predicate_phase(
                    path,
                    &setup,
                    task,
                    predicate.as_ref().unwrap(),
                    &pred_col_indices,
                )
            })
            .collect();
        let phase1: Vec<(arrow::array::RecordBatch, RowSelection)> =
            phase1_results.into_iter().collect::<DaftResult<Vec<_>>>()?;

        // Phase 2: decode data columns in parallel across (RG, col) pairs.
        if data_col_indices.is_empty() {
            // All read columns are predicate columns; just convert and finalize.
            let mut rg_batches = Vec::with_capacity(phase1.len());
            for (pred_batch, _) in &phase1 {
                let daft_batch = RecordBatch::try_from(pred_batch)?;
                let table = finalize_batch(
                    daft_batch,
                    None, // predicate already applied
                    true,
                    &setup.read_daft_schema,
                    &setup.return_daft_schema,
                )?;
                rg_batches.push(table);
            }
            let mut table = RecordBatch::concat(rg_batches.as_slice())?;
            if let Some(limit) = num_rows {
                table = table.head(limit)?;
            }
            return Ok(table);
        }

        // Build flat (task_idx, col_idx) pairs for data columns.
        let col_tasks: Vec<(usize, usize)> = (0..setup.rg_tasks.len())
            .flat_map(|ti| data_col_indices.iter().map(move |&ci| (ti, ci)))
            .collect();

        let col_results: Vec<DaftResult<(usize, arrow::array::RecordBatch)>> = col_tasks
            .par_iter()
            .map(|&(task_idx, col_idx)| {
                let (_, ref selection) = phase1[task_idx];
                let batch = decode_rg_column(
                    path,
                    &setup,
                    &setup.rg_tasks[task_idx],
                    col_idx,
                    Some(selection),
                )?;
                Ok((task_idx, batch))
            })
            .collect();
        let col_results: Vec<(usize, arrow::array::RecordBatch)> =
            col_results.into_iter().collect::<DaftResult<Vec<_>>>()?;

        // Group by task_idx, hconcat pred + data batches.
        let num_tasks = setup.rg_tasks.len();
        let mut grouped: Vec<Vec<arrow::array::RecordBatch>> = vec![Vec::new(); num_tasks];
        for (ti, batch) in col_results {
            grouped[ti].push(batch);
        }

        let mut rg_batches = Vec::with_capacity(num_tasks);
        for (ti, data_batches) in grouped.into_iter().enumerate() {
            let (ref pred_batch, _) = phase1[ti];
            let mut all_batches = vec![pred_batch.clone()];
            all_batches.extend(data_batches);
            let merged = hconcat_record_batches(&all_batches)?;
            let daft_batch = RecordBatch::try_from(&merged)?;
            let table = finalize_batch(
                daft_batch,
                None, // predicate already applied
                true,
                &setup.read_daft_schema,
                &setup.return_daft_schema,
            )?;
            rg_batches.push(table);
        }

        let mut table = RecordBatch::concat(rg_batches.as_slice())?;
        if let Some(limit) = num_rows {
            table = table.head(limit)?;
        }
        Ok(table)
    } else {
        // No predicate pushed: split all columns across (RG, col) pairs.
        let col_tasks: Vec<(usize, usize)> = (0..setup.rg_tasks.len())
            .flat_map(|ti| all_col_indices.iter().map(move |&ci| (ti, ci)))
            .collect();

        // Compute base selection per RG once.
        let base_selections: Vec<Option<RowSelection>> = setup
            .rg_tasks
            .iter()
            .map(|task| {
                let rg_rows = setup.parquet_metadata.row_group(task.rg_idx).num_rows() as usize;
                let mut sel = build_base_row_selection(&setup, task, rg_rows);
                if !setup.predicate_pushed && task.local_offset > 0 {
                    let rg_rows = setup.parquet_metadata.row_group(task.rg_idx).num_rows() as usize;
                    let offset_sel = build_offset_row_selection(task.local_offset, rg_rows);
                    sel = combine_selections(sel, Some(offset_sel));
                }
                sel
            })
            .collect();

        let col_results: Vec<DaftResult<(usize, arrow::array::RecordBatch)>> = col_tasks
            .par_iter()
            .map(|&(task_idx, col_idx)| {
                let batch = decode_rg_column(
                    path,
                    &setup,
                    &setup.rg_tasks[task_idx],
                    col_idx,
                    base_selections[task_idx].as_ref(),
                )?;
                Ok((task_idx, batch))
            })
            .collect();
        let col_results: Vec<(usize, arrow::array::RecordBatch)> =
            col_results.into_iter().collect::<DaftResult<Vec<_>>>()?;

        // Group by task_idx, hconcat.
        let num_tasks = setup.rg_tasks.len();
        let mut grouped: Vec<Vec<arrow::array::RecordBatch>> = vec![Vec::new(); num_tasks];
        for (ti, batch) in col_results {
            grouped[ti].push(batch);
        }

        let mut rg_batches = Vec::with_capacity(num_tasks);
        for data_batches in grouped {
            let merged = hconcat_record_batches(&data_batches)?;
            let daft_batch = RecordBatch::try_from(&merged)?;
            let table = finalize_batch(
                daft_batch,
                predicate.as_ref(),
                false,
                &setup.read_daft_schema,
                &setup.return_daft_schema,
            )?;
            rg_batches.push(table);
        }

        let mut table = RecordBatch::concat(rg_batches.as_slice())?;
        // Apply limit: for no-predicate, local_limit per RG handles most cases,
        // but with predicate (non-pushed), we need post-concat limit.
        if has_predicate && let Some(limit) = num_rows {
            table = table.head(limit)?;
        }
        // For non-predicate with limit, tasks already have local_limit, but
        // concat may overshoot by up to 1 RG. Apply final limit.
        if !has_predicate && let Some(limit) = num_rows {
            table = table.head(limit)?;
        }
        Ok(table)
    }
}

/// Stream a local parquet file as Daft [`RecordBatch`]es using the sync arrow-rs reader,
/// dispatching per-row-group decode as async tasks on the compute runtime.
///
/// Performs sync metadata read, then per-RG tasks on the DAFTCPU pool with
/// semaphore-gated parallelism.
#[allow(clippy::too_many_arguments)]
pub async fn local_parquet_stream_arrowrs(
    path: &str,
    columns: Option<&[&str]>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    batch_size: Option<usize>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    delete_rows: Option<&[i64]>,
    maintain_order: bool,
) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
    // 1. Sync setup: read metadata, infer schemas, prune RGs, compute tasks.
    let setup = Arc::new(local_parquet_setup(
        path,
        columns,
        None, // start_offset not supported in stream path
        num_rows,
        row_groups,
        predicate.as_ref(),
        schema_infer_options,
        batch_size,
        field_id_mapping,
        delete_rows,
    )?);

    if setup.rg_tasks.is_empty() {
        return Ok(futures::stream::empty().boxed());
    }

    // Zero-column read (e.g. metadata-only query): emit a single row-count-only batch.
    if setup.return_daft_schema.is_empty() {
        let total: usize = setup
            .rg_tasks
            .iter()
            .map(|t| setup.parquet_metadata.row_group(t.rg_idx).num_rows() as usize)
            .sum();
        let batch = row_count_batch(setup.return_daft_schema.clone(), total, num_rows);
        return Ok(futures::stream::once(async move { Ok(batch) }).boxed());
    }

    // 2. Semaphore: limit concurrent RG decodes.
    // All columns are decoded in a single block_in_place call per RG,
    // so concurrency is limited only by available CPUs.
    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2);
    let num_parallel_tasks = num_cpus.min(setup.rg_tasks.len());
    let semaphore = Arc::new(tokio::sync::Semaphore::new(num_parallel_tasks));

    let compute_runtime = get_compute_runtime();

    // 3. Per-RG mpsc channels.
    let num_tasks = setup.rg_tasks.len();
    let (output_senders, output_receivers): (Vec<_>, Vec<_>) = (0..num_tasks)
        .map(|_| tokio::sync::mpsc::channel(1))
        .unzip();

    // 4. Driver task on compute runtime: spawn per-RG decode tasks.
    let path_owned = path.to_string();
    let inner_runtime = compute_runtime.clone();
    let driver = compute_runtime.spawn(async move {
        let mut rg_handles = Vec::with_capacity(num_tasks);
        for (task_idx, sender) in (0..num_tasks).zip(output_senders) {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let setup = setup.clone();
            let path = path_owned.clone();
            let pred = predicate.clone();

            let handle = inner_runtime.spawn(async move {
                let result = tokio::task::block_in_place(|| {
                    decode_single_rg_col_parallel(&path, &setup, task_idx, pred.as_ref())
                });
                let _ = sender.send(result).await;
                drop(permit);
            });
            rg_handles.push(handle);
        }

        // Wait for all per-RG tasks to complete.
        futures::future::try_join_all(rg_handles).await?;
        DaftResult::Ok(())
    });

    // 5. Flatten receivers into a stream, combined with the driver future.
    let stream_of_streams =
        futures::stream::iter(output_receivers.into_iter().map(ReceiverStream::new));
    let driver = driver.map(|x| x?);
    let combined = if maintain_order {
        combine_stream(stream_of_streams.flatten(), driver).boxed()
    } else {
        combine_stream(stream_of_streams.flatten_unordered(None), driver).boxed()
    };

    Ok(combined)
}

/// Stream a single parquet file as Daft [`RecordBatch`]es using the arrow-rs reader.
///
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
    let io_client_saved = io_client.clone();
    let io_stats_saved = io_stats.clone();
    let mut reader = DaftAsyncFileReader::new(uri.to_string(), io_client, io_stats, metadata, None);
    let mut parquet_metadata = reader.get_metadata(None).await.map_err(parquet_err)?;

    // 1b. Apply field ID mapping (Iceberg schema evolution) if provided.
    if let Some(ref mapping) = field_id_mapping {
        parquet_metadata = apply_field_ids_to_arrowrs_parquet_metadata(parquet_metadata, mapping)?;
    }

    // 1c. For StringEncoding::Raw, strip STRING/UTF8 logical types so arrow-rs
    // reads BYTE_ARRAY as Binary (no UTF-8 validation).
    if schema_infer_options.string_encoding == StringEncoding::Raw {
        parquet_metadata = strip_string_types_from_parquet_metadata(parquet_metadata)?;
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

    // 4. Compute schemas.
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

    // 5. Row group pruning.
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

    // Zero-column read.
    if return_daft_schema.is_empty() {
        let total: usize = rg_indices
            .iter()
            .map(|&i| parquet_metadata.row_group(i).num_rows() as usize)
            .sum();
        let batch = row_count_batch(return_daft_schema, total, num_rows);
        return Ok(futures::stream::once(async move { Ok(batch) }).boxed());
    }

    // 6. Compute column indices and per-RG selections.
    let predicate_columns: Option<HashSet<String>> =
        row_filter_result.as_ref().map(|(_, cols)| cols.clone());
    let read_col_set_owned: Option<HashSet<String>> = read_col_set
        .as_ref()
        .map(|s| s.iter().map(|x| (*x).to_string()).collect());
    let arrow_schema_arc = Arc::new(arrow_schema.clone());
    let all_col_indices = compute_root_indices(&arrow_schema, read_col_set_owned.as_ref(), None);

    let use_offset_selection = predicate_pushed && start_offset.is_some_and(|o| o > 0);
    let mut rg_global_starts = Vec::with_capacity(parquet_metadata.num_row_groups());
    let mut cumulative = 0usize;
    for i in 0..parquet_metadata.num_row_groups() {
        rg_global_starts.push(cumulative);
        cumulative += parquet_metadata.row_group(i).num_rows() as usize;
    }

    let global_start = start_offset.unwrap_or(0);
    let per_rg_selections: Vec<Option<RowSelection>> = {
        let mut cumulative_rows = 0usize;
        rg_indices
            .iter()
            .map(|&rg_idx| {
                let rg_rows = parquet_metadata.row_group(rg_idx).num_rows() as usize;
                let rg_start_in_stream = cumulative_rows;
                cumulative_rows += rg_rows;

                let offset_sel = if use_offset_selection || (!predicate_pushed && global_start > 0)
                {
                    let local_offset = global_start.saturating_sub(rg_start_in_stream);
                    if local_offset > 0 {
                        Some(build_offset_row_selection(local_offset, rg_rows))
                    } else {
                        None
                    }
                } else {
                    None
                };

                let delete_sel = if let Some(deletes) = delete_rows
                    && !deletes.is_empty()
                {
                    Some(build_single_rg_delete_selection(
                        deletes,
                        rg_global_starts[rg_idx],
                        rg_rows,
                    ))
                } else {
                    None
                };

                combine_selections(offset_sel, delete_sel)
            })
            .collect()
    };

    // Fallback: few columns -> single-stream with prefetched I/O for cross-RG coalescing.
    if all_col_indices.len() < MIN_COLS_FOR_COL_PARALLELISM {
        let prefetch_container = prefetch_column_ranges(
            uri,
            &io_client_saved,
            &io_stats_saved,
            &parquet_metadata,
            &rg_indices,
            &all_col_indices,
        )
        .await?;

        let reader2 = PrefetchedAsyncFileReader::new(prefetch_container, parquet_metadata.clone());
        let options2 = ArrowReaderOptions::new().with_schema(arrow_schema_arc);
        let arm2 = ArrowReaderMetadata::try_new(parquet_metadata.clone(), options2)
            .map_err(parquet_err)?;
        let mut builder2 = ParquetRecordBatchStreamBuilder::new_with_metadata(reader2, arm2);
        if let Some(ref col_set) = read_col_set {
            let mask = build_projection_mask(col_set, &arrow_schema, builder2.parquet_schema());
            builder2 = builder2.with_projection(mask);
        }
        builder2 = builder2.with_row_groups(rg_indices.clone());
        if !use_offset_selection && let Some(offset) = start_offset {
            builder2 = builder2.with_offset(offset);
        }
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
            let delete_selection = if let Some(deletes) = delete_rows
                && !deletes.is_empty()
            {
                Some(build_delete_row_selection(
                    deletes,
                    &rg_indices,
                    &parquet_metadata,
                ))
            } else {
                None
            };
            if let Some(selection) = combine_selections(offset_selection, delete_selection) {
                builder2 = builder2.with_row_selection(selection);
            }
        }
        if let Some((row_filter, _)) = row_filter_result {
            builder2 = builder2.with_row_filter(row_filter);
        }
        if (predicate_pushed || predicate.is_none())
            && let Some(limit) = num_rows
        {
            builder2 = builder2.with_limit(limit);
        }
        builder2 = builder2.with_batch_size(batch_size.unwrap_or(DEFAULT_BATCH_SIZE));
        let stream = builder2.build().map_err(parquet_err)?;
        let mapped = stream.map(move |result| {
            let arrow_batch = result.map_err(parquet_err)?;
            let table = RecordBatch::try_from(&arrow_batch)?;
            finalize_batch(
                table,
                predicate.as_ref(),
                predicate_pushed,
                &read_daft_schema,
                &return_daft_schema,
            )
        });
        return Ok(mapped.boxed());
    }

    // 7. Per-RG column-parallel streaming with prefetched I/O.
    // Pre-fetch ALL column ranges upfront for cross-RG + cross-column coalescing.
    let prefetch_container = prefetch_column_ranges(
        uri,
        &io_client_saved,
        &io_stats_saved,
        &parquet_metadata,
        &rg_indices,
        &all_col_indices,
    )
    .await?;

    let num_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(num_cpus * 2));

    let rg_indices_owned = rg_indices;
    let per_rg_selections_owned = per_rg_selections;
    let limit = num_rows;

    let stream = futures::stream::iter(0..rg_indices_owned.len()).then(move |ri| {
        let rg_idx = rg_indices_owned[ri];
        let base_sel = per_rg_selections_owned[ri].clone();
        let container = prefetch_container.clone();
        let pm = parquet_metadata.clone();
        let as_arc = arrow_schema_arc.clone();
        let sem = semaphore.clone();
        let pred = predicate.clone();
        let pred_cols = predicate_columns.clone();
        let read_col_set_owned = read_col_set_owned.clone();
        let all_col_indices = all_col_indices.clone();
        let read_daft_schema = read_daft_schema.clone();
        let return_daft_schema = return_daft_schema.clone();
        let rg_rows = pm.row_group(rg_idx).num_rows() as usize;

        async move {
            if predicate_pushed {
                let pred_cols = pred_cols.as_ref().unwrap();
                let pred_col_indices = compute_root_indices(&as_arc, Some(pred_cols), None);
                let data_col_indices =
                    compute_root_indices(&as_arc, read_col_set_owned.as_ref(), Some(pred_cols));

                // Phase 1: predicate columns from prefetched cache.
                let (pred_batch, final_selection) = {
                    let _permit = sem.acquire().await.unwrap();
                    decode_rg_predicate_phase_async_prefetched(
                        &container,
                        &pm,
                        &as_arc,
                        pred.as_ref().unwrap(),
                        &pred_col_indices,
                        rg_idx,
                        base_sel,
                        pred_cols,
                        rg_rows,
                    )
                    .await?
                };

                if data_col_indices.is_empty() {
                    let daft_batch = RecordBatch::try_from(&pred_batch)?;
                    return finalize_batch(
                        daft_batch,
                        None,
                        true,
                        &read_daft_schema,
                        &return_daft_schema,
                    );
                }

                // Phase 2: data columns from prefetched cache.
                let col_futs: Vec<_> = data_col_indices
                    .iter()
                    .map(|&col_idx| {
                        let container = container.clone();
                        let pm = pm.clone();
                        let as_arc = as_arc.clone();
                        let sel = final_selection.clone();
                        let sem = sem.clone();
                        async move {
                            let _permit = sem.acquire().await.unwrap();
                            decode_rg_column_async_prefetched(
                                &container,
                                &pm,
                                &as_arc,
                                col_idx,
                                rg_idx,
                                Some(sel),
                                rg_rows,
                            )
                            .await
                        }
                    })
                    .collect();
                let col_batches: Vec<arrow::array::RecordBatch> =
                    futures::future::try_join_all(col_futs).await?;

                let mut all = vec![pred_batch];
                all.extend(col_batches);
                let merged = hconcat_record_batches(&all)?;
                let daft_batch = RecordBatch::try_from(&merged)?;
                finalize_batch(
                    daft_batch,
                    None,
                    true,
                    &read_daft_schema,
                    &return_daft_schema,
                )
            } else {
                // No predicate: decode all columns from prefetched cache.
                let col_futs: Vec<_> = all_col_indices
                    .iter()
                    .map(|&col_idx| {
                        let container = container.clone();
                        let pm = pm.clone();
                        let as_arc = as_arc.clone();
                        let sel = base_sel.clone();
                        let sem = sem.clone();
                        async move {
                            let _permit = sem.acquire().await.unwrap();
                            decode_rg_column_async_prefetched(
                                &container, &pm, &as_arc, col_idx, rg_idx, sel, rg_rows,
                            )
                            .await
                        }
                    })
                    .collect();
                let col_batches: Vec<arrow::array::RecordBatch> =
                    futures::future::try_join_all(col_futs).await?;

                let merged = hconcat_record_batches(&col_batches)?;
                let daft_batch = RecordBatch::try_from(&merged)?;
                finalize_batch(
                    daft_batch,
                    pred.as_ref(),
                    false,
                    &read_daft_schema,
                    &return_daft_schema,
                )
            }
        }
    });

    // Apply limit across emitted batches.
    if let Some(limit) = limit {
        let limited = stream.scan(0usize, move |emitted, result| {
            let remaining = limit.saturating_sub(*emitted);
            if remaining == 0 {
                return futures::future::ready(None);
            }
            match result {
                Ok(batch) => {
                    let batch_len = batch.len();
                    if batch_len <= remaining {
                        *emitted += batch_len;
                        futures::future::ready(Some(Ok(batch)))
                    } else {
                        *emitted += remaining;
                        futures::future::ready(Some(batch.head(remaining)))
                    }
                }
                Err(e) => futures::future::ready(Some(Err(e))),
            }
        });
        Ok(limited.boxed())
    } else {
        Ok(stream.boxed())
    }
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
        match row_group_metadata_to_table_stats(rg_meta, schema) {
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
        assert_eq!(&*result.schema.fields()[0].name, "x");
        assert_eq!(&*result.schema.fields()[1].name, "y");

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
        assert_eq!(&*result.schema.fields()[0].name, "x");
        assert_eq!(&*result.schema.fields()[1].name, "z");

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

    fn val_gt_3_predicate() -> ExprRef {
        use daft_dsl::{lit, resolved_col};
        resolved_col("val").gt(lit(3i32))
    }

    fn extract_val_column(batch: &RecordBatch) -> Vec<i32> {
        let idx = batch.schema.get_index("val").unwrap();
        let series = &batch.columns()[idx];
        let arr = series.i32().unwrap();
        (0..arr.len()).map(|i| arr.get(i).unwrap()).collect()
    }

    #[test]
    fn test_offset_with_predicate() {
        let dir = std::env::temp_dir().join("daft_test_offset_pred");
        std::fs::create_dir_all(&dir).unwrap();
        let uri = write_test_parquet(&dir, 10);

        let result = local_parquet_read_arrowrs(
            &uri,
            Some(&["val"]),
            Some(3),
            None,
            None,
            Some(val_gt_3_predicate()),
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
            None,
        )
        .unwrap();

        let vals = extract_val_column(&result);
        assert_eq!(vals, vec![4, 5, 6, 7, 8, 9]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_offset_limit_predicate() {
        let dir = std::env::temp_dir().join("daft_test_offset_limit_pred");
        std::fs::create_dir_all(&dir).unwrap();
        let uri = write_test_parquet(&dir, 10);

        let result = local_parquet_read_arrowrs(
            &uri,
            Some(&["val"]),
            Some(3),
            Some(2),
            None,
            Some(val_gt_3_predicate()),
            ParquetSchemaInferenceOptions::default(),
            None,
            None,
            None,
        )
        .unwrap();

        let vals = extract_val_column(&result);
        assert_eq!(vals, vec![4, 5]);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
