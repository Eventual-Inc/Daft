use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, RecordBatch as ArrowRecordBatch},
    datatypes::{Field as ArrowField, Schema as ArrowSchema},
};
use common_error::DaftResult;
use common_runtime::get_compute_runtime;
use daft_core::prelude::*;
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr};
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use parquet::{arrow::arrow_reader::RowSelection, file::metadata::ParquetMetaData};

use super::{
    chunk_source::ChunkSource,
    field_reader::{decode_one_streaming, leaves_for_top_fields},
    util::{parquet_err, project_to_schema, truncate_mask_to_n_trues},
};
use crate::helpers::substitute_missing_cols;

/// Standard per-RG path: pre-filter the phase-1 predicate arrays once, then
/// spawn one streaming decoder per data column and zip chunks into batches.
#[allow(clippy::too_many_arguments)]
pub(super) async fn process_rg_streaming(
    rg_pos: usize,
    rg_idx: usize,
    chunk_source: Arc<ChunkSource>,
    metadata: Arc<ParquetMetaData>,
    arrow_schema: Arc<ArrowSchema>,
    read_col_indices: Arc<[usize]>,
    pred_col_indices: Arc<[usize]>,
    data_col_indices: Arc<[usize]>,
    selection: Option<RowSelection>,
    pred_arrays: Arc<Vec<Vec<ArrayRef>>>,
    pred_masks: Arc<Vec<Option<BooleanArray>>>,
    chunk_size: usize,
    return_daft_schema: Arc<Schema>,
    predicate_for_fallback: Option<ExprRef>,
    predicate_pushed: bool,
) -> BoxStream<'static, DaftResult<RecordBatch>> {
    // Await this RG's pre-spawned coalesced fetch. Later RGs continue in the
    // background — overlaps remote I/O with decode.
    let data_leaves: Arc<[usize]> =
        leaves_for_top_fields(&metadata, data_col_indices.as_ref()).into();
    let rg_chunks = match chunk_source.read_rg_chunks(rg_idx, data_leaves).await {
        Ok(c) => Arc::new(c),
        Err(e) => return futures::stream::once(async move { Err(e) }).boxed(),
    };
    // Pre-filter pred arrays once; sliced per-chunk for zero-copy assembly.
    let filtered_pred: Vec<ArrayRef> = pred_col_indices
        .iter()
        .enumerate()
        .map(|(pp, _)| {
            let arr = pred_arrays[pp][rg_pos].clone();
            match &pred_masks[rg_pos] {
                Some(mask) => arrow::compute::filter(&arr, mask)
                    .map_err(parquet_err)
                    .unwrap_or_else(|_| arr.clone()),
                None => arr,
            }
        })
        .collect();

    let total_rows_for_rg: usize = if !pred_col_indices.is_empty() && !filtered_pred.is_empty() {
        filtered_pred[0].len()
    } else if let Some(sel) = &selection {
        sel.row_count()
    } else {
        metadata.row_group(rg_idx).num_rows() as usize
    };

    if total_rows_for_rg == 0 {
        // Empty RG → empty stream. Emitting a 0-row schema-bearing batch
        // would make streaming writers (e.g. iceberg) land an empty snapshot.
        // The bulk path preserves schema via `read_parquet`.
        return futures::stream::empty().boxed();
    }
    let _ = return_daft_schema;

    let compute = get_compute_runtime();
    let mut col_receivers: Vec<tokio::sync::mpsc::Receiver<DaftResult<ArrayRef>>> =
        Vec::with_capacity(data_col_indices.len());
    let mut col_handles: Vec<_> = Vec::with_capacity(data_col_indices.len());
    for &col_idx in data_col_indices.iter() {
        let (tx, rx) = tokio::sync::mpsc::channel::<DaftResult<ArrayRef>>(1);
        let chunks = rg_chunks.clone();
        let metadata = metadata.clone();
        let arrow_field = arrow_schema.field(col_idx).clone();
        let sel = selection.clone();
        let h = compute.spawn(async move {
            decode_one_streaming(
                chunks,
                metadata,
                rg_idx,
                col_idx,
                arrow_field,
                sel,
                chunk_size,
                tx,
            )
            .await;
            DaftResult::Ok(())
        });
        col_handles.push(h);
        col_receivers.push(rx);
    }

    struct ZipState {
        col_receivers: Vec<tokio::sync::mpsc::Receiver<DaftResult<ArrayRef>>>,
        _col_handles: Vec<common_runtime::RuntimeTask<DaftResult<()>>>,
        rows_so_far: usize,
        total_rows_for_rg: usize,
        filtered_pred: Vec<ArrayRef>,
    }

    let state = ZipState {
        col_receivers,
        _col_handles: col_handles,
        rows_so_far: 0,
        total_rows_for_rg,
        filtered_pred,
    };

    futures::stream::unfold(state, move |mut state| {
        let arrow_schema = arrow_schema.clone();
        let read_col_indices = read_col_indices.clone();
        let pred_col_indices = pred_col_indices.clone();
        let data_col_indices = data_col_indices.clone();
        let return_daft_schema = return_daft_schema.clone();
        let predicate_for_fallback = predicate_for_fallback.clone();
        async move {
            // With data cols: recv one chunk per col. Without: emit pred slices directly.
            let chunk_rows: usize;
            let data_chunks: Vec<ArrayRef> = if !data_col_indices.is_empty() {
                let mut got = Vec::with_capacity(state.col_receivers.len());
                for r in &mut state.col_receivers {
                    match r.recv().await {
                        Some(Ok(a)) => got.push(a),
                        Some(Err(e)) => return Some((Err(e), state)),
                        None => return None,
                    }
                }
                if got.is_empty() {
                    return None;
                }
                chunk_rows = got[0].len();
                got
            } else {
                let remaining = state.total_rows_for_rg.saturating_sub(state.rows_so_far);
                if remaining == 0 {
                    return None;
                }
                chunk_rows = remaining.min(chunk_size);
                Vec::new()
            };

            // Assemble output cols from pred slices and/or data chunks.
            let mut fields = Vec::with_capacity(read_col_indices.len());
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(read_col_indices.len());
            for &col_idx in read_col_indices.iter() {
                fields.push(Arc::new(arrow_schema.field(col_idx).clone()));
                if let Some(pp) = pred_col_indices.iter().position(|&i| i == col_idx) {
                    let sliced = state.filtered_pred[pp].slice(state.rows_so_far, chunk_rows);
                    arrays.push(sliced);
                } else {
                    let dp = data_col_indices
                        .iter()
                        .position(|&i| i == col_idx)
                        .expect("col_idx must be in pred or data set");
                    arrays.push(data_chunks[dp].clone());
                }
            }

            let schema_a = Arc::new(ArrowSchema::new(fields));
            let arrow_batch = match ArrowRecordBatch::try_new(schema_a, arrays) {
                Ok(b) => b,
                Err(e) => return Some((Err(parquet_err(e)), state)),
            };
            let mut daft_batch = match RecordBatch::try_from(&arrow_batch) {
                Ok(b) => b,
                Err(e) => return Some((Err(e), state)),
            };

            // Predicate fallback when not pushed: eval per chunk.
            if let Some(ref pred) = predicate_for_fallback
                && !predicate_pushed
            {
                let pred = match substitute_missing_cols(pred, &daft_batch.schema) {
                    Ok(p) => p,
                    Err(e) => return Some((Err(e), state)),
                };
                let bound = match BoundExpr::try_new(pred, &daft_batch.schema) {
                    Ok(b) => b,
                    Err(e) => return Some((Err(e), state)),
                };
                let mask = match daft_batch.eval_expression(&bound) {
                    Ok(m) => m,
                    Err(e) => return Some((Err(e), state)),
                };
                daft_batch = match daft_batch.mask_filter(&mask) {
                    Ok(b) => b,
                    Err(e) => return Some((Err(e), state)),
                };
            }

            let projected = match project_to_schema(daft_batch, &return_daft_schema) {
                Ok(b) => b,
                Err(e) => return Some((Err(e), state)),
            };

            state.rows_so_far += chunk_rows;
            Some((Ok(projected), state))
        }
    })
    .boxed()
}

/// Chunked-pred path: used when the projection is fully covered by predicate
/// columns. Streams pred cols chunk-by-chunk, evaluates per chunk, filters
/// in-place. Honors a shared remaining-rows counter for cross-RG limit
/// accounting.
#[allow(clippy::too_many_arguments)]
pub(super) async fn process_rg_chunked_pred(
    rg_idx: usize,
    chunk_source: Arc<ChunkSource>,
    metadata: Arc<ParquetMetaData>,
    arrow_schema: Arc<ArrowSchema>,
    pred_col_indices: Arc<[usize]>,
    read_col_indices: Arc<[usize]>,
    selection: Option<RowSelection>,
    predicate: ExprRef,
    chunk_size: usize,
    remaining_atomic: Arc<std::sync::atomic::AtomicUsize>,
    return_daft_schema: Arc<Schema>,
) -> BoxStream<'static, DaftResult<RecordBatch>> {
    use std::sync::atomic::Ordering;
    let compute = get_compute_runtime();

    let pred_leaves: Arc<[usize]> =
        leaves_for_top_fields(&metadata, pred_col_indices.as_ref()).into();
    let rg_chunks = match chunk_source.read_rg_chunks(rg_idx, pred_leaves).await {
        Ok(c) => Arc::new(c),
        Err(e) => return futures::stream::once(async move { Err(e) }).boxed(),
    };

    let mut col_receivers: Vec<tokio::sync::mpsc::Receiver<DaftResult<ArrayRef>>> =
        Vec::with_capacity(pred_col_indices.len());
    let mut col_handles: Vec<_> = Vec::with_capacity(pred_col_indices.len());
    for &col_idx in pred_col_indices.iter() {
        let (tx, rx) = tokio::sync::mpsc::channel::<DaftResult<ArrayRef>>(1);
        let chunks = rg_chunks.clone();
        let metadata = metadata.clone();
        let arrow_field = arrow_schema.field(col_idx).clone();
        let sel = selection.clone();
        let h = compute.spawn(async move {
            decode_one_streaming(
                chunks,
                metadata,
                rg_idx,
                col_idx,
                arrow_field,
                sel,
                chunk_size,
                tx,
            )
            .await;
            DaftResult::Ok(())
        });
        col_handles.push(h);
        col_receivers.push(rx);
    }

    // Build schemas + bind predicate once per RG (was ~50µs/chunk overhead).
    let chunk_arrow_schema = {
        let fields: Vec<Arc<ArrowField>> = pred_col_indices
            .iter()
            .map(|&i| Arc::new(arrow_schema.field(i).clone()))
            .collect();
        Arc::new(ArrowSchema::new(fields))
    };
    let chunk_daft_schema = match Schema::try_from(chunk_arrow_schema.as_ref()) {
        Ok(s) => Arc::new(s),
        Err(e) => return futures::stream::once(async move { Err(e) }).boxed(),
    };
    let bound_pred = match substitute_missing_cols(&predicate, &chunk_daft_schema) {
        Ok(p) => match BoundExpr::try_new(p, &chunk_daft_schema) {
            Ok(b) => Arc::new(b),
            Err(e) => return futures::stream::once(async move { Err(e) }).boxed(),
        },
        Err(e) => return futures::stream::once(async move { Err(e) }).boxed(),
    };
    let out_arrow_schema = {
        let fields: Vec<Arc<ArrowField>> = read_col_indices
            .iter()
            .map(|&i| Arc::new(arrow_schema.field(i).clone()))
            .collect();
        Arc::new(ArrowSchema::new(fields))
    };
    // read_col → pred slot mapping for output assembly.
    let out_to_pred_slot: Arc<[usize]> = read_col_indices
        .iter()
        .map(|&col_idx| {
            pred_col_indices
                .iter()
                .position(|&i| i == col_idx)
                .expect("read col must be in pred set for chunked-pred path")
        })
        .collect::<Vec<_>>()
        .into();

    struct State {
        col_receivers: Vec<tokio::sync::mpsc::Receiver<DaftResult<ArrayRef>>>,
        _col_handles: Vec<common_runtime::RuntimeTask<DaftResult<()>>>,
    }
    let state = State {
        col_receivers,
        _col_handles: col_handles,
    };

    futures::stream::unfold(state, move |mut state| {
        let chunk_arrow_schema = chunk_arrow_schema.clone();
        let bound_pred = bound_pred.clone();
        let out_arrow_schema = out_arrow_schema.clone();
        let out_to_pred_slot = out_to_pred_slot.clone();
        let remaining_atomic = remaining_atomic.clone();
        let return_daft_schema = return_daft_schema.clone();
        async move {
            loop {
                let remaining = remaining_atomic.load(Ordering::Acquire);
                if remaining == 0 {
                    return None;
                }

                let mut chunks = Vec::with_capacity(state.col_receivers.len());
                for r in &mut state.col_receivers {
                    match r.recv().await {
                        Some(Ok(arr)) => chunks.push(arr),
                        Some(Err(e)) => return Some((Err(e), state)),
                        None => return None,
                    }
                }
                if chunks.is_empty() {
                    return None;
                }

                let arrow_batch =
                    match ArrowRecordBatch::try_new(chunk_arrow_schema.clone(), chunks.clone()) {
                        Ok(b) => b,
                        Err(e) => return Some((Err(parquet_err(e)), state)),
                    };
                let daft_batch = match RecordBatch::try_from(&arrow_batch) {
                    Ok(b) => b,
                    Err(e) => return Some((Err(e), state)),
                };

                let mask_series = match daft_batch.eval_expression(&bound_pred) {
                    Ok(m) => m,
                    Err(e) => return Some((Err(e), state)),
                };
                let bool_arr = match mask_series.bool() {
                    Ok(b) => b,
                    Err(e) => return Some((Err(e), state)),
                };
                let mut arrow_bool: BooleanArray = match bool_arr.as_arrow() {
                    Ok(b) => b.clone(),
                    Err(e) => return Some((Err(e), state)),
                };

                let true_count = arrow_bool.true_count();
                let take = if true_count > remaining {
                    arrow_bool = truncate_mask_to_n_trues(&arrow_bool, remaining);
                    remaining
                } else {
                    true_count
                };

                remaining_atomic.fetch_sub(take, Ordering::Release);

                if take == 0 {
                    continue;
                }

                let mut filtered = Vec::with_capacity(chunks.len());
                for arr in &chunks {
                    let f = match arrow::compute::filter(arr, &arrow_bool) {
                        Ok(f) => f,
                        Err(e) => return Some((Err(parquet_err(e)), state)),
                    };
                    filtered.push(f);
                }

                let arrays: Vec<ArrayRef> = out_to_pred_slot
                    .iter()
                    .map(|&pp| filtered[pp].clone())
                    .collect();
                let out_batch = match ArrowRecordBatch::try_new(out_arrow_schema.clone(), arrays) {
                    Ok(b) => b,
                    Err(e) => return Some((Err(parquet_err(e)), state)),
                };
                let daft_out = match RecordBatch::try_from(&out_batch) {
                    Ok(b) => b,
                    Err(e) => return Some((Err(e), state)),
                };
                let projected = match project_to_schema(daft_out, &return_daft_schema) {
                    Ok(b) => b,
                    Err(e) => return Some((Err(e), state)),
                };
                return Some((Ok(projected), state));
            }
        }
    })
    .boxed()
}
