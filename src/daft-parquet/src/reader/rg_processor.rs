use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, RecordBatch as ArrowRecordBatch},
    datatypes::{Field as ArrowField, Schema as ArrowSchema},
};
use common_error::DaftResult;
use common_runtime::{RuntimeTask, get_compute_runtime};
use daft_core::prelude::*;
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr};
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use parquet::{arrow::arrow_reader::RowSelection, file::metadata::ParquetMetaData};
use tokio::sync::mpsc;

use super::{
    chunk_source::{ChunkSource, OffsetBytes},
    field_reader::{decode_one_streaming, leaves_for_top_fields},
    util::{parquet_err, project_to_schema, truncate_mask_to_n_trues},
};
use crate::helpers::substitute_missing_cols;

type ColRx = mpsc::Receiver<DaftResult<ArrayRef>>;
type ColHandle = RuntimeTask<DaftResult<()>>;

fn err_stream<T: Send + 'static>(e: common_error::DaftError) -> BoxStream<'static, DaftResult<T>> {
    futures::stream::once(async move { Err(e) }).boxed()
}

fn schema_from_indices(arrow_schema: &ArrowSchema, indices: &[usize]) -> Arc<ArrowSchema> {
    let fields: Vec<Arc<ArrowField>> = indices
        .iter()
        .map(|&i| Arc::new(arrow_schema.field(i).clone()))
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

fn spawn_col_decoders(
    col_indices: &[usize],
    rg_chunks: &Arc<std::collections::HashMap<usize, OffsetBytes>>,
    metadata: &Arc<ParquetMetaData>,
    arrow_schema: &Arc<ArrowSchema>,
    selection: Option<&RowSelection>,
    rg_idx: usize,
    chunk_size: usize,
) -> (Vec<ColRx>, Vec<ColHandle>) {
    let compute = get_compute_runtime();
    let mut rxs = Vec::with_capacity(col_indices.len());
    let mut handles = Vec::with_capacity(col_indices.len());
    for &col_idx in col_indices {
        let (tx, rx) = mpsc::channel::<DaftResult<ArrayRef>>(1);
        let chunks = rg_chunks.clone();
        let metadata = metadata.clone();
        let arrow_field = arrow_schema.field(col_idx).clone();
        let sel = selection.cloned();
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
        handles.push(h);
        rxs.push(rx);
    }
    (rxs, handles)
}

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
    let data_leaves: Arc<[usize]> =
        leaves_for_top_fields(&metadata, data_col_indices.as_ref()).into();
    // Final read for this RG → evict so the cached `Arc<RgBytesMap>` is dropped.
    let rg_chunks = match chunk_source.read_rg_chunks(rg_idx, data_leaves).await {
        Ok(c) => Arc::new(c),
        Err(e) => return err_stream(e),
    };
    // Pred mask length must equal pred-array length within an RG (both derived from
    // the same row count post-base-selection) — filter failure is an invariant break.
    let filtered_pred: Vec<ArrayRef> = (0..pred_col_indices.len())
        .map(|pp| {
            let arr = pred_arrays[pp][rg_pos].clone();
            match &pred_masks[rg_pos] {
                Some(mask) => arrow::compute::filter(&arr, mask)
                    .expect("pred mask length must equal pred array length within an RG"),
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
        // Empty stream (not 0-row schema-bearing) — streaming writers would otherwise
        // land an empty snapshot. The bulk path preserves schema via `read_parquet`.
        return futures::stream::empty().boxed();
    }
    let _ = return_daft_schema;

    let (col_receivers, _col_handles) = spawn_col_decoders(
        &data_col_indices,
        &rg_chunks,
        &metadata,
        &arrow_schema,
        selection.as_ref(),
        rg_idx,
        chunk_size,
    );

    let state = (
        col_receivers,
        _col_handles,
        0usize,
        total_rows_for_rg,
        filtered_pred,
    );

    futures::stream::unfold(state, move |mut state| {
        let arrow_schema = arrow_schema.clone();
        let read_col_indices = read_col_indices.clone();
        let pred_col_indices = pred_col_indices.clone();
        let data_col_indices = data_col_indices.clone();
        let return_daft_schema = return_daft_schema.clone();
        let predicate_for_fallback = predicate_for_fallback.clone();
        async move {
            let chunk_rows: usize;
            let data_chunks: Vec<ArrayRef> = if !data_col_indices.is_empty() {
                let mut got = Vec::with_capacity(state.0.len());
                for r in &mut state.0 {
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
                let remaining = state.3.saturating_sub(state.2);
                if remaining == 0 {
                    return None;
                }
                chunk_rows = remaining.min(chunk_size);
                Vec::new()
            };

            let mut fields = Vec::with_capacity(read_col_indices.len());
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(read_col_indices.len());
            for &col_idx in read_col_indices.iter() {
                fields.push(Arc::new(arrow_schema.field(col_idx).clone()));
                if let Some(pp) = pred_col_indices.iter().position(|&i| i == col_idx) {
                    arrays.push(state.4[pp].slice(state.2, chunk_rows));
                } else {
                    let dp = data_col_indices
                        .iter()
                        .position(|&i| i == col_idx)
                        .expect("col_idx must be in pred or data set");
                    arrays.push(data_chunks[dp].clone());
                }
            }

            let batch_res = ArrowRecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays)
                .map_err(parquet_err)
                .and_then(|b| RecordBatch::try_from(&b));
            let mut daft_batch = match batch_res {
                Ok(b) => b,
                Err(e) => return Some((Err(e), state)),
            };

            if let Some(ref pred) = predicate_for_fallback
                && !predicate_pushed
            {
                let eval = substitute_missing_cols(pred, &daft_batch.schema)
                    .and_then(|p| BoundExpr::try_new(p, &daft_batch.schema))
                    .and_then(|bound| daft_batch.eval_expression(&bound))
                    .and_then(|mask| daft_batch.mask_filter(&mask));
                daft_batch = match eval {
                    Ok(b) => b,
                    Err(e) => return Some((Err(e), state)),
                };
            }

            let projected = match project_to_schema(daft_batch, &return_daft_schema) {
                Ok(b) => b,
                Err(e) => return Some((Err(e), state)),
            };

            state.2 += chunk_rows;
            Some((Ok(projected), state))
        }
    })
    .boxed()
}

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

    let pred_leaves: Arc<[usize]> =
        leaves_for_top_fields(&metadata, pred_col_indices.as_ref()).into();
    // Sole phase-2 path for this RG — safe to evict the cached `Arc<RgBytesMap>`.
    let rg_chunks = match chunk_source.read_rg_chunks(rg_idx, pred_leaves).await {
        Ok(c) => Arc::new(c),
        Err(e) => return err_stream(e),
    };

    let (col_receivers, _col_handles) = spawn_col_decoders(
        &pred_col_indices,
        &rg_chunks,
        &metadata,
        &arrow_schema,
        selection.as_ref(),
        rg_idx,
        chunk_size,
    );

    // Build schemas + bind predicate once per RG (was ~50µs/chunk overhead).
    let chunk_arrow_schema = schema_from_indices(&arrow_schema, &pred_col_indices);
    let chunk_daft_schema = match Schema::try_from(chunk_arrow_schema.as_ref()) {
        Ok(s) => Arc::new(s),
        Err(e) => return err_stream(e),
    };
    let bound_pred = match substitute_missing_cols(&predicate, &chunk_daft_schema)
        .and_then(|p| BoundExpr::try_new(p, &chunk_daft_schema))
    {
        Ok(b) => Arc::new(b),
        Err(e) => return err_stream(e),
    };
    let out_arrow_schema = schema_from_indices(&arrow_schema, &read_col_indices);
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

    let state = (col_receivers, _col_handles);

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

                let mut chunks = Vec::with_capacity(state.0.len());
                for r in &mut state.0 {
                    match r.recv().await {
                        Some(Ok(arr)) => chunks.push(arr),
                        Some(Err(e)) => return Some((Err(e), state)),
                        None => return None,
                    }
                }
                if chunks.is_empty() {
                    return None;
                }

                let batch_res =
                    ArrowRecordBatch::try_new(chunk_arrow_schema.clone(), chunks.clone())
                        .map_err(parquet_err)
                        .and_then(|b| RecordBatch::try_from(&b));
                let daft_batch = match batch_res {
                    Ok(b) => b,
                    Err(e) => return Some((Err(e), state)),
                };

                let bool_res = daft_batch.eval_expression(&bound_pred).and_then(|m| {
                    let b = m.bool()?;
                    Ok(b.as_arrow()?.clone())
                });
                let mut arrow_bool: BooleanArray = match bool_res {
                    Ok(b) => b,
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
                    match arrow::compute::filter(arr, &arrow_bool) {
                        Ok(f) => filtered.push(f),
                        Err(e) => return Some((Err(parquet_err(e)), state)),
                    }
                }

                let arrays: Vec<ArrayRef> = out_to_pred_slot
                    .iter()
                    .map(|&pp| filtered[pp].clone())
                    .collect();
                let out_res = ArrowRecordBatch::try_new(out_arrow_schema.clone(), arrays)
                    .map_err(parquet_err)
                    .and_then(|b| RecordBatch::try_from(&b))
                    .and_then(|b| project_to_schema(b, &return_daft_schema));
                return Some(match out_res {
                    Ok(p) => (Ok(p), state),
                    Err(e) => (Err(e), state),
                });
            }
        }
    })
    .boxed()
}
