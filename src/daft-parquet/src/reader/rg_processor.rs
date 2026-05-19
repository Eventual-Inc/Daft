use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, RecordBatch as ArrowRecordBatch},
    datatypes::Schema as ArrowSchema,
};
use common_error::DaftResult;
use common_runtime::{JoinSet, get_compute_runtime};
use daft_core::prelude::*;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use parquet::{arrow::arrow_reader::RowSelection, file::metadata::ParquetMetaData};
use snafu::ResultExt;
use tokio::sync::mpsc;

use super::{
    RgTaskCtx,
    chunk_source::OffsetBytes,
    field_reader::{decode_one_streaming, leaves_for_top_fields},
    util::{project_to_schema, schema_from_indices},
};
use crate::{ArrowSnafu, helpers::substitute_missing_cols};

type ColRx = mpsc::Receiver<DaftResult<ArrayRef>>;

fn err_stream<T: Send + 'static>(e: common_error::DaftError) -> BoxStream<'static, DaftResult<T>> {
    futures::stream::once(async move { Err(e) }).boxed()
}

/// Recv one chunk from each column receiver in lockstep.
///
/// - `Some(Ok(v))` — every receiver yielded; `v` is the per-col array slice for this chunk.
/// - `Some(Err(e))` — a receiver propagated a decode error.
/// - `None` — at least one receiver closed (stream end). Decoders close their
///   channel after the final chunk, so the first `None` we see ends the RG.
pub(super) async fn recv_one_chunk(receivers: &mut [ColRx]) -> Option<DaftResult<Vec<ArrayRef>>> {
    let mut out = Vec::with_capacity(receivers.len());
    for r in receivers {
        match r.recv().await {
            Some(Ok(a)) => out.push(a),
            Some(Err(e)) => return Some(Err(e)),
            None => return None,
        }
    }
    Some(Ok(out))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_col_decoders(
    col_indices: &[usize],
    rg_chunks: &Arc<std::collections::HashMap<usize, OffsetBytes>>,
    metadata: &Arc<ParquetMetaData>,
    arrow_schema: &Arc<ArrowSchema>,
    selection: Option<&RowSelection>,
    rg_idx: usize,
    chunk_size: usize,
    path: &Arc<str>,
) -> (Vec<ColRx>, JoinSet<DaftResult<()>>) {
    let compute = get_compute_runtime();
    let mut rxs = Vec::with_capacity(col_indices.len());
    let mut joinset: JoinSet<DaftResult<()>> = JoinSet::new();
    for &col_idx in col_indices {
        let (tx, rx) = mpsc::channel::<DaftResult<ArrayRef>>(1);
        let chunks = rg_chunks.clone();
        let metadata = metadata.clone();
        let arrow_field = arrow_schema.field(col_idx).clone();
        let sel = selection.cloned();
        let path = path.clone();
        joinset.spawn_on(
            async move {
                decode_one_streaming(
                    chunks,
                    metadata,
                    rg_idx,
                    col_idx,
                    arrow_field,
                    sel,
                    chunk_size,
                    path,
                    tx,
                )
                .await;
                DaftResult::Ok(())
            },
            &compute,
        );
        rxs.push(rx);
    }
    (rxs, joinset)
}

struct StreamingState {
    ctx: Arc<RgTaskCtx>,
    col_receivers: Vec<ColRx>,
    offset: usize,
    /// Phase-1 predicate arrays for this RG, already mask-filtered.
    /// Indexed by `ctx.plan.pred_col_indices` position.
    filtered_pred: Vec<ArrayRef>,
}

pub(super) async fn process_rg_streaming(
    ctx: Arc<RgTaskCtx>,
    rg_idx: usize,
    selection: Option<RowSelection>,
    filtered_pred: Vec<ArrayRef>,
) -> BoxStream<'static, DaftResult<RecordBatch>> {
    // Default mode invariant: data_col_indices is non-empty. PredOnly handles
    // the empty case via `process_rg_predicate_only`.
    debug_assert!(!ctx.plan.data_col_indices.is_empty());

    let data_leaves: Arc<[usize]> =
        leaves_for_top_fields(&ctx.metadata, &ctx.plan.data_col_indices).into();
    let rg_chunks = match ctx.chunk_source.read_rg_chunks(rg_idx, data_leaves).await {
        Ok(c) => Arc::new(c),
        Err(e) => return err_stream(e.into()),
    };

    let (col_receivers, mut col_decoders) = spawn_col_decoders(
        &ctx.plan.data_col_indices,
        &rg_chunks,
        &ctx.metadata,
        &ctx.arrow_schema,
        selection.as_ref(),
        rg_idx,
        ctx.chunk_size,
        &ctx.path,
    );

    let state = StreamingState {
        ctx,
        col_receivers,
        offset: 0,
        filtered_pred,
    };

    let stream = futures::stream::unfold(state, |mut state| async move {
        let data_chunks = match recv_one_chunk(&mut state.col_receivers).await {
            Some(Ok(v)) => v,
            Some(Err(e)) => return Some((Err(e), state)),
            None => return None,
        };
        let chunk_rows = data_chunks[0].len();
        let ctx = &state.ctx;
        let plan = &ctx.plan;

        let mut fields = Vec::with_capacity(plan.read_col_indices.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(plan.read_col_indices.len());
        for &col_idx in &plan.read_col_indices {
            fields.push(Arc::new(ctx.arrow_schema.field(col_idx).clone()));
            if let Some(pp) = plan.pred_col_indices.iter().position(|&i| i == col_idx) {
                arrays.push(state.filtered_pred[pp].slice(state.offset, chunk_rows));
            } else {
                let dp = plan
                    .data_col_indices
                    .iter()
                    .position(|&i| i == col_idx)
                    .expect("col_idx must be in pred or data set");
                arrays.push(data_chunks[dp].clone());
            }
        }

        let batch_res = ArrowRecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays)
            .with_context(|_| ArrowSnafu {
                path: ctx.path.to_string(),
            })
            .map_err(common_error::DaftError::from)
            .and_then(|b| RecordBatch::try_from(&b));
        let mut daft_batch = match batch_res {
            Ok(b) => b,
            Err(e) => return Some((Err(e), state)),
        };

        if let Some(pred) = ctx.predicate.as_ref()
            && !plan.predicate_pushed
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

        let projected = match project_to_schema(daft_batch, &plan.return_daft_schema) {
            Ok(b) => b,
            Err(e) => return Some((Err(e), state)),
        };

        state.offset += chunk_rows;
        Some((Ok(projected), state))
    })
    .boxed();

    common_runtime::combine_stream(stream, async move { col_decoders.join_all().await }).boxed()
}

struct PredicateOnlyState {
    ctx: Arc<RgTaskCtx>,
    chunk_arrow_schema: Arc<ArrowSchema>,
    bound_pred: BoundExpr,
    col_receivers: Vec<ColRx>,
}

pub(super) async fn process_rg_predicate_only(
    ctx: Arc<RgTaskCtx>,
    rg_idx: usize,
    selection: Option<RowSelection>,
) -> BoxStream<'static, DaftResult<RecordBatch>> {
    let plan = &ctx.plan;
    let predicate = ctx
        .predicate
        .clone()
        .expect("predicate-only path requires a predicate");

    let pred_leaves: Arc<[usize]> =
        leaves_for_top_fields(&ctx.metadata, &plan.pred_col_indices).into();
    let rg_chunks = match ctx.chunk_source.read_rg_chunks(rg_idx, pred_leaves).await {
        Ok(c) => Arc::new(c),
        Err(e) => return err_stream(e.into()),
    };

    let (col_receivers, mut col_decoders) = spawn_col_decoders(
        &plan.pred_col_indices,
        &rg_chunks,
        &ctx.metadata,
        &ctx.arrow_schema,
        selection.as_ref(),
        rg_idx,
        ctx.chunk_size,
        &ctx.path,
    );

    // Build schemas + bind predicate once per RG (was ~50µs/chunk overhead).
    let chunk_arrow_schema = schema_from_indices(&ctx.arrow_schema, &plan.pred_col_indices);
    let chunk_daft_schema = match Schema::try_from(chunk_arrow_schema.as_ref()) {
        Ok(s) => Arc::new(s),
        Err(e) => return err_stream(e),
    };
    let bound_pred = match substitute_missing_cols(&predicate, &chunk_daft_schema)
        .and_then(|p| BoundExpr::try_new(p, &chunk_daft_schema))
    {
        Ok(b) => b,
        Err(e) => return err_stream(e),
    };
    let state = PredicateOnlyState {
        ctx,
        chunk_arrow_schema,
        bound_pred,
        col_receivers,
    };

    let stream = futures::stream::unfold(state, |mut state| async move {
        loop {
            let chunks = match recv_one_chunk(&mut state.col_receivers).await {
                Some(Ok(v)) => v,
                Some(Err(e)) => return Some((Err(e), state)),
                None => return None,
            };

            let batch_res =
                ArrowRecordBatch::try_new(state.chunk_arrow_schema.clone(), chunks.clone())
                    .with_context(|_| ArrowSnafu {
                        path: state.ctx.path.to_string(),
                    })
                    .map_err(common_error::DaftError::from)
                    .and_then(|b| RecordBatch::try_from(&b));
            let daft_batch = match batch_res {
                Ok(b) => b,
                Err(e) => return Some((Err(e), state)),
            };

            let bool_res = daft_batch.eval_expression(&state.bound_pred).and_then(|m| {
                let b = m.bool()?;
                Ok(b.as_arrow()?.clone())
            });
            let arrow_bool: BooleanArray = match bool_res {
                Ok(b) => b,
                Err(e) => return Some((Err(e), state)),
            };

            if arrow_bool.true_count() == 0 {
                continue;
            }

            let mut filtered = Vec::with_capacity(chunks.len());
            for arr in &chunks {
                match arrow::compute::filter(arr, &arrow_bool).with_context(|_| ArrowSnafu {
                    path: state.ctx.path.to_string(),
                }) {
                    Ok(f) => filtered.push(f),
                    Err(e) => return Some((Err(e.into()), state)),
                }
            }

            // Build a RecordBatch over the predicate columns; project_to_schema
            // picks the subset for `return_daft_schema` (which is read_col_indices,
            // a subset of pred_col_indices in this path).
            let out_res = ArrowRecordBatch::try_new(state.chunk_arrow_schema.clone(), filtered)
                .with_context(|_| ArrowSnafu {
                    path: state.ctx.path.to_string(),
                })
                .map_err(common_error::DaftError::from)
                .and_then(|b| RecordBatch::try_from(&b))
                .and_then(|b| project_to_schema(b, &state.ctx.plan.return_daft_schema));
            return Some(match out_res {
                Ok(p) => (Ok(p), state),
                Err(e) => (Err(e), state),
            });
        }
    })
    .boxed();

    common_runtime::combine_stream(stream, async move { col_decoders.join_all().await }).boxed()
}
