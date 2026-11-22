use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef},
    datatypes::Schema as ArrowSchema,
};
use common_error::{DaftError, DaftResult};
use common_runtime::{JoinSet, get_compute_runtime};
use daft_core::prelude::*;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, future::try_join_all, stream::BoxStream};
use parquet::{arrow::arrow_reader::RowSelection, file::metadata::ParquetMetaData};
use tokio::sync::mpsc;

use super::{
    RgTaskCtx,
    chunk_source::ChunkSource,
    field_reader::{decode_one_streaming, leaves_for_top_fields},
    util::{
        eval_predicate_mask, filter_arrays_by_mask, project_to_schema, record_batch_from_arrow,
        schema_from_indices,
    },
};
use crate::helpers::substitute_missing_cols;

type ColRx = mpsc::Receiver<DaftResult<ArrayRef>>;

fn err_stream<T: Send + 'static>(e: common_error::DaftError) -> BoxStream<'static, DaftResult<T>> {
    futures::stream::once(async move { Err(e) }).boxed()
}

/// Recv one chunk from each column receiver in lockstep.
///
/// - `Ok(Some(v))` — every receiver yielded; `v` is the per-col array slice for this chunk.
/// - `Err(e)` — a receiver propagated a decode error, or the receivers went out
///   of lockstep (some closed while others yielded — a decoder exited early).
/// - `Ok(None)` — every receiver closed (stream end). A closed channel alone
///   does not prove a clean finish — a panicked decoder also drops its sender —
///   so callers that report success on EOF must also harvest the decoder tasks.
pub(super) async fn recv_one_chunk(receivers: &mut [ColRx]) -> DaftResult<Option<Vec<ArrayRef>>> {
    // Zero receivers would return `Ok(Some(vec![]))` forever; every caller
    // spawns at least one decoder column.
    debug_assert!(!receivers.is_empty());
    let chunks = try_join_all(
        receivers
            .iter_mut()
            .map(|r| async { r.recv().await.transpose() }),
    )
    .await?;
    let closed = chunks.iter().filter(|c| c.is_none()).count();
    if closed == 0 {
        Ok(Some(chunks.into_iter().flatten().collect()))
    } else if closed == chunks.len() {
        Ok(None)
    } else {
        Err(DaftError::InternalError(
            "Parquet column decoders went out of lockstep: some columns ended while others still \
             had chunks (a column decoder may have exited early)"
                .to_string(),
        ))
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn spawn_col_decoders(
    col_indices: &[usize],
    chunk_source: &Arc<ChunkSource>,
    metadata: &Arc<ParquetMetaData>,
    arrow_schema: &Arc<ArrowSchema>,
    selection: Option<&RowSelection>,
    rg_idx: usize,
    chunk_size: usize,
    path: &Arc<str>,
) -> DaftResult<(Vec<ColRx>, JoinSet<DaftResult<()>>)> {
    // The reader picks its own per-RG access pattern (batched pre-fetch for
    // local, lazy per-column for remote). See `ChunkSource::open_rg`.
    let all_leaves: Arc<[usize]> = leaves_for_top_fields(metadata.as_ref(), col_indices).into();
    let rg_reader = chunk_source.clone().open_rg(rg_idx, all_leaves).await?;

    let compute = get_compute_runtime();
    let mut rxs = Vec::with_capacity(col_indices.len());
    let mut joinset: JoinSet<DaftResult<()>> = JoinSet::new();
    for &col_idx in col_indices {
        let (tx, rx) = mpsc::channel::<DaftResult<ArrayRef>>(1);
        let rg_reader = rg_reader.clone();
        let metadata = metadata.clone();
        let arrow_field = arrow_schema.field(col_idx).clone();
        let sel = selection.cloned();
        let path = path.clone();
        let col_leaves: Arc<[usize]> = leaves_for_top_fields(metadata.as_ref(), &[col_idx]).into();
        joinset.spawn_on(
            async move {
                // Test-only injection: a file path containing this marker
                // panics the decoder task for the named column, simulating a
                // decoder bug. Keyed off the path so parallel tests don't
                // interfere, and off the column so a test can target the
                // predicate-prefilter decoders without also tripping the
                // data-column ones.
                #[cfg(test)]
                assert!(
                    !path.contains(&format!("inject-decoder-panic-{}--", arrow_field.name())),
                    "injected decoder panic"
                );
                let chunks = match rg_reader.read_col(col_leaves).await {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = tx.send(Err(e.into())).await;
                        return DaftResult::Ok(());
                    }
                };
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
    Ok((rxs, joinset))
}

struct StreamingState {
    ctx: Arc<RgTaskCtx>,
    col_receivers: Vec<ColRx>,
    offset: usize,
    /// Phase-1 predicate arrays for this RG, already mask-filtered.
    /// Indexed by `ctx.plan.pred_col_indices` position.
    filtered_pred: Vec<ArrayRef>,
}

pub(super) async fn process_rg_with_data_cols(
    ctx: Arc<RgTaskCtx>,
    rg_idx: usize,
    selection: Option<RowSelection>,
    filtered_pred: Vec<ArrayRef>,
) -> BoxStream<'static, DaftResult<RecordBatch>> {
    // Default mode invariant: data_col_indices is non-empty. PredOnly handles
    // the empty case via `process_rg_predicate_only`.
    debug_assert!(!ctx.plan.data_col_indices.is_empty());

    let (col_receivers, mut col_decoders) = match spawn_col_decoders(
        &ctx.plan.data_col_indices,
        &ctx.chunk_source,
        &ctx.metadata,
        &ctx.arrow_schema,
        selection.as_ref(),
        rg_idx,
        ctx.chunk_size,
        &ctx.path,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => return err_stream(e),
    };

    let state = StreamingState {
        ctx,
        col_receivers,
        offset: 0,
        filtered_pred,
    };

    let stream = futures::stream::unfold(state, |mut state| async move {
        let data_chunks = match recv_one_chunk(&mut state.col_receivers).await {
            Ok(Some(v)) => v,
            Ok(None) => return None,
            Err(e) => return Some((Err(e), state)),
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

        let mut daft_batch = match record_batch_from_arrow(
            Arc::new(ArrowSchema::new(fields)),
            arrays,
            ctx.path.as_ref(),
        ) {
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

    let (col_receivers, mut col_decoders) = match spawn_col_decoders(
        &plan.pred_col_indices,
        &ctx.chunk_source,
        &ctx.metadata,
        &ctx.arrow_schema,
        selection.as_ref(),
        rg_idx,
        ctx.chunk_size,
        &ctx.path,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => return err_stream(e),
    };

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
                Ok(Some(v)) => v,
                Ok(None) => return None,
                Err(e) => return Some((Err(e), state)),
            };

            let daft_batch = match record_batch_from_arrow(
                state.chunk_arrow_schema.clone(),
                chunks.clone(),
                state.ctx.path.as_ref(),
            ) {
                Ok(b) => b,
                Err(e) => return Some((Err(e), state)),
            };

            let arrow_bool = match eval_predicate_mask(&daft_batch, &state.bound_pred) {
                Ok(b) => b,
                Err(e) => return Some((Err(e), state)),
            };

            if arrow_bool.true_count() == 0 {
                continue;
            }

            let filtered =
                match filter_arrays_by_mask(&chunks, &arrow_bool, state.ctx.path.as_ref()) {
                    Ok(f) => f,
                    Err(e) => return Some((Err(e), state)),
                };

            // Build a RecordBatch over the predicate columns; project_to_schema
            // picks the subset for `return_daft_schema` (which is read_col_indices,
            // a subset of pred_col_indices in this path).
            let out_res = record_batch_from_arrow(
                state.chunk_arrow_schema.clone(),
                filtered,
                state.ctx.path.as_ref(),
            )
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

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;

    use super::*;

    fn arr() -> ArrayRef {
        Arc::new(Int64Array::from(vec![1, 2, 3]))
    }

    #[tokio::test]
    async fn recv_one_chunk_lockstep_then_eof() {
        let (tx1, rx1) = mpsc::channel(1);
        let (tx2, rx2) = mpsc::channel(1);
        tx1.send(Ok(arr())).await.unwrap();
        tx2.send(Ok(arr())).await.unwrap();
        drop(tx1);
        drop(tx2);
        let mut rxs = vec![rx1, rx2];
        let chunk = recv_one_chunk(&mut rxs).await.unwrap();
        assert_eq!(chunk.map(|c| c.len()), Some(2));
        assert!(recv_one_chunk(&mut rxs).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn recv_one_chunk_mixed_closure_is_error() {
        // One column still has a chunk while the other already closed — a
        // decoder exited early (e.g. panicked); must not look like clean EOF.
        let (tx1, rx1) = mpsc::channel(1);
        let (tx2, rx2) = mpsc::channel::<DaftResult<ArrayRef>>(1);
        tx1.send(Ok(arr())).await.unwrap();
        drop(tx1);
        drop(tx2);
        let mut rxs = vec![rx1, rx2];
        assert!(recv_one_chunk(&mut rxs).await.is_err());
    }

    #[tokio::test]
    async fn recv_one_chunk_propagates_decode_error() {
        let (tx1, rx1) = mpsc::channel::<DaftResult<ArrayRef>>(1);
        tx1.send(Err(DaftError::InternalError("decode failed".to_string())))
            .await
            .unwrap();
        let mut rxs = vec![rx1];
        assert!(recv_one_chunk(&mut rxs).await.is_err());
    }
}
