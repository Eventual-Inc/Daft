mod chunk_source;
mod field_reader;
mod rg_processor;
mod util;

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, BooleanArray, RecordBatch as ArrowRecordBatch},
    datatypes::Schema as ArrowSchema,
};
use chunk_source::{ChunkSource, LocalChunkSource, fetch_remote_chunk_source, open_local_file};
use common_error::DaftResult;
use common_runtime::get_compute_runtime;
use daft_core::prelude::*;
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr, optimization::get_required_columns};
use daft_recordbatch::RecordBatch;
use field_reader::{decode_one, leaves_for_top_fields};
use futures::{Stream, StreamExt, stream::BoxStream};
use parquet::{
    arrow::arrow_reader::{ArrowReaderMetadata, RowSelection, RowSelector},
    file::metadata::ParquetMetaData,
};
use rg_processor::{process_rg_predicate_only, process_rg_streaming};
use util::{cap_selection_to, parquet_err, project_schema, truncate_mask_to_n_trues};

use crate::{
    helpers::{
        bool_array_to_row_selection, build_offset_row_selection, build_single_rg_delete_selection,
        combine_selections, predicate_pushable_cols, prune_row_groups, refine_selection,
        substitute_missing_cols,
    },
    metadata::{
        apply_field_ids_to_arrowrs_parquet_metadata, strip_string_types_from_parquet_metadata,
    },
    read::{ParquetReadOptions, ParquetSchemaInferenceOptions, StringEncoding},
};

pub enum ParquetSource<'a> {
    Local {
        path: &'a str,
    },
    Url {
        uri: &'a str,
        io_client: Arc<daft_io::IOClient>,
        io_stats: Option<daft_io::IOStatsRef>,
    },
}

impl ParquetSource<'_> {
    fn label(&self) -> &str {
        match self {
            Self::Local { path } => path,
            Self::Url { uri, .. } => uri,
        }
    }
}

const DEFAULT_BATCH_SIZE: usize = 128 * 1024;

pub async fn stream_parquet(
    source: ParquetSource<'_>,
    opts: &ParquetReadOptions,
) -> DaftResult<(Arc<Schema>, BoxStream<'static, DaftResult<RecordBatch>>)> {
    let (chunk_source, arrow_metadata, effective_row_groups_owned) =
        open_chunk_source(&source, opts).await?;
    let chunk_source = Arc::new(chunk_source);
    let row_groups: Option<&[i64]> = effective_row_groups_owned
        .as_deref()
        .or(opts.row_groups.as_deref());

    let chunk_size = opts.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);
    let prepared = prepare_metadata(
        arrow_metadata,
        opts.field_id_mapping.as_ref(),
        opts.schema_infer,
    )?;
    let plan = resolve_column_plan(&prepared, opts)?;

    let rg_indices = prune_row_groups(
        &prepared.parquet_metadata,
        row_groups,
        opts.predicate.as_ref(),
        &plan.read_daft_schema,
        source.label(),
    )?;
    if rg_indices.is_empty() {
        return Ok(empty_stream(plan.return_daft_schema));
    }
    if plan.read_col_indices.is_empty() {
        return count_only_stream(
            &prepared.parquet_metadata,
            &rg_indices,
            opts.num_rows,
            plan.return_daft_schema,
        );
    }

    let global_starts = build_global_starts(&prepared.parquet_metadata);
    let base_selections = build_base_selections(
        &prepared.parquet_metadata,
        &rg_indices,
        &global_starts,
        opts,
    );
    let no_pred_active = count_active_rgs(&base_selections);

    // Output is the predicate columns — skip the prefilter and stream
    // chunks per RG directly, with shared limit short-circuit.
    let is_predicate_only_path = plan.predicate_pushed && plan.data_col_indices.is_empty();

    let prefilter = if plan.predicate_pushed && !is_predicate_only_path {
        prefilter_with_predicate(
            chunk_source.clone(),
            prepared.parquet_metadata.clone(),
            prepared.arrow_schema.clone(),
            &rg_indices,
            &plan,
            &base_selections,
            opts,
        )
        .await?
    } else {
        Prefilter::passthrough(base_selections, no_pred_active)
    };

    let return_schema = plan.return_daft_schema.clone();
    let ctx = Arc::new(RgTaskCtx {
        chunk_source,
        metadata: prepared.parquet_metadata,
        arrow_schema: prepared.arrow_schema,
        plan,
        predicate: opts.predicate.clone(),
        chunk_size,
        is_predicate_only_path,
    });
    let remaining_atomic = is_predicate_only_path.then(|| {
        Arc::new(std::sync::atomic::AtomicUsize::new(
            opts.num_rows.unwrap_or(usize::MAX),
        ))
    });
    let stream = build_rg_stream(ctx, rg_indices, prefilter, remaining_atomic);

    Ok((return_schema, apply_cross_rg_limit(stream, opts.num_rows)))
}

async fn open_chunk_source(
    source: &ParquetSource<'_>,
    opts: &ParquetReadOptions,
) -> DaftResult<(ChunkSource, ArrowReaderMetadata, Option<Vec<i64>>)> {
    match source {
        ParquetSource::Local { path } => {
            let (file, file_len, arrow_metadata) = open_local_file(path).await?;
            let cs = ChunkSource::Local(LocalChunkSource {
                file,
                file_len,
                metadata: arrow_metadata.metadata().clone(),
            });
            Ok((cs, arrow_metadata, None))
        }
        ParquetSource::Url {
            uri,
            io_client,
            io_stats,
        } => {
            Box::pin(fetch_remote_chunk_source(
                uri,
                io_client.clone(),
                io_stats.clone(),
                opts,
            ))
            .await
        }
    }
}

struct PreparedMetadata {
    parquet_metadata: Arc<ParquetMetaData>,
    arrow_schema: Arc<ArrowSchema>,
}

fn prepare_metadata(
    arrow_metadata: ArrowReaderMetadata,
    field_id_mapping: Option<&Arc<BTreeMap<i32, Field>>>,
    opts: ParquetSchemaInferenceOptions,
) -> DaftResult<PreparedMetadata> {
    let mut parquet_metadata = arrow_metadata.metadata().clone();
    if let Some(mapping) = field_id_mapping {
        parquet_metadata = apply_field_ids_to_arrowrs_parquet_metadata(parquet_metadata, mapping)?;
    }
    let raw_encoding = opts.string_encoding == StringEncoding::Raw;
    if raw_encoding {
        parquet_metadata = strip_string_types_from_parquet_metadata(parquet_metadata)?;
    }
    let arrow_schema = crate::schema_inference::infer_schema_from_parquet_metadata_arrowrs(
        &parquet_metadata,
        Some(opts.coerce_int96_timestamp_unit),
        raw_encoding,
    )
    .map_err(parquet_err)?;
    Ok(PreparedMetadata {
        parquet_metadata,
        arrow_schema: Arc::new(arrow_schema),
    })
}

#[derive(Clone)]
pub(super) struct ColumnPlan {
    pub(super) read_col_indices: Vec<usize>,
    pub(super) pred_col_indices: Vec<usize>,
    pub(super) data_col_indices: Vec<usize>,
    pub(super) predicate_pushed: bool,
    pub(super) return_daft_schema: Arc<Schema>,
    pub(super) read_daft_schema: Schema,
}

fn resolve_column_plan(
    prepared: &PreparedMetadata,
    opts: &ParquetReadOptions,
) -> DaftResult<ColumnPlan> {
    let daft_schema = Schema::try_from(prepared.arrow_schema.as_ref())?;
    let user_cols = opts.columns.as_deref();

    let mut read_col_names: HashSet<String> = match user_cols {
        Some(cols) => cols.iter().cloned().collect(),
        None => daft_schema.field_names().map(str::to_string).collect(),
    };

    let (pred_cols, predicate_pushed) = match opts.predicate.as_ref() {
        Some(pred) => match predicate_pushable_cols(pred, &daft_schema) {
            Some(cols) => (Some(cols), true),
            None => {
                let extra: HashSet<String> = get_required_columns(pred)
                    .into_iter()
                    .filter(|c| daft_schema.get_field(c).is_ok())
                    .collect();
                (Some(extra), false)
            }
        },
        None => (None, false),
    };

    if let Some(filter_cols) = &pred_cols {
        read_col_names.extend(filter_cols.iter().cloned());
    }

    let read_daft_schema = project_schema(&daft_schema, &read_col_names);
    let return_daft_schema = match user_cols {
        Some(cols) => Arc::new(project_schema(
            &daft_schema,
            &cols.iter().cloned().collect(),
        )),
        None => Arc::new(daft_schema),
    };

    let arrow_fields = prepared.arrow_schema.fields();
    let read_col_indices: Vec<usize> = arrow_fields
        .iter()
        .enumerate()
        .filter(|(_, f)| read_col_names.contains(f.name()))
        .map(|(i, _)| i)
        .collect();

    let (pred_col_indices, data_col_indices): (Vec<usize>, Vec<usize>) = if predicate_pushed {
        let pred_set: HashSet<&str> = pred_cols
            .as_ref()
            .unwrap()
            .iter()
            .map(String::as_str)
            .collect();
        read_col_indices
            .iter()
            .copied()
            .partition(|&i| pred_set.contains(arrow_fields[i].name().as_str()))
    } else {
        (Vec::new(), read_col_indices.clone())
    };

    Ok(ColumnPlan {
        read_col_indices,
        pred_col_indices,
        data_col_indices,
        predicate_pushed,
        return_daft_schema,
        read_daft_schema,
    })
}

fn build_global_starts(metadata: &ParquetMetaData) -> Vec<usize> {
    let mut starts = Vec::with_capacity(metadata.num_row_groups());
    let mut acc = 0usize;
    for i in 0..metadata.num_row_groups() {
        starts.push(acc);
        acc += metadata.row_group(i).num_rows() as usize;
    }
    starts
}

fn build_base_selections(
    metadata: &ParquetMetaData,
    rg_indices: &[usize],
    global_starts: &[usize],
    opts: &ParquetReadOptions,
) -> Vec<Option<RowSelection>> {
    // With a predicate, limit is enforced post-filter, not at RG-build time.
    let start_offset = opts.start_offset.unwrap_or(0);
    let delete_rows = opts.delete_rows.as_deref();
    let mut limit_remaining: Option<usize> =
        opts.predicate.is_none().then_some(opts.num_rows).flatten();
    rg_indices
        .iter()
        .map(|&rg_idx| {
            let rg_rows = metadata.row_group(rg_idx).num_rows() as usize;
            let mut sel = build_offset_selection(start_offset, global_starts[rg_idx], rg_rows);
            if let Some(deletes) = delete_rows
                && !deletes.is_empty()
            {
                sel = combine_selections(
                    sel,
                    Some(build_single_rg_delete_selection(
                        deletes,
                        global_starts[rg_idx],
                        rg_rows,
                    )),
                );
            }
            if let Some(remaining) = limit_remaining.as_mut() {
                sel = apply_limit_to_selection(sel, remaining, rg_rows);
            }
            sel
        })
        .collect()
}

fn build_offset_selection(
    global_offset: usize,
    rg_global_start: usize,
    rg_rows: usize,
) -> Option<RowSelection> {
    let rg_end = rg_global_start + rg_rows;
    match global_offset {
        0 => None,
        o if o >= rg_end => Some(vec![RowSelector::skip(rg_rows)].into()),
        o if o > rg_global_start => Some(build_offset_row_selection(o - rg_global_start, rg_rows)),
        _ => None,
    }
}

fn apply_limit_to_selection(
    sel: Option<RowSelection>,
    remaining: &mut usize,
    rg_rows: usize,
) -> Option<RowSelection> {
    let contrib = sel.as_ref().map(|s| s.row_count()).unwrap_or(rg_rows);
    if *remaining == 0 {
        Some(RowSelection::from(vec![RowSelector::skip(rg_rows)]))
    } else if contrib > *remaining {
        let cap = *remaining;
        *remaining = 0;
        Some(cap_selection_to(sel.as_ref(), cap, rg_rows))
    } else {
        *remaining -= contrib;
        sel
    }
}

/// Drops trailing all-skip RGs when the limit was already exhausted.
fn count_active_rgs(base_selections: &[Option<RowSelection>]) -> usize {
    base_selections
        .iter()
        .rposition(|sel| sel.as_ref().is_none_or(|s| s.iter().any(|r| !r.skip)))
        .map_or(0, |i| i + 1)
}

/// Output of the predicate prefilter — late-materialization artifacts the
/// main per-RG pass reuses to avoid decoding rows that won't survive.
///
/// - `selections`: row selections to apply when decoding data columns,
///   refined to skip rows the predicate already rejected.
/// - `pred_per_rg`: cached predicate-column arrays, so the main pass doesn't
///   re-decode them when assembling output batches.
/// - `pred_masks`: boolean masks to filter data-column chunks before output.
/// - `active_rg_count`: surviving RGs after the prefilter exhausted `num_rows`.
struct Prefilter {
    selections: Vec<Option<RowSelection>>,
    /// Indexed `[rg_pos][pred_col_pos]`. One outer entry per active RG.
    pred_per_rg: Vec<Vec<ArrayRef>>,
    pred_masks: Vec<Option<BooleanArray>>,
    active_rg_count: usize,
}

impl Prefilter {
    /// Passthrough variant: no predicate to evaluate, just trim trailing
    /// all-skip RGs and forward the offset/delete-derived selections.
    fn passthrough(mut base_selections: Vec<Option<RowSelection>>, active_rg_count: usize) -> Self {
        base_selections.truncate(active_rg_count);
        Self {
            selections: base_selections,
            pred_per_rg: Vec::new(),
            pred_masks: Vec::new(),
            active_rg_count,
        }
    }
}

/// Fetches one RG's predicate-column bytes, then fans out per-column decode
/// tasks and joins them into a `Vec<ArrayRef>`. One spawn per `(rg, col)`,
/// sharing the fetched chunks via `Arc<HashMap>`.
async fn decode_pred_cols_for_rg(
    chunk_source: Arc<ChunkSource>,
    metadata: Arc<ParquetMetaData>,
    arrow_schema: Arc<ArrowSchema>,
    pred_col_indices: Arc<[usize]>,
    pred_leaves: Arc<[usize]>,
    rg_idx: usize,
    selection: Option<RowSelection>,
) -> DaftResult<Vec<ArrayRef>> {
    let rg_chunks = Arc::new(chunk_source.read_rg_chunks(rg_idx, pred_leaves).await?);
    let compute = get_compute_runtime();
    let mut col_handles = Vec::with_capacity(pred_col_indices.len());
    for &col_idx in pred_col_indices.iter() {
        let chunks = rg_chunks.clone();
        let metadata = metadata.clone();
        let arrow_field = arrow_schema.field(col_idx).clone();
        let selection = selection.clone();
        col_handles.push(compute.spawn(async move {
            decode_one(
                chunks.as_ref(),
                &metadata,
                rg_idx,
                col_idx,
                &arrow_field,
                selection.as_ref(),
            )
        }));
    }
    let mut arrays = Vec::with_capacity(col_handles.len());
    for ch in col_handles {
        arrays.push(ch.await??);
    }
    Ok(arrays)
}

async fn prefilter_with_predicate(
    chunk_source: Arc<ChunkSource>,
    metadata: Arc<ParquetMetaData>,
    arrow_schema: Arc<ArrowSchema>,
    rg_indices: &[usize],
    plan: &ColumnPlan,
    base_selections: &[Option<RowSelection>],
    opts: &ParquetReadOptions,
) -> DaftResult<Prefilter> {
    let predicate = opts
        .predicate
        .as_ref()
        .expect("prefilter_with_predicate requires a predicate");
    let need_refined_selections = !plan.data_col_indices.is_empty();
    let compute = get_compute_runtime();
    let pred_leaves: Arc<[usize]> = leaves_for_top_fields(&metadata, &plan.pred_col_indices).into();
    let pred_cols: Arc<[usize]> = plan.pred_col_indices.as_slice().into();

    // Spawn pred decode for ALL RGs in parallel; dropped handles abort their tasks.
    let mut handles = Vec::with_capacity(rg_indices.len());
    for (rg_pos, &rg_idx) in rg_indices.iter().enumerate() {
        handles.push(compute.spawn(decode_pred_cols_for_rg(
            chunk_source.clone(),
            metadata.clone(),
            arrow_schema.clone(),
            pred_cols.clone(),
            pred_leaves.clone(),
            rg_idx,
            base_selections[rg_pos].clone(),
        )));
    }

    let pred_fields: Vec<Arc<arrow::datatypes::Field>> = plan
        .pred_col_indices
        .iter()
        .map(|&i| Arc::new(arrow_schema.field(i).clone()))
        .collect();
    let pred_arrow_schema = Arc::new(ArrowSchema::new(pred_fields));

    let mut remaining = opts.num_rows.unwrap_or(usize::MAX);
    let mut pred_per_rg: Vec<Vec<ArrayRef>> = Vec::new();
    let mut masks: Vec<Option<BooleanArray>> = Vec::new();
    let mut refined: Vec<Option<RowSelection>> = Vec::new();

    for (rg_pos, handle) in handles.into_iter().enumerate() {
        if remaining == 0 {
            drop(handle);
            continue;
        }
        let arrays = handle.await??;
        let pred_batch = ArrowRecordBatch::try_new(pred_arrow_schema.clone(), arrays.clone())
            .map_err(parquet_err)?;
        let daft_pred = RecordBatch::try_from(&pred_batch)?;
        let pred_for_rg = substitute_missing_cols(predicate, &daft_pred.schema)?;
        let bound = BoundExpr::try_new(pred_for_rg, &daft_pred.schema)?;
        let mask = daft_pred
            .eval_expression(&bound)?
            .bool()?
            .as_arrow()?
            .clone();

        let true_count = mask.true_count();
        let (capped_mask, contributed) = if true_count > remaining {
            (truncate_mask_to_n_trues(&mask, remaining), remaining)
        } else {
            (mask, true_count)
        };
        remaining -= contributed;

        refined.push(if need_refined_selections {
            let pred_sel = bool_array_to_row_selection(&capped_mask);
            Some(match &base_selections[rg_pos] {
                Some(base) => refine_selection(base, &pred_sel),
                None => pred_sel,
            })
        } else {
            None
        });
        masks.push(Some(capped_mask));
        pred_per_rg.push(arrays);
    }

    let active = pred_per_rg.len();
    Ok(Prefilter {
        selections: refined,
        pred_per_rg,
        pred_masks: masks,
        active_rg_count: active,
    })
}

/// Drains the per-RG joinset, surfacing `JoinError` (task panic) as
/// `DaftError::External`. Mirrors `rg_processor::drive_col_handles` —
/// `combine_stream` awaits this only after the receiver stream is exhausted, so
/// errors arrive as the final stream item.
async fn drive_rg_joinset(mut joinset: tokio::task::JoinSet<DaftResult<()>>) -> DaftResult<()> {
    while let Some(res) = joinset.join_next().await {
        res.map_err(|e| common_error::DaftError::External(e.into()))??;
    }
    Ok(())
}

/// Shared per-file state for an RG-decoding task. One `Arc<RgTaskCtx>` is
/// cloned per RG task — replaces the previous fistful-of-Arcs cloning ritual.
pub(super) struct RgTaskCtx {
    pub(super) chunk_source: Arc<ChunkSource>,
    pub(super) metadata: Arc<ParquetMetaData>,
    pub(super) arrow_schema: Arc<ArrowSchema>,
    pub(super) plan: ColumnPlan,
    pub(super) predicate: Option<ExprRef>,
    pub(super) chunk_size: usize,
    pub(super) is_predicate_only_path: bool,
}

/// One bounded channel per RG, drained in RG order. In-file output is always
/// RG-ordered: `maintain_order=false` at the scan layer only reorders BETWEEN
/// scan tasks; downstream code (and tests) expect file-order output within
/// a single file.
fn build_rg_stream(
    ctx: Arc<RgTaskCtx>,
    rg_indices: Vec<usize>,
    prefilter: Prefilter,
    remaining_atomic: Option<Arc<std::sync::atomic::AtomicUsize>>,
) -> BoxStream<'static, DaftResult<RecordBatch>> {
    use tokio_stream::wrappers::ReceiverStream;

    let active = prefilter.active_rg_count;
    // Capacity 1: each task runs ~one batch ahead — cross-RG overlap, bounded memory.
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..active)
        .map(|_| tokio::sync::mpsc::channel::<DaftResult<RecordBatch>>(1))
        .unzip();

    let mut pred_per_rg = prefilter.pred_per_rg.into_iter();
    let mut pred_masks = prefilter.pred_masks.into_iter();
    let mut selections = prefilter.selections.into_iter();

    let compute = get_compute_runtime();
    let mut joinset: tokio::task::JoinSet<DaftResult<()>> = tokio::task::JoinSet::new();
    for (rg_pos, sender) in senders.into_iter().enumerate() {
        let ctx = ctx.clone();
        let rg_idx = rg_indices[rg_pos];
        let selection = selections.next().flatten();
        let pred_arrays = pred_per_rg.next().unwrap_or_default();
        let pred_mask = pred_masks.next().flatten();
        let remaining = remaining_atomic.clone();
        joinset.spawn_on(
            async move {
                let mut sub_stream = if ctx.is_predicate_only_path {
                    process_rg_predicate_only(
                        ctx,
                        rg_idx,
                        selection,
                        remaining.expect("predicate-only path requires remaining counter"),
                    )
                    .await
                } else {
                    process_rg_streaming(ctx, rg_idx, selection, pred_arrays, pred_mask).await
                };
                while let Some(item) = sub_stream.next().await {
                    if sender.send(item).await.is_err() {
                        break;
                    }
                }
                DaftResult::Ok(())
            },
            compute.runtime.handle(),
        );
    }

    let inner_streams = receivers.into_iter().map(ReceiverStream::new);
    let merged: BoxStream<'static, DaftResult<RecordBatch>> =
        Box::pin(futures::stream::iter(inner_streams).flatten());
    common_runtime::combine_stream(merged, drive_rg_joinset(joinset)).boxed()
}

fn empty_stream(schema: Arc<Schema>) -> (Arc<Schema>, BoxStream<'static, DaftResult<RecordBatch>>) {
    let s = schema.clone();
    (
        schema,
        futures::stream::once(async move { Ok(RecordBatch::empty(Some(s))) }).boxed(),
    )
}

fn count_only_stream(
    metadata: &ParquetMetaData,
    rg_indices: &[usize],
    num_rows: Option<usize>,
    return_schema: Arc<Schema>,
) -> DaftResult<(Arc<Schema>, BoxStream<'static, DaftResult<RecordBatch>>)> {
    let total: usize = rg_indices
        .iter()
        .map(|&i| metadata.row_group(i).num_rows() as usize)
        .sum();
    let n = num_rows.map(|n| n.min(total)).unwrap_or(total);
    let batch = RecordBatch::new_with_size(return_schema.clone(), Vec::new(), n)?;
    Ok((
        return_schema,
        futures::stream::once(async move { Ok(batch) }).boxed(),
    ))
}

fn apply_cross_rg_limit(
    stream: impl Stream<Item = DaftResult<RecordBatch>> + Send + 'static,
    limit: Option<usize>,
) -> BoxStream<'static, DaftResult<RecordBatch>> {
    let Some(limit) = limit else {
        return Box::pin(stream);
    };
    let mut remaining = limit;
    let bounded = stream.filter_map(move |res| {
        let out = match res {
            Err(e) => Some(Err(e)),
            Ok(_) if remaining == 0 => None,
            Ok(b) if b.num_rows() <= remaining => {
                remaining -= b.num_rows();
                Some(Ok(b))
            }
            Ok(b) => {
                let take = remaining;
                remaining = 0;
                Some(b.head(take))
            }
        };
        async move { out }
    });
    Box::pin(bounded)
}
