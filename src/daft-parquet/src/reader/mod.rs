//! Single-file parquet reader: per-(row_group × column) decode using
//! arrow-rs's low-level array_reader API, with a per-column-chunk byte
//! window (one chunk in memory at a time, not the whole file).
//!
//! Predicate pushdown is two-phase: decode pred cols across all RGs in
//! parallel, evaluate per RG to get a `RowSelection`, then decode data cols
//! under that selection. The chunked-pred path (in `rg_processor.rs`)
//! short-circuits when the projection is entirely pred cols.

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
use rg_processor::{process_rg_chunked_pred, process_rg_streaming};
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
    read::{ParquetSchemaInferenceOptions, StringEncoding},
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
const MANY_RG_THRESHOLD: usize = 4;

/// Streaming entry point. Opens the chunk source, then forwards to the planner.
#[allow(clippy::too_many_arguments)]
pub async fn stream_parquet(
    source: ParquetSource<'_>,
    columns: Option<&[&str]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    row_groups: Option<&[i64]>,
    predicate: Option<ExprRef>,
    schema_infer_options: ParquetSchemaInferenceOptions,
    batch_size: Option<usize>,
    field_id_mapping: Option<Arc<BTreeMap<i32, Field>>>,
    delete_rows: Option<&[i64]>,
) -> DaftResult<(Arc<Schema>, BoxStream<'static, DaftResult<RecordBatch>>)> {
    let (chunk_source, arrow_metadata, effective_row_groups_owned) = open_chunk_source(
        &source,
        columns,
        row_groups,
        start_offset,
        num_rows,
        predicate.as_ref(),
        field_id_mapping.as_deref(),
    )
    .await?;
    let effective_row_groups: Option<&[i64]> = effective_row_groups_owned.as_deref().or(row_groups);

    stream_parquet_from_source(
        Arc::new(chunk_source),
        arrow_metadata,
        source.label(),
        columns,
        start_offset,
        num_rows,
        effective_row_groups,
        predicate,
        schema_infer_options,
        batch_size,
        field_id_mapping,
        delete_rows,
    )
    .await
}

/// Open the chunk source for a parquet URI. Remote sources prefetch projection ∪
/// predicate cols and pre-prune row groups by offset/limit; the pre-pruned set
/// (if any) is returned so the planner uses the same set the prefetcher fetched.
async fn open_chunk_source(
    source: &ParquetSource<'_>,
    columns: Option<&[&str]>,
    row_groups: Option<&[i64]>,
    start_offset: Option<usize>,
    num_rows: Option<usize>,
    predicate: Option<&ExprRef>,
    field_id_mapping: Option<&BTreeMap<i32, Field>>,
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
            let prefetch_cols_owned: Option<Vec<String>> = columns.map(|cols| {
                let mut acc: Vec<String> = cols.iter().map(|s| (*s).to_string()).collect();
                if let Some(pred) = predicate {
                    for c in get_required_columns(pred) {
                        if !acc.contains(&c) {
                            acc.push(c);
                        }
                    }
                }
                acc
            });
            let prefetch_cols_refs: Option<Vec<&str>> = prefetch_cols_owned
                .as_ref()
                .map(|v| v.iter().map(String::as_str).collect());
            fetch_remote_chunk_source(
                uri,
                io_client.clone(),
                io_stats.clone(),
                prefetch_cols_refs.as_deref(),
                row_groups,
                start_offset,
                num_rows,
                field_id_mapping,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn stream_parquet_from_source(
    chunk_source: Arc<ChunkSource>,
    arrow_metadata: ArrowReaderMetadata,
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
) -> DaftResult<(Arc<Schema>, BoxStream<'static, DaftResult<RecordBatch>>)> {
    let chunk_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);
    let prepared = prepare_metadata(
        arrow_metadata,
        field_id_mapping.as_ref(),
        schema_infer_options,
    )?;
    let plan = resolve_column_plan(&prepared, columns, predicate.as_ref())?;

    let rg_indices = prune_row_groups(
        &prepared.parquet_metadata,
        row_groups,
        predicate.as_ref(),
        &plan.read_daft_schema,
        path,
    )?;
    if rg_indices.is_empty() {
        return Ok(empty_stream(plan.return_daft_schema));
    }
    if plan.read_col_indices.is_empty() {
        return count_only_stream(
            &prepared.parquet_metadata,
            &rg_indices,
            num_rows,
            plan.return_daft_schema,
        );
    }

    let layout = build_rg_layout(&prepared.parquet_metadata, &rg_indices);
    let base_selections = build_base_selections(
        &rg_indices,
        &layout,
        start_offset.unwrap_or(0),
        delete_rows,
        num_rows,
        predicate.is_none(),
    );
    let no_pred_active = count_active_rgs(&base_selections);

    let is_chunked_pred = plan.predicate_pushed
        && plan.data_col_indices.is_empty()
        && (num_rows.is_some() || rg_indices.len() <= MANY_RG_THRESHOLD);

    let phase1 = if plan.predicate_pushed && !is_chunked_pred {
        phase1_predicate_decode(
            chunk_source.clone(),
            prepared.parquet_metadata.clone(),
            prepared.arrow_schema.clone(),
            &rg_indices,
            &plan.pred_col_indices,
            &base_selections,
            !plan.data_col_indices.is_empty(),
            predicate.clone().unwrap(),
            num_rows,
        )
        .await?
    } else {
        Phase1Output {
            final_selections: base_selections,
            pred_arrays: Vec::new(),
            pred_masks: Vec::new(),
            active_rg_count: no_pred_active,
        }
    };

    let stream = build_rg_stream(
        chunk_source,
        prepared.parquet_metadata,
        prepared.arrow_schema,
        rg_indices.into(),
        Arc::new(plan.clone()),
        phase1.final_selections.into(),
        Arc::new(phase1.pred_arrays),
        Arc::new(phase1.pred_masks),
        phase1.active_rg_count,
        chunk_size,
        is_chunked_pred,
        predicate,
        num_rows,
    );

    Ok((
        plan.return_daft_schema,
        apply_cross_rg_limit(stream, num_rows),
    ))
}

// ───── metadata + schema preparation ─────────────────────────────────────────

struct PreparedMetadata {
    parquet_metadata: Arc<ParquetMetaData>,
    arrow_schema: Arc<ArrowSchema>,
    daft_schema: Schema,
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
    let arrow_schema = Arc::new(arrow_schema);
    let daft_schema = Schema::try_from(arrow_schema.as_ref())?;
    Ok(PreparedMetadata {
        parquet_metadata,
        arrow_schema,
        daft_schema,
    })
}

// ───── projection / predicate column plan ────────────────────────────────────

#[derive(Clone)]
struct ColumnPlan {
    /// Indices into `arrow_schema.fields()` for projection ∪ pred cols.
    read_col_indices: Vec<usize>,
    /// Subset of `read_col_indices` referenced by the predicate (empty if not pushed).
    pred_col_indices: Vec<usize>,
    /// `read_col_indices − pred_col_indices` — cols decoded under the refined RowSelection.
    data_col_indices: Vec<usize>,
    /// True if the predicate is fully pushable into the row-group decoder.
    predicate_pushed: bool,
    /// Schema returned to the caller (projection only).
    return_daft_schema: Arc<Schema>,
    /// Schema used for RG-statistics pruning (projection ∪ pred cols).
    read_daft_schema: Schema,
}

fn resolve_column_plan(
    prepared: &PreparedMetadata,
    columns: Option<&[&str]>,
    predicate: Option<&ExprRef>,
) -> DaftResult<ColumnPlan> {
    let daft_schema = &prepared.daft_schema;
    let user_col_set: Option<HashSet<&str>> = columns.map(|c| c.iter().copied().collect());

    let mut read_col_names: HashSet<String> = match &user_col_set {
        Some(s) => s.iter().map(|s| (*s).to_string()).collect(),
        None => daft_schema.field_names().map(str::to_string).collect(),
    };

    let (pred_cols, predicate_pushed) = match predicate {
        Some(pred) => match predicate_pushable_cols(pred, daft_schema) {
            Some(cols) => (Some(cols), true),
            None => {
                // Not pushable but still read its cols for fallback eval.
                let mut extra = HashSet::new();
                for c in get_required_columns(pred) {
                    if daft_schema.get_field(&c).is_ok() {
                        extra.insert(c);
                    }
                }
                (Some(extra), false)
            }
        },
        None => (None, false),
    };

    if let Some(filter_cols) = &pred_cols {
        for c in filter_cols {
            read_col_names.insert(c.clone());
        }
    }

    let read_daft_schema = project_schema(daft_schema, &read_col_names);
    let return_daft_schema = match &user_col_set {
        Some(s) => Arc::new(project_schema(
            daft_schema,
            &s.iter().map(|n| (*n).to_string()).collect(),
        )),
        None => Arc::new(daft_schema.clone()),
    };

    let arrow_fields = prepared.arrow_schema.fields();
    let read_col_indices: Vec<usize> = arrow_fields
        .iter()
        .enumerate()
        .filter(|(_, f)| read_col_names.contains(f.name()))
        .map(|(i, _)| i)
        .collect();

    let pred_col_indices: Vec<usize> = if predicate_pushed {
        let pred_set: HashSet<&str> = pred_cols
            .as_ref()
            .unwrap()
            .iter()
            .map(String::as_str)
            .collect();
        arrow_fields
            .iter()
            .enumerate()
            .filter(|(_, f)| pred_set.contains(f.name().as_str()))
            .map(|(i, _)| i)
            .collect()
    } else {
        Vec::new()
    };

    let data_col_indices: Vec<usize> = if predicate_pushed {
        read_col_indices
            .iter()
            .copied()
            .filter(|i| !pred_col_indices.contains(i))
            .collect()
    } else {
        read_col_indices.clone()
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

// ───── row-group layout + per-RG base selections ─────────────────────────────

struct RgLayout {
    /// Per-file-RG cumulative row offsets, indexed by original RG index.
    global_starts: Vec<usize>,
    /// Per-active-RG row counts, indexed by position in `rg_indices`.
    row_counts: Vec<usize>,
}

fn build_rg_layout(metadata: &ParquetMetaData, rg_indices: &[usize]) -> RgLayout {
    let total = metadata.num_row_groups();
    let mut global_starts = Vec::with_capacity(total);
    let mut cumulative = 0usize;
    for i in 0..total {
        global_starts.push(cumulative);
        cumulative += metadata.row_group(i).num_rows() as usize;
    }
    let row_counts = rg_indices
        .iter()
        .map(|&i| metadata.row_group(i).num_rows() as usize)
        .collect();
    RgLayout {
        global_starts,
        row_counts,
    }
}

fn build_base_selections(
    rg_indices: &[usize],
    layout: &RgLayout,
    start_offset: usize,
    delete_rows: Option<&[i64]>,
    num_rows: Option<usize>,
    push_limit: bool,
) -> Vec<Option<RowSelection>> {
    let mut limit_remaining: Option<usize> = if push_limit { num_rows } else { None };
    rg_indices
        .iter()
        .zip(layout.row_counts.iter())
        .map(|(&rg_idx, &rg_rows)| {
            let mut sel =
                build_offset_selection(start_offset, layout.global_starts[rg_idx], rg_rows);
            if let Some(deletes) = delete_rows
                && !deletes.is_empty()
            {
                sel = combine_selections(
                    sel,
                    Some(build_single_rg_delete_selection(
                        deletes,
                        layout.global_starts[rg_idx],
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
    if global_offset == 0 {
        return None;
    }
    let rg_end = rg_global_start + rg_rows;
    if global_offset >= rg_end {
        Some(RowSelection::from(vec![RowSelector::skip(rg_rows)]))
    } else if global_offset > rg_global_start {
        Some(build_offset_row_selection(
            global_offset - rg_global_start,
            rg_rows,
        ))
    } else {
        None
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

/// Number of leading RGs that contribute at least one row (drops trailing
/// all-skip RGs from the active set when the limit was already exhausted).
fn count_active_rgs(base_selections: &[Option<RowSelection>]) -> usize {
    let mut last = 0;
    for (idx, sel) in base_selections.iter().enumerate() {
        let contributes = sel.as_ref().is_none_or(|s| s.iter().any(|r| !r.skip));
        if contributes {
            last = idx + 1;
        }
    }
    last
}

// ───── phase-1 predicate decode ──────────────────────────────────────────────

struct Phase1Output {
    /// Per-RG final selection: refined (pred mask intersected with base) or `None`
    /// when the projection is entirely pred cols (the mask alone drives assembly).
    final_selections: Vec<Option<RowSelection>>,
    /// Decoded predicate columns, transposed `[col][rg]` for cheap per-col slicing
    /// during phase-2 assembly.
    pred_arrays: Vec<Vec<ArrayRef>>,
    /// Per-RG boolean mask from predicate evaluation.
    pred_masks: Vec<Option<BooleanArray>>,
    /// Number of RGs that contributed rows (after applying the limit).
    active_rg_count: usize,
}

#[allow(clippy::too_many_arguments)]
async fn phase1_predicate_decode(
    chunk_source: Arc<ChunkSource>,
    metadata: Arc<ParquetMetaData>,
    arrow_schema: Arc<ArrowSchema>,
    rg_indices: &[usize],
    pred_col_indices: &[usize],
    base_selections: &[Option<RowSelection>],
    need_phase2_selections: bool,
    predicate: ExprRef,
    num_rows: Option<usize>,
) -> DaftResult<Phase1Output> {
    let compute = get_compute_runtime();
    let pred_leaves: Arc<[usize]> = leaves_for_top_fields(&metadata, pred_col_indices).into();
    let pred_cols_arc: Arc<[usize]> = pred_col_indices.to_vec().into();

    // Spawn pred decode for ALL RGs in parallel; dropped handles abort their tasks.
    let mut handles = Vec::with_capacity(rg_indices.len());
    for (rg_pos, &rg_idx) in rg_indices.iter().enumerate() {
        let source = chunk_source.clone();
        let metadata = metadata.clone();
        let arrow_schema = arrow_schema.clone();
        let pred_cols = pred_cols_arc.clone();
        let pred_leaves = pred_leaves.clone();
        let selection = base_selections[rg_pos].clone();
        handles.push(compute.spawn(async move {
            decode_pred_cols_for_rg(
                source,
                metadata,
                arrow_schema,
                rg_idx,
                pred_cols,
                pred_leaves,
                selection,
            )
            .await
        }));
    }

    // Await in RG order; limit lets us short-circuit early.
    let mut remaining = num_rows.unwrap_or(usize::MAX);
    let mut pred_per_rg: Vec<Vec<ArrayRef>> = Vec::new();
    let mut masks: Vec<Option<BooleanArray>> = Vec::new();
    let mut refined: Vec<Option<RowSelection>> = Vec::new();

    for (rg_pos, handle) in handles.into_iter().enumerate() {
        if remaining == 0 {
            drop(handle);
            continue;
        }
        let arrays = handle.await??;
        let mask =
            eval_predicate_on_pred_arrays(&arrow_schema, pred_col_indices, &arrays, &predicate)?;

        let (capped_mask, contributed) = cap_mask_to_remaining(mask, remaining);
        remaining -= contributed;

        if need_phase2_selections {
            let pred_sel = bool_array_to_row_selection(&capped_mask);
            let final_sel = match &base_selections[rg_pos] {
                Some(base) => refine_selection(base, &pred_sel),
                None => pred_sel,
            };
            refined.push(Some(final_sel));
        } else {
            refined.push(None);
        }
        masks.push(Some(capped_mask));
        pred_per_rg.push(arrays);
    }

    let active = pred_per_rg.len();
    let pred_arrays = transpose_pred_arrays(pred_per_rg, pred_col_indices.len(), active);

    Ok(Phase1Output {
        final_selections: refined,
        pred_arrays,
        pred_masks: masks,
        active_rg_count: active,
    })
}

/// Batch-read all pred-col leaves for one RG, decode each pred column in parallel.
async fn decode_pred_cols_for_rg(
    chunk_source: Arc<ChunkSource>,
    metadata: Arc<ParquetMetaData>,
    arrow_schema: Arc<ArrowSchema>,
    rg_idx: usize,
    pred_col_indices: Arc<[usize]>,
    pred_leaves: Arc<[usize]>,
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

fn eval_predicate_on_pred_arrays(
    arrow_schema: &ArrowSchema,
    pred_col_indices: &[usize],
    arrays: &[ArrayRef],
    predicate: &ExprRef,
) -> DaftResult<BooleanArray> {
    let fields: Vec<Arc<arrow::datatypes::Field>> = pred_col_indices
        .iter()
        .map(|&i| Arc::new(arrow_schema.field(i).clone()))
        .collect();
    let pred_arrow_schema = Arc::new(ArrowSchema::new(fields));
    let pred_batch =
        ArrowRecordBatch::try_new(pred_arrow_schema, arrays.to_vec()).map_err(parquet_err)?;
    let daft_pred = RecordBatch::try_from(&pred_batch)?;
    let pred_for_rg = substitute_missing_cols(predicate, &daft_pred.schema)?;
    let bound = BoundExpr::try_new(pred_for_rg, &daft_pred.schema)?;
    let mask_series = daft_pred.eval_expression(&bound)?;
    Ok(mask_series.bool()?.as_arrow()?.clone())
}

/// Truncate the mask to at most `remaining` true bits. Returns `(capped_mask,
/// rows_contributed)`.
fn cap_mask_to_remaining(mask: BooleanArray, remaining: usize) -> (BooleanArray, usize) {
    let true_count = mask.true_count();
    if true_count > remaining {
        (truncate_mask_to_n_trues(&mask, remaining), remaining)
    } else {
        (mask, true_count)
    }
}

fn transpose_pred_arrays(
    pred_per_rg: Vec<Vec<ArrayRef>>,
    num_pred_cols: usize,
    active: usize,
) -> Vec<Vec<ArrayRef>> {
    let mut out: Vec<Vec<ArrayRef>> = (0..num_pred_cols)
        .map(|_| Vec::with_capacity(active))
        .collect();
    for arrays in pred_per_rg {
        for (col_pos, arr) in arrays.into_iter().enumerate() {
            out[col_pos].push(arr);
        }
    }
    out
}

// ───── per-RG stream dispatch ────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn build_rg_stream(
    chunk_source: Arc<ChunkSource>,
    metadata: Arc<ParquetMetaData>,
    arrow_schema: Arc<ArrowSchema>,
    rg_indices: Arc<[usize]>,
    plan: Arc<ColumnPlan>,
    final_selections: Arc<[Option<RowSelection>]>,
    pred_arrays: Arc<Vec<Vec<ArrayRef>>>,
    pred_masks: Arc<Vec<Option<BooleanArray>>>,
    active_rg_count: usize,
    chunk_size: usize,
    is_chunked_pred: bool,
    predicate: Option<ExprRef>,
    num_rows: Option<usize>,
) -> impl Stream<Item = DaftResult<RecordBatch>> + Send + 'static {
    let remaining_atomic = Arc::new(std::sync::atomic::AtomicUsize::new(
        num_rows.unwrap_or(usize::MAX),
    ));
    let read_col_arc: Arc<[usize]> = plan.read_col_indices.clone().into();
    let pred_col_arc: Arc<[usize]> = plan.pred_col_indices.clone().into();
    let data_col_arc: Arc<[usize]> = plan.data_col_indices.clone().into();
    let predicate_pushed = plan.predicate_pushed;
    let return_schema = plan.return_daft_schema.clone();

    futures::stream::iter(0..active_rg_count)
        .then(move |rg_pos| {
            let source = chunk_source.clone();
            let metadata = metadata.clone();
            let arrow_schema = arrow_schema.clone();
            let read_cols = read_col_arc.clone();
            let pred_cols = pred_col_arc.clone();
            let data_cols = data_col_arc.clone();
            let final_selections = final_selections.clone();
            let rg_indices = rg_indices.clone();
            let pred_arrays = pred_arrays.clone();
            let pred_masks = pred_masks.clone();
            let return_schema = return_schema.clone();
            let predicate = predicate.clone();
            let remaining = remaining_atomic.clone();
            async move {
                let rg_idx = rg_indices[rg_pos];
                if is_chunked_pred {
                    process_rg_chunked_pred(
                        rg_idx,
                        source,
                        metadata,
                        arrow_schema,
                        pred_cols,
                        read_cols,
                        final_selections[rg_pos].clone(),
                        predicate.unwrap(),
                        chunk_size,
                        remaining,
                        return_schema,
                    )
                    .await
                } else {
                    process_rg_streaming(
                        rg_pos,
                        rg_idx,
                        source,
                        metadata,
                        arrow_schema,
                        read_cols,
                        pred_cols,
                        data_cols,
                        final_selections[rg_pos].clone(),
                        pred_arrays,
                        pred_masks,
                        chunk_size,
                        return_schema,
                        predicate,
                        predicate_pushed,
                    )
                    .await
                }
            }
        })
        .flatten()
}

// ───── early-out streams + cross-RG limit ────────────────────────────────────

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

/// Trim the final batch to land exactly on the row cap.
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

// ───── bulk variant ──────────────────────────────────────────────────────────

/// Bulk variant: collect the full stream into one concatenated `RecordBatch`.
/// Returns a schema-bearing empty batch if the stream produced none.
#[allow(clippy::too_many_arguments)]
pub async fn read_parquet(
    source: ParquetSource<'_>,
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
    use futures::TryStreamExt;
    let (schema, stream) = stream_parquet(
        source,
        columns,
        start_offset,
        num_rows,
        row_groups,
        predicate,
        schema_infer_options,
        batch_size,
        field_id_mapping,
        delete_rows,
    )
    .await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    if batches.is_empty() {
        return Ok(RecordBatch::empty(Some(schema)));
    }
    RecordBatch::concat(&batches)
}
