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
    // With a predicate, row-count-based RG pruning is unsafe: rows survive the
    // limit only after filtering, so later RGs may still be needed.
    let prefetch_num_rows = if predicate.is_some() { None } else { num_rows };
    let (chunk_source, arrow_metadata, effective_row_groups_owned) = open_chunk_source(
        &source,
        columns,
        row_groups,
        start_offset,
        prefetch_num_rows,
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
            let prefetch_cols_refs = prefetch_cols_owned
                .as_ref()
                .map(|v| v.iter().map(String::as_str).collect::<Vec<_>>());
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

#[derive(Clone)]
struct ColumnPlan {
    read_col_indices: Vec<usize>,
    pred_col_indices: Vec<usize>,
    data_col_indices: Vec<usize>,
    predicate_pushed: bool,
    return_daft_schema: Arc<Schema>,
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

struct RgLayout {
    global_starts: Vec<usize>,
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

/// Drops trailing all-skip RGs when the limit was already exhausted.
fn count_active_rgs(base_selections: &[Option<RowSelection>]) -> usize {
    base_selections
        .iter()
        .rposition(|sel| sel.as_ref().is_none_or(|s| s.iter().any(|r| !r.skip)))
        .map_or(0, |i| i + 1)
}

struct Phase1Output {
    final_selections: Vec<Option<RowSelection>>,
    pred_arrays: Vec<Vec<ArrayRef>>,
    pred_masks: Vec<Option<BooleanArray>>,
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

    // Spawn pred decode for ALL RGs in parallel; dropped handles abort their tasks.
    let mut handles = Vec::with_capacity(rg_indices.len());
    for (rg_pos, &rg_idx) in rg_indices.iter().enumerate() {
        let source = chunk_source.clone();
        let metadata = metadata.clone();
        let arrow_schema = arrow_schema.clone();
        let pred_cols: Vec<usize> = pred_col_indices.to_vec();
        let pred_leaves = pred_leaves.clone();
        let selection = base_selections[rg_pos].clone();
        handles.push(compute.spawn(async move {
            let rg_chunks = Arc::new(source.read_rg_chunks(rg_idx, pred_leaves).await?);
            let compute = get_compute_runtime();
            let mut col_handles = Vec::with_capacity(pred_cols.len());
            for &col_idx in &pred_cols {
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
            DaftResult::Ok(arrays)
        }));
    }

    let pred_fields: Vec<Arc<arrow::datatypes::Field>> = pred_col_indices
        .iter()
        .map(|&i| Arc::new(arrow_schema.field(i).clone()))
        .collect();
    let pred_arrow_schema = Arc::new(ArrowSchema::new(pred_fields));

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
        let pred_batch = ArrowRecordBatch::try_new(pred_arrow_schema.clone(), arrays.clone())
            .map_err(parquet_err)?;
        let daft_pred = RecordBatch::try_from(&pred_batch)?;
        let pred_for_rg = substitute_missing_cols(&predicate, &daft_pred.schema)?;
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

        refined.push(if need_phase2_selections {
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
    let mut pred_arrays: Vec<Vec<ArrayRef>> = (0..pred_col_indices.len())
        .map(|_| Vec::with_capacity(active))
        .collect();
    for arrays in pred_per_rg {
        for (col_pos, arr) in arrays.into_iter().enumerate() {
            pred_arrays[col_pos].push(arr);
        }
    }

    Ok(Phase1Output {
        final_selections: refined,
        pred_arrays,
        pred_masks: masks,
        active_rg_count: active,
    })
}

/// One bounded channel per RG, drained in RG order. In-file output is always
/// RG-ordered: `maintain_order=false` at the scan layer only reorders BETWEEN
/// scan tasks; downstream code (and tests) expect file-order output within
/// a single file.
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
) -> BoxStream<'static, DaftResult<RecordBatch>> {
    use tokio_stream::wrappers::ReceiverStream;

    let remaining_atomic = Arc::new(std::sync::atomic::AtomicUsize::new(
        num_rows.unwrap_or(usize::MAX),
    ));
    let read_col_arc: Arc<[usize]> = plan.read_col_indices.clone().into();
    let pred_col_arc: Arc<[usize]> = plan.pred_col_indices.clone().into();
    let data_col_arc: Arc<[usize]> = plan.data_col_indices.clone().into();
    let predicate_pushed = plan.predicate_pushed;
    let return_schema = plan.return_daft_schema.clone();

    // Capacity 1: each task runs ~one batch ahead — cross-RG overlap, bounded memory.
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..active_rg_count)
        .map(|_| tokio::sync::mpsc::channel::<DaftResult<RecordBatch>>(1))
        .unzip();

    let compute = get_compute_runtime();
    let mut joinset: tokio::task::JoinSet<DaftResult<()>> = tokio::task::JoinSet::new();
    for (rg_pos, sender) in senders.into_iter().enumerate() {
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
        joinset.spawn_on(
            async move {
                let rg_idx = rg_indices[rg_pos];
                let mut sub_stream = if is_chunked_pred {
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

    let driver = async move {
        while let Some(res) = joinset.join_next().await {
            res.map_err(|e| common_error::DaftError::External(e.into()))??;
        }
        DaftResult::Ok(())
    };

    let inner_streams = receivers.into_iter().map(ReceiverStream::new);
    let merged: BoxStream<'static, DaftResult<RecordBatch>> =
        Box::pin(futures::stream::iter(inner_streams).flatten());
    common_runtime::combine_stream(merged, driver).boxed()
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

/// Collects the full stream into one concatenated `RecordBatch`, or a
/// schema-bearing empty batch if the stream produced none.
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
