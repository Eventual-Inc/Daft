mod chunk_source;
mod field_reader;
mod rg_processor;
mod util;

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use arrow::{array::ArrayRef, datatypes::Schema as ArrowSchema};
use chunk_source::{
    ChunkSource, ChunkSourceBuilder, LocalChunkSource, open_local_file, prepare_remote_chunk_source,
};
use common_error::DaftResult;
use common_runtime::{JoinSet, get_compute_runtime};
use daft_core::prelude::*;
use daft_dsl::{ExprRef, expr::bound_expr::BoundExpr, optimization::get_required_columns};
use daft_recordbatch::RecordBatch;
use field_reader::leaves_for_top_fields;
use futures::{Stream, StreamExt, stream::BoxStream};
use parquet::{
    arrow::arrow_reader::{ArrowReaderMetadata, RowSelection, RowSelector},
    file::metadata::ParquetMetaData,
};
use rg_processor::{
    process_rg_predicate_only, process_rg_with_data_cols, recv_one_chunk, spawn_col_decoders,
};
use snafu::ResultExt;
use util::{
    cap_selection_to, eval_predicate_mask, filter_arrays_by_mask, project_schema,
    record_batch_from_arrow, schema_from_indices, truncate_mask_to_n_trues,
};

use crate::{
    ArrowSnafu,
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
    let (cs_builder, arrow_metadata) = open_chunk_source(&source, opts).await?;
    let path = cs_builder.path().clone();

    let chunk_size = opts.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);
    let prepared = prepare_metadata(
        arrow_metadata,
        opts.field_id_mapping.as_ref(),
        opts.schema_infer,
        &path,
    )?;
    let plan = resolve_column_plan(&prepared, opts)?;

    // Single RG-level pruning pass: user row_groups + positional (start_offset,
    // num_rows) + predicate stats. Must run before `cs_builder.build`, which is
    // what spawns remote byte fetches.
    let rg_indices = prune_row_groups(
        &prepared.parquet_metadata,
        opts.row_groups.as_deref(),
        opts.start_offset.unwrap_or(0),
        opts.num_rows,
        opts.predicate.as_ref(),
        &plan.read_daft_schema,
        source.label(),
    )?;
    if rg_indices.is_empty() {
        return Ok((
            plan.return_daft_schema.clone(),
            futures::stream::once(
                async move { Ok(RecordBatch::empty(Some(plan.return_daft_schema))) },
            )
            .boxed(),
        ));
    }
    if plan.read_col_indices.is_empty() {
        return count_only_stream(
            &prepared.parquet_metadata,
            &rg_indices,
            opts.num_rows,
            plan.return_daft_schema,
        );
    }

    // Now spawn the byte-range fetches (remote) — pruned set only.
    let chunk_source = Arc::new(cs_builder.build(prepared.parquet_metadata.clone(), &rg_indices));

    let global_starts = build_global_starts(&prepared.parquet_metadata);
    let base_selections = build_base_selections(
        &prepared.parquet_metadata,
        &rg_indices,
        &global_starts,
        opts,
    );

    let rg_inputs = build_rg_inputs(
        &chunk_source,
        &prepared.parquet_metadata,
        &prepared.arrow_schema,
        &rg_indices,
        &plan,
        base_selections,
        opts,
        &path,
    )
    .await?;

    let return_schema = plan.return_daft_schema.clone();
    let ctx = Arc::new(RgTaskCtx {
        path,
        chunk_source,
        metadata: prepared.parquet_metadata,
        arrow_schema: prepared.arrow_schema,
        plan,
        predicate: opts.predicate.clone(),
        chunk_size,
    });
    let stream = build_rg_stream(ctx, rg_indices, rg_inputs);

    Ok((return_schema, apply_cross_rg_limit(stream, opts.num_rows)))
}

async fn open_chunk_source(
    source: &ParquetSource<'_>,
    opts: &ParquetReadOptions,
) -> crate::Result<(ChunkSourceBuilder, ArrowReaderMetadata)> {
    match source {
        ParquetSource::Local { path } => {
            let (file, file_len, arrow_metadata) = open_local_file(path).await?;
            let cs = LocalChunkSource {
                path: Arc::from(*path),
                file,
                file_len,
                metadata: arrow_metadata.metadata().clone(),
            };
            Ok((ChunkSourceBuilder::Local(cs), arrow_metadata))
        }
        ParquetSource::Url {
            uri,
            io_client,
            io_stats,
        } => {
            Box::pin(prepare_remote_chunk_source(
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
    path: &str,
) -> crate::Result<PreparedMetadata> {
    let mut parquet_metadata = arrow_metadata.metadata().clone();
    if let Some(mapping) = field_id_mapping {
        parquet_metadata =
            apply_field_ids_to_arrowrs_parquet_metadata(parquet_metadata, mapping, path)?;
    }
    let raw_encoding = opts.string_encoding == StringEncoding::Raw;
    if raw_encoding {
        parquet_metadata = strip_string_types_from_parquet_metadata(parquet_metadata, path)?;
    }
    let arrow_schema = crate::schema_inference::infer_schema_from_parquet_metadata_arrowrs(
        &parquet_metadata,
        Some(opts.coerce_int96_timestamp_unit),
        raw_encoding,
    )
    .with_context(|_| ArrowSnafu {
        path: path.to_string(),
    })?;
    Ok(PreparedMetadata {
        parquet_metadata,
        arrow_schema: Arc::new(arrow_schema),
    })
}

#[derive(Clone)]
pub(super) struct ColumnPlan {
    /// Physical Parquet column indices that must be read, including projection and filter columns.
    pub(super) read_col_indices: Vec<usize>,
    /// Physical Parquet column indices used only for predicate pushdown evaluation.
    pub(super) pred_col_indices: Vec<usize>,
    /// Physical Parquet column indices returned as data after predicate pushdown.
    pub(super) data_col_indices: Vec<usize>,
    /// Whether the predicate can be evaluated while reading Parquet row groups.
    pub(super) predicate_pushed: bool,
    /// Schema visible to callers after applying the requested projection.
    pub(super) return_daft_schema: Arc<Schema>,
    /// Schema for all columns read from Parquet, including predicate helper columns.
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

/// Row-level offset selection within the RG straddling `start_offset`. RGs
/// entirely before the offset are already pruned by `prune_row_groups`, so
/// only the straddling case (or no-skip) reaches here.
fn build_offset_selection(
    global_offset: usize,
    rg_global_start: usize,
    rg_rows: usize,
) -> Option<RowSelection> {
    if global_offset > rg_global_start {
        Some(build_offset_row_selection(
            global_offset - rg_global_start,
            rg_rows,
        ))
    } else {
        None
    }
}

/// Cap the selection for the RG straddling the `num_rows` boundary. RGs past
/// the limit are already pruned by `prune_row_groups`, so we only need to
/// cap-or-passthrough — never produce an all-skip selection.
fn apply_limit_to_selection(
    sel: Option<RowSelection>,
    remaining: &mut usize,
    rg_rows: usize,
) -> Option<RowSelection> {
    let contrib = sel.as_ref().map(|s| s.row_count()).unwrap_or(rg_rows);
    if contrib > *remaining {
        let cap = *remaining;
        *remaining = 0;
        Some(cap_selection_to(sel.as_ref(), cap, rg_rows))
    } else {
        *remaining -= contrib;
        sel
    }
}

/// Per-RG inputs to the streaming decode pass.
///
/// - `selection`: row selection to apply when decoding data columns —
///   offset/delete-derived, refined to skip rows the predicate already rejected.
/// - `pred_arrays`: predicate-column arrays already filtered by the predicate
///   mask, so the main pass slices them directly without re-decoding or
///   re-filtering. Empty when there's no predicate to prefilter.
struct RgInputs {
    selection: Option<RowSelection>,
    pred_arrays: Vec<ArrayRef>,
}

/// Build the per-RG input bundles for the streaming decoder.
///
/// Two paths in the same function:
///
/// 1. **Predicate prefilter** (predicate is pushed AND the projection has
///    data cols beyond pred cols): per RG, decode pred cols chunk-by-chunk,
///    evaluate the predicate, build filtered pred arrays + a refined
///    `RowSelection` so the per-RG data-col decoder can page-skip rows that
///    won't survive. With `opts.num_rows` set, decoding stops early once the
///    limit is satisfied. Unbounded: `chunk_size = usize::MAX` so the
///    streaming decoder emits one chunk per col, matching bulk-decode perf.
///
/// 2. **Passthrough** (everything else — no predicate, predicate-only
///    projection, or non-pushable predicate): trim trailing all-skip RGs and
///    forward the offset/delete-derived selections. The per-RG processor
///    handles any predicate at decode time.
#[allow(clippy::too_many_arguments)]
async fn build_rg_inputs(
    chunk_source: &Arc<ChunkSource>,
    metadata: &Arc<ParquetMetaData>,
    arrow_schema: &Arc<ArrowSchema>,
    rg_indices: &[usize],
    plan: &ColumnPlan,
    base_selections: Vec<Option<RowSelection>>,
    opts: &ParquetReadOptions,
    path: &Arc<str>,
) -> DaftResult<Vec<RgInputs>> {
    // Passthrough fast path.
    let Some(predicate) = opts
        .predicate
        .as_ref()
        .filter(|_| plan.predicate_pushed && !plan.data_col_indices.is_empty())
    else {
        return Ok(base_selections
            .into_iter()
            .map(|selection| RgInputs {
                selection,
                pred_arrays: Vec::new(),
            })
            .collect());
    };

    // Predicate prefilter path.
    // Limit set → chunked streaming for early termination. Unbounded → one
    // chunk per col so per-chunk arrow setup costs collapse to per-RG.
    let chunk_size = match opts.num_rows {
        Some(_) => opts.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1),
        None => usize::MAX,
    };
    let pred_leaves: Arc<[usize]> = leaves_for_top_fields(metadata, &plan.pred_col_indices).into();
    let pred_arrow_schema = schema_from_indices(arrow_schema, &plan.pred_col_indices);
    // Bind predicate once across all RGs — same chunk schema everywhere.
    let chunk_daft_schema = Arc::new(Schema::try_from(pred_arrow_schema.as_ref())?);
    let bound_pred = BoundExpr::try_new(
        substitute_missing_cols(predicate, &chunk_daft_schema)?,
        &chunk_daft_schema,
    )?;

    let mut remaining = opts.num_rows.unwrap_or(usize::MAX);
    let mut out: Vec<RgInputs> = Vec::with_capacity(rg_indices.len());

    for (rg_pos, &rg_idx) in rg_indices.iter().enumerate() {
        if remaining == 0 {
            break;
        }
        let base_sel = base_selections[rg_pos].clone();
        let total_selected = base_sel
            .as_ref()
            .map(|s| s.row_count())
            .unwrap_or_else(|| metadata.row_group(rg_idx).num_rows() as usize);

        let rg_chunks = Arc::new(
            chunk_source
                .read_rg_chunks(rg_idx, pred_leaves.clone())
                .await?,
        );
        let (mut col_receivers, _col_handles) = spawn_col_decoders(
            &plan.pred_col_indices,
            &rg_chunks,
            metadata,
            arrow_schema,
            base_sel.as_ref(),
            rg_idx,
            chunk_size,
            path,
        );

        // Per-chunk masks + filtered arrays; concatenated after the loop.
        let mut chunk_masks: Vec<arrow::array::BooleanArray> = Vec::new();
        let mut chunk_filtered_per_col: Vec<Vec<ArrayRef>> = (0..plan.pred_col_indices.len())
            .map(|_| Vec::new())
            .collect();
        let mut processed_rows = 0usize;

        loop {
            let chunks = match recv_one_chunk(&mut col_receivers).await {
                Some(Ok(v)) => v,
                Some(Err(e)) => return Err(e),
                None => break,
            };
            let chunk_rows = chunks[0].len();
            let daft_pred =
                record_batch_from_arrow(pred_arrow_schema.clone(), chunks.clone(), path.as_ref())?;
            let mask = eval_predicate_mask(&daft_pred, &bound_pred)?;

            let true_count = mask.true_count();
            let (capped_mask, contributed) = if true_count > remaining {
                (truncate_mask_to_n_trues(&mask, remaining), remaining)
            } else {
                (mask, true_count)
            };

            let filtered = filter_arrays_by_mask(&chunks, &capped_mask, path.as_ref())?;
            for (col_pos, filtered) in filtered.into_iter().enumerate() {
                chunk_filtered_per_col[col_pos].push(filtered);
            }
            chunk_masks.push(capped_mask);
            processed_rows += chunk_rows;
            remaining -= contributed;
            if remaining == 0 {
                break;
            }
        }
        // Dropping receivers closes the channels → spawned decoders abort.
        drop(col_receivers);

        // Concat per-col filtered chunks into one ArrayRef per col. Zero rows
        // processed (e.g. base_sel selected 0 rows) → empty array of the col's
        // data type.
        let pred_arrays: Vec<ArrayRef> = plan
            .pred_col_indices
            .iter()
            .enumerate()
            .map(|(col_pos, &col_idx)| {
                let chunks = &chunk_filtered_per_col[col_pos];
                if chunks.is_empty() {
                    arrow::array::new_empty_array(arrow_schema.field(col_idx).data_type())
                } else {
                    let refs: Vec<&dyn arrow::array::Array> =
                        chunks.iter().map(|a| a.as_ref()).collect();
                    arrow::compute::concat(&refs).expect("concat per-col chunks")
                }
            })
            .collect();

        // Build a RowSelection over the base-selected row space: chunked masks
        // → selectors over processed_rows; tail-skip unprocessed rows so the
        // total length matches `total_selected`.
        let mut selectors: Vec<RowSelector> = Vec::new();
        for cm in &chunk_masks {
            for s in bool_array_to_row_selection(cm).iter() {
                selectors.push(*s);
            }
        }
        let unprocessed = total_selected - processed_rows;
        if unprocessed > 0 {
            selectors.push(RowSelector::skip(unprocessed));
        }
        let pred_sel = RowSelection::from(selectors);
        let selection = Some(match &base_sel {
            Some(base) => refine_selection(base, &pred_sel),
            None => pred_sel,
        });

        out.push(RgInputs {
            selection,
            pred_arrays,
        });
    }

    Ok(out)
}

/// Shared per-file state for an RG-decoding task. One `Arc<RgTaskCtx>` is
/// cloned per RG task — replaces the previous fistful-of-Arcs cloning ritual.
///
/// Per-RG processor selection happens in `build_rg_stream` based on
/// `plan.data_col_indices.is_empty()`: empty → PredOnly path, non-empty →
/// Default path.
pub(super) struct RgTaskCtx {
    pub(super) path: Arc<str>,
    pub(super) chunk_source: Arc<ChunkSource>,
    pub(super) metadata: Arc<ParquetMetaData>,
    pub(super) arrow_schema: Arc<ArrowSchema>,
    pub(super) plan: ColumnPlan,
    pub(super) predicate: Option<ExprRef>,
    pub(super) chunk_size: usize,
}

/// One bounded channel per RG, drained in RG order. In-file output is always
/// RG-ordered: `maintain_order=false` at the scan layer only reorders BETWEEN
/// scan tasks; downstream code (and tests) expect file-order output within
/// a single file.
fn build_rg_stream(
    ctx: Arc<RgTaskCtx>,
    rg_indices: Vec<usize>,
    rg_inputs: Vec<RgInputs>,
) -> BoxStream<'static, DaftResult<RecordBatch>> {
    use tokio_stream::wrappers::ReceiverStream;

    // Capacity 1: each task runs ~one batch ahead — cross-RG overlap, bounded memory.
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..rg_inputs.len())
        .map(|_| tokio::sync::mpsc::channel::<DaftResult<RecordBatch>>(1))
        .unzip();

    let compute = get_compute_runtime();
    let mut joinset: JoinSet<DaftResult<()>> = JoinSet::new();
    for (rg_pos, (sender, inputs)) in senders.into_iter().zip(rg_inputs).enumerate() {
        let ctx = ctx.clone();
        let rg_idx = rg_indices[rg_pos];
        joinset.spawn_on(
            async move {
                let mut sub_stream = if ctx.plan.data_col_indices.is_empty() {
                    process_rg_predicate_only(ctx.clone(), rg_idx, inputs.selection).await
                } else {
                    process_rg_with_data_cols(ctx, rg_idx, inputs.selection, inputs.pred_arrays)
                        .await
                };
                while let Some(item) = sub_stream.next().await {
                    if sender.send(item).await.is_err() {
                        break;
                    }
                }
                DaftResult::Ok(())
            },
            &compute,
        );
    }

    let inner_streams = receivers.into_iter().map(ReceiverStream::new);
    let merged: BoxStream<'static, DaftResult<RecordBatch>> =
        Box::pin(futures::stream::iter(inner_streams).flatten());
    common_runtime::combine_stream(merged, async move { joinset.join_all().await }).boxed()
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
    // `scan` ends the stream when the closure yields `None`, so once we hit
    // the limit, upstream RG tasks see the receiver close and abort their
    // decoders. `filter_map` would keep polling upstream forever.
    let bounded = stream.scan(limit, |remaining, res| {
        let out = match res {
            Err(e) => Some(Err(e)),
            Ok(_) if *remaining == 0 => None,
            Ok(b) if b.num_rows() <= *remaining => {
                *remaining -= b.num_rows();
                Some(Ok(b))
            }
            Ok(b) => {
                let take = std::mem::replace(remaining, 0);
                Some(b.head(take))
            }
        };
        std::future::ready(out)
    });
    Box::pin(bounded)
}
