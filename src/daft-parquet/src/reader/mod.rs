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
use futures::{StreamExt, stream::BoxStream};
use parquet::{
    arrow::arrow_reader::{ArrowReaderMetadata, RowSelection},
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
    schema_inference::arrow_schema_to_daft_schema,
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

/// Streaming entry point. Returns the projection schema alongside the stream so
/// the bulk variant can preserve schema when the stream produces zero batches
/// (per-RG processors emit `stream::empty()` for 0-row RGs).
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
    let (chunk_source, arrow_metadata, effective_row_groups_owned): (
        ChunkSource,
        ArrowReaderMetadata,
        Option<Vec<i64>>,
    ) = match &source {
        ParquetSource::Local { path } => {
            let (file, file_len, arrow_metadata) = open_local_file(path).await?;
            let cs = ChunkSource::Local(LocalChunkSource {
                file,
                file_len,
                metadata: arrow_metadata.metadata().clone(),
            });
            (cs, arrow_metadata, None)
        }
        ParquetSource::Url {
            uri,
            io_client,
            io_stats,
        } => {
            // Prefetch projection ∪ predicate cols.
            let prefetch_cols_owned: Option<Vec<String>> = columns.map(|cols| {
                let mut acc: Vec<String> = cols.iter().map(|s| (*s).to_string()).collect();
                if let Some(ref pred) = predicate {
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
                .map(|v| v.iter().map(|s| s.as_str()).collect());
            let (cs, arrow_metadata, override_rgs) = fetch_remote_chunk_source(
                uri,
                io_client.clone(),
                io_stats.clone(),
                prefetch_cols_refs.as_deref(),
                row_groups,
                start_offset,
                num_rows,
                field_id_mapping.as_deref(),
            )
            .await?;
            (cs, arrow_metadata, override_rgs)
        }
    };

    // Remote prefetch already pruned by limit/offset; pass that set down.
    let effective_row_groups: Option<&[i64]> = match &effective_row_groups_owned {
        Some(rgs) => Some(rgs.as_slice()),
        None => row_groups,
    };

    let chunk_source = Arc::new(chunk_source);
    stream_parquet_from_source(
        chunk_source,
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
    const DEFAULT_BATCH_SIZE: usize = 128 * 1024;
    let chunk_size: usize = batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);

    let mut parquet_metadata: Arc<ParquetMetaData> = arrow_metadata.metadata().clone();
    if let Some(ref mapping) = field_id_mapping {
        parquet_metadata = apply_field_ids_to_arrowrs_parquet_metadata(parquet_metadata, mapping)?;
    }
    // Strip UTF8 logical type so BYTE_ARRAY decodes as Binary (no validation).
    let raw_encoding = schema_infer_options.string_encoding == StringEncoding::Raw;
    if raw_encoding {
        parquet_metadata = strip_string_types_from_parquet_metadata(parquet_metadata)?;
    }

    let arrow_schema_raw = crate::schema_inference::infer_schema_from_parquet_metadata_arrowrs(
        &parquet_metadata,
        Some(schema_infer_options.coerce_int96_timestamp_unit),
        raw_encoding,
    )
    .map_err(parquet_err)?;
    let arrow_schema = Arc::new(arrow_schema_raw);
    let daft_schema = arrow_schema_to_daft_schema(&arrow_schema)?;

    // Read cols = projection ∪ predicate cols; return cols = projection.
    let user_col_set: Option<HashSet<&str>> = columns.map(|c| c.iter().copied().collect());
    let mut read_col_names: HashSet<String> = user_col_set
        .as_ref()
        .map(|s| s.iter().map(|s| (*s).to_string()).collect())
        .unwrap_or_else(|| daft_schema.field_names().map(|s| s.to_string()).collect());

    let (row_filter_pred_cols, predicate_pushed): (Option<HashSet<String>>, bool) =
        match predicate.as_ref() {
            Some(pred) => match predicate_pushable_cols(pred, &daft_schema) {
                Some(cols) => (Some(cols), true),
                None => {
                    // Predicate not pushable; still read its cols for fallback eval.
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

    if let Some(ref filter_cols) = row_filter_pred_cols {
        for c in filter_cols {
            read_col_names.insert(c.clone());
        }
    }

    let read_daft_schema = project_schema(&daft_schema, &read_col_names);
    let return_daft_schema = match &user_col_set {
        Some(s) => {
            let names: HashSet<String> = s.iter().map(|n| (*n).to_string()).collect();
            project_schema(&daft_schema, &names)
        }
        None => daft_schema.clone(),
    };
    let return_daft_schema = Arc::new(return_daft_schema);

    let rg_indices_vec = prune_row_groups(
        &parquet_metadata,
        row_groups,
        predicate.as_ref(),
        &read_daft_schema,
        path,
    )?;

    if rg_indices_vec.is_empty() {
        let schema = return_daft_schema.clone();
        let s = return_daft_schema.clone();
        return Ok((
            schema,
            futures::stream::once(async move { Ok(RecordBatch::empty(Some(s))) }).boxed(),
        ));
    }

    let total_rgs_in_file = parquet_metadata.num_row_groups();
    let mut rg_global_starts = Vec::with_capacity(total_rgs_in_file);
    {
        let mut cumulative = 0usize;
        for i in 0..total_rgs_in_file {
            rg_global_starts.push(cumulative);
            cumulative += parquet_metadata.row_group(i).num_rows() as usize;
        }
    }
    let rg_row_counts: Vec<usize> = rg_indices_vec
        .iter()
        .map(|&i| parquet_metadata.row_group(i).num_rows() as usize)
        .collect();

    let read_col_indices: Vec<usize> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| read_col_names.contains(f.name()))
        .map(|(i, _)| i)
        .collect();

    // Zero-column read (count-only): emit a row-count batch.
    if read_col_indices.is_empty() {
        let total: usize = rg_row_counts.iter().sum();
        let limited = num_rows.map(|n| n.min(total)).unwrap_or(total);
        let batch = RecordBatch::new_with_size(return_daft_schema.clone(), Vec::new(), limited)?;
        return Ok((
            return_daft_schema,
            futures::stream::once(async move { Ok(batch) }).boxed(),
        ));
    }

    let pred_col_set: HashSet<String> = row_filter_pred_cols
        .as_ref()
        .map(|s| s.iter().cloned().collect())
        .unwrap_or_default();
    let pred_col_indices: Vec<usize> = if predicate_pushed {
        arrow_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| pred_col_set.contains(f.name()))
            .map(|(i, _)| i)
            .collect()
    } else {
        Vec::new()
    };

    // Per-RG base selections: offset + delete_rows + limit (when no predicate).
    // With a predicate, per-RG row counts aren't known until phase 1 finishes,
    // so limit is applied there instead.
    let global_offset = start_offset.unwrap_or(0);
    let push_limit_into_base = predicate.is_none();
    let mut limit_remaining: Option<usize> = if push_limit_into_base { num_rows } else { None };
    let base_selections: Vec<Option<RowSelection>> = rg_indices_vec
        .iter()
        .zip(rg_row_counts.iter())
        .map(|(&rg_idx, &rg_rows)| {
            let mut sel: Option<RowSelection> = None;
            // Skip first N rows in this RG if the global offset falls inside it.
            if global_offset > 0 {
                let rg_global_start = rg_global_starts[rg_idx];
                let rg_global_end = rg_global_start + rg_rows;
                if global_offset >= rg_global_end {
                    sel = Some(RowSelection::from(vec![
                        parquet::arrow::arrow_reader::RowSelector::skip(rg_rows),
                    ]));
                } else if global_offset > rg_global_start {
                    let local_off = global_offset - rg_global_start;
                    sel = combine_selections(
                        sel,
                        Some(build_offset_row_selection(local_off, rg_rows)),
                    );
                }
            }
            if let Some(deletes) = delete_rows
                && !deletes.is_empty()
            {
                let del_sel =
                    build_single_rg_delete_selection(deletes, rg_global_starts[rg_idx], rg_rows);
                sel = combine_selections(sel, Some(del_sel));
            }
            if let Some(ref mut remaining) = limit_remaining {
                let contrib = match &sel {
                    Some(s) => s.row_count(),
                    None => rg_rows,
                };
                if *remaining == 0 {
                    sel = Some(RowSelection::from(vec![
                        parquet::arrow::arrow_reader::RowSelector::skip(rg_rows),
                    ]));
                } else if contrib > *remaining {
                    let cap = *remaining;
                    *remaining = 0;
                    sel = Some(cap_selection_to(sel.as_ref(), cap, rg_rows));
                } else {
                    *remaining -= contrib;
                }
            }
            sel
        })
        .collect();

    let num_rgs = rg_indices_vec.len();

    // Drop trailing all-skip RGs (e.g. those after limit was hit) so we don't
    // spawn decode tasks that skip everything.
    let no_pred_active_rg_count: usize = {
        let mut last = 0;
        for (idx, sel) in base_selections.iter().enumerate() {
            let contributes = match sel {
                Some(s) => s.iter().any(|r| !r.skip),
                None => true,
            };
            if contributes {
                last = idx + 1;
            }
        }
        last
    };

    // Phase 1: decode pred cols across all RGs, evaluate per RG into a bool
    // mask, build a refined RowSelection for phase 2. Pred arrays are kept and
    // reused during assembly so cols in both predicate and projection (e.g.
    // `where(col > 50)`) are decoded once.
    let data_col_indices: Vec<usize> = if predicate_pushed {
        read_col_indices
            .iter()
            .copied()
            .filter(|i| !pred_col_indices.contains(i))
            .collect()
    } else {
        read_col_indices.clone()
    };

    // Skip bool→RowSelection conversion when the projection is fully pred
    // cols — the bool mask alone suffices for assembly.
    let need_phase2_selections = !data_col_indices.is_empty();

    // Chunked-pred path (stream pred cols + eval per chunk inside the per-RG
    // processor) is preferable when:
    //  - there's a limit (early-stop within an RG); OR
    //  - there are few RGs (chunked-pred per-RG overhead is small).
    // For many-RG no-limit cases, the phase-1 path (decode all pred cols in
    // PARALLEL across RGs, then emit pre-filtered slices) wins because the
    // RG-sequential nature of chunked-pred adds up.
    let many_rg_threshold = 4;
    let is_chunked_pred = predicate_pushed
        && data_col_indices.is_empty()
        && (num_rows.is_some() || rg_indices_vec.len() <= many_rg_threshold);

    // Spawn pred decode for ALL RGs in parallel; await in RG order so the
    // limit can short-circuit early. Dropped handles abort their tasks.
    type PredPhase1Out = (
        Vec<Option<RowSelection>>,
        Vec<Vec<ArrayRef>>,
        Vec<Option<BooleanArray>>,
        usize,
    );
    let (final_selections, pred_arrays, pred_masks, active_rg_count): PredPhase1Out =
        if predicate_pushed && !is_chunked_pred {
            let compute = get_compute_runtime();
            let mut pred_rg_handles = Vec::with_capacity(num_rgs);
            let pred_leaves: Arc<[usize]> =
                leaves_for_top_fields(&parquet_metadata, &pred_col_indices).into();
            for rg_pos in 0..num_rgs {
                let rg_idx = rg_indices_vec[rg_pos];
                let pred_cols_arc: Arc<[usize]> = pred_col_indices.clone().into();
                let source_c = chunk_source.clone();
                let metadata_c = parquet_metadata.clone();
                let arrow_schema_c = arrow_schema.clone();
                let selection_c = base_selections[rg_pos].clone();
                let pred_leaves_c = pred_leaves.clone();
                let h = compute.spawn(async move {
                    // Batch-read all pred-col leaves for this RG.
                    let rg_chunks = Arc::new(
                        source_c
                            .read_rg_chunks(rg_idx, pred_leaves_c.clone())
                            .await?,
                    );
                    let compute_inner = get_compute_runtime();
                    let mut col_handles = Vec::with_capacity(pred_cols_arc.len());
                    for &col_idx in pred_cols_arc.iter() {
                        let chunks_i = rg_chunks.clone();
                        let metadata_i = metadata_c.clone();
                        let arrow_field = arrow_schema_c.field(col_idx).clone();
                        let selection_i = selection_c.clone();
                        col_handles.push(compute_inner.spawn(async move {
                            decode_one(
                                chunks_i.as_ref(),
                                &metadata_i,
                                rg_idx,
                                col_idx,
                                &arrow_field,
                                selection_i.as_ref(),
                            )
                        }));
                    }
                    let mut arrays = Vec::with_capacity(col_handles.len());
                    for ch in col_handles {
                        arrays.push(ch.await??);
                    }
                    DaftResult::Ok(arrays)
                });
                pred_rg_handles.push(h);
            }

            let pred_for_eval = predicate.clone().unwrap();
            let mut remaining = num_rows.unwrap_or(usize::MAX);
            let mut pred_per_rg: Vec<Vec<ArrayRef>> = Vec::new();
            let mut masks: Vec<Option<BooleanArray>> = Vec::new();
            let mut refined: Vec<Option<RowSelection>> = Vec::new();

            for (rg_pos, handle) in pred_rg_handles.into_iter().enumerate() {
                if remaining == 0 {
                    drop(handle);
                    continue;
                }
                let arrays = handle.await??;

                let mut fields = Vec::with_capacity(pred_col_indices.len());
                for &col_idx in &pred_col_indices {
                    fields.push(Arc::new(arrow_schema.field(col_idx).clone()));
                }
                let pred_arrow_schema = Arc::new(ArrowSchema::new(fields));
                let pred_batch = ArrowRecordBatch::try_new(pred_arrow_schema, arrays.clone())
                    .map_err(parquet_err)?;
                let daft_pred = RecordBatch::try_from(&pred_batch)?;

                let pred_for_rg = substitute_missing_cols(&pred_for_eval, &daft_pred.schema)?;
                let bound = BoundExpr::try_new(pred_for_rg, &daft_pred.schema)?;
                let mask_series = daft_pred.eval_expression(&bound)?;
                let bool_arr = mask_series.bool()?;
                let mut arrow_bool: BooleanArray = bool_arr.as_arrow()?.clone();

                let true_count = arrow_bool.true_count();
                if true_count > remaining {
                    arrow_bool = truncate_mask_to_n_trues(&arrow_bool, remaining);
                    remaining = 0;
                } else {
                    remaining -= true_count;
                }

                if need_phase2_selections {
                    let mut pred_sel = bool_array_to_row_selection(&arrow_bool);
                    let final_sel = match &base_selections[rg_pos] {
                        Some(base) => refine_selection(base, &pred_sel),
                        None => std::mem::take(&mut pred_sel),
                    };
                    refined.push(Some(final_sel));
                } else {
                    refined.push(None);
                }
                masks.push(Some(arrow_bool));
                pred_per_rg.push(arrays);
            }

            let active = pred_per_rg.len();
            // Transpose [rg][col] → [col][rg].
            let mut pred_arrays: Vec<Vec<ArrayRef>> = (0..pred_col_indices.len())
                .map(|_| Vec::with_capacity(active))
                .collect();
            for arrays in pred_per_rg {
                for (col_pos, arr) in arrays.into_iter().enumerate() {
                    pred_arrays[col_pos].push(arr);
                }
            }

            (refined, pred_arrays, masks, active)
        } else {
            (
                base_selections.clone(),
                Vec::new(),
                Vec::new(),
                no_pred_active_rg_count,
            )
        };

    // Per-RG sub-streams flattened in order; peak memory bounded to
    // chunk_size × num_cols + filtered pred arrays for the active RG.
    let return_schema_for_stream = return_daft_schema.clone();
    let arrow_schema_for_stream = arrow_schema.clone();
    let predicate_for_fallback = predicate.clone();
    let read_col_indices_arc: Arc<[usize]> = read_col_indices.clone().into();
    let pred_col_indices_arc: Arc<[usize]> = pred_col_indices.clone().into();
    let data_col_indices_arc: Arc<[usize]> = data_col_indices.clone().into();
    let final_selections_arc: Arc<[Option<RowSelection>]> = final_selections.into();
    let rg_indices_vec_arc: Arc<[usize]> = rg_indices_vec.clone().into();
    let pred_arrays_arc = Arc::new(pred_arrays);
    let pred_masks_arc = Arc::new(pred_masks);
    let parquet_metadata_for_stream = parquet_metadata.clone();
    let source_for_stream = chunk_source.clone();

    let remaining_atomic = Arc::new(std::sync::atomic::AtomicUsize::new(
        num_rows.unwrap_or(usize::MAX),
    ));
    let predicate_for_chunked = predicate.clone();
    let stream = futures::stream::iter(0..active_rg_count)
        .then(move |rg_pos| {
            let source = source_for_stream.clone();
            let metadata = parquet_metadata_for_stream.clone();
            let arrow_schema = arrow_schema_for_stream.clone();
            let read_col_indices = read_col_indices_arc.clone();
            let pred_col_indices = pred_col_indices_arc.clone();
            let data_col_indices = data_col_indices_arc.clone();
            let final_selections = final_selections_arc.clone();
            let rg_indices_vec = rg_indices_vec_arc.clone();
            let pred_arrays = pred_arrays_arc.clone();
            let pred_masks = pred_masks_arc.clone();
            let return_daft_schema = return_schema_for_stream.clone();
            let predicate_for_fallback = predicate_for_fallback.clone();
            let predicate_for_chunked = predicate_for_chunked.clone();
            let remaining_atomic = remaining_atomic.clone();
            async move {
                let rg_idx = rg_indices_vec[rg_pos];
                if is_chunked_pred {
                    process_rg_chunked_pred(
                        rg_idx,
                        source,
                        metadata,
                        arrow_schema,
                        pred_col_indices,
                        read_col_indices,
                        final_selections[rg_pos].clone(),
                        predicate_for_chunked.unwrap(),
                        chunk_size,
                        remaining_atomic,
                        return_daft_schema,
                    )
                    .await
                } else {
                    process_rg_streaming(
                        rg_pos,
                        rg_idx,
                        source,
                        metadata,
                        arrow_schema,
                        read_col_indices,
                        pred_col_indices,
                        data_col_indices,
                        final_selections[rg_pos].clone(),
                        pred_arrays,
                        pred_masks,
                        chunk_size,
                        return_daft_schema,
                        predicate_for_fallback,
                        predicate_pushed,
                    )
                    .await
                }
            }
        })
        .flatten();

    // Cross-RG limit: trim the final batch to land on the cap.
    let limited: BoxStream<'static, DaftResult<RecordBatch>> = if let Some(limit) = num_rows {
        let mut remaining: usize = limit;
        let bounded = stream.filter_map(move |res| {
            let out = match res {
                Err(e) => Some(Err(e)),
                Ok(b) if remaining == 0 => {
                    let _ = b;
                    None
                }
                Ok(b) => {
                    if b.num_rows() <= remaining {
                        remaining -= b.num_rows();
                        Some(Ok(b))
                    } else {
                        let take = remaining;
                        remaining = 0;
                        Some(b.head(take))
                    }
                }
            };
            async move { out }
        });
        Box::pin(bounded)
    } else {
        Box::pin(stream)
    };

    Ok((return_daft_schema, limited))
}

/// Bulk variant: collect the full stream into one concatenated `RecordBatch`.
/// Returns an empty batch if the stream produced none.
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
        // Bulk callers expect schema-bearing output even for empty parquet.
        return Ok(RecordBatch::empty(Some(schema)));
    }
    RecordBatch::concat(&batches)
}
