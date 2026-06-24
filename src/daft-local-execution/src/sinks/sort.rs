use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};

use arrow_row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_schema::SortOptions;
use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkOutput, BlockingSinkSinkResult,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    resource_manager::{MemoryReservation, get_or_init_memory_manager},
    spill::{SpillConfig, SpillRunReader, SpillRunWriter, SpilledRun},
};

/// Each spilled sorted run is written in batches of at most this many rows so the k-way merge only
/// holds one such batch per run resident at a time.
const MERGE_RUN_BATCH_ROWS: usize = 64 * 1024;

/// Build the arrow-row `SortField`s for the sort keys, encoding per-key `descending` / `nulls_first`
/// so that byte-order of the encoded rows matches the desired total sort order.
fn build_sort_fields(
    sort_by: &[BoundExpr],
    descending: &[bool],
    nulls_first: &[bool],
    schema: &SchemaRef,
) -> DaftResult<Vec<SortField>> {
    sort_by
        .iter()
        .zip(descending.iter())
        .zip(nulls_first.iter())
        .map(|((e, desc), nf)| {
            let dtype = e.as_ref().to_field(schema)?.dtype.to_arrow()?;
            Ok(SortField::new_with_options(
                dtype,
                SortOptions {
                    descending: *desc,
                    nulls_first: *nf,
                },
            ))
        })
        .collect()
}

/// Encode the sort-key columns of `batch` into arrow-row `Rows` (lexicographically comparable).
fn encode_keys(
    batch: &RecordBatch,
    sort_by: &[BoundExpr],
    converter: &RowConverter,
) -> DaftResult<Rows> {
    let key_rb = batch.eval_expression_list(sort_by)?;
    let arrow_rb: arrow_array::RecordBatch = key_rb.try_into()?;
    converter
        .convert_columns(&arrow_rb.columns().to_vec())
        .map_err(DaftError::from)
}

pub(crate) struct SortState {
    /// In-memory, not-yet-sorted morsels.
    buffer: Vec<MicroPartition>,
    buffer_bytes: usize,
    /// Sorted runs already spilled to disk by this state.
    runs: Vec<SpilledRun>,
    /// Round-robin index into the spill dirs.
    spill_rr: usize,
    /// Resident reservation against the shared spill pool (only used when spilling is enabled).
    reservation: MemoryReservation,
}

struct SortParams {
    sort_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    schema: SchemaRef,
    /// `Some` enables external merge sort (spill-to-disk). `None` keeps everything in memory.
    spill_config: Option<SpillConfig>,
    /// Target rows per output morsel emitted during the merge.
    output_morsel_rows: usize,
}

pub struct SortSink {
    params: Arc<SortParams>,
}

impl SortSink {
    pub fn new(
        sort_by: Vec<BoundExpr>,
        descending: Vec<bool>,
        nulls_first: Vec<bool>,
        schema: SchemaRef,
        spill_config: Option<SpillConfig>,
        output_morsel_rows: usize,
    ) -> Self {
        // Only enable spilling when every sort-key type can be encoded by arrow-row (the k-way
        // merge relies on it). Otherwise fall back to in-memory sort — correctness over OOM-safety
        // for exotic key types. The opt-out via threshold is handled at build time now.
        let spill_config = spill_config.filter(|_sc| {
            build_sort_fields(&sort_by, &descending, &nulls_first, &schema)
                .map(|fields| RowConverter::supports_fields(&fields))
                .unwrap_or(false)
        });
        Self {
            params: Arc::new(SortParams {
                sort_by,
                descending,
                nulls_first,
                schema,
                spill_config,
                output_morsel_rows,
            }),
        }
    }
}

impl BlockingSink for SortSink {
    type State = SortState;

    #[instrument(skip_all, name = "SortSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    let mut state = state;

                    state.buffer_bytes += input.size_bytes();
                    let added = input.size_bytes() as u64;
                    state.buffer.push(input);

                    if let Some(sc) = &params.spill_config {
                        let cap = sc.cap();
                        let over_cap =
                            cap.is_some_and(|c| state.reservation.held() + added > c);
                        if over_cap || !state.reservation.try_grow(added) {
                            // Spill the entire in-memory buffer as one sorted run.
                            let SortState {
                                buffer,
                                buffer_bytes,
                                runs,
                                spill_rr,
                                reservation,
                            } = &mut state;
                            let to_spill = std::mem::take(buffer);
                            *buffer_bytes = 0;
                            let sorted = MicroPartition::concat(to_spill)?.sort(
                                &params.sort_by,
                                &params.descending,
                                &params.nulls_first,
                            )?;
                            if sorted.len() > 0 {
                                let dir = &sc.spill_dirs[*spill_rr % sc.spill_dirs.len()];
                                *spill_rr += 1;
                                let mut writer =
                                    SpillRunWriter::open(dir, "daft_sort_run_", &params.schema)?;
                                for rb in sorted.record_batches() {
                                    let n = rb.len();
                                    let mut start = 0;
                                    while start < n {
                                        let end = (start + MERGE_RUN_BATCH_ROWS).min(n);
                                        writer.write_batch(&rb.slice(start, end)?)?;
                                        start = end;
                                    }
                                }
                                runs.push(writer.finish()?);
                            }
                            // Everything in memory was spilled; release the reservation.
                            let held = reservation.held();
                            reservation.shrink(held);
                        }
                    }
                    Ok(state)
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "SortSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.params.clone();
        spawner
            .spawn(
                async move {
                    let mut all_runs: Vec<SpilledRun> = vec![];
                    let mut all_buffers: Vec<MicroPartition> = vec![];
                    for state in states {
                        all_buffers.extend(state.buffer);
                        all_runs.extend(state.runs);
                    }

                    // Fast path: nothing spilled — behave exactly like the in-memory sort.
                    if all_runs.is_empty() {
                        // No data at all: still emit one empty partition carrying the schema.
                        if all_buffers.is_empty() {
                            return Ok(BlockingSinkOutput::Partitions(vec![
                                MicroPartition::empty(Some(params.schema.clone())),
                            ]));
                        }
                        let concated = MicroPartition::concat(all_buffers)?;
                        let sorted = concated.sort(
                            &params.sort_by,
                            &params.descending,
                            &params.nulls_first,
                        )?;
                        return Ok(BlockingSinkOutput::Partitions(vec![sorted]));
                    }

                    // Sort the in-memory residual into a final (in-memory) run.
                    let tail = if all_buffers.is_empty() {
                        None
                    } else {
                        let sorted = MicroPartition::concat(all_buffers)?.sort(
                            &params.sort_by,
                            &params.descending,
                            &params.nulls_first,
                        )?;
                        (sorted.len() > 0).then_some(sorted)
                    };

                    let mut merged = kway_merge(all_runs, tail, &params)?;
                    // The merge can produce zero morsels if every run was empty; always emit at
                    // least one partition so downstream consumers see the schema.
                    if merged.is_empty() {
                        merged.push(MicroPartition::empty(Some(params.schema.clone())));
                    }
                    Ok(BlockingSinkOutput::Partitions(merged))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Sort".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Sort
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut lines = vec![];
        assert!(!self.params.sort_by.is_empty());
        let pairs = self
            .params
            .sort_by
            .iter()
            .zip(self.params.descending.iter())
            .zip(self.params.nulls_first.iter())
            .map(|((sb, d), nf)| {
                format!(
                    "({}, {}, {})",
                    sb,
                    if *d { "descending" } else { "ascending" },
                    if *nf { "nulls first" } else { "nulls last" }
                )
            })
            .join(", ");
        lines.push(format!("Sort: Sort by = {}", pairs));
        if self.params.spill_config.is_some() {
            lines.push("Spill: enabled (external merge sort)".to_string());
        }
        lines
    }

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        Ok(SortState {
            buffer: Vec::new(),
            buffer_bytes: 0,
            runs: Vec::new(),
            spill_rr: 0,
            reservation: get_or_init_memory_manager().reservation(),
        })
    }

    fn max_concurrency(&self) -> usize {
        1
    }
}

/// One participant in the k-way merge: an iterator over sorted batches plus the currently-resident
/// batch and its encoded key rows.
struct MergeSource {
    iter: Box<dyn Iterator<Item = DaftResult<RecordBatch>> + Send>,
    batch: RecordBatch,
    rows: Rows,
    cursor: usize,
    /// Bumped each time a new batch is loaded; used to detect output-run boundaries.
    generation: u64,
}

impl MergeSource {
    /// Load the next non-empty batch; returns `false` when the source is exhausted.
    fn advance_batch(
        &mut self,
        sort_by: &[BoundExpr],
        converter: &RowConverter,
    ) -> DaftResult<bool> {
        match load_next_encoded(&mut *self.iter, sort_by, converter)? {
            Some((batch, rows)) => {
                self.batch = batch;
                self.rows = rows;
                self.cursor = 0;
                self.generation += 1;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

/// Pull the next non-empty batch from `iter` and encode its sort keys.
fn load_next_encoded(
    iter: &mut (dyn Iterator<Item = DaftResult<RecordBatch>> + Send),
    sort_by: &[BoundExpr],
    converter: &RowConverter,
) -> DaftResult<Option<(RecordBatch, Rows)>> {
    for batch in iter {
        let batch = batch?;
        if batch.len() == 0 {
            continue;
        }
        let rows = encode_keys(&batch, sort_by, converter)?;
        return Ok(Some((batch, rows)));
    }
    Ok(None)
}

/// Accumulates a maximal run of consecutive output rows from the same source batch so it can be
/// emitted as a single zero-copy `slice` rather than row-by-row gathers.
struct RunAccum {
    batch: RecordBatch,
    src_idx: usize,
    generation: u64,
    start: usize,
    len: usize,
}

/// Streaming k-way merge of pre-sorted runs (on disk) plus an optional in-memory tail run.
/// Keeps only one batch per run resident; emits globally key-sorted morsels of
/// `params.output_morsel_rows` rows.
fn kway_merge(
    runs: Vec<SpilledRun>,
    tail: Option<MicroPartition>,
    params: &SortParams,
) -> DaftResult<Vec<MicroPartition>> {
    let sort_fields = build_sort_fields(
        &params.sort_by,
        &params.descending,
        &params.nulls_first,
        &params.schema,
    )?;
    if !RowConverter::supports_fields(&sort_fields) {
        // Defensive: spilling is disabled up-front for unsupported key types, so this should be
        // unreachable. Fall back to reading everything back and sorting in memory.
        return merge_fallback(runs, tail, params);
    }
    let converter = RowConverter::new(sort_fields).map_err(DaftError::from)?;

    // Build sources: one per spilled run, plus the in-memory tail.
    let mut iters: Vec<Box<dyn Iterator<Item = DaftResult<RecordBatch>> + Send>> = Vec::new();
    for run in &runs {
        iters.push(Box::new(SpillRunReader::open(&run.path)?));
    }
    if let Some(tail) = tail {
        let batches: Vec<RecordBatch> = tail.record_batches().to_vec();
        iters.push(Box::new(batches.into_iter().map(Ok)));
    }

    let mut sources: Vec<Option<MergeSource>> = Vec::with_capacity(iters.len());
    let mut heap: BinaryHeap<Reverse<(OwnedRow, usize)>> = BinaryHeap::new();
    for mut iter in iters {
        let idx = sources.len();
        match load_next_encoded(&mut *iter, &params.sort_by, &converter)? {
            Some((batch, rows)) => {
                let head = rows.row(0).owned();
                sources.push(Some(MergeSource {
                    iter,
                    batch,
                    rows,
                    cursor: 0,
                    generation: 0,
                }));
                heap.push(Reverse((head, idx)));
            }
            None => sources.push(None),
        }
    }

    let mut out_morsels: Vec<MicroPartition> = vec![];
    let mut out_slices: Vec<RecordBatch> = vec![];
    let mut buffered_rows = 0usize;
    let mut run: Option<RunAccum> = None;

    let flush_run = |run: &mut Option<RunAccum>, out_slices: &mut Vec<RecordBatch>| -> DaftResult<()> {
        if let Some(r) = run.take() {
            out_slices.push(r.batch.slice(r.start, r.start + r.len)?);
        }
        Ok(())
    };

    while let Some(Reverse((_row, src_idx))) = heap.pop() {
        let (batch_clone, generation, row_idx, batch_len) = {
            let src = sources[src_idx].as_ref().expect("popped source must exist");
            (src.batch.clone(), src.generation, src.cursor, src.batch.len())
        };

        // Extend the current output run, or flush it and start a new one.
        match &mut run {
            Some(r)
                if r.src_idx == src_idx
                    && r.generation == generation
                    && r.start + r.len == row_idx =>
            {
                r.len += 1;
            }
            _ => {
                flush_run(&mut run, &mut out_slices)?;
                run = Some(RunAccum {
                    batch: batch_clone,
                    src_idx,
                    generation,
                    start: row_idx,
                    len: 1,
                });
            }
        }
        buffered_rows += 1;

        // Advance the popped source and push its new head (loading the next batch if needed).
        {
            let src = sources[src_idx].as_mut().unwrap();
            src.cursor += 1;
        }
        let next_cursor = sources[src_idx].as_ref().unwrap().cursor;
        if next_cursor >= batch_len {
            let advanced = sources[src_idx]
                .as_mut()
                .unwrap()
                .advance_batch(&params.sort_by, &converter)?;
            if advanced {
                let head = sources[src_idx].as_ref().unwrap().rows.row(0).owned();
                heap.push(Reverse((head, src_idx)));
            } else {
                sources[src_idx] = None;
            }
        } else {
            let head = sources[src_idx]
                .as_ref()
                .unwrap()
                .rows
                .row(next_cursor)
                .owned();
            heap.push(Reverse((head, src_idx)));
        }

        if buffered_rows >= params.output_morsel_rows {
            flush_run(&mut run, &mut out_slices)?;
            let morsel = RecordBatch::concat(&out_slices)?;
            out_morsels.push(MicroPartition::new_loaded(
                params.schema.clone(),
                Arc::new(vec![morsel]),
                None,
            ));
            out_slices.clear();
            buffered_rows = 0;
        }
    }

    flush_run(&mut run, &mut out_slices)?;
    if !out_slices.is_empty() {
        let morsel = RecordBatch::concat(&out_slices)?;
        out_morsels.push(MicroPartition::new_loaded(
            params.schema.clone(),
            Arc::new(vec![morsel]),
            None,
        ));
    }

    Ok(out_morsels)
}

/// In-memory fallback used only when the sort keys cannot be arrow-row encoded (spilling should
/// have been disabled up-front in that case): read every spilled run back and sort it all at once.
fn merge_fallback(
    runs: Vec<SpilledRun>,
    tail: Option<MicroPartition>,
    params: &SortParams,
) -> DaftResult<Vec<MicroPartition>> {
    let mut batches: Vec<RecordBatch> = vec![];
    for run in &runs {
        for batch in SpillRunReader::open(&run.path)? {
            batches.push(batch?);
        }
    }
    let mut mps = vec![MicroPartition::new_loaded(
        params.schema.clone(),
        Arc::new(batches),
        None,
    )];
    if let Some(tail) = tail {
        mps.push(tail);
    }
    let sorted = MicroPartition::concat(mps)?.sort(
        &params.sort_by,
        &params.descending,
        &params.nulls_first,
    )?;
    Ok(vec![sorted])
}
