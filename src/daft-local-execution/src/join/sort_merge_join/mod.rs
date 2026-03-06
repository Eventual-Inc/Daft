mod anti_semi_join;
mod inner_join;
mod left_join;
mod outer_join;
mod right_join;

use std::{
    cmp::Ordering,
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{
    array::ops::build_multi_array_bicompare, join::JoinType, prelude::SchemaRef, series::Series,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use tracing::Span;

use crate::{
    ExecutionTaskSpawner,
    join::join_operator::{
        BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeResult, ProbeOutput,
        ProbeResult,
    },
    pipeline::NodeName,
    resource_manager::get_or_init_spill_manager,
    sorter::{ExternalSorter, SortParams},
};

// ---------------------------------------------------------------------------
// States
// ---------------------------------------------------------------------------

pub(crate) struct SortMergeJoinBuildState {
    sorter: ExternalSorter,
}

type SortedIterator = Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>;

pub(crate) struct SortMergeJoinProbeState {
    iterator: Arc<Mutex<SortedIterator>>,
    buffer: VecDeque<Arc<MicroPartition>>,
    buffer_size: usize,
    exhausted: bool,
}

// ---------------------------------------------------------------------------
// Params (shared across per-join-type modules, mirrors HashJoinParams)
// ---------------------------------------------------------------------------

pub(crate) struct SortMergeJoinParams {
    pub left_on: Vec<BoundExpr>,
    pub right_on: Vec<BoundExpr>,
    pub left_schema: SchemaRef,
    pub right_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub join_type: JoinType,
    pub memory_limit_bytes: Option<usize>,
    pub spill_batch_size: usize,
}

// ---------------------------------------------------------------------------
// Operator
// ---------------------------------------------------------------------------

pub struct SortMergeJoinOperator {
    params: Arc<SortMergeJoinParams>,
}

impl SortMergeJoinOperator {
    pub fn new(params: SortMergeJoinParams) -> Self {
        Self {
            params: Arc::new(params),
        }
    }
}

// ---------------------------------------------------------------------------
// Buffer management utilities (shared by all join types)
// ---------------------------------------------------------------------------

/// Build a comparator for cross-batch row comparison using Arrow-native sort
/// primitives. Uses `build_multi_array_bicompare` which handles NaN correctly
/// (Daft semantics: NaN > all finite) and avoids per-row Literal allocation.
fn build_row_comparator(
    left_rb: &RecordBatch,
    right_rb: &RecordBatch,
    num_keys: usize,
) -> DaftResult<Box<dyn Fn(usize, usize) -> Ordering + Send + Sync>> {
    let left_cols: Vec<_> = (0..num_keys)
        .map(|i| left_rb.get_column(i).clone())
        .collect();
    let right_cols: Vec<_> = (0..num_keys)
        .map(|i| right_rb.get_column(i).clone())
        .collect();
    // SMJ always uses ascending sort with nulls last for join keys.
    let descending = vec![false; num_keys];
    let nulls_first = vec![false; num_keys];
    build_multi_array_bicompare(&left_cols, &right_cols, &descending, &nulls_first)
}

/// Pre-extracted probe-side key columns for efficient repeated comparisons.
///
/// In hot loops (evict, load, overlap), the probe side stays fixed while the
/// build side changes every iteration.  `ProbeComparator` extracts the probe
/// columns and sort-option vectors once, eliminating redundant per-iteration
/// clones of the probe-side `Series` and `Vec` allocations.  The per-iteration
/// cost of extracting build-side columns and calling `build_multi_array_bicompare`
/// remains, since the build `RecordBatch` differs each time.
struct ProbeComparator {
    probe_cols: Vec<Series>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
}

impl ProbeComparator {
    fn new(probe_keys: &RecordBatch, num_keys: usize) -> Self {
        let probe_cols: Vec<_> = (0..num_keys)
            .map(|i| probe_keys.get_column(i).clone())
            .collect();
        let descending = vec![false; num_keys];
        let nulls_first = vec![false; num_keys];
        Self {
            probe_cols,
            descending,
            nulls_first,
        }
    }

    /// Compare `build_rb[build_idx]` against `probe[probe_idx]`.
    fn compare(
        &self,
        build_rb: &RecordBatch,
        build_idx: usize,
        probe_idx: usize,
    ) -> DaftResult<Ordering> {
        let build_cols: Vec<_> = (0..self.probe_cols.len())
            .map(|i| build_rb.get_column(i).clone())
            .collect();
        let cmp = build_multi_array_bicompare(
            &build_cols,
            &self.probe_cols,
            &self.descending,
            &self.nulls_first,
        )?;
        Ok(cmp(build_idx, probe_idx))
    }
}

/// Binary-search for the upper bound: returns the count of rows <= target.
/// Hoists comparator construction outside the loop for efficiency.
pub(crate) fn find_upper_bound(
    params: &SortMergeJoinParams,
    keys: &RecordBatch,
    target_key_rb: &RecordBatch,
    target_idx: usize,
) -> DaftResult<usize> {
    let cmp = build_row_comparator(keys, target_key_rb, params.left_on.len())?;
    let mut low = 0;
    let mut high = keys.len();
    while low < high {
        let mid = low + (high - low) / 2;
        if cmp(mid, target_idx) != Ordering::Greater {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    Ok(low)
}

/// Extract evaluated join-key columns as a single RecordBatch.
pub(crate) fn get_keys(
    mp: &Arc<MicroPartition>,
    exprs: &[BoundExpr],
) -> DaftResult<Arc<RecordBatch>> {
    let keys = mp.eval_expression_list(exprs)?;
    let rb = keys.concat_or_get()?;
    if let Some(batch) = rb {
        Ok(Arc::new(batch))
    } else {
        Ok(Arc::new(RecordBatch::empty(Some(keys.schema()))))
    }
}

/// Evict build blocks whose max key < probe's min key from the front of the
/// buffer. Returns the evicted blocks (caller decides what to do with them
/// based on join type).
pub(crate) fn evict_build_blocks(
    params: &SortMergeJoinParams,
    state: &mut SortMergeJoinProbeState,
    probe_keys: &RecordBatch,
) -> DaftResult<Vec<Arc<MicroPartition>>> {
    let pcmp = ProbeComparator::new(probe_keys, params.left_on.len());
    let mut evicted = Vec::new();
    while !state.buffer.is_empty() {
        let front = state.buffer.front().unwrap();
        let front_keys = get_keys(front, &params.left_on)?;
        if front_keys.is_empty() {
            let removed = state.buffer.pop_front().unwrap();
            state.buffer_size -= removed.size_bytes();
            continue;
        }
        if pcmp.compare(&front_keys, front_keys.len() - 1, 0)? == Ordering::Less {
            let removed = state.buffer.pop_front().unwrap();
            state.buffer_size -= removed.size_bytes();
            evicted.push(removed);
        } else {
            break;
        }
    }
    Ok(evicted)
}

/// Load more build blocks until we cover probe.last_key or hit mem limit.
/// Returns whether the buffer now covers the full probe range.
pub(crate) fn load_build_blocks(
    params: &SortMergeJoinParams,
    state: &mut SortMergeJoinProbeState,
    probe_keys: &RecordBatch,
) -> DaftResult<bool> {
    let last_probe_idx = probe_keys.len() - 1;
    let mem_limit = crate::resource_manager::resolve_memory_limit(params.memory_limit_bytes);
    let pcmp = ProbeComparator::new(probe_keys, params.left_on.len());

    // Check if existing buffer already covers probe range.
    if !state.buffer.is_empty() {
        let back = state.buffer.back().unwrap();
        let back_keys = get_keys(back, &params.left_on)?;
        if !back_keys.is_empty()
            && pcmp.compare(&back_keys, back_keys.len() - 1, last_probe_idx)? != Ordering::Less
        {
            return Ok(true);
        }
    }

    if state.exhausted {
        return Ok(false);
    }

    let mut covers = false;
    let mut build_iter = state.iterator.lock().unwrap();
    let mut iterator_drained = true;
    for res in build_iter.by_ref() {
        let part = res?;
        if part.is_empty() {
            continue;
        }
        let part_keys = get_keys(&part, &params.left_on)?;
        state.buffer_size += part.size_bytes();
        state.buffer.push_back(part);

        if state.buffer_size as u64 >= mem_limit {
            iterator_drained = false;
            break;
        }
        if !part_keys.is_empty()
            && pcmp.compare(&part_keys, part_keys.len() - 1, last_probe_idx)? != Ordering::Less
        {
            covers = true;
            iterator_drained = false;
            break;
        }
    }
    if iterator_drained {
        state.exhausted = true;
    }
    Ok(covers)
}

/// Determine how many probe rows the current buffer can cover.
/// Returns the slice length.
pub(crate) fn compute_probe_slice_len(
    params: &SortMergeJoinParams,
    state: &SortMergeJoinProbeState,
    probe_keys: &RecordBatch,
) -> DaftResult<usize> {
    let last_build_block = state.buffer.back().unwrap();
    let last_build_keys = get_keys(last_build_block, &params.left_on)?;

    if last_build_keys.is_empty() {
        return Ok(probe_keys.len());
    }

    let ub = find_upper_bound(
        params,
        probe_keys,
        &last_build_keys,
        last_build_keys.len() - 1,
    )?;
    if ub == 0 {
        // Build buffer max key < probe min key — force progress.
        Ok(probe_keys.len())
    } else {
        Ok(ub)
    }
}

/// Result of finding overlapping build blocks for a probe slice.
pub(crate) struct OverlapResult {
    /// Concatenated build-side data from overlapping blocks.
    pub left_mp: Arc<MicroPartition>,
    /// Build-side data sliced to only rows with keys <= probe.last_key.
    /// For Inner/Semi/Right this equals `left_mp`. For Outer/Anti/Left this
    /// is the prefix of `left_mp` that the merge kernel should see, preventing
    /// tail rows (keys > probe.last_key) from being treated as "unmatched".
    pub left_mp_for_join: Arc<MicroPartition>,
    /// Index range [start_block..end_block) in the build buffer.
    pub start_block: usize,
    pub end_block: usize,
}

/// Find overlapping build blocks for the given probe slice and concatenate them.
/// Also computes a sliced version (`left_mp_for_join`) that excludes tail rows
/// beyond the probe's last key — essential for Outer/Anti/Left joins to avoid
/// emitting tail rows as "unmatched" when they might match future probe batches.
pub(crate) fn find_overlapping_build_blocks(
    params: &SortMergeJoinParams,
    state: &SortMergeJoinProbeState,
    probe_slice_keys: &RecordBatch,
    probe_len: usize,
) -> DaftResult<OverlapResult> {
    let pcmp = ProbeComparator::new(probe_slice_keys, params.left_on.len());

    // Find first overlapping block: block.last_key >= probe.first_key
    let mut start_block = state.buffer.len();
    for (i, block) in state.buffer.iter().enumerate() {
        let bk = get_keys(block, &params.left_on)?;
        if !bk.is_empty() && pcmp.compare(&bk, bk.len() - 1, 0)? != Ordering::Less {
            start_block = i;
            break;
        }
    }

    // Find last overlapping block: block.first_key <= probe.last_key
    let probe_last_idx = probe_len - 1;
    let mut end_block = start_block;
    for i in start_block..state.buffer.len() {
        let bk = get_keys(&state.buffer[i], &params.left_on)?;
        if !bk.is_empty() && pcmp.compare(&bk, 0, probe_last_idx)? == Ordering::Greater {
            break;
        }
        end_block = i + 1;
    }

    let overlapping: Vec<_> = state
        .buffer
        .iter()
        .skip(start_block)
        .take(end_block - start_block)
        .cloned()
        .collect();

    let left_mp = Arc::new(MicroPartition::concat_or_empty(
        &overlapping,
        params.left_schema.clone(),
    )?);

    // Compute sliced version for joins that must not see tail rows.
    // For Outer/Anti/Left, the last overlapping block may contain rows with
    // keys > probe.last_key. The merge kernel would treat those as "unmatched"
    // and emit them with NULLs, but the split logic will keep them in the
    // buffer for future probes. We must exclude them from the kernel input.
    let left_mp_for_join = if matches!(
        params.join_type,
        JoinType::Outer | JoinType::Anti | JoinType::Left
    ) && !left_mp.is_empty()
    {
        let left_keys = get_keys(&left_mp, &params.left_on)?;
        let ub = find_upper_bound(params, &left_keys, probe_slice_keys, probe_last_idx)?;
        if ub < left_mp.len() {
            Arc::new(left_mp.slice(0, ub)?)
        } else {
            left_mp.clone()
        }
    } else {
        left_mp.clone()
    };

    Ok(OverlapResult {
        left_mp,
        left_mp_for_join,
        start_block,
        end_block,
    })
}

/// Remove overlapping build blocks after a join.
///
/// All join types use the same split logic: split blocks at probe.last_key
/// boundary and keep rows with keys > probe.last_key for future probes.
/// The merge kernel receives `left_mp_for_join` (sliced to exclude tail rows)
/// so Outer/Anti/Left joins don't emit tail rows as "unmatched" prematurely.
pub(crate) fn remove_or_split_build_blocks_after_join(
    params: &SortMergeJoinParams,
    state: &mut SortMergeJoinProbeState,
    probe_slice_keys: &RecordBatch,
    probe_slice_last_idx: usize,
    start_block: usize,
    end_block: usize,
) -> DaftResult<()> {
    for idx in (start_block..end_block).rev() {
        let block = &state.buffer[idx];
        let bk = get_keys(block, &params.left_on)?;
        if bk.is_empty() {
            state.buffer_size -= block.size_bytes();
            state.buffer.remove(idx);
            continue;
        }

        let split = find_upper_bound(params, &bk, probe_slice_keys, probe_slice_last_idx)?;

        if split >= block.len() {
            // Entire block consumed.
            state.buffer_size -= block.size_bytes();
            state.buffer.remove(idx);
        } else if split > 0 {
            // Partial split: keep rows [split..len).
            let remainder = Arc::new(block.slice(split, block.len())?);
            let old_size = block.size_bytes();
            let new_size = remainder.size_bytes();
            state.buffer[idx] = remainder;
            state.buffer_size = state.buffer_size - old_size + new_size;
        }
        // split == 0: no rows consumed, block stays as-is.
    }
    Ok(())
}

/// Emit unmatched build-side rows as NULL-padded output for Left/Outer joins.
pub(crate) fn emit_unmatched_build_as_left_or_outer(
    params: &SortMergeJoinParams,
    build_mp: &Arc<MicroPartition>,
) -> DaftResult<Arc<MicroPartition>> {
    if build_mp.is_empty() {
        return Ok(Arc::new(MicroPartition::empty(Some(
            params.output_schema.clone(),
        ))));
    }
    let empty_right = MicroPartition::empty(Some(params.right_schema.clone()));
    Ok(Arc::new(MicroPartition::sort_merge_join(
        build_mp,
        &empty_right,
        &params.left_on,
        &params.right_on,
        params.join_type,
        true,
    )?))
}

/// Emit unmatched probe-side rows as NULL-padded output for Right/Outer joins.
pub(crate) fn emit_unmatched_probe_as_right_or_outer(
    params: &SortMergeJoinParams,
    probe_mp: &Arc<MicroPartition>,
) -> DaftResult<Arc<MicroPartition>> {
    if probe_mp.is_empty() {
        return Ok(Arc::new(MicroPartition::empty(Some(
            params.output_schema.clone(),
        ))));
    }
    let empty_left = MicroPartition::empty(Some(params.left_schema.clone()));
    Ok(Arc::new(MicroPartition::sort_merge_join(
        &empty_left,
        probe_mp,
        &params.left_on,
        &params.right_on,
        params.join_type,
        true,
    )?))
}

// ---------------------------------------------------------------------------
// JoinOperator impl (dispatch only)
// ---------------------------------------------------------------------------

impl JoinOperator for SortMergeJoinOperator {
    type BuildState = SortMergeJoinBuildState;
    type FinalizedBuildState =
        Arc<Mutex<Option<Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send>>>>;
    type ProbeState = SortMergeJoinProbeState;

    fn build(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::BuildState,
        _spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self> {
        state.sorter.push(input).map(|()| state).into()
    }

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self> {
        let iter = state.sorter.finish()?;
        Ok(Arc::new(Mutex::new(Some(iter))))
    }

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        let spill_manager = get_or_init_spill_manager(None).clone();
        let params = Arc::new(SortParams {
            sort_by: self.params.left_on.clone(),
            descending: vec![false; self.params.left_on.len()],
            nulls_first: vec![false; self.params.left_on.len()],
        });
        let mem_limit =
            crate::resource_manager::resolve_memory_limit(self.params.memory_limit_bytes);
        Ok(SortMergeJoinBuildState {
            sorter: ExternalSorter::new(
                params,
                spill_manager,
                mem_limit,
                self.params.left_schema.clone(),
                self.params.spill_batch_size,
            ),
        })
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Self::FinalizedBuildState,
    ) -> Self::ProbeState {
        let mut guard = finalized_build_state.lock().unwrap();
        let iter = guard.take().expect(
            "SortMergeJoin requires exactly one probe worker; make_probe_state called twice",
        );
        SortMergeJoinProbeState {
            iterator: Arc::new(Mutex::new(iter)),
            buffer: VecDeque::new(),
            buffer_size: 0,
            exhausted: false,
        }
    }

    fn probe(
        &self,
        input: Arc<MicroPartition>,
        state: Self::ProbeState,
        _spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        if input.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(
                self.params.output_schema.clone(),
            )));
            return Ok((state, ProbeOutput::NeedMoreInput(Some(empty)))).into();
        }

        let params = &self.params;
        let result = match params.join_type {
            JoinType::Inner => inner_join::probe_inner(params, input, state),
            JoinType::Left => left_join::probe_left(params, input, state),
            JoinType::Right => right_join::probe_right(params, input, state),
            JoinType::Outer => outer_join::probe_outer(params, input, state),
            JoinType::Semi => anti_semi_join::probe_semi(params, input, state),
            JoinType::Anti => anti_semi_join::probe_anti(params, input, state),
        };
        result.into()
    }

    fn finalize_probe(
        &self,
        mut states: Vec<Self::ProbeState>,
        spawner: &ExecutionTaskSpawner,
        sender: crate::channel::Sender<Arc<MicroPartition>>,
        runtime_stats: Arc<dyn crate::runtime_stats::RuntimeStats>,
    ) -> ProbeFinalizeResult {
        let state = states.pop().unwrap();
        let params = self.params.clone();

        spawner
            .spawn(
                async move {
                    match params.join_type {
                        JoinType::Inner | JoinType::Semi | JoinType::Right => {
                            // Inner/Semi have no unmatched rows to emit.
                            // Right join: unmatched probe rows were emitted during probe;
                            // no build-side finalization needed.
                            Ok(())
                        }
                        JoinType::Left => {
                            left_join::finalize_left(&params, state, &sender, &runtime_stats).await
                        }
                        JoinType::Outer => {
                            outer_join::finalize_outer(&params, state, &sender, &runtime_stats)
                                .await
                        }
                        JoinType::Anti => {
                            anti_semi_join::finalize_anti(&params, state, &sender, &runtime_stats)
                                .await
                        }
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        match self.params.join_type {
            JoinType::Inner => "Sort Merge Join (Inner)".into(),
            JoinType::Left => "Sort Merge Join (Left)".into(),
            JoinType::Right => "Sort Merge Join (Right)".into(),
            JoinType::Outer => "Sort Merge Join (Outer)".into(),
            JoinType::Anti => "Sort Merge Join (Anti)".into(),
            JoinType::Semi => "Sort Merge Join (Semi)".into(),
        }
    }

    fn op_type(&self) -> NodeType {
        NodeType::SortMergeJoinProbe
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![format!(
            "Sort Merge Join ({:?}, External):",
            self.params.join_type
        )];
        display.push(format!(
            "Left keys: [{}]",
            self.params
                .left_on
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        display.push(format!(
            "Right keys: [{}]",
            self.params
                .right_on
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        display.push(format!(
            "Output schema: {}",
            self.params.output_schema.short_string()
        ));
        display
    }

    fn max_probe_concurrency(&self) -> usize {
        1
    }

    fn needs_probe_finalization(&self) -> bool {
        matches!(
            self.params.join_type,
            JoinType::Left | JoinType::Outer | JoinType::Anti
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cmp::Ordering,
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use common_error::DaftResult;
    use common_metrics::{StatSnapshot, ops::NodeType, snapshot::DefaultSnapshot};
    use daft_core::{
        datatypes::{Field, Int64Array},
        join::JoinType,
        prelude::{DataType, Schema},
        series::IntoSeries,
    };
    use daft_dsl::{expr::bound_expr::BoundExpr, resolved_col};
    use daft_micropartition::MicroPartition;
    use daft_recordbatch::RecordBatch;

    use super::*;
    use crate::{
        join::join_operator::{JoinOperator, ProbeOutput},
        runtime_stats::RuntimeStats,
    };

    /// No-op runtime stats for tests.
    struct MockRuntimeStats;
    impl RuntimeStats for MockRuntimeStats {
        fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
            self
        }
        fn build_snapshot(&self, _ordering: std::sync::atomic::Ordering) -> StatSnapshot {
            StatSnapshot::Default(DefaultSnapshot {
                cpu_us: 0,
                rows_in: 0,
                rows_out: 0,
            })
        }
        fn add_rows_in(&self, _rows: u64) {}
        fn add_rows_out(&self, _rows: u64) {}
        fn add_cpu_us(&self, _cpu_us: u64) {}
    }

    /// Test helper: run an async finalize function, collect all sent blocks.
    fn run_finalize<F, Fut>(f: F) -> Vec<Arc<MicroPartition>>
    where
        F: FnOnce(crate::channel::Sender<Arc<MicroPartition>>, Arc<dyn RuntimeStats>) -> Fut
            + Send
            + 'static,
        Fut: std::future::Future<Output = DaftResult<()>> + Send,
    {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, mut rx) = crate::channel::create_channel(1024);
            let stats: Arc<dyn RuntimeStats> = Arc::new(MockRuntimeStats);
            tokio::spawn(async move {
                f(tx, stats).await.unwrap();
            });
            let mut results = Vec::new();
            while let Some(mp) = rx.recv().await {
                results.push(mp);
            }
            results
        })
    }

    fn make_schema(col_names: &[&str]) -> Arc<Schema> {
        Arc::new(Schema::new(
            col_names
                .iter()
                .map(|n| Field::new(*n, DataType::Int64))
                .collect::<Vec<_>>(),
        ))
    }

    fn make_params_with_type(col_names: &[&str], join_type: JoinType) -> SortMergeJoinParams {
        let schema = make_schema(col_names);
        let left_on: Vec<BoundExpr> = col_names
            .iter()
            .map(|n| BoundExpr::try_new(resolved_col(*n), &schema).unwrap())
            .collect();
        let right_on = left_on.clone();
        SortMergeJoinParams {
            left_on,
            right_on,
            left_schema: schema.clone(),
            right_schema: schema.clone(),
            output_schema: schema.clone(),
            join_type,
            memory_limit_bytes: None,
            spill_batch_size: 8192,
        }
    }

    fn make_params(col_names: &[&str]) -> SortMergeJoinParams {
        make_params_with_type(col_names, JoinType::Inner)
    }

    fn make_mp(col_name: &str, values: Vec<i64>) -> Arc<MicroPartition> {
        let schema = make_schema(&[col_name]);
        if values.is_empty() {
            return Arc::new(MicroPartition::empty(Some(schema)));
        }
        let array = Int64Array::from_vec(col_name, values);
        let rb = RecordBatch::from_nonempty_columns(vec![array.into_series()]).unwrap();
        Arc::new(MicroPartition::new_loaded(schema, Arc::new(vec![rb]), None))
    }

    fn make_rb(col_name: &str, values: Vec<i64>) -> RecordBatch {
        let array = Int64Array::from_vec(col_name, values);
        RecordBatch::from_nonempty_columns(vec![array.into_series()]).unwrap()
    }

    fn make_empty_rb(col_name: &str) -> RecordBatch {
        RecordBatch::empty(Some(make_schema(&[col_name])))
    }

    fn make_probe_state(blocks: Vec<Arc<MicroPartition>>) -> SortMergeJoinProbeState {
        let size: usize = blocks.iter().map(|b| b.size_bytes()).sum();
        let empty_iter: Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send> =
            Box::new(std::iter::empty());
        SortMergeJoinProbeState {
            iterator: Arc::new(Mutex::new(empty_iter)),
            buffer: VecDeque::from(blocks),
            buffer_size: size,
            exhausted: true,
        }
    }

    /// Compare a single row across all join-key columns (test-only helper).
    fn compare_rows(
        params: &SortMergeJoinParams,
        left_rb: &RecordBatch,
        left_idx: usize,
        right_rb: &RecordBatch,
        right_idx: usize,
    ) -> DaftResult<Ordering> {
        let cmp = build_row_comparator(left_rb, right_rb, params.left_on.len())?;
        Ok(cmp(left_idx, right_idx))
    }

    #[test]
    fn test_compare_rows_equal() {
        let params = make_params(&["a"]);
        let left = make_rb("a", vec![10, 20, 30]);
        let right = make_rb("a", vec![10, 20, 30]);
        assert_eq!(
            compare_rows(&params, &left, 1, &right, 1).unwrap(),
            Ordering::Equal
        );
    }

    #[test]
    fn test_compare_rows_less() {
        let params = make_params(&["a"]);
        let left = make_rb("a", vec![10, 20, 30]);
        let right = make_rb("a", vec![15, 25, 35]);
        assert_eq!(
            compare_rows(&params, &left, 0, &right, 0).unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn test_compare_rows_greater() {
        let params = make_params(&["a"]);
        let left = make_rb("a", vec![10, 20, 30]);
        let right = make_rb("a", vec![5, 15, 25]);
        assert_eq!(
            compare_rows(&params, &left, 2, &right, 2).unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_rows_cross_batch() {
        let params = make_params(&["a"]);
        let left = make_rb("a", vec![100]);
        let right = make_rb("a", vec![50, 100, 200]);
        assert_eq!(
            compare_rows(&params, &left, 0, &right, 0).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            compare_rows(&params, &left, 0, &right, 1).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            compare_rows(&params, &left, 0, &right, 2).unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn test_build_row_comparator_basic() {
        let left = make_rb("a", vec![1, 2, 3]);
        let right = make_rb("a", vec![2, 2, 2]);
        let cmp = build_row_comparator(&left, &right, 1).unwrap();
        assert_eq!(cmp(0, 0), Ordering::Less);
        assert_eq!(cmp(1, 1), Ordering::Equal);
        assert_eq!(cmp(2, 2), Ordering::Greater);
    }

    #[test]
    fn test_find_upper_bound_all_less() {
        let params = make_params(&["a"]);
        let keys = make_rb("a", vec![1, 2, 3, 4, 5]);
        let target = make_rb("a", vec![10]);
        assert_eq!(find_upper_bound(&params, &keys, &target, 0).unwrap(), 5);
    }

    #[test]
    fn test_find_upper_bound_all_greater() {
        let params = make_params(&["a"]);
        let keys = make_rb("a", vec![10, 20, 30]);
        let target = make_rb("a", vec![5]);
        assert_eq!(find_upper_bound(&params, &keys, &target, 0).unwrap(), 0);
    }

    #[test]
    fn test_find_upper_bound_mixed() {
        let params = make_params(&["a"]);
        let keys = make_rb("a", vec![1, 3, 5, 7, 9]);
        let target = make_rb("a", vec![5]);
        assert_eq!(find_upper_bound(&params, &keys, &target, 0).unwrap(), 3);
    }

    #[test]
    fn test_find_upper_bound_empty() {
        let params = make_params(&["a"]);
        let keys = make_empty_rb("a");
        let target = make_rb("a", vec![5]);
        assert_eq!(find_upper_bound(&params, &keys, &target, 0).unwrap(), 0);
    }

    #[test]
    fn test_get_keys_extracts_columns() {
        let params = make_params(&["a"]);
        let mp = make_mp("a", vec![10, 20, 30]);
        let keys = get_keys(&mp, &params.left_on).unwrap();
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_find_overlapping_blocks_all_overlap() {
        let params = make_params(&["a"]);
        let state = make_probe_state(vec![
            make_mp("a", vec![1, 2, 3]),
            make_mp("a", vec![4, 5, 6]),
            make_mp("a", vec![7, 8, 9]),
        ]);
        let probe_keys = make_rb("a", vec![1, 5, 9]);
        let result = find_overlapping_build_blocks(&params, &state, &probe_keys, 3).unwrap();
        assert_eq!(result.start_block, 0);
        assert_eq!(result.end_block, 3);
        assert_eq!(result.left_mp.len(), 9);
    }

    #[test]
    fn test_find_overlapping_blocks_partial() {
        let params = make_params(&["a"]);
        let state = make_probe_state(vec![
            make_mp("a", vec![1, 2, 3]),
            make_mp("a", vec![10, 11, 12]),
            make_mp("a", vec![20, 21, 22]),
        ]);
        let probe_keys = make_rb("a", vec![10, 15]);
        let result = find_overlapping_build_blocks(&params, &state, &probe_keys, 2).unwrap();
        assert_eq!(result.start_block, 1);
        assert_eq!(result.end_block, 2);
        assert_eq!(result.left_mp.len(), 3);
    }

    #[test]
    fn test_find_overlapping_blocks_none() {
        let params = make_params(&["a"]);
        let state = make_probe_state(vec![
            make_mp("a", vec![1, 2, 3]),
            make_mp("a", vec![4, 5, 6]),
        ]);
        let probe_keys = make_rb("a", vec![100, 200]);
        let result = find_overlapping_build_blocks(&params, &state, &probe_keys, 2).unwrap();
        assert_eq!(result.start_block, result.end_block);
        assert_eq!(result.left_mp.len(), 0);
    }

    #[test]
    fn test_find_overlapping_blocks_left_mp_for_join_sliced() {
        let params = make_params_with_type(&["a"], JoinType::Outer);
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])]);
        let probe_keys = make_rb("a", vec![3, 4, 5]);
        let result = find_overlapping_build_blocks(&params, &state, &probe_keys, 3).unwrap();
        assert_eq!(result.left_mp.len(), 10);
        assert_eq!(result.left_mp_for_join.len(), 5);
    }

    #[test]
    fn test_find_overlapping_blocks_inner_mp_for_join_unsliced() {
        let params = make_params_with_type(&["a"], JoinType::Inner);
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10])]);
        let probe_keys = make_rb("a", vec![3, 4, 5]);
        let result = find_overlapping_build_blocks(&params, &state, &probe_keys, 3).unwrap();
        assert_eq!(result.left_mp.len(), 10);
        assert_eq!(result.left_mp_for_join.len(), 10);
    }

    #[test]
    fn test_split_removes_fully_consumed_blocks() {
        let params = make_params(&["a"]);
        let mut state = make_probe_state(vec![
            make_mp("a", vec![1, 2, 3]),
            make_mp("a", vec![4, 5, 6]),
        ]);
        let probe_keys = make_rb("a", vec![1, 6]);
        remove_or_split_build_blocks_after_join(&params, &mut state, &probe_keys, 1, 0, 2).unwrap();
        assert!(state.buffer.is_empty());
    }

    #[test]
    fn test_split_keeps_remainder() {
        let params = make_params(&["a"]);
        let mut state = make_probe_state(vec![make_mp("a", vec![1, 2, 3, 4, 5])]);
        let probe_keys = make_rb("a", vec![1, 3]);
        remove_or_split_build_blocks_after_join(&params, &mut state, &probe_keys, 1, 0, 1).unwrap();
        assert_eq!(state.buffer.len(), 1);
        assert_eq!(state.buffer[0].len(), 2);
    }

    #[test]
    fn test_evict_build_blocks_evicts_below_range() {
        let params = make_params(&["a"]);
        let mut state = make_probe_state(vec![
            make_mp("a", vec![1, 2]),
            make_mp("a", vec![3, 4]),
            make_mp("a", vec![10, 11]),
        ]);
        let probe_keys = make_rb("a", vec![5, 6, 7]);
        let evicted = evict_build_blocks(&params, &mut state, &probe_keys).unwrap();
        assert_eq!(evicted.len(), 2);
        assert_eq!(state.buffer.len(), 1);
    }

    #[test]
    fn test_evict_build_blocks_evicts_none() {
        let params = make_params(&["a"]);
        let mut state =
            make_probe_state(vec![make_mp("a", vec![5, 6]), make_mp("a", vec![10, 11])]);
        let probe_keys = make_rb("a", vec![5, 6]);
        let evicted = evict_build_blocks(&params, &mut state, &probe_keys).unwrap();
        assert_eq!(evicted.len(), 0);
        assert_eq!(state.buffer.len(), 2);
    }

    // ========== NEEDS_PROBE_FINALIZATION ==========

    #[test]
    fn test_needs_probe_finalization_per_join_type() {
        for (jt, expected) in [
            (JoinType::Inner, false),
            (JoinType::Semi, false),
            (JoinType::Right, false),
            (JoinType::Left, true),
            (JoinType::Outer, true),
            (JoinType::Anti, true),
        ] {
            let op = SortMergeJoinOperator::new(make_params_with_type(&["a"], jt));
            assert_eq!(
                op.needs_probe_finalization(),
                expected,
                "needs_probe_finalization for {:?}",
                jt
            );
        }
    }

    // ========== MULTI-COLUMN COMPARATOR ==========

    #[test]
    fn test_multi_col_comparator_equal() {
        let params = make_params(&["a", "b"]);
        let left = {
            let a = Int64Array::from_vec("a", vec![1]);
            let b = Int64Array::from_vec("b", vec![10]);
            RecordBatch::from_nonempty_columns(vec![a.into_series(), b.into_series()]).unwrap()
        };
        let right = {
            let a = Int64Array::from_vec("a", vec![1]);
            let b = Int64Array::from_vec("b", vec![10]);
            RecordBatch::from_nonempty_columns(vec![a.into_series(), b.into_series()]).unwrap()
        };
        assert_eq!(
            compare_rows(&params, &left, 0, &right, 0).unwrap(),
            Ordering::Equal
        );
    }

    #[test]
    fn test_multi_col_comparator_first_col_differs() {
        let params = make_params(&["a", "b"]);
        let left = {
            let a = Int64Array::from_vec("a", vec![1]);
            let b = Int64Array::from_vec("b", vec![10]);
            RecordBatch::from_nonempty_columns(vec![a.into_series(), b.into_series()]).unwrap()
        };
        let right = {
            let a = Int64Array::from_vec("a", vec![2]);
            let b = Int64Array::from_vec("b", vec![5]);
            RecordBatch::from_nonempty_columns(vec![a.into_series(), b.into_series()]).unwrap()
        };
        assert_eq!(
            compare_rows(&params, &left, 0, &right, 0).unwrap(),
            Ordering::Less
        );
    }

    // ========== NAME / OP_TYPE ==========

    #[test]
    fn test_operator_name_per_join_type() {
        let expected = [
            (JoinType::Inner, "Sort Merge Join (Inner)"),
            (JoinType::Left, "Sort Merge Join (Left)"),
            (JoinType::Right, "Sort Merge Join (Right)"),
            (JoinType::Outer, "Sort Merge Join (Outer)"),
            (JoinType::Anti, "Sort Merge Join (Anti)"),
            (JoinType::Semi, "Sort Merge Join (Semi)"),
        ];
        for (jt, name) in expected {
            let op = SortMergeJoinOperator::new(make_params_with_type(&["a"], jt));
            assert_eq!(op.name().as_ref(), name, "name for {:?}", jt);
        }
    }

    #[test]
    fn test_operator_op_type() {
        let op = SortMergeJoinOperator::new(make_params(&["a"]));
        assert!(matches!(op.op_type(), NodeType::SortMergeJoinProbe));
    }

    #[test]
    fn test_max_probe_concurrency() {
        let op = SortMergeJoinOperator::new(make_params(&["a"]));
        assert_eq!(op.max_probe_concurrency(), 1);
    }

    // ========== EMIT UNMATCHED ==========

    #[test]
    fn test_emit_unmatched_build_no_matches() {
        let params = make_params_with_type(&["a"], JoinType::Left);
        let build = make_mp("a", vec![1, 2, 3, 4, 5]);
        let result = emit_unmatched_build_as_left_or_outer(&params, &build).unwrap();
        assert_eq!(result.len(), 5, "all build rows unmatched");
    }

    #[test]
    fn test_emit_unmatched_build_empty() {
        let params = make_params_with_type(&["a"], JoinType::Left);
        let build = make_mp("a", vec![]);
        let result = emit_unmatched_build_as_left_or_outer(&params, &build).unwrap();
        assert_eq!(result.len(), 0, "empty build -> 0 rows");
    }

    // ========== PROBE TESTS: INNER ==========

    #[test]
    fn test_probe_inner_basic_match() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Inner));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let input = make_mp("a", vec![1, 2, 3]);
        let (_, output) = inner_join::probe_inner(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(result.len(), 3, "inner probe: 3 matches");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    #[test]
    fn test_probe_inner_no_match() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Inner));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let input = make_mp("a", vec![10, 20, 30]);
        let (_, output) = inner_join::probe_inner(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(result.len(), 0, "inner probe: no matches");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    #[test]
    fn test_probe_inner_empty_build() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Inner));
        let state = make_probe_state(vec![]);
        let input = make_mp("a", vec![1, 2, 3]);
        let (_, output) = inner_join::probe_inner(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(result.len(), 0, "inner probe: empty build -> 0 rows");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    #[test]
    fn test_probe_inner_empty_probe() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Inner));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let input = make_mp("a", vec![]);
        let (_, output) = inner_join::probe_inner(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(result.len(), 0, "inner probe: empty probe -> 0 rows");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    #[test]
    fn test_probe_inner_duplicates() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Inner));
        let state = make_probe_state(vec![make_mp("a", vec![1, 1, 2])]);
        let input = make_mp("a", vec![1, 2]);
        let (_, output) = inner_join::probe_inner(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(
                result.len(),
                3,
                "inner probe: 2 matches for key=1 + 1 for key=2"
            );
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    // ========== PROBE TESTS: LEFT ==========

    #[test]
    fn test_probe_left_all_match() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Left));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let input = make_mp("a", vec![1, 2, 3]);
        let (_, output) = left_join::probe_left(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(result.len(), 3, "left probe: all 3 build rows matched");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    #[test]
    fn test_probe_left_partial_match() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Left));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let input = make_mp("a", vec![2]);
        let (new_state, output) = left_join::probe_left(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            // Left join: overlap gives us build rows 1,2,3 joined with probe [2].
            // merge_left_join: build[0]=1 no match → null, build[1]=2 match, build[2]=3 no match → null
            // So output = 3 rows from the overlapping left_mp_for_join
            assert!(result.len() >= 1, "left probe: at least the match");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
        // Finalize: remaining build blocks should emit unmatched
        let fin = run_finalize(|tx, stats| async move {
            left_join::finalize_left(&params, new_state, &tx, &stats).await
        });
        assert!(!fin.is_empty());
    }

    #[test]
    fn test_probe_left_no_probe_data() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Left));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let input = make_mp("a", vec![]);
        let (new_state, output) = left_join::probe_left(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(
                result.len(),
                0,
                "left probe: empty probe -> 0 rows from probe"
            );
        } else {
            panic!("Expected NeedMoreInput with output");
        }
        // Finalize: all build rows should be emitted as unmatched
        let fin = run_finalize(|tx, stats| async move {
            left_join::finalize_left(&params, new_state, &tx, &stats).await
        });
        assert!(!fin.is_empty());
        assert_eq!(
            fin.iter().map(|b| b.len()).sum::<usize>(),
            3,
            "left finalize: all 3 build rows unmatched"
        );
    }

    // ========== PROBE TESTS: RIGHT ==========

    #[test]
    fn test_probe_right_all_match() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Right));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let input = make_mp("a", vec![1, 2, 3]);
        let (_, output) = right_join::probe_right(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(result.len(), 3, "right probe: all 3 probe rows matched");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    #[test]
    fn test_probe_right_empty_build_emits_probe() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Right));
        let state = make_probe_state(vec![]);
        let input = make_mp("a", vec![1, 2, 3]);
        let (_, output) = right_join::probe_right(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            // Right join with empty build → all probe rows emitted as unmatched
            assert_eq!(
                result.len(),
                3,
                "right probe: empty build -> 3 unmatched probe rows"
            );
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    #[test]
    fn test_probe_right_no_match() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Right));
        let state = make_probe_state(vec![make_mp("a", vec![10, 20, 30])]);
        let input = make_mp("a", vec![1, 2, 3]);
        let (_, output) = right_join::probe_right(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            // Right join preserves all probe rows even without matches
            assert_eq!(
                result.len(),
                3,
                "right probe: 3 unmatched probe rows preserved"
            );
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    // ========== PROBE TESTS: OUTER ==========

    #[test]
    fn test_probe_outer_all_match() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Outer));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let input = make_mp("a", vec![1, 2, 3]);
        let (_, output) = outer_join::probe_outer(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(result.len(), 3, "outer probe: all 3 rows matched");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    #[test]
    fn test_probe_outer_disjoint() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Outer));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let input = make_mp("a", vec![10, 20, 30]);
        let (new_state, output) = outer_join::probe_outer(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            // Outer join: build rows evicted as unmatched (3) + probe rows join with empty build
            // Total should include all 6 rows (3 unmatched build + 3 unmatched probe)
            assert!(
                result.len() >= 3,
                "outer probe: at least 3 unmatched probe rows"
            );
        } else {
            panic!("Expected NeedMoreInput with output");
        }
        // Finalize: all build blocks were evicted during probe for disjoint keys
        let fin = run_finalize(|tx, stats| async move {
            outer_join::finalize_outer(&params, new_state, &tx, &stats).await
        });
        // Build blocks were already emitted as unmatched during probe, so finalize should be empty
        assert_eq!(
            fin.iter().map(|b| b.len()).sum::<usize>(),
            0,
            "finalize should produce 0 rows since build blocks were already evicted during probe"
        );
    }

    #[test]
    fn test_probe_outer_empty_build() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Outer));
        let state = make_probe_state(vec![]);
        let input = make_mp("a", vec![1, 2, 3]);
        let (_, output) = outer_join::probe_outer(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(
                result.len(),
                3,
                "outer probe: empty build -> 3 unmatched probe rows"
            );
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    #[test]
    fn test_finalize_outer_remaining_build() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Outer));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        // Directly finalize without probing -> all build rows emitted as unmatched
        let fin = run_finalize(|tx, stats| async move {
            outer_join::finalize_outer(&params, state, &tx, &stats).await
        });
        assert!(!fin.is_empty());
        assert_eq!(
            fin.iter().map(|b| b.len()).sum::<usize>(),
            3,
            "outer finalize: 3 unmatched build rows"
        );
    }

    // ========== PROBE TESTS: SEMI ==========

    #[test]
    fn test_probe_semi_basic() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Semi));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3, 4, 5])]);
        let input = make_mp("a", vec![2, 4]);
        let (_, output) = anti_semi_join::probe_semi(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(result.len(), 2, "semi probe: 2 matching build rows");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    #[test]
    fn test_probe_semi_no_match() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Semi));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let input = make_mp("a", vec![10, 20]);
        let (_, output) = anti_semi_join::probe_semi(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(result.len(), 0, "semi probe: no matches -> 0 rows");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    #[test]
    fn test_probe_semi_empty_build() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Semi));
        let state = make_probe_state(vec![]);
        let input = make_mp("a", vec![1, 2, 3]);
        let (_, output) = anti_semi_join::probe_semi(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(result.len(), 0, "semi probe: empty build -> 0 rows");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    // ========== PROBE TESTS: ANTI ==========

    #[test]
    fn test_probe_anti_all_unmatched() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Anti));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let input = make_mp("a", vec![10, 20, 30]);
        let (new_state, output) = anti_semi_join::probe_anti(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            // Build rows evicted as unmatched (anti) → output contains build rows
            assert!(result.len() >= 3, "anti probe: 3 unmatched build rows");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
        // Finalize: build blocks were already evicted during probe
        let fin = run_finalize(|tx, stats| async move {
            anti_semi_join::finalize_anti(&params, new_state, &tx, &stats).await
        });
        // Build blocks were already emitted as unmatched during probe, so finalize should be empty
        assert_eq!(
            fin.iter().map(|b| b.len()).sum::<usize>(),
            0,
            "finalize should produce 0 rows since build blocks were already evicted during probe"
        );
    }

    #[test]
    fn test_probe_anti_all_matched() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Anti));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let input = make_mp("a", vec![1, 2, 3]);
        let (new_state, output) = anti_semi_join::probe_anti(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            // Anti join: build rows that DON'T match -> 0 rows when all match
            assert_eq!(result.len(), 0, "anti probe: all matched -> 0 anti rows");
        } else {
            panic!("Expected NeedMoreInput with output");
        }
        // Finalize: no remaining build rows (all matched)
        let fin = run_finalize(|tx, stats| async move {
            anti_semi_join::finalize_anti(&params, new_state, &tx, &stats).await
        });
        assert_eq!(
            fin.iter().map(|b| b.len()).sum::<usize>(),
            0,
            "anti finalize: no remaining build rows"
        );
    }

    #[test]
    fn test_probe_anti_partial_match() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Anti));
        // Build=[1,2,3,4,5], probe=[2,4]. Build row 1 (max key < probe min key 2)
        // is evicted before overlap processing. The overlap region covers build rows
        // [2,3,4,5] vs probe [2,4]. In the overlap, rows 3 and 5 are unmatched.
        // Row 1 (evicted) is emitted as part of the probe output (evicted unmatched).
        // Total output: evicted row 1 + overlap unmatched rows 3, 5 = 3 rows.
        // However, evict_build_blocks only evicts blocks whose ENTIRE max key < probe
        // min key. Since build is a single block [1,2,3,4,5] with max=5 >= probe min=2,
        // it is NOT evicted. The overlap covers the full build block.
        // Anti join on overlap: build [1,2,3,4,5] vs probe [2,4] → unmatched = [1,3,5].
        // But the overlap is sliced: left_mp_for_join is sliced to keys <= probe.last_key=4,
        // so left_mp_for_join = [1,2,3,4]. Anti join: [1,2,3,4] vs [2,4] → unmatched = [1,3] = 2 rows.
        // Row 5 (key > probe.last_key) remains in the buffer for future probe batches.
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3, 4, 5])]);
        let input = make_mp("a", vec![2, 4]);
        let (_, output) = anti_semi_join::probe_anti(&params, input, state).unwrap();
        if let ProbeOutput::NeedMoreInput(Some(result)) = output {
            assert_eq!(
                result.len(),
                2,
                "anti probe: 2 unmatched build rows from overlap (rows 1,3; row 5 held for future)"
            );
        } else {
            panic!("Expected NeedMoreInput with output");
        }
    }

    #[test]
    fn test_finalize_anti_remaining_build() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Anti));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        // Directly finalize without probing -> all build rows emitted
        let fin = run_finalize(|tx, stats| async move {
            anti_semi_join::finalize_anti(&params, state, &tx, &stats).await
        });
        assert!(!fin.is_empty());
        assert_eq!(
            fin.iter().map(|b| b.len()).sum::<usize>(),
            3,
            "anti finalize: 3 unmatched build rows"
        );
    }

    #[test]
    fn test_finalize_left_remaining_build() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Left));
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3])]);
        let fin = run_finalize(|tx, stats| async move {
            left_join::finalize_left(&params, state, &tx, &stats).await
        });
        assert!(!fin.is_empty());
        assert_eq!(
            fin.iter().map(|b| b.len()).sum::<usize>(),
            3,
            "left finalize: 3 unmatched build rows"
        );
    }

    // ========== LOAD_BUILD_BLOCKS ==========

    /// Helper: create a probe state with a live iterator (not exhausted).
    fn make_probe_state_with_iter(
        initial_buffer: Vec<Arc<MicroPartition>>,
        remaining: Vec<Arc<MicroPartition>>,
    ) -> SortMergeJoinProbeState {
        let size: usize = initial_buffer.iter().map(|b| b.size_bytes()).sum();
        let iter: Box<dyn Iterator<Item = DaftResult<Arc<MicroPartition>>> + Send> =
            Box::new(remaining.into_iter().map(Ok));
        SortMergeJoinProbeState {
            iterator: Arc::new(Mutex::new(iter)),
            buffer: VecDeque::from(initial_buffer),
            buffer_size: size,
            exhausted: false,
        }
    }

    #[test]
    fn test_load_build_blocks_single() {
        let params = make_params(&["a"]);
        let build_data = vec![make_mp("a", vec![1, 2, 3])];
        let mut state = make_probe_state_with_iter(vec![], build_data);
        let probe_keys = make_rb("a", vec![1, 2]);
        let covers = load_build_blocks(&params, &mut state, &probe_keys).unwrap();
        assert!(covers, "single block [1,2,3] should cover probe [1,2]");
        assert_eq!(state.buffer.len(), 1);
    }

    #[test]
    fn test_load_build_blocks_multiple() {
        let params = make_params(&["a"]);
        let build_data = vec![
            make_mp("a", vec![1, 2]),
            make_mp("a", vec![3, 4]),
            make_mp("a", vec![5, 6]),
        ];
        let mut state = make_probe_state_with_iter(vec![], build_data);
        let probe_keys = make_rb("a", vec![3, 4, 5]);
        let covers = load_build_blocks(&params, &mut state, &probe_keys).unwrap();
        assert!(covers, "multiple blocks should cover probe [3,4,5]");
        // Should have loaded enough blocks to cover probe.last_key = 5
        assert!(state.buffer.len() >= 2, "at least 2 blocks loaded");
    }

    #[test]
    fn test_load_build_blocks_empty() {
        let params = make_params(&["a"]);
        // No data in iterator at all
        let mut state = make_probe_state_with_iter(vec![], vec![]);
        let probe_keys = make_rb("a", vec![1, 2, 3]);
        let covers = load_build_blocks(&params, &mut state, &probe_keys).unwrap();
        assert!(!covers, "empty build iterator cannot cover any probe");
        assert_eq!(state.buffer.len(), 0);
    }

    // ========== COMPUTE_PROBE_SLICE_LEN ==========

    #[test]
    fn test_compute_probe_slice_len_small_probe() {
        let params = make_params(&["a"]);
        // Build buffer covers [1..100], probe is small [1..10]
        let state = make_probe_state(vec![make_mp("a", (1..=100).collect())]);
        let probe_keys = make_rb("a", (1..=10).collect());
        let slice_len = compute_probe_slice_len(&params, &state, &probe_keys).unwrap();
        // Build max key=100 covers all probe keys, so slice_len should be full probe
        assert_eq!(
            slice_len, 10,
            "small probe fully covered → full probe length"
        );
    }

    #[test]
    fn test_compute_probe_slice_len_large_probe() {
        let params = make_params(&["a"]);
        // Build buffer only covers [1..5], probe goes up to 100
        let state = make_probe_state(vec![make_mp("a", vec![1, 2, 3, 4, 5])]);
        let probe_keys = make_rb("a", (1..=100).collect());
        let slice_len = compute_probe_slice_len(&params, &state, &probe_keys).unwrap();
        // Build max key=5, so upper bound in probe for key<=5 is index 5 (keys 1..5)
        assert!(
            slice_len > 0 && slice_len <= 100,
            "slice_len should be bounded"
        );
        assert_eq!(
            slice_len, 5,
            "only probe rows with key<=5 should be in slice"
        );
    }

    #[test]
    fn test_compute_probe_slice_len_zero_build() {
        let params = make_params(&["a"]);
        // Build buffer has a single empty MP (edge case: last_build_keys is empty)
        let state = make_probe_state(vec![make_mp("a", vec![])]);
        let probe_keys = make_rb("a", vec![1, 2, 3]);
        let slice_len = compute_probe_slice_len(&params, &state, &probe_keys).unwrap();
        // When last_build_keys is empty, returns probe_keys.len()
        assert_eq!(slice_len, 3, "empty build keys → full probe length");
    }

    // ========== FINALIZE INCREMENTAL STREAMING ==========

    #[test]
    fn test_finalize_left_multi_block_buffer_yields_individually() {
        // Task 7.6: Verify that finalize with multi-block build buffer yields
        // blocks individually (one result entry per block), NOT concatenated.
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Left));
        let state = make_probe_state(vec![
            make_mp("a", vec![1, 2]),
            make_mp("a", vec![3, 4]),
            make_mp("a", vec![5, 6, 7]),
        ]);
        let fin = run_finalize(|tx, stats| async move {
            left_join::finalize_left(&params, state, &tx, &stats).await
        });
        // Should yield 3 separate result entries — one per build block.
        assert_eq!(
            fin.len(),
            3,
            "finalize should yield one result per build block"
        );
        assert_eq!(fin[0].len(), 2, "first block: 2 rows");
        assert_eq!(fin[1].len(), 2, "second block: 2 rows");
        assert_eq!(fin[2].len(), 3, "third block: 3 rows");
    }

    #[test]
    fn test_finalize_outer_multi_block_buffer_yields_individually() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Outer));
        let state = make_probe_state(vec![
            make_mp("a", vec![10, 20]),
            make_mp("a", vec![30]),
            make_mp("a", vec![40, 50]),
        ]);
        let fin = run_finalize(|tx, stats| async move {
            outer_join::finalize_outer(&params, state, &tx, &stats).await
        });
        assert_eq!(
            fin.len(),
            3,
            "finalize should yield one result per build block"
        );
        assert_eq!(fin[0].len(), 2);
        assert_eq!(fin[1].len(), 1);
        assert_eq!(fin[2].len(), 2);
    }

    #[test]
    fn test_finalize_anti_multi_block_buffer_yields_individually() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Anti));
        let state = make_probe_state(vec![
            make_mp("a", vec![1]),
            make_mp("a", vec![2, 3]),
            make_mp("a", vec![4, 5, 6]),
            make_mp("a", vec![7]),
        ]);
        let fin = run_finalize(|tx, stats| async move {
            anti_semi_join::finalize_anti(&params, state, &tx, &stats).await
        });
        assert_eq!(
            fin.len(),
            4,
            "finalize should yield one result per build block"
        );
        assert_eq!(fin[0].len(), 1);
        assert_eq!(fin[1].len(), 2);
        assert_eq!(fin[2].len(), 3);
        assert_eq!(fin[3].len(), 1);
    }

    // ========== FINALIZE WITH NON-EXHAUSTED BUILD ITERATOR ==========

    #[test]
    fn test_finalize_left_with_remaining_iterator_blocks() {
        // Task 7.7: Verify that finalize drains both the buffer AND
        // the non-exhausted iterator, yielding blocks individually.
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Left));
        let state = make_probe_state_with_iter(
            vec![make_mp("a", vec![1, 2])], // 1 block in buffer
            vec![
                make_mp("a", vec![3, 4]), // 2 blocks in iterator
                make_mp("a", vec![5, 6, 7]),
            ],
        );
        let fin = run_finalize(|tx, stats| async move {
            left_join::finalize_left(&params, state, &tx, &stats).await
        });
        // 1 from buffer + 2 from iterator = 3 results
        assert_eq!(fin.len(), 3, "finalize should drain buffer + iterator");
        assert_eq!(fin[0].len(), 2, "buffer block: 2 rows");
        assert_eq!(fin[1].len(), 2, "iterator block 1: 2 rows");
        assert_eq!(fin[2].len(), 3, "iterator block 2: 3 rows");
    }

    #[test]
    fn test_finalize_outer_with_remaining_iterator_blocks() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Outer));
        let state = make_probe_state_with_iter(
            vec![], // empty buffer
            vec![make_mp("a", vec![10, 20]), make_mp("a", vec![30, 40, 50])],
        );
        let fin = run_finalize(|tx, stats| async move {
            outer_join::finalize_outer(&params, state, &tx, &stats).await
        });
        // 0 from buffer + 2 from iterator = 2 results
        assert_eq!(
            fin.len(),
            2,
            "finalize should drain iterator even with empty buffer"
        );
        assert_eq!(fin[0].len(), 2);
        assert_eq!(fin[1].len(), 3);
    }

    #[test]
    fn test_finalize_anti_with_remaining_iterator_blocks() {
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Anti));
        let state = make_probe_state_with_iter(
            vec![make_mp("a", vec![1])], // 1 in buffer
            vec![
                make_mp("a", vec![2]), // 3 in iterator
                make_mp("a", vec![3, 4]),
                make_mp("a", vec![5]),
            ],
        );
        let fin = run_finalize(|tx, stats| async move {
            anti_semi_join::finalize_anti(&params, state, &tx, &stats).await
        });
        // 1 from buffer + 3 from iterator = 4 results
        assert_eq!(
            fin.len(),
            4,
            "finalize should drain buffer + all iterator blocks"
        );
        assert_eq!(fin[0].len(), 1, "buffer block");
        assert_eq!(fin[1].len(), 1, "iterator block 1");
        assert_eq!(fin[2].len(), 2, "iterator block 2");
        assert_eq!(fin[3].len(), 1, "iterator block 3");
    }

    #[test]
    fn test_finalize_left_skips_empty_blocks() {
        // Verify that empty blocks in both buffer and iterator are skipped.
        let params = Arc::new(make_params_with_type(&["a"], JoinType::Left));
        let state = make_probe_state_with_iter(
            vec![make_mp("a", vec![]), make_mp("a", vec![1, 2])],
            vec![make_mp("a", vec![]), make_mp("a", vec![3])],
        );
        let fin = run_finalize(|tx, stats| async move {
            left_join::finalize_left(&params, state, &tx, &stats).await
        });
        // Empty blocks are skipped, so only non-empty blocks appear
        assert_eq!(fin.len(), 2, "empty blocks should be skipped");
        assert_eq!(fin[0].len(), 2);
        assert_eq!(fin[1].len(), 1);
    }
}
