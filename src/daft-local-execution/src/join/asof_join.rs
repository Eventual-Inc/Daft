use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::Arc,
};

use arrow_array::Array;
use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_core::{
    array::ops::{
        GroupIndices, VecIndices, arrow::comparison::build_multi_array_is_equal_from_arrays,
    },
    kernels::search_sorted::{DynPartialComparator, build_partial_compare_with_nulls},
    prelude::{DataType as DaftDataType, Field, Schema, SchemaRef, Series, UInt64Array},
};
use daft_dsl::{
    Expr,
    expr::bound_expr::BoundExpr,
    join::{get_right_cols_to_drop, infer_asof_join_schema},
};
use daft_groupby::IntoGroups;
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
};

// ASOF join: for each left row, find the right row with the largest on_key <= left.on_key
// (the most-recent right event at or before the left event).
//
// BUILD: all left RecordBatches are accumulated before any processing begins (the sort and
//        group structure require the full left table). Once all left input has arrived,
//        finalize_build groups rows by by_key, sorts each group by on_key ascending,
//        materialises per-group on_key arrays for cache-friendly binary search, and builds
//        a hash map (by_key hash -> group index) for O(1) group lookup during probe.
//
// PROBE: right batches are processed concurrently — each worker holds its own ProbeState
//        (best_match array + the right RBs it has seen). For each right row r, hash
//        r.by_key to find its group, binary-search that group's sorted on_key array for
//        the first left row with on_key >= r.on_key, then record r as that left row's best
//        match if r.on_key > the current best (i.e. r is a closer, more-recent event).
//
//   Left table:                        Right table (2 RBs, one per worker):
//     idx | by_key | on_key              RB0: idx | by_key | on_key
//     ----+--------+-------                  -----+--------+-------
//      0  |   A    |   1                       0  |   A    |   2
//      1  |   A    |   3              RB1: idx | by_key | on_key
//      2  |   A    |   5                  -----+--------+-------
//      3  |   B    |   2                   0  |   B    |   4
//      4  |   B    |   7
//
//   After build: group A sorted indices=[0,1,2] (on_keys=[1,3,5]),
//                group B sorted indices=[3,4]   (on_keys=[2,7])
//
//   Probe (each worker processes its RB independently, binary-searching within each group):
//     RB0 r0(by=A, on=2) -> group A, first left >= 2 is idx=1 (on=3) -> worker0.best_match[1] = (RB0, r0)
//     RB1 r0(by=B, on=4) -> group B, first left >= 4 is idx=4 (on=7) -> worker1.best_match[4] = (RB1, r0)
//
//   Per-worker best_match after probe (each keyed by left idx):
//     worker0:  idx 1 -> (RB0, r0, on=2)        worker1:  idx 4 -> (RB1, r0, on=4)
//
// FINALIZE: merge the per-worker best_match arrays (each worker may have seen different
//           right batches, so their candidates are compared and the overall best kept),
//           then forward_fill so left rows with no direct match inherit the match of their
//           preceding row in the same group.
//
//   After forward_fill (walking each group's sorted indices in order):
//     idx | by_key | on_key | matched right on_key
//     ----+--------+--------+---------------------
//      0  |   A    |   1    | null  <- no right event at or before on_key=1 in group A
//      1  |   A    |   3    | 2
//      2  |   A    |   5    | 2     <- forward-filled from idx=1
//      3  |   B    |   2    | null  <- no right event at or before on_key=2 in group B
//      4  |   B    |   7    | 4

pub(crate) struct AsofJoinBuildState {
    record_batches: Vec<RecordBatch>,
}

impl AsofJoinBuildState {
    fn finalize(
        self,
        left_schema: SchemaRef,
        left_by: &[BoundExpr],
        left_on: &BoundExpr,
    ) -> DaftResult<AsofJoinFinalizedBuildState> {
        let left_rb = RecordBatch::concat_or_empty(&self.record_batches, Some(left_schema))?;
        if left_rb.is_empty() {
            return Ok(AsofJoinFinalizedBuildState {
                left_rb,
                grouped_sorted_indices: vec![],
                grouped_sorted_materialized_on_keys: vec![],
                grouped_materialized_by_keys: vec![],
                grouped_by_key_hash_map: HashMap::new(),
            });
        }

        let left_on_series: Series = left_rb.eval_expression(left_on)?;
        let left_on_arr: Arc<dyn Array> = left_on_series.to_arrow()?;

        let on_key_sort_cmp =
            build_partial_compare_with_nulls(left_on_arr.as_ref(), left_on_arr.as_ref(), false)?;

        let (grouped_sorted_indices, grouped_reps, grouped_by_key_hash_map) = if left_by.is_empty()
        {
            let mut bucket: VecIndices = (0..left_rb.len() as u64).collect();
            bucket.retain(|idx| left_on_arr.is_valid(*idx as usize));
            bucket.sort_unstable_by(|&a, &b| on_key_sort_cmp(a as usize, b as usize).unwrap());
            (vec![bucket], RecordBatch::empty(None), HashMap::new())
        } else {
            let left_by_rb = left_rb.eval_expression_list(left_by)?;
            let (grouped_reps_indices, grouped_indices) = left_by_rb.make_groups()?;

            let grouped_sorted_indices: GroupIndices = grouped_indices
                .into_iter()
                .map(|mut bucket| {
                    bucket.retain(|idx| left_on_arr.is_valid(*idx as usize));
                    bucket.sort_unstable_by(|&a, &b| {
                        on_key_sort_cmp(a as usize, b as usize).unwrap()
                    });
                    bucket
                })
                .collect();

            let grouped_reps_indices_arr = UInt64Array::from_vec("key_idx", grouped_reps_indices);
            let grouped_reps = left_by_rb.take(&grouped_reps_indices_arr)?;
            let grouped_hashes = grouped_reps.hash_rows()?;
            let mut grouped_by_key_hash_map: HashMap<u64, Vec<usize>> =
                HashMap::with_capacity(grouped_hashes.len());
            for (group_idx, &hash) in grouped_hashes.values().iter().enumerate() {
                grouped_by_key_hash_map
                    .entry(hash)
                    .or_default()
                    .push(group_idx);
            }

            (
                grouped_sorted_indices,
                grouped_reps,
                grouped_by_key_hash_map,
            )
        };

        let grouped_sorted_materialized_on_keys: Vec<Arc<dyn Array>> = grouped_sorted_indices
            .iter()
            .map(|bucket| {
                let per_group_indices = UInt64Array::from_iter(
                    Field::new("k", DaftDataType::UInt64),
                    bucket.iter().copied().map(Some),
                );
                left_on_series.take(&per_group_indices)?.to_arrow()
            })
            .collect::<DaftResult<_>>()?;

        let grouped_materialized_by_keys = grouped_reps
            .as_materialized_series()
            .iter()
            .map(|s| s.to_arrow())
            .collect::<DaftResult<_>>()?;
        Ok(AsofJoinFinalizedBuildState {
            left_rb,
            grouped_sorted_indices,
            grouped_sorted_materialized_on_keys,
            grouped_materialized_by_keys,
            grouped_by_key_hash_map,
        })
    }
}

pub(crate) struct AsofJoinFinalizedBuildState {
    // Original left RecordBatch (all rows concatenated)
    left_rb: RecordBatch,
    // grouped_sorted_indices[g] holds the left row indices for group g, sorted by on_key for binary search.
    grouped_sorted_indices: GroupIndices,
    // Per-group materialized on_key arrays in sorted order, parallel to grouped_sorted_indices. Pre-extracted so
    // binary search walks a compact sequential array rather than chasing scattered indices into left_rb,
    // keeping the CPU cache hot.
    grouped_sorted_materialized_on_keys: Vec<Arc<dyn Array>>,
    // Vec indexed by by_key column; each Arrow array holds one representative by_key value per group.
    // Used to build the equality comparator that confirms a hash match is a true match (not a collision).
    grouped_materialized_by_keys: Vec<Arc<dyn Array>>,
    // Maps a by_key hash to a list of candidate group indices; multiple candidates exist only on hash collision.
    grouped_by_key_hash_map: HashMap<u64, Vec<usize>>,
}

type ByKeyHashesAndComparator<'a> = (
    &'a UInt64Array,
    &'a (dyn Fn(usize, usize) -> bool + Send + Sync),
);

impl AsofJoinFinalizedBuildState {
    /// Find the group index for right row `right_idx`.
    /// Returns `None` if the row belongs to no group (hash miss or equality miss).
    fn find_left_group(
        &self,
        right_idx: usize,
        by_key_hashes_and_comparator: Option<ByKeyHashesAndComparator<'_>>,
    ) -> Option<usize> {
        match by_key_hashes_and_comparator {
            None => Some(0),
            Some((hashes, eq_cmp)) => {
                let h = hashes.values()[right_idx];
                let candidates = self.grouped_by_key_hash_map.get(&h)?;
                candidates.iter().copied().find(|&g| eq_cmp(g, right_idx))
            }
        }
    }

    /// Binary-search `bucket` for the first left row with on_key >= right_on_arr[right_idx].
    /// Returns the best potential left row index, or `None` if no valid match exists.
    fn search_bucket(
        &self,
        bucket: &[u64],
        on_key_cmp: &DynPartialComparator,
        right_idx: usize,
    ) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = bucket.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match on_key_cmp(mid, right_idx) {
                Some(Ordering::Less) => lo = mid + 1,
                _ => hi = mid,
            }
        }
        bucket.get(lo).map(|&idx| idx as usize)
    }
}

pub(crate) struct AsofJoinProbeState {
    build_contents: Arc<AsofJoinFinalizedBuildState>,
    // Per left row: (rb_idx, row_idx) of the current best right match.
    best_match: Vec<Option<(u32, u32)>>,
    // Each entry pairs a pruned RecordBatch with its on_key Arrow array for cross-batch comparisons.
    right_rbs_and_on_keys: Vec<(RecordBatch, Arc<dyn Array>)>,
}

impl AsofJoinProbeState {
    fn probe_batch(
        &mut self,
        right_rb: &RecordBatch,
        right_on: &BoundExpr,
        right_by: &[BoundExpr],
        right_cols_to_drop: &HashSet<String>,
    ) -> DaftResult<()> {
        let rb_idx = self.right_rbs_and_on_keys.len();
        let build_state = self.build_contents.clone();

        let right_on_arr: Arc<dyn Array> = right_rb.eval_expression(right_on)?.to_arrow()?;
        self.right_rbs_and_on_keys.push((
            prune_right_batch(right_rb, right_cols_to_drop),
            right_on_arr.clone(),
        ));

        // Build one comparator per by_key group: compares that group's sorted left on_key array
        // against the current right batch's on_key array, used for binary search in search_bucket.
        let grouped_on_key_cmps: Vec<DynPartialComparator> = build_state
            .grouped_sorted_materialized_on_keys
            .iter()
            .map(|sorted_key_arr| {
                build_partial_compare_with_nulls(
                    sorted_key_arr.as_ref(),
                    right_on_arr.as_ref(),
                    false,
                )
            })
            .collect::<DaftResult<_>>()?;

        // this way update_best_match can compare same-rb candidates without rebuilding it.
        let mut cmp_cache: HashMap<(usize, usize), DynPartialComparator> = HashMap::new();
        cmp_cache.insert(
            (rb_idx, rb_idx),
            build_partial_compare_with_nulls(right_on_arr.as_ref(), right_on_arr.as_ref(), false)?,
        );

        // Passed to find_left_group() to hash and equality-match the right row's by_key against left groups.
        let by_key_hashes_and_comparator: Option<(_, _)> =
            if !build_state.grouped_by_key_hash_map.is_empty() {
                let right_by_rb = right_rb.eval_expression_list(right_by)?;
                let hashes = right_by_rb.hash_rows()?;
                let num_by_cols = build_state.grouped_materialized_by_keys.len();
                let right_by_arrs = right_by_rb
                    .as_materialized_series()
                    .iter()
                    .map(|s| s.to_arrow())
                    .collect::<DaftResult<Vec<_>>>()?;
                let eq_cmp = build_multi_array_is_equal_from_arrays(
                    &build_state.grouped_materialized_by_keys,
                    &right_by_arrs,
                    &vec![false; num_by_cols],
                    &vec![false; num_by_cols],
                )?;
                Some((hashes, eq_cmp))
            } else {
                None
            };
        let by_key_hashes_and_comparator_ref = by_key_hashes_and_comparator
            .as_ref()
            .map(|(h, eq)| (h, eq.as_ref()));

        let right_on_key_arrs: Vec<Arc<dyn Array>> = self
            .right_rbs_and_on_keys
            .iter()
            .map(|(_, on_key_arr)| on_key_arr.clone())
            .collect();

        for right_idx in 0..right_rb.len() {
            if !right_on_arr.is_valid(right_idx) {
                continue;
            }

            let Some(group_idx) =
                build_state.find_left_group(right_idx, by_key_hashes_and_comparator_ref)
            else {
                continue;
            };

            let bucket = &build_state.grouped_sorted_indices[group_idx];
            let Some(matched_left_idx) =
                build_state.search_bucket(bucket, &grouped_on_key_cmps[group_idx], right_idx)
            else {
                continue;
            };

            update_best_match(
                &mut self.best_match[matched_left_idx],
                &right_on_key_arrs,
                rb_idx,
                right_idx,
                &mut cmp_cache,
            )?;
        }

        Ok(())
    }
}

fn update_best_match(
    slot: &mut Option<(u32, u32)>,
    on_key_arrs: &[Arc<dyn Array>],
    candidate_rb_idx: usize,
    candidate_right_idx: usize,
    cmp_cache: &mut HashMap<(usize, usize), DynPartialComparator>,
) -> DaftResult<()> {
    let is_better = match *slot {
        None => true,
        Some((existing_rb_idx, existing_right_idx)) => is_candidate_better(
            candidate_rb_idx,
            candidate_right_idx,
            on_key_arrs[candidate_rb_idx].as_ref(),
            existing_rb_idx as usize,
            existing_right_idx as usize,
            on_key_arrs[existing_rb_idx as usize].as_ref(),
            cmp_cache,
        )?,
    };
    if is_better {
        *slot = Some((candidate_rb_idx as u32, candidate_right_idx as u32));
    }
    Ok(())
}

fn is_candidate_better(
    candidate_rb_idx: usize,
    candidate_right_idx: usize,
    candidate_on_arr: &dyn Array,
    existing_rb_idx: usize,
    existing_right_idx: usize,
    existing_on_arr: &dyn Array,
    cmp_cache: &mut HashMap<(usize, usize), DynPartialComparator>,
) -> DaftResult<bool> {
    let cmp = match cmp_cache.entry((candidate_rb_idx, existing_rb_idx)) {
        Entry::Occupied(e) => e.into_mut(),
        Entry::Vacant(e) => e.insert(build_partial_compare_with_nulls(
            candidate_on_arr,
            existing_on_arr,
            false,
        )?),
    };
    Ok(matches!(
        cmp(candidate_right_idx, existing_right_idx),
        Some(Ordering::Greater)
    ))
}

/// For each left row that has no match, if the previous left row in the same
/// group has a match, carry that match forward. This implements the "as-of"
/// semantics where unmatched rows inherit the most recent prior match.
fn forward_fill(global_best: &mut [Option<(u32, u32)>], grouped_sorted_indices: &GroupIndices) {
    for bucket in grouped_sorted_indices {
        for i in 1..bucket.len() {
            let prev_left_idx = bucket[i - 1] as usize;
            let curr_left_idx = bucket[i] as usize;
            if global_best[curr_left_idx].is_none() && global_best[prev_left_idx].is_some() {
                global_best[curr_left_idx] = global_best[prev_left_idx];
            }
        }
    }
}

fn prune_right_batch(rb: &RecordBatch, right_cols_to_drop: &HashSet<String>) -> RecordBatch {
    let kept_indices: Vec<usize> = rb
        .schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| (!right_cols_to_drop.contains(f.name.as_ref())).then_some(i))
        .collect();
    rb.get_columns(&kept_indices)
}

fn build_right_output(
    global_best: &[Option<(u32, u32)>],
    global_right_rbs: Vec<RecordBatch>,
    pruned_right_schema: SchemaRef,
) -> DaftResult<RecordBatch> {
    let mut right_row_offsets = vec![0usize; global_right_rbs.len() + 1];
    for (i, rb) in global_right_rbs.iter().enumerate() {
        right_row_offsets[i + 1] = right_row_offsets[i] + rb.len();
    }
    let right_idx_arr = UInt64Array::from_iter(
        Field::new("right_idx", DaftDataType::UInt64),
        global_best.iter().map(|best| {
            best.map(|(global_rb_idx, local_right_idx)| {
                (right_row_offsets[global_rb_idx as usize] + local_right_idx as usize) as u64
            })
        }),
    );
    let global_right_rb_concat = if global_right_rbs.is_empty() {
        RecordBatch::empty(Some(pruned_right_schema))
    } else {
        RecordBatch::concat(global_right_rbs.iter().collect::<Vec<_>>().as_slice())?
    };
    global_right_rb_concat.take(&right_idx_arr)
}

fn build_join_output(
    left_rb: &RecordBatch,
    right_rb: RecordBatch,
    join_schema: SchemaRef,
) -> DaftResult<MicroPartition> {
    let mut join_series: Vec<Series> = Vec::with_capacity(join_schema.len());
    for s in left_rb.as_materialized_series() {
        join_series.push(s.clone());
    }
    for s in right_rb.as_materialized_series() {
        join_series.push(s.clone());
    }
    let output_rb = RecordBatch::new_with_size(join_schema.clone(), join_series, left_rb.len())?;
    Ok(MicroPartition::new_loaded(
        join_schema,
        Arc::new(vec![output_rb]),
        None,
    ))
}

pub struct AsofJoinOperator {
    left_by: Vec<BoundExpr>,
    right_by: Vec<BoundExpr>,
    left_on: BoundExpr,
    right_on: BoundExpr,
    left_schema: SchemaRef,
    right_cols_to_drop: HashSet<String>,
    join_schema: SchemaRef,
}

impl AsofJoinOperator {
    pub fn new(
        left_by: Vec<BoundExpr>,
        right_by: Vec<BoundExpr>,
        left_on: BoundExpr,
        right_on: BoundExpr,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
    ) -> DaftResult<Self> {
        let right_cols_to_drop = get_right_cols_to_drop(&right_by, &left_on, &right_on, |e| {
            let (unwrapped, _) = e.inner().clone().unwrap_alias();
            match unwrapped.as_ref() {
                Expr::Column(_) => Some(unwrapped.name().to_string()),
                _ => None,
            }
        });
        let join_schema = infer_asof_join_schema(&left_schema, &right_schema, &right_cols_to_drop)?;

        Ok(Self {
            left_by,
            right_by,
            left_on,
            right_on,
            left_schema,
            right_cols_to_drop,
            join_schema,
        })
    }
}

impl JoinOperator for AsofJoinOperator {
    type BuildState = AsofJoinBuildState;
    type FinalizedBuildState = Arc<AsofJoinFinalizedBuildState>;
    type ProbeState = AsofJoinProbeState;

    fn build(
        &self,
        input: MicroPartition,
        mut state: Self::BuildState,
        _spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self> {
        state.record_batches.extend(
            input
                .record_batches()
                .iter()
                .filter(|rb| !rb.is_empty())
                .cloned(),
        );
        Ok(state).into()
    }

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self> {
        let finalized_build_state =
            state.finalize(self.left_schema.clone(), &self.left_by, &self.left_on)?;
        Ok(Arc::new(finalized_build_state))
    }

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        Ok(AsofJoinBuildState {
            record_batches: Vec::new(),
        })
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Self::FinalizedBuildState,
    ) -> Self::ProbeState {
        let n = finalized_build_state.left_rb.num_rows();
        AsofJoinProbeState {
            build_contents: finalized_build_state,
            best_match: vec![None::<(u32, u32)>; n],
            right_rbs_and_on_keys: Vec::new(),
        }
    }

    fn probe(
        &self,
        input: MicroPartition,
        mut state: Self::ProbeState,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        let right_on = self.right_on.clone();
        let right_by = self.right_by.clone();
        let right_cols_to_drop = self.right_cols_to_drop.clone();

        spawner
            .spawn(
                async move {
                    let build_state = state.build_contents.clone();
                    if build_state.left_rb.is_empty() {
                        return Ok((state, ProbeOutput::NeedMoreInput(None)));
                    }
                    for right_rb in input.record_batches() {
                        if right_rb.is_empty() {
                            continue;
                        }
                        state.probe_batch(right_rb, &right_on, &right_by, &right_cols_to_drop)?;
                    }
                    Ok((state, ProbeOutput::NeedMoreInput(None)))
                },
                Span::current(),
            )
            .into()
    }

    fn finalize_probe(
        &self,
        states: Vec<Self::ProbeState>,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeFinalizeResult {
        let join_schema = self.join_schema.clone();
        let left_field_names: HashSet<&str> = self.left_schema.field_names().collect();
        let pruned_right_schema: SchemaRef = Arc::new(Schema::new(
            self.join_schema
                .fields()
                .iter()
                .filter(|f| !left_field_names.contains(f.name.as_ref()))
                .cloned(),
        ));
        spawner
            .spawn(
                async move {
                    let build_state = states
                        .first()
                        .expect("AsofJoin probe finalize: expected at least one state")
                        .build_contents
                        .clone();

                    if build_state.left_rb.is_empty() {
                        return Ok(Some(MicroPartition::new_loaded(
                            join_schema.clone(),
                            Arc::new(vec![RecordBatch::empty(Some(join_schema))]),
                            None,
                        )));
                    }

                    // Each state's best_match stores a local_rb_idx scoped to that state's
                    // right_rbs_and_on_keys list. global_rb_offsets[k] converts state k's local_rb_idx
                    // to a global_rb_idx into the flat global_right_on_key_arrs / global_right_rbs.
                    let global_rb_offsets: Vec<usize> = states
                        .iter()
                        .scan(0usize, |acc, state| {
                            let offset = *acc;
                            *acc += state.right_rbs_and_on_keys.len();
                            Some(offset)
                        })
                        .collect();

                    let global_right_on_key_arrs: Vec<Arc<dyn Array>> = states
                        .iter()
                        .flat_map(|s| {
                            s.right_rbs_and_on_keys
                                .iter()
                                .map(|(_, on_key_arr)| on_key_arr.clone())
                        })
                        .collect();

                    let (state_best_matches, right_rbs_per_state): (Vec<_>, Vec<_>) = states
                        .into_iter()
                        .map(|s| {
                            let (rbs, _): (Vec<_>, Vec<_>) =
                                s.right_rbs_and_on_keys.into_iter().unzip();
                            (s.best_match, rbs)
                        })
                        .unzip();
                    let global_right_rbs: Vec<RecordBatch> =
                        right_rbs_per_state.into_iter().flatten().collect();

                    let global_right_on_key_arrs = Arc::new(global_right_on_key_arrs);
                    let global_rb_offsets = Arc::new(global_rb_offsets);
                    let state_best_matches = Arc::new(state_best_matches);

                    let total_left_rows = build_state.left_rb.num_rows();
                    let rows_per_chunk =
                        (total_left_rows / get_compute_pool_num_threads()).max(1024);

                    let chunk_tasks: Vec<_> = (0..total_left_rows)
                        .step_by(rows_per_chunk)
                        .map(|start| {
                            let end = (start + rows_per_chunk).min(total_left_rows);
                            let mut chunk: Vec<Option<(u32, u32)>> = vec![None; end - start];
                            let global_right_on_key_arrs = global_right_on_key_arrs.clone();
                            let global_rb_offsets = global_rb_offsets.clone();
                            let state_best_matches = state_best_matches.clone();

                            get_compute_runtime().spawn(async move {
                                let mut cmp_cache: HashMap<(usize, usize), DynPartialComparator> =
                                    HashMap::new();

                                for (chunk_row_idx, curr_best_match) in chunk.iter_mut().enumerate()
                                {
                                    let global_left_idx = start + chunk_row_idx;
                                    for (state_idx, best_match) in
                                        state_best_matches.iter().enumerate()
                                    {
                                        let Some((candidate_local_rb_idx, candidate_right_idx)) =
                                            best_match[global_left_idx]
                                        else {
                                            continue;
                                        };
                                        let candidate_global_rb_idx = global_rb_offsets[state_idx]
                                            + candidate_local_rb_idx as usize;

                                        update_best_match(
                                            curr_best_match,
                                            &global_right_on_key_arrs,
                                            candidate_global_rb_idx,
                                            candidate_right_idx as usize,
                                            &mut cmp_cache,
                                        )?;
                                    }
                                }
                                DaftResult::Ok(chunk)
                            })
                        })
                        .collect();

                    let mut global_best: Vec<Option<(u32, u32)>> = vec![None; total_left_rows];
                    for (i, task) in chunk_tasks.into_iter().enumerate() {
                        let chunk = task.await.map_err(|_| {
                            DaftError::InternalError("compute merge task dropped".into())
                        })??;
                        let start = i * rows_per_chunk;
                        global_best[start..start + chunk.len()].copy_from_slice(&chunk);
                    }

                    forward_fill(&mut global_best, &build_state.grouped_sorted_indices);
                    let right_rb =
                        build_right_output(&global_best, global_right_rbs, pruned_right_schema)?;
                    Ok(Some(build_join_output(
                        &build_state.left_rb,
                        right_rb,
                        join_schema,
                    )?))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "Asof Join".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::AsofJoin
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["Asof Join".to_string()]
    }

    fn needs_probe_finalization(&self) -> bool {
        true
    }
}
