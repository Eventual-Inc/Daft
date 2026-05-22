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
    join::AsofJoinStrategy,
    kernels::search_sorted::{DynPartialComparator, build_partial_compare_with_nulls, is_nearer},
    prelude::{DataType as DaftDataType, Field, Schema, SchemaRef, Series, UInt64Array},
};
use daft_dsl::expr::bound_expr::BoundExpr;
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
}

/// First left row with on_key >= right_on_arr[right_idx] (ceiling).
/// Used by Backward strategy. Returns `None` if the bucket is empty.
fn search_ceil(
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

/// Last left row with on_key <= right_on_arr[right_idx] (floor).
/// Used by Forward strategy. Returns `None` if no such row exists.
fn search_floor(
    bucket: &[u64],
    on_key_cmp: &DynPartialComparator,
    right_idx: usize,
) -> Option<usize> {
    let mut lo = 0usize;
    let mut hi = bucket.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match on_key_cmp(mid, right_idx) {
            Some(Ordering::Greater) => hi = mid,
            _ => lo = mid + 1,
        }
    }
    lo.checked_sub(1)
        .and_then(|i| bucket.get(i))
        .map(|&idx| idx as usize)
}

/// Returns `start..end` — the bucket positions this right row must be offered to.
///
/// The range covers the floor (last left ≤ right) and ceil (first left ≥ right),
/// each extended through any adjacent duplicates sharing the same on_key.
/// Left rows outside this range either have a strictly closer right candidate
/// elsewhere in the bucket, or will be covered by `nearest_fill`.
///
/// `on_key_cmp(pos, right_idx)` — sorted_left[pos] vs right[right_idx]
/// `self_cmp(i, j)`             — sorted_left[i]   vs sorted_left[j]
fn search_nearest(
    bucket: &[u64],
    on_key_cmp: &DynPartialComparator,
    self_cmp: &DynPartialComparator,
    right_idx: usize,
) -> std::ops::Range<usize> {
    if bucket.is_empty() {
        return 0..0;
    }

    let mut lo = 0usize;
    let mut hi = bucket.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match on_key_cmp(mid, right_idx) {
            Some(Ordering::Less) => lo = mid + 1,
            _ => hi = mid,
        }
    }

    // Floor at lo-1: walk backward to include all left rows with the same on_key.
    let start = match lo.checked_sub(1) {
        None => lo,
        Some(fp) => {
            let mut pos = fp;
            while pos > 0 && self_cmp(pos - 1, pos) == Some(Ordering::Equal) {
                pos -= 1;
            }
            pos
        }
    };

    // Ceil at lo: walk forward to include all left rows with the same on_key.
    let end = if lo < bucket.len() {
        let mut pos = lo;
        while pos + 1 < bucket.len() && self_cmp(pos, pos + 1) == Some(Ordering::Equal) {
            pos += 1;
        }
        pos + 1
    } else {
        lo
    };

    start..end
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
        right_cols_to_keep: &HashSet<String>,
        dir: AsofJoinStrategy,
    ) -> DaftResult<()> {
        let rb_idx = self.right_rbs_and_on_keys.len();
        let build_state = self.build_contents.clone();

        let right_on_arr: Arc<dyn Array> = right_rb.eval_expression(right_on)?.to_arrow()?;
        self.right_rbs_and_on_keys.push((
            prune_right_batch(right_rb, right_cols_to_keep),
            right_on_arr.clone(),
        ));
        // Build one comparator per by_key group: compares that group's sorted left on_key array
        // against the current right batch's on_key array, used for binary search.
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

        if dir == AsofJoinStrategy::Nearest {
            let left_self_cmps: Vec<DynPartialComparator> = build_state
                .grouped_sorted_materialized_on_keys
                .iter()
                .map(|arr| build_partial_compare_with_nulls(arr.as_ref(), arr.as_ref(), false))
                .collect::<DaftResult<_>>()?;

            for right_idx in 0..right_rb.len() {
                if !right_on_arr.is_valid(right_idx) {
                    continue;
                }
                let Some(group_idx) =
                    build_state.find_left_group(right_idx, by_key_hashes_and_comparator_ref)
                else {
                    continue;
                };
                let sorted_on_key_arr = &build_state.grouped_sorted_materialized_on_keys[group_idx];
                let bucket = &build_state.grouped_sorted_indices[group_idx];
                let candidate = MatchCandidate {
                    rb_idx,
                    row_idx: right_idx,
                };

                for pos in search_nearest(
                    bucket,
                    &grouped_on_key_cmps[group_idx],
                    &left_self_cmps[group_idx],
                    right_idx,
                ) {
                    update_nearest_match(
                        &mut self.best_match[bucket[pos] as usize],
                        &right_on_key_arrs,
                        candidate,
                        sorted_on_key_arr.as_ref(),
                        pos,
                    )?;
                }
            }
        } else {
            let mut cmp_cache: HashMap<(usize, usize), DynPartialComparator> = HashMap::new();
            cmp_cache.insert(
                (rb_idx, rb_idx),
                build_partial_compare_with_nulls(
                    right_on_arr.as_ref(),
                    right_on_arr.as_ref(),
                    false,
                )?,
            );

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
                let Some(matched_left_idx) = (match dir {
                    AsofJoinStrategy::Backward => {
                        search_ceil(bucket, &grouped_on_key_cmps[group_idx], right_idx)
                    }
                    AsofJoinStrategy::Forward => {
                        search_floor(bucket, &grouped_on_key_cmps[group_idx], right_idx)
                    }
                    AsofJoinStrategy::Nearest => unreachable!(),
                }) else {
                    continue;
                };
                update_best_match(
                    &mut self.best_match[matched_left_idx],
                    &right_on_key_arrs,
                    MatchCandidate {
                        rb_idx,
                        row_idx: right_idx,
                    },
                    &mut cmp_cache,
                    dir,
                )?;
            }
        }

        Ok(())
    }
}

#[derive(Clone, Copy)]
struct MatchCandidate {
    rb_idx: usize,
    row_idx: usize,
}

fn update_best_match(
    slot: &mut Option<(u32, u32)>,
    on_key_arrs: &[Arc<dyn Array>],
    candidate: MatchCandidate,
    cmp_cache: &mut HashMap<(usize, usize), DynPartialComparator>,
    dir: AsofJoinStrategy,
) -> DaftResult<()> {
    let preferred_ordering = match dir {
        AsofJoinStrategy::Backward => Ordering::Greater,
        AsofJoinStrategy::Forward => Ordering::Less,
        AsofJoinStrategy::Nearest => unreachable!("use update_nearest_match for Nearest"),
    };
    let is_better = match *slot {
        None => true,
        Some((existing_rb_idx, existing_right_idx)) => is_candidate_better(
            candidate,
            MatchCandidate {
                rb_idx: existing_rb_idx as usize,
                row_idx: existing_right_idx as usize,
            },
            on_key_arrs,
            cmp_cache,
            preferred_ordering,
        )?,
    };
    if is_better {
        *slot = Some((candidate.rb_idx as u32, candidate.row_idx as u32));
    }
    Ok(())
}

fn update_nearest_match(
    slot: &mut Option<(u32, u32)>,
    on_key_arrs: &[Arc<dyn Array>],
    candidate: MatchCandidate,
    left_on_arr: &dyn Array,
    left_on_idx: usize,
) -> DaftResult<()> {
    let nearer = match *slot {
        None => true,
        Some((existing_rb_idx, existing_right_idx)) => is_nearer(
            on_key_arrs[candidate.rb_idx].as_ref(),
            candidate.row_idx,
            on_key_arrs[existing_rb_idx as usize].as_ref(),
            existing_right_idx as usize,
            left_on_arr,
            left_on_idx,
        ),
    };
    if nearer {
        *slot = Some((candidate.rb_idx as u32, candidate.row_idx as u32));
    }
    Ok(())
}

fn is_candidate_better(
    candidate: MatchCandidate,
    existing: MatchCandidate,
    on_key_arrs: &[Arc<dyn Array>],
    cmp_cache: &mut HashMap<(usize, usize), DynPartialComparator>,
    preferred_ordering: Ordering,
) -> DaftResult<bool> {
    let cmp = match cmp_cache.entry((candidate.rb_idx, existing.rb_idx)) {
        Entry::Occupied(e) => e.into_mut(),
        Entry::Vacant(e) => e.insert(build_partial_compare_with_nulls(
            on_key_arrs[candidate.rb_idx].as_ref(),
            on_key_arrs[existing.rb_idx].as_ref(),
            false,
        )?),
    };
    Ok(cmp(candidate.row_idx, existing.row_idx) == Some(preferred_ordering))
}

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

fn backward_fill(global_best: &mut [Option<(u32, u32)>], grouped_sorted_indices: &GroupIndices) {
    for bucket in grouped_sorted_indices {
        for i in (0..bucket.len().saturating_sub(1)).rev() {
            let next_left_idx = bucket[i + 1] as usize;
            let curr_left_idx = bucket[i] as usize;
            if global_best[curr_left_idx].is_none() && global_best[next_left_idx].is_some() {
                global_best[curr_left_idx] = global_best[next_left_idx];
            }
        }
    }
}

/// For left rows that received no direct probe assignment, fill from both directions and keep
/// whichever candidate is closer to the left row's on_key. Ties prefer the larger (forward)
/// right value, matching the `is_nearer` tie-break convention.
fn nearest_fill(
    global_best: &mut [Option<(u32, u32)>],
    grouped_sorted_indices: &GroupIndices,
    left_on_arr: &dyn Array,
    global_right_on_key_arrs: &[Arc<dyn Array>],
) {
    let mut fwd = global_best.to_owned();
    forward_fill(&mut fwd, grouped_sorted_indices);
    let mut bwd = global_best.to_owned();
    backward_fill(&mut bwd, grouped_sorted_indices);

    for i in 0..global_best.len() {
        if global_best[i].is_some() {
            continue;
        }
        global_best[i] = match (fwd[i], bwd[i]) {
            (None, None) => continue,
            (Some(_), None) => fwd[i],
            (None, Some(_)) => bwd[i],
            (Some((fwd_rb, fwd_row)), Some((bwd_rb, bwd_row))) => {
                let fwd_arr = global_right_on_key_arrs[fwd_rb as usize].as_ref();
                let bwd_arr = global_right_on_key_arrs[bwd_rb as usize].as_ref();
                if is_nearer(
                    fwd_arr,
                    fwd_row as usize,
                    bwd_arr,
                    bwd_row as usize,
                    left_on_arr,
                    i,
                ) {
                    fwd[i]
                } else {
                    bwd[i]
                }
            }
        };
    }
}

fn prune_right_batch(rb: &RecordBatch, right_cols_to_keep: &HashSet<String>) -> RecordBatch {
    let kept_indices: Vec<usize> = rb
        .schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| right_cols_to_keep.contains(f.name.as_ref()).then_some(i))
        .collect();
    rb.get_columns(&kept_indices)
}

fn build_right_output(
    global_best: &[Option<(u32, u32)>],
    global_right_rbs: Vec<RecordBatch>,
    pruned_right_schema: SchemaRef,
) -> DaftResult<RecordBatch> {
    let mut rb_start_offsets = vec![0usize; global_right_rbs.len() + 1];
    for (i, rb) in global_right_rbs.iter().enumerate() {
        rb_start_offsets[i + 1] = rb_start_offsets[i] + rb.len();
    }
    let matched_global_indices = UInt64Array::from_iter(
        Field::new("right_idx", DaftDataType::UInt64),
        global_best.iter().map(|best| {
            best.map(|(global_rb_idx, local_right_idx)| {
                (rb_start_offsets[global_rb_idx as usize] + local_right_idx as usize) as u64
            })
        }),
    );
    let global_right_rb_concat = if global_right_rbs.is_empty() {
        RecordBatch::empty(Some(pruned_right_schema))
    } else {
        RecordBatch::concat(global_right_rbs.iter().collect::<Vec<_>>().as_slice())?
    };
    global_right_rb_concat.take(&matched_global_indices)
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
    join_schema: SchemaRef,
    right_cols_to_keep: HashSet<String>,
    strategy: AsofJoinStrategy,
}

impl AsofJoinOperator {
    pub fn new(
        left_by: Vec<BoundExpr>,
        right_by: Vec<BoundExpr>,
        left_on: BoundExpr,
        right_on: BoundExpr,
        strategy: AsofJoinStrategy,
        left_schema: SchemaRef,
        join_schema: SchemaRef,
    ) -> DaftResult<Self> {
        let right_cols_to_keep = join_schema
            .fields()
            .iter()
            .skip(left_schema.len())
            .map(|f| f.name.to_string())
            .collect();
        Ok(Self {
            left_by,
            right_by,
            left_on,
            right_on,
            left_schema,
            join_schema,
            right_cols_to_keep,
            strategy,
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
        let right_cols_to_keep = self.right_cols_to_keep.clone();
        let strategy = self.strategy;

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
                        state.probe_batch(
                            right_rb,
                            &right_on,
                            &right_by,
                            &right_cols_to_keep,
                            strategy,
                        )?;
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
        let strategy = self.strategy;
        let left_on = self.left_on.clone();
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

                    let left_on_arr: Option<Arc<dyn Array>> =
                        if matches!(strategy, AsofJoinStrategy::Nearest) {
                            Some(build_state.left_rb.eval_expression(&left_on)?.to_arrow()?)
                        } else {
                            None
                        };

                    let mut global_rb_offsets: Vec<usize> = Vec::with_capacity(states.len());
                    let mut global_right_on_key_arrs: Vec<Arc<dyn Array>> = Vec::new();
                    let mut state_best_matches: Vec<Vec<Option<(u32, u32)>>> =
                        Vec::with_capacity(states.len());
                    let mut global_right_rbs: Vec<RecordBatch> = Vec::new();
                    let mut rb_count = 0;

                    for state in states {
                        global_rb_offsets.push(rb_count);
                        rb_count += state.right_rbs_and_on_keys.len();
                        for (rb, on_key_arr) in state.right_rbs_and_on_keys {
                            global_right_on_key_arrs.push(on_key_arr);
                            global_right_rbs.push(rb);
                        }
                        state_best_matches.push(state.best_match);
                    }

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
                            let left_on_arr = left_on_arr.clone();

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
                                        let candidate = MatchCandidate {
                                            rb_idx: candidate_global_rb_idx,
                                            row_idx: candidate_right_idx as usize,
                                        };

                                        match strategy {
                                            AsofJoinStrategy::Nearest => {
                                                update_nearest_match(
                                                    curr_best_match,
                                                    &global_right_on_key_arrs,
                                                    candidate,
                                                    left_on_arr
                                                        .as_deref()
                                                        .expect("left_on_arr required for Nearest"),
                                                    global_left_idx,
                                                )?;
                                            }
                                            _ => {
                                                update_best_match(
                                                    curr_best_match,
                                                    &global_right_on_key_arrs,
                                                    candidate,
                                                    &mut cmp_cache,
                                                    strategy,
                                                )?;
                                            }
                                        }
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

                    match strategy {
                        AsofJoinStrategy::Backward => {
                            forward_fill(&mut global_best, &build_state.grouped_sorted_indices);
                        }
                        AsofJoinStrategy::Forward => {
                            backward_fill(&mut global_best, &build_state.grouped_sorted_indices);
                        }
                        AsofJoinStrategy::Nearest => {
                            nearest_fill(
                                &mut global_best,
                                &build_state.grouped_sorted_indices,
                                left_on_arr
                                    .as_deref()
                                    .expect("left_on_arr required for Nearest fill"),
                                &global_right_on_key_arrs,
                            );
                        }
                    }

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
