use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, hash_map::Entry},
    sync::Arc,
};

use arrow::compute::SortOptions;
use arrow_array::{Array, Float64Array as ArrowFloat64Array, UInt64Array as ArrowUInt64Array};
use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use common_runtime::{get_compute_pool_num_threads, get_compute_runtime};
use daft_core::{
    array::ops::{
        GroupIndices, VecIndices, arrow::comparison::build_multi_array_is_equal_from_arrays,
    },
    join::AsofJoinStrategy,
    kernels::search_sorted::{
        DynPartialComparator, build_partial_compare_with_nulls, make_daft_comparator,
    },
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

/// Returns true if `a_arr[a_idx]` is nearer to `pivot_arr[pivot_idx]` than `b_arr[b_idx]`.
fn nearest_cmp(
    a_arr: &dyn Array,
    a_idx: usize,
    b_arr: &dyn Array,
    b_idx: usize,
    pivot_arr: &dyn Array,
    pivot_idx: usize,
) -> bool {
    if !a_arr.is_valid(a_idx) {
        return false;
    }
    if !b_arr.is_valid(b_idx) {
        return true;
    }
    if !pivot_arr.is_valid(pivot_idx) {
        return false;
    }

    let a = a_arr.slice(a_idx, 1);
    let b = b_arr.slice(b_idx, 1);
    let p = pivot_arr.slice(pivot_idx, 1);

    let sort_opts = SortOptions::new(false, false);

    // Compute |a - pivot| and |b - pivot| by always subtracting smaller from larger.
    let a_dist = {
        let cmp = make_daft_comparator(a.as_ref(), p.as_ref(), sort_opts)
            .expect("make_comparator failed for a vs pivot");
        if cmp(0, 0).is_ge() {
            arrow::compute::kernels::numeric::sub(&a, &p)
        } else {
            arrow::compute::kernels::numeric::sub(&p, &a)
        }
        .expect("sub failed for a distance")
    };
    let b_dist = {
        let cmp = make_daft_comparator(b.as_ref(), p.as_ref(), sort_opts)
            .expect("make_comparator failed for b vs pivot");
        if cmp(0, 0).is_ge() {
            arrow::compute::kernels::numeric::sub(&b, &p)
        } else {
            arrow::compute::kernels::numeric::sub(&p, &b)
        }
        .expect("sub failed for b distance")
    };

    let dist_cmp = make_daft_comparator(a_dist.as_ref(), b_dist.as_ref(), sort_opts)
        .expect("make_comparator failed for distance comparison");
    match dist_cmp(0, 0) {
        Ordering::Less => true,
        Ordering::Greater => false,
        // Tie: prefer larger value (forward/later).
        Ordering::Equal => make_daft_comparator(a.as_ref(), b.as_ref(), sort_opts)
            .expect("make_comparator failed for tie-breaking")(0, 0)
        .is_gt(),
    }
}

/// Captures the arrays upfront; each call only passes `(a_idx, b_idx, pivot_idx)`.
type DynPartialNearestComparator = Box<dyn Fn(usize, usize, usize) -> bool + Send + Sync>;

fn build_partial_nearest_comparator(
    sorted_arr: Arc<dyn Array>,
    pivot_arr: Arc<dyn Array>,
) -> DynPartialNearestComparator {
    Box::new(move |a_idx: usize, b_idx: usize, pivot_idx: usize| {
        nearest_cmp(
            sorted_arr.as_ref(),
            a_idx,
            sorted_arr.as_ref(),
            b_idx,
            pivot_arr.as_ref(),
            pivot_idx,
        )
    })
}

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

    /// Backward: first left row with on_key >= right_on_arr[right_idx] (ceiling).
    /// Forward:  last left row with on_key <= right_on_arr[right_idx] (floor).
    /// Returns `None` if no valid match exists.
    fn search_bucket(
        &self,
        bucket: &[u64],
        on_key_cmp: &DynPartialComparator,
        right_idx: usize,
        dir: AsofJoinStrategy,
    ) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = bucket.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match dir {
                AsofJoinStrategy::Backward => match on_key_cmp(mid, right_idx) {
                    Some(Ordering::Less) => lo = mid + 1,
                    _ => hi = mid,
                },
                AsofJoinStrategy::Forward => match on_key_cmp(mid, right_idx) {
                    Some(Ordering::Greater) => hi = mid,
                    _ => lo = mid + 1,
                },
                AsofJoinStrategy::Nearest => unreachable!("use search_bucket_nearest for Nearest"),
            }
        }
        match dir {
            AsofJoinStrategy::Backward => bucket.get(lo).map(|&idx| idx as usize),
            AsofJoinStrategy::Forward => lo
                .checked_sub(1)
                .and_then(|i| bucket.get(i))
                .map(|&idx| idx as usize),
            AsofJoinStrategy::Nearest => unreachable!("use search_bucket_nearest for Nearest"),
        }
    }

    /// binary-searches for both the floor (last left <= right) and ceiling (first left >= right),
    /// returns the nearest left row to `right_idx` (floor or ceil).
    fn search_bucket_nearest(
        &self,
        bucket: &[u64],
        on_key_cmp: &DynPartialComparator,
        right_idx: usize,
        nearest_cmp: &DynPartialNearestComparator,
    ) -> Option<usize> {
        if bucket.is_empty() {
            return None;
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
        let ceil_pos = if lo < bucket.len() { Some(lo) } else { None };
        let floor_pos = lo.checked_sub(1);

        match (floor_pos, ceil_pos) {
            (None, Some(cp)) => Some(cp),
            (Some(fp), None) => Some(fp),
            (Some(fp), Some(cp)) => {
                if nearest_cmp(cp, fp, right_idx) {
                    Some(cp)
                } else {
                    Some(fp)
                }
            }
            (None, None) => None,
        }
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
            let grouped_nearest_cmps: Vec<DynPartialNearestComparator> = build_state
                .grouped_sorted_materialized_on_keys
                .iter()
                .map(|sorted_key_arr| {
                    build_partial_nearest_comparator(sorted_key_arr.clone(), right_on_arr.clone())
                })
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
                let sorted_on_key_arr = &build_state.grouped_sorted_materialized_on_keys[group_idx];
                let bucket = &build_state.grouped_sorted_indices[group_idx];
                let Some(matched_left_bucket_idx) = build_state.search_bucket_nearest(
                    bucket,
                    &grouped_on_key_cmps[group_idx],
                    right_idx,
                    &grouped_nearest_cmps[group_idx],
                ) else {
                    continue;
                };
                update_nearest_match(
                    &mut self.best_match[bucket[matched_left_bucket_idx] as usize],
                    &right_on_key_arrs,
                    MatchCandidate {
                        rb_idx,
                        row_idx: right_idx,
                    },
                    sorted_on_key_arr.as_ref(),
                    matched_left_bucket_idx,
                )?;
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
                let Some(matched_left_idx) = build_state.search_bucket(
                    bucket,
                    &grouped_on_key_cmps[group_idx],
                    right_idx,
                    dir,
                ) else {
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
        Some((existing_rb_idx, existing_right_idx)) => nearest_cmp(
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
/// whichever candidate is closer to the left row's on_key. Ties prefer the forward-filled
/// (earlier/floor) match, consistent with pandas bdiff <= fdiff behaviour.
fn nearest_fill(
    global_best: &mut Vec<Option<(u32, u32)>>,
    grouped_sorted_indices: &GroupIndices,
    left_on_arr: &dyn Array,
    global_right_on_key_arrs: &[Arc<dyn Array>],
) {
    let mut fwd = global_best.clone();
    forward_fill(&mut fwd, grouped_sorted_indices);
    let mut bwd = global_best.clone();
    backward_fill(&mut bwd, grouped_sorted_indices);

    // Resolve uncontested rows immediately (only one direction has a candidate).
    for i in 0..global_best.len() {
        if global_best[i].is_some() {
            continue;
        }
        global_best[i] = match (fwd[i], bwd[i]) {
            (Some(_), None) => fwd[i],
            (None, Some(_)) => bwd[i],
            _ => continue, // both None or both Some — latter handled below
        };
    }

    // Contested rows: both a forward and a backward candidate exist.
    let contested: Vec<usize> = (0..global_best.len())
        .filter(|&i| global_best[i].is_none() && fwd[i].is_some() && bwd[i].is_some())
        .collect();

    if contested.is_empty() {
        return;
    }

    // Offset table so (rb_idx, row_idx) maps to a global index into the concatenated right array.
    let mut rb_starts = vec![0usize; global_right_on_key_arrs.len() + 1];
    for (i, arr) in global_right_on_key_arrs.iter().enumerate() {
        rb_starts[i + 1] = rb_starts[i] + arr.len();
    }

    let right_refs: Vec<&dyn Array> = global_right_on_key_arrs
        .iter()
        .map(|a| a.as_ref())
        .collect();
    let right_concat = arrow::compute::concat(&right_refs).expect("nearest_fill: concat failed");

    let left_indices = ArrowUInt64Array::from_iter_values(contested.iter().map(|&i| i as u64));
    let fwd_indices = ArrowUInt64Array::from_iter_values(contested.iter().map(|&i| {
        let (rb, row) = fwd[i].unwrap();
        (rb_starts[rb as usize] + row as usize) as u64
    }));
    let bwd_indices = ArrowUInt64Array::from_iter_values(contested.iter().map(|&i| {
        let (rb, row) = bwd[i].unwrap();
        (rb_starts[rb as usize] + row as usize) as u64
    }));

    let left_vals =
        arrow::compute::take(left_on_arr, &left_indices, None).expect("nearest_fill: take failed");
    let fwd_vals = arrow::compute::take(right_concat.as_ref(), &fwd_indices, None)
        .expect("nearest_fill: take failed");
    let bwd_vals = arrow::compute::take(right_concat.as_ref(), &bwd_indices, None)
        .expect("nearest_fill: take failed");

    // Cast to Float64 for uniform subtraction across all numeric types.
    let f64_dt = arrow::datatypes::DataType::Float64;
    let left_f64 = arrow::compute::cast(&left_vals, &f64_dt).expect("nearest_fill: cast failed");
    let fwd_f64 = arrow::compute::cast(&fwd_vals, &f64_dt).expect("nearest_fill: cast failed");
    let bwd_f64 = arrow::compute::cast(&bwd_vals, &f64_dt).expect("nearest_fill: cast failed");

    // Vectorized distance: sub then scalar abs on the typed result.
    let fwd_sub = arrow::compute::kernels::numeric::sub(&fwd_f64, &left_f64)
        .expect("nearest_fill: sub failed");
    let bwd_sub = arrow::compute::kernels::numeric::sub(&bwd_f64, &left_f64)
        .expect("nearest_fill: sub failed");

    let fwd_sub_f64 = fwd_sub
        .as_any()
        .downcast_ref::<ArrowFloat64Array>()
        .unwrap();
    let bwd_sub_f64 = bwd_sub
        .as_any()
        .downcast_ref::<ArrowFloat64Array>()
        .unwrap();
    let fwd_vals_f64 = fwd_f64
        .as_any()
        .downcast_ref::<ArrowFloat64Array>()
        .unwrap();
    let bwd_vals_f64 = bwd_f64
        .as_any()
        .downcast_ref::<ArrowFloat64Array>()
        .unwrap();

    for (k, &i) in contested.iter().enumerate() {
        let fwd_dist = fwd_sub_f64.value(k).abs();
        let bwd_dist = bwd_sub_f64.value(k).abs();
        global_best[i] = if fwd_dist < bwd_dist {
            fwd[i]
        } else if bwd_dist < fwd_dist {
            bwd[i]
        } else {
            // Tie: prefer the larger right on_key value (forward/later), mirroring nearest_cmp.
            if fwd_vals_f64.value(k) > bwd_vals_f64.value(k) {
                fwd[i]
            } else {
                bwd[i]
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

                    match strategy {
                        LocalAsofStrategy::Directional(dir) => {
                            finalize_directional(
                                states,
                                build_state,
                                join_schema,
                                pruned_right_schema,
                                dir,
                            )
                            .await
                        }
                        LocalAsofStrategy::Nearest => {
                            finalize_nearest(states, build_state, join_schema, pruned_right_schema)
                                .await
                        }
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

                                        if let Some(arr) = &left_on_arr {
                                            update_nearest_match(
                                                curr_best_match,
                                                &global_right_on_key_arrs,
                                                candidate,
                                                arr.as_ref(),
                                                global_left_idx,
                                            )?;
                                        } else {
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
