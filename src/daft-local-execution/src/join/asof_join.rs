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
    array::ops::{GroupIndices, VecIndices, arrow::comparison::build_multi_array_is_equal},
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

type ByKeyHashesAndComparator<'a> = (
    &'a UInt64Array,
    &'a (dyn Fn(usize, usize) -> bool + Send + Sync),
);

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
                group_buckets: vec![],
                group_bucket_sorted_keys: vec![],
                group_reps: RecordBatch::empty(None),
                group_reps_series: vec![],
                group_hash_map: HashMap::new(),
            });
        }

        let left_on_series: Series = left_rb.eval_expression(left_on)?;
        let left_on_arr: Arc<dyn Array> = left_on_series.to_arrow()?;

        let on_key_sort_cmp =
            build_partial_compare_with_nulls(left_on_arr.as_ref(), left_on_arr.as_ref(), false)?;

        let (group_buckets, group_reps, group_hash_map) = if left_by.is_empty() {
            let mut bucket: VecIndices = (0..left_rb.len() as u64).collect();
            bucket.retain(|idx| left_on_arr.is_valid(*idx as usize));
            bucket.sort_unstable_by(|&a, &b| on_key_sort_cmp(a as usize, b as usize).unwrap());
            (vec![bucket], RecordBatch::empty(None), HashMap::new())
        } else {
            let left_by_rb = left_rb.eval_expression_list(left_by)?;
            let (key_idxs, raw_groups) = left_by_rb.make_groups()?;

            let group_buckets: GroupIndices = raw_groups
                .into_iter()
                .map(|mut bucket| {
                    bucket.retain(|idx| left_on_arr.is_valid(*idx as usize));
                    bucket.sort_unstable_by(|&a, &b| {
                        on_key_sort_cmp(a as usize, b as usize).unwrap_or(Ordering::Equal)
                    });
                    bucket
                })
                .collect();

            let key_idx_arr = UInt64Array::from_vec("key_idx", key_idxs);
            let group_reps = left_by_rb.take(&key_idx_arr)?;
            let group_hashes = group_reps.hash_rows()?;
            let mut group_hash_map: HashMap<u64, Vec<usize>> =
                HashMap::with_capacity(group_hashes.len());
            for (g, &h) in group_hashes.values().iter().enumerate() {
                group_hash_map.entry(h).or_default().push(g);
            }

            (group_buckets, group_reps, group_hash_map)
        };

        let group_bucket_sorted_keys: Vec<Arc<dyn Array>> = group_buckets
            .iter()
            .map(|bucket| {
                let indexes = UInt64Array::from_iter(
                    Field::new("k", DaftDataType::UInt64),
                    bucket.iter().copied().map(Some),
                );
                left_on_series.take(&indexes)?.to_arrow()
            })
            .collect::<DaftResult<_>>()?;

        let group_reps_series = group_reps
            .as_materialized_series()
            .into_iter()
            .cloned()
            .collect();
        Ok(AsofJoinFinalizedBuildState {
            left_rb,
            group_buckets,
            group_bucket_sorted_keys,
            group_reps,
            group_reps_series,
            group_hash_map,
        })
    }
}

pub(crate) struct AsofJoinFinalizedBuildState {
    // Original left RecordBatch (all rows concatenated)
    left_rb: RecordBatch,
    // group_buckets[g] holds the left row indices for group g, sorted by on_key for binary search.
    group_buckets: GroupIndices,
    // Compact sorted-key arrays parallel to group_buckets. Used for binary search to avoid cache misses.
    group_bucket_sorted_keys: Vec<Arc<dyn Array>>,
    // group_reps[g] holds the by_key values to confirm an actual match (not just a hash match).
    group_reps: RecordBatch,
    // Materialized series view of group_reps, parallel to group_buckets.
    group_reps_series: Vec<Series>,
    // Maps a by_key hash to a list of candidate group indices; multiple candidates exist only on hash collision.
    group_hash_map: HashMap<u64, Vec<usize>>,
}

impl AsofJoinFinalizedBuildState {
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
                let candidates = self.group_hash_map.get(&h)?;
                candidates.iter().copied().find(|&g| eq_cmp(g, right_idx))
            }
        }
    }
}

pub(crate) struct AsofJoinProbeState {
    build_contents: Arc<AsofJoinFinalizedBuildState>,
    // Per left row: (rb_idx, row_idx) of the current best right match.
    best_match: Vec<Option<(u32, u32)>>,
    // All right RecordBatches seen so far, stored for final output construction.
    right_tables: Vec<RecordBatch>,
    // Per right RecordBatch: its on_key as an Arrow array, used to build cross-batch comparators
    // for the "is this a better match?" check during probe.
    right_on_key_arrs: Vec<Arc<dyn Array>>,
}

impl AsofJoinProbeState {
    fn probe_batch(
        &mut self,
        right_rb: &RecordBatch,
        right_on: &BoundExpr,
        right_by: &[BoundExpr],
        right_cols_to_drop: &HashSet<String>,
    ) -> DaftResult<()> {
        let rb_idx = self.right_tables.len();
        let build_state = self.build_contents.clone();

        let right_on_series = right_rb.eval_expression(right_on)?;
        let right_on_arr: Arc<dyn Array> = right_on_series.to_arrow()?;
        self.right_on_key_arrs.push(right_on_arr.clone());
        self.right_tables
            .push(prune_right_batch(right_rb, right_cols_to_drop));

        // One comparator per group
        let group_sorted_key_cmps: Vec<DynPartialComparator> = build_state
            .group_bucket_sorted_keys
            .iter()
            .map(|sk| build_partial_compare_with_nulls(sk.as_ref(), right_on_arr.as_ref(), false))
            .collect::<DaftResult<_>>()?;

        // this way update_best_match can compare same-rb candidates without rebuilding it.
        let mut cmp_cache: HashMap<(usize, usize), DynPartialComparator> = HashMap::new();
        cmp_cache.insert(
            (rb_idx, rb_idx),
            build_partial_compare_with_nulls(right_on_arr.as_ref(), right_on_arr.as_ref(), false)?,
        );

        // Passed to find_left_group() to hash and equality-match the right row's by_key against left groups.
        let by_key_hashes_and_comparator: Option<(_, _)> = if !build_state.group_hash_map.is_empty()
        {
            let right_by_rb = right_rb.eval_expression_list(right_by)?;
            let hashes = right_by_rb.hash_rows()?;
            let num_by_cols = build_state.group_reps.num_columns();
            let eq_cmp = build_multi_array_is_equal(
                &build_state.group_reps_series,
                &right_by_rb
                    .as_materialized_series()
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>(),
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

        for right_idx in 0..right_rb.len() {
            if !right_on_series.is_valid(right_idx) {
                continue;
            }

            let Some(group_idx) =
                build_state.find_left_group(right_idx, by_key_hashes_and_comparator_ref)
            else {
                continue;
            };

            let bucket = &build_state.group_buckets[group_idx];
            let Some(matched_left_idx) =
                build_state.search_bucket(bucket, &group_sorted_key_cmps[group_idx], right_idx)
            else {
                continue;
            };

            update_best_match(
                &mut self.best_match[matched_left_idx],
                &self.right_on_key_arrs,
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

fn backfill(global_best: &mut [Option<(u32, u32)>], group_buckets: &GroupIndices) {
    for bucket in group_buckets {
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
    all_right_tables: Vec<RecordBatch>,
    pruned_right_schema: SchemaRef,
) -> DaftResult<RecordBatch> {
    let mut right_row_offsets = vec![0usize; all_right_tables.len() + 1];
    for (i, rb) in all_right_tables.iter().enumerate() {
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
    let right_rb_concat = if all_right_tables.is_empty() {
        RecordBatch::empty(Some(pruned_right_schema))
    } else {
        RecordBatch::concat(all_right_tables.iter().collect::<Vec<_>>().as_slice())?
    };
    right_rb_concat.take(&right_idx_arr)
}

fn build_join_output(
    left_rb: &RecordBatch,
    right_out: RecordBatch,
    join_schema: SchemaRef,
) -> DaftResult<MicroPartition> {
    let mut join_series: Vec<Series> = Vec::with_capacity(join_schema.len());
    for s in left_rb.as_materialized_series() {
        join_series.push(s.clone());
    }
    for s in right_out.as_materialized_series() {
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
        let join_table = state.finalize(self.left_schema.clone(), &self.left_by, &self.left_on)?;
        Ok(Arc::new(join_table))
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
            right_tables: Vec::new(),
            right_on_key_arrs: Vec::new(),
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
                    // right_tables list. global_rb_offsets[k] converts state k's local_rb_idx
                    // to a global_rb_idx into the flat global_right_on_key_arrs / all_right_tables.
                    let global_rb_offsets: Vec<usize> = states
                        .iter()
                        .scan(0usize, |acc, state| {
                            let offset = *acc;
                            *acc += state.right_tables.len();
                            Some(offset)
                        })
                        .collect();

                    let global_right_on_key_arrs: Vec<Arc<dyn Array>> = states
                        .iter()
                        .flat_map(|s| s.right_on_key_arrs.iter().cloned())
                        .collect();

                    let (state_best_matches, right_tables_per_state): (Vec<_>, Vec<_>) = states
                        .into_iter()
                        .map(|s| (s.best_match, s.right_tables))
                        .unzip();
                    let all_right_tables: Vec<RecordBatch> =
                        right_tables_per_state.into_iter().flatten().collect();

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

                    backfill(&mut global_best, &build_state.group_buckets);
                    let right_out =
                        build_right_output(&global_best, all_right_tables, pruned_right_schema)?;
                    Ok(Some(build_join_output(
                        &build_state.left_rb,
                        right_out,
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
