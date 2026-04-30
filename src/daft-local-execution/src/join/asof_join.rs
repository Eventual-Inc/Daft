use std::{
    cmp::Ordering,
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

use arrow_array::Array;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{
    array::ops::arrow::comparison::build_multi_array_is_equal,
    kernels::search_sorted::{DynPartialComparator, build_partial_compare_with_nulls},
    prelude::{DataType as DaftDataType, Field, SchemaRef, Series, UInt64Array},
};
use daft_dsl::{
    Expr,
    expr::bound_expr::BoundExpr,
    join::{get_right_cols_to_drop, infer_asof_join_schema},
};
use daft_groupby::IntoGroups;

type ByKeyHashesAndComparator<'a> = (
    &'a UInt64Array,
    &'a (dyn Fn(usize, usize) -> bool + Send + Sync),
);
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
pub(crate) struct AsofJoinBuildState {
    tables: Vec<MicroPartition>,
}

// Produced once by finalize_build() and shared with all probe workers.
// Contains the sorted group structure needed for binary-search matching.
pub(crate) struct AsofJoinFinalizedBuildState {
    // Original left RecordBatch (all rows concatenated)
    left_rb: RecordBatch,
    // Concatenated on_key as a Daft Series – used for null-validity checks.
    left_on_series: Series,
    // Concatenated on_key as an Arrow array – used for sort and binary search comparators.
    left_on_arr: Arc<dyn Array>,
    // group_hash_map maps a by_key hash to a list of candidate group indices (Vec<usize>).
    // Multiple candidates exist only on hash collision; typically there is just one.
    // For each candidate group index g:
    //   - group_reps[g] holds the by_key values to confirm an actual match (not just a hash match).
    //   - group_buckets[g] holds the left row indices for group g, sorted by on_key for binary search.
    group_buckets: Vec<Vec<u64>>,
    group_reps: RecordBatch,
    group_hash_map: HashMap<u64, Vec<usize>>,
    // Whether the join has by_key columns.
    has_by_keys: bool,
    // Total number of left rows.
    total_left_rows: usize,
}

impl AsofJoinFinalizedBuildState {
    fn new(
        left_mps: Vec<MicroPartition>,
        left_schema: SchemaRef,
        left_by: &[BoundExpr],
        left_on: &BoundExpr,
    ) -> DaftResult<Self> {
        let left_mp = MicroPartition::concat_or_empty(left_mps, left_schema.clone())?;
        let left_rb = match left_mp.concat_or_get()? {
            Some(rb) => rb,
            None => {
                let empty_left = RecordBatch::empty(Some(left_schema));
                let left_on_series = empty_left.eval_expression(left_on)?;
                let left_on_arr: Arc<dyn Array> = left_on_series.to_arrow()?;
                return Ok(Self {
                    left_rb: empty_left,
                    left_on_series,
                    left_on_arr,
                    group_buckets: vec![],
                    group_reps: RecordBatch::empty(None),
                    group_hash_map: HashMap::new(),
                    has_by_keys: !left_by.is_empty(),
                    total_left_rows: 0,
                });
            }
        };

        let total_left_rows = left_rb.len();
        let left_on_series = left_rb.eval_expression(left_on)?;
        let left_on_arr: Arc<dyn Array> = left_on_series.to_arrow()?;

        let on_key_sort_cmp =
            build_partial_compare_with_nulls(left_on_arr.as_ref(), left_on_arr.as_ref(), false)?;

        let (group_buckets, group_reps, group_hash_map) = if left_by.is_empty() {
            let mut bucket: Vec<u64> = (0..total_left_rows as u64).collect();
            bucket.sort_unstable_by(|&a, &b| {
                on_key_sort_cmp(a as usize, b as usize).unwrap_or(Ordering::Equal)
            });
            (vec![bucket], RecordBatch::empty(None), HashMap::new())
        } else {
            let left_by_rb = left_rb.eval_expression_list(left_by)?;
            let (key_idxs, raw_groups) = left_by_rb.make_groups()?;

            // Sort each bucket by on_key value
            let group_buckets: Vec<Vec<u64>> = raw_groups
                .into_iter()
                .map(|group| {
                    let mut bucket: Vec<u64> = group.into_vec();
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

        Ok(Self {
            left_rb,
            left_on_series,
            left_on_arr,
            group_buckets,
            group_reps,
            group_hash_map,
            has_by_keys: !left_by.is_empty(),
            total_left_rows,
        })
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
            match on_key_cmp(bucket[mid] as usize, right_idx) {
                Some(Ordering::Less) => lo = mid + 1,
                _ => hi = mid,
            }
        }
        let candidate_left_idx = *bucket.get(lo)? as usize;
        self.left_on_series
            .is_valid(candidate_left_idx)
            .then_some(candidate_left_idx)
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
    // Per left row: (morsel_idx, row_idx) of the current best right match.
    best_match: Vec<Option<(u32, u32)>>,
    // All right RecordBatches seen so far, stored for final output construction.
    right_tables: Vec<RecordBatch>,
    // Per right morsel: its on_key as an Arrow array, used to build cross-morsel comparators
    // for the "is this a better match?" check during probe.
    right_on_key_arrs: Vec<Arc<dyn Array>>,
}

impl AsofJoinProbeState {
    fn probe_batch(
        &mut self,
        right_rb: &RecordBatch,
        right_on: &BoundExpr,
        right_by: &[BoundExpr],
        current_morsel_idx: usize,
    ) -> DaftResult<()> {
        let table = self.build_contents.clone();

        let right_on_series = right_rb.eval_expression(right_on)?;
        let right_on_arr: Arc<dyn Array> = right_on_series.to_arrow()?;

        //we use this later when we call search_bucket()
        let on_key_cmp = build_partial_compare_with_nulls(
            table.left_on_arr.as_ref(),
            right_on_arr.as_ref(),
            false,
        )?;

        // we use this later for update_best_match()
        let mut cmp_cache: HashMap<usize, DynPartialComparator> = HashMap::new();
        cmp_cache.insert(
            current_morsel_idx,
            build_partial_compare_with_nulls(right_on_arr.as_ref(), right_on_arr.as_ref(), false)?,
        );

        // we use this later when we call find_left_group()
        let by_key_hashes_and_comparator: Option<(_, _)> = if table.has_by_keys {
            let right_by_rb = right_rb.eval_expression_list(right_by)?;
            let hashes = right_by_rb.hash_rows()?;
            let num_by_cols = table.group_reps.num_columns();
            let eq_cmp = build_multi_array_is_equal(
                &table
                    .group_reps
                    .as_materialized_series()
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>(),
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
                table.find_left_group(right_idx, by_key_hashes_and_comparator_ref)
            else {
                continue;
            };

            let bucket = &table.group_buckets[group_idx];
            let Some(candidate_left_idx) = table.search_bucket(bucket, &on_key_cmp, right_idx)
            else {
                continue;
            };

            self.update_best_match(
                candidate_left_idx,
                right_idx,
                current_morsel_idx,
                &right_on_arr,
                &mut cmp_cache,
            )?;
        }

        self.right_on_key_arrs.push(right_on_arr);
        Ok(())
    }
    fn update_best_match(
        &mut self,
        candidate_left_idx: usize,
        right_idx: usize,
        current_morsel_idx: usize,
        right_on_arr: &Arc<dyn Array>,
        cmp_cache: &mut HashMap<usize, DynPartialComparator>,
    ) -> DaftResult<()> {
        let is_better = match self.best_match[candidate_left_idx] {
            None => true,
            Some(old_packed) => {
                let (old_morsel_idx, old_row) = (old_packed.0 as usize, old_packed.1 as usize);
                let cmp = match cmp_cache.entry(old_morsel_idx) {
                    Entry::Occupied(e) => e.into_mut(),
                    Entry::Vacant(e) => {
                        let old_arr = self.right_on_key_arrs[old_morsel_idx].clone();
                        let c = build_partial_compare_with_nulls(
                            right_on_arr.as_ref(),
                            old_arr.as_ref(),
                            false,
                        )?;
                        e.insert(c)
                    }
                };
                matches!(cmp(right_idx, old_row), Some(Ordering::Greater))
            }
        };
        if is_better {
            self.best_match[candidate_left_idx] =
                Some((current_morsel_idx as u32, right_idx as u32));
        }
        Ok(())
    }
}

pub struct AsofJoinOperator {
    left_by: Vec<BoundExpr>,
    right_by: Vec<BoundExpr>,
    left_on: BoundExpr,
    right_on: BoundExpr,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    right_cols_to_drop: std::collections::HashSet<String>,
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
            right_schema,
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
        if !input.is_empty() {
            state.tables.push(input);
        }
        Ok(state).into()
    }

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self> {
        let join_table = AsofJoinFinalizedBuildState::new(
            state.tables,
            self.left_schema.clone(),
            &self.left_by,
            &self.left_on,
        )?;
        Ok(Arc::new(join_table))
    }

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        Ok(AsofJoinBuildState { tables: Vec::new() })
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Self::FinalizedBuildState,
    ) -> Self::ProbeState {
        let n = finalized_build_state.total_left_rows;
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

        spawner
            .spawn(
                async move {
                    let table = state.build_contents.clone();
                    if table.total_left_rows == 0 {
                        return Ok((state, ProbeOutput::NeedMoreInput(None)));
                    }
                    for right_rb in input.record_batches() {
                        if right_rb.is_empty() {
                            continue;
                        }
                        let current_morsel_idx = state.right_tables.len();
                        state.probe_batch(right_rb, &right_on, &right_by, current_morsel_idx)?;
                        state.right_tables.push(right_rb.clone());
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
        let state = states
            .into_iter()
            .next()
            .expect("AsofJoin probe finalize: expected exactly one state");

        let join_schema = self.join_schema.clone();
        let right_cols_to_drop = self.right_cols_to_drop.clone();
        let right_schema = self.right_schema.clone();

        spawner
            .spawn(
                async move {
                    let mut state = state;
                    let table = state.build_contents.clone();

                    if table.total_left_rows == 0 {
                        return Ok(Some(MicroPartition::new_loaded(
                            join_schema.clone(),
                            Arc::new(vec![RecordBatch::empty(Some(join_schema))]),
                            None,
                        )));
                    }
                    // backfill
                    for bucket in &table.group_buckets {
                        for i in 1..bucket.len() {
                            let prev_global = bucket[i - 1] as usize;
                            let curr_global = bucket[i] as usize;
                            // Skip null-on-key left rows: they are invalid match targets.
                            if !table.left_on_series.is_valid(curr_global) {
                                continue;
                            }
                            if state.best_match[curr_global].is_none()
                                && state.best_match[prev_global].is_some()
                            {
                                state.best_match[curr_global] = state.best_match[prev_global];
                            }
                        }
                    }
                    let mut morsel_offsets = vec![0usize; state.right_tables.len() + 1];
                    for (i, rb) in state.right_tables.iter().enumerate() {
                        morsel_offsets[i + 1] = morsel_offsets[i] + rb.len();
                    }

                    let n = table.total_left_rows;
                    let right_idx_arr = UInt64Array::from_iter(
                        Field::new("right_idx", DaftDataType::UInt64),
                        (0..n).map(|i| {
                            state.best_match[i].map(|(morsel_idx, row_idx)| {
                                (morsel_offsets[morsel_idx as usize] + row_idx as usize) as u64
                            })
                        }),
                    );

                    let right_rb_concat = if state.right_tables.is_empty() {
                        RecordBatch::empty(Some(right_schema))
                    } else {
                        RecordBatch::concat(
                            state.right_tables.iter().collect::<Vec<_>>().as_slice(),
                        )?
                    };
                    let right_out = right_rb_concat.take(&right_idx_arr)?;

                    //build final table
                    let mut join_series: Vec<Series> = Vec::with_capacity(join_schema.len());

                    for s in table.left_rb.as_materialized_series() {
                        join_series.push(s.clone());
                    }

                    for s in right_out.as_materialized_series() {
                        if !right_cols_to_drop.contains(s.name()) {
                            join_series.push(s.clone());
                        }
                    }

                    let output_rb =
                        RecordBatch::new_with_size(join_schema.clone(), join_series, n)?;

                    Ok(Some(MicroPartition::new_loaded(
                        join_schema,
                        Arc::new(vec![output_rb]),
                        None,
                    )))
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

    fn max_probe_concurrency(&self) -> usize {
        1
    }

    fn needs_probe_finalization(&self) -> bool {
        true
    }
}
