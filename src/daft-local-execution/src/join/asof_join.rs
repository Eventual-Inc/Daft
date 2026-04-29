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
                let left_on_rb = empty_left.eval_expression_list(std::slice::from_ref(left_on))?;
                let left_on_series = left_on_rb.get_column(0).clone();
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
        let left_on_rb = left_rb.eval_expression_list(std::slice::from_ref(left_on))?;
        let left_on_series = left_on_rb.get_column(0).clone();
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

    /// Binary-search `bucket` for the first left row with on_key >= right_on_arr[r].
    /// Returns the global left row index, or `None` if no valid match exists.
    fn search_bucket(
        &self,
        bucket: &[u64],
        on_key_cmp: &DynPartialComparator,
        r: usize,
    ) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = bucket.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match on_key_cmp(bucket[mid] as usize, r) {
                Some(Ordering::Less) => lo = mid + 1,
                _ => hi = mid,
            }
        }
        let left_global = *bucket.get(lo)? as usize;
        self.left_on_series
            .is_valid(left_global)
            .then_some(left_global)
    }
    /// Find the group index for right row `r`.
    /// Returns `None` if the row belongs to no group (hash miss or equality miss).
    fn find_left_group(
        &self,
        r: usize,
        by_key_eval: Option<(&UInt64Array, &(dyn Fn(usize, usize) -> bool + Send + Sync))>,
    ) -> Option<usize> {
        match by_key_eval {
            None => Some(0),
            Some((hashes, eq_cmp)) => {
                let h = hashes.values()[r];
                let candidates = self.group_hash_map.get(&h)?;
                candidates.iter().copied().find(|&g| eq_cmp(g, r))
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

pub struct AsofJoinOperator {
    left_by: Vec<BoundExpr>,
    right_by: Vec<BoundExpr>,
    left_on: BoundExpr,
    right_on: BoundExpr,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
}

impl AsofJoinOperator {
    pub fn new(
        left_by: Vec<BoundExpr>,
        right_by: Vec<BoundExpr>,
        left_on: BoundExpr,
        right_on: BoundExpr,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
    ) -> Self {
        Self {
            left_by,
            right_by,
            left_on,
            right_on,
            left_schema,
            right_schema,
        }
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
            .expect("Expect exactly one state for AsofJoin probe finalize");
        let left_by = self.left_by.clone();
        let right_by = self.right_by.clone();
        let left_on = self.left_on.clone();
        let right_on = self.right_on.clone();
        let left_schema = self.left_schema.clone();
        let right_schema = self.right_schema.clone();

        spawner
            .spawn(
                async move {
                    let left_mp =
                        MicroPartition::concat_or_empty(state.build_contents, left_schema)?;
                    let right_mp =
                        MicroPartition::concat_or_empty(state.probe_contents, right_schema)?;
                    let joined =
                        left_mp.asof_join(&right_mp, &left_by, &right_by, &left_on, &right_on)?;
                    Ok(Some(joined))
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
