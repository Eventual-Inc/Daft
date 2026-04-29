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

pub(crate) struct AsofJoinBuilt {
    // Original left RecordBatch (all rows concatenated)
    left_rb: RecordBatch,
    // Concatenated on_key as a Daft Series – used for null-validity checks.
    left_on_series: Series,
    // Concatenated on_key as an Arrow array – used for sort and binary search comparators.
    left_on_arr: Arc<dyn Array>,

    // group_hash_map maps a right row's by_key hash to candidate group's index, g, for fast lookup,
    // group_reps[g] holds the by_key values for group g
    // group_buckets[g] holds the left row indices for group g sorted by on_key for binary search.
    group_buckets: Vec<Vec<u64>>,
    group_reps: RecordBatch,
    group_hash_map: HashMap<u64, Vec<usize>>,
    // Whether the join has by_key columns.
    has_by_keys: bool,
    // Total number of left rows.
    total_left_rows: usize,
}

impl AsofJoinBuilt {
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

            // group_hash_map maps a right row's by_key hash to candidate group's index, g, for fast lookup,
            // group_reps[g] holds the by_key values for group g
            // group_buckets[g] holds the left row indices for group g sorted by on_key for binary search.

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
}

pub(crate) struct AsofJoinProbeState {
    build_contents: Vec<MicroPartition>,
    probe_contents: Vec<MicroPartition>,
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
    type FinalizedBuildState = Vec<MicroPartition>;
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
        Ok(state.tables).into()
    }

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        Ok(AsofJoinBuildState { tables: Vec::new() })
    }

    fn make_probe_state(
        &self,
        finalized_build_state: Self::FinalizedBuildState,
    ) -> Self::ProbeState {
        AsofJoinProbeState {
            build_contents: finalized_build_state,
            probe_contents: Vec::new(),
        }
    }

    fn probe(
        &self,
        input: MicroPartition,
        mut state: Self::ProbeState,
        _spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        if !input.is_empty() {
            state.probe_contents.push(input);
        }
        Ok((state, ProbeOutput::NeedMoreInput(None))).into()
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
