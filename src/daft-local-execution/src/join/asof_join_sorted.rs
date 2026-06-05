use std::{cmp::Ordering, collections::HashSet, sync::Arc};

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{
    datatypes::UInt64Array,
    join::AsofJoinStrategy,
    kernels::cmp::build_partial_compare_with_nulls,
    prelude::SchemaRef,
    series::Series,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{RecordBatch, asof_join_sorted};
use tracing::Span;

use crate::{
    ExecutionTaskSpawner,
    join::join_operator::{
        BuildStateResult, FinalizeBuildResult, JoinOperator, ProbeFinalizeResult, ProbeOutput,
        ProbeResult,
    },
    pipeline::NodeName,
};

pub(crate) struct AsofJoinSortedBuildState {
    /// One RecordBatch per incoming left MicroPartition, preserving partition order.
    left_partitions: Vec<RecordBatch>,
}

#[derive(Clone)]
pub(crate) struct AsofJoinSortedFinalizedBuildState {
    left_partitions: Vec<RecordBatch>,
}

pub(crate) struct AsofJoinSortedProbeState {
    build: Arc<AsofJoinSortedFinalizedBuildState>,
    /// One RecordBatch per incoming right MicroPartition, preserving partition order.
    right_partitions: Vec<RecordBatch>,
    /// Last row(s) per by-group for each right partition (used as the left-neighbor boundary).
    right_suffixes: Vec<RecordBatch>,
    /// First row(s) per by-group for each right partition (used as the right-neighbor boundary).
    right_prefixes: Vec<RecordBatch>,
}

pub struct AsofJoinSortedOperator {
    left_by: Vec<BoundExpr>,
    right_by: Vec<BoundExpr>,
    left_on: BoundExpr,
    right_on: BoundExpr,
    left_schema: SchemaRef,
    join_schema: SchemaRef,
    right_cols_to_keep: HashSet<String>,
    strategy: AsofJoinStrategy,
}

impl AsofJoinSortedOperator {
    pub fn new(
        left_by: Vec<BoundExpr>,
        right_by: Vec<BoundExpr>,
        left_on: BoundExpr,
        right_on: BoundExpr,
        strategy: AsofJoinStrategy,
        left_schema: SchemaRef,
        join_schema: SchemaRef,
    ) -> DaftResult<Self> {
        let left_field_names: HashSet<&str> = left_schema.field_names().collect();
        let right_cols_to_keep = join_schema
            .fields()
            .iter()
            .filter(|f| !left_field_names.contains(f.name.as_ref()))
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

/// Build a key RecordBatch with layout [by_col_0, ..., by_col_k, on_col] from `rb`.
fn build_key_rb(rb: &RecordBatch, by: &[BoundExpr], on: &BoundExpr) -> DaftResult<RecordBatch> {
    let mut cols: Vec<Series> = rb
        .eval_expression_list(by)?
        .as_materialized_series()
        .into_iter()
        .cloned()
        .collect();
    cols.push(rb.eval_expression(on)?);
    RecordBatch::from_nonempty_columns(cols)
}

/// Return the last row of each by-key group in a sorted RecordBatch.
/// With no by-keys, returns just the single last row.
fn last_rows_per_group(rb: &RecordBatch, by: &[BoundExpr]) -> DaftResult<RecordBatch> {
    if rb.is_empty() {
        return Ok(rb.clone());
    }
    if by.is_empty() {
        return rb.slice(rb.len() - 1, rb.len());
    }

    let key_series: Vec<Series> = rb
        .eval_expression_list(by)?
        .as_materialized_series()
        .into_iter()
        .cloned()
        .collect();
    let arrs: Vec<_> = key_series
        .iter()
        .map(|s| s.to_arrow())
        .collect::<DaftResult<Vec<_>>>()?;
    let cmps = arrs
        .iter()
        .map(|a| build_partial_compare_with_nulls(a.as_ref(), a.as_ref(), false))
        .collect::<DaftResult<Vec<_>>>()?;

    let n = rb.len();
    let mut indices: Vec<u64> = Vec::new();
    for i in 0..(n - 1) {
        if cmps.iter().any(|cmp| !matches!(cmp(i, i + 1), Some(Ordering::Equal))) {
            indices.push(i as u64);
        }
    }
    indices.push((n - 1) as u64);

    rb.take(&UInt64Array::from_vec("idx", indices))
}

/// Return the first row of each by-key group in a sorted RecordBatch.
/// With no by-keys, returns just the single first row.
fn first_rows_per_group(rb: &RecordBatch, by: &[BoundExpr]) -> DaftResult<RecordBatch> {
    if rb.is_empty() {
        return Ok(rb.clone());
    }
    if by.is_empty() {
        return rb.slice(0, 1);
    }

    let key_series: Vec<Series> = rb
        .eval_expression_list(by)?
        .as_materialized_series()
        .into_iter()
        .cloned()
        .collect();
    let arrs: Vec<_> = key_series
        .iter()
        .map(|s| s.to_arrow())
        .collect::<DaftResult<Vec<_>>>()?;
    let cmps = arrs
        .iter()
        .map(|a| build_partial_compare_with_nulls(a.as_ref(), a.as_ref(), false))
        .collect::<DaftResult<Vec<_>>>()?;

    let n = rb.len();
    let mut indices: Vec<u64> = vec![0];
    for i in 1..n {
        if cmps.iter().any(|cmp| !matches!(cmp(i - 1, i), Some(Ordering::Equal))) {
            indices.push(i as u64);
        }
    }

    rb.take(&UInt64Array::from_vec("idx", indices))
}

impl JoinOperator for AsofJoinSortedOperator {
    type BuildState = AsofJoinSortedBuildState;
    type FinalizedBuildState = Arc<AsofJoinSortedFinalizedBuildState>;
    type ProbeState = AsofJoinSortedProbeState;

    fn make_build_state(&self) -> DaftResult<Self::BuildState> {
        Ok(AsofJoinSortedBuildState { left_partitions: Vec::new() })
    }

    fn build(
        &self,
        input: MicroPartition,
        mut state: Self::BuildState,
        _spawner: &ExecutionTaskSpawner,
    ) -> BuildStateResult<Self> {
        let non_empty: Vec<RecordBatch> =
            input.record_batches().iter().filter(|rb| !rb.is_empty()).cloned().collect();
        RecordBatch::concat_or_empty(&non_empty, Some(self.left_schema.clone()))
            .map(|rb| {
                state.left_partitions.push(rb);
                state
            })
            .into()
    }

    fn finalize_build(&self, state: Self::BuildState) -> FinalizeBuildResult<Self> {
        Ok(Arc::new(AsofJoinSortedFinalizedBuildState {
            left_partitions: state.left_partitions,
        }))
    }

    fn make_probe_state(&self, finalized_build_state: Self::FinalizedBuildState) -> Self::ProbeState {
        AsofJoinSortedProbeState {
            build: finalized_build_state,
            right_partitions: Vec::new(),
            right_suffixes: Vec::new(),
            right_prefixes: Vec::new(),
        }
    }

    fn probe(
        &self,
        input: MicroPartition,
        mut state: Self::ProbeState,
        _spawner: &ExecutionTaskSpawner,
    ) -> ProbeResult<Self> {
        // Use input.schema() as the fallback so empty partitions retain a valid schema.
        let schema = input.schema();
        let non_empty: Vec<RecordBatch> =
            input.record_batches().iter().filter(|rb| !rb.is_empty()).cloned().collect();
        let right_by = self.right_by.clone();
        RecordBatch::concat_or_empty(&non_empty, Some(schema))
            .and_then(|rb| {
                // Pre-compute boundary rows so finalize_probe only needs O(n_groups) rows
                // from the neighbor partition instead of the full O(partition_size).
                let suffix = last_rows_per_group(&rb, &right_by)?;
                let prefix = first_rows_per_group(&rb, &right_by)?;
                state.right_suffixes.push(suffix);
                state.right_prefixes.push(prefix);
                state.right_partitions.push(rb);
                Ok((state, ProbeOutput::NeedMoreInput(None)))
            })
            .into()
    }

    fn finalize_probe(
        &self,
        states: Vec<Self::ProbeState>,
        spawner: &ExecutionTaskSpawner,
    ) -> ProbeFinalizeResult {
        let strategy = self.strategy;
        let left_by = self.left_by.clone();
        let left_on = self.left_on.clone();
        let right_by = self.right_by.clone();
        let right_on = self.right_on.clone();
        let join_schema = self.join_schema.clone();
        let right_cols_to_keep = self.right_cols_to_keep.clone();

        spawner
            .spawn(
                async move {
                    let build = states[0].build.clone();
                    let mut right_partitions: Vec<RecordBatch> = Vec::new();
                    let mut right_suffixes: Vec<RecordBatch> = Vec::new();
                    let mut right_prefixes: Vec<RecordBatch> = Vec::new();
                    for mut s in states {
                        right_partitions.append(&mut s.right_partitions);
                        right_suffixes.append(&mut s.right_suffixes);
                        right_prefixes.append(&mut s.right_prefixes);
                    }

                    let left_partitions = &build.left_partitions;
                    let n = left_partitions.len();

                    if n != right_partitions.len() {
                        return Err(common_error::DaftError::ValueError(format!(
                            "aligned asof join requires equal partition counts: left={} right={}",
                            n,
                            right_partitions.len()
                        )));
                    }

                    if n == 0 {
                        let empty = RecordBatch::empty(Some(join_schema.clone()));
                        return Ok(Some(MicroPartition::new_loaded(
                            join_schema,
                            Arc::new(vec![empty]),
                            None,
                        )));
                    }

                    // Precompute right payload column indices (same schema across all partitions).
                    let right_kept_col_indices: Vec<usize> = right_partitions[0]
                        .schema
                        .fields()
                        .iter()
                        .enumerate()
                        .filter_map(|(i, f)| {
                            right_cols_to_keep.contains(f.name.as_ref()).then_some(i)
                        })
                        .collect();

                    let mut output_batches: Vec<RecordBatch> = Vec::with_capacity(n);

                    for i in 0..n {
                        let left_rb = &left_partitions[i];
                        if left_rb.is_empty() {
                            continue;
                        }

                        // For the neighbor partition, include only the boundary rows
                        // (last row per group for backward, first row per group for forward).
                        // This avoids scanning O(partition_size) neighbor rows when only
                        // O(n_groups) boundary rows can ever be the match.
                        let right_parts: Vec<RecordBatch> = match strategy {
                            AsofJoinStrategy::Backward => {
                                let mut parts = Vec::new();
                                if let Some(prev) = i.checked_sub(1) {
                                    parts.push(right_suffixes[prev].clone());
                                }
                                parts.push(right_partitions[i].clone());
                                parts
                            }
                            AsofJoinStrategy::Forward => {
                                let mut parts = vec![right_partitions[i].clone()];
                                if i + 1 < n {
                                    parts.push(right_prefixes[i + 1].clone());
                                }
                                parts
                            }
                            AsofJoinStrategy::Nearest => {
                                let mut parts = Vec::new();
                                if let Some(prev) = i.checked_sub(1) {
                                    parts.push(right_suffixes[prev].clone());
                                }
                                parts.push(right_partitions[i].clone());
                                if i + 1 < n {
                                    parts.push(right_prefixes[i + 1].clone());
                                }
                                parts
                            }
                        };

                        let right_data_rb = RecordBatch::concat_or_empty(&right_parts, None)?;

                        let left_key_rb = build_key_rb(left_rb, &left_by, &left_on)?;
                        let right_key_rb = build_key_rb(&right_data_rb, &right_by, &right_on)?;

                        let (_, right_indices) =
                            asof_join_sorted(&left_key_rb, &right_key_rb, strategy)?;

                        let matched_right = right_data_rb
                            .get_columns(&right_kept_col_indices)
                            .take(&right_indices)?;

                        let mut join_series: Vec<Series> =
                            left_rb.as_materialized_series().into_iter().cloned().collect();
                        join_series
                            .extend(matched_right.as_materialized_series().into_iter().cloned());
                        let output_rb = RecordBatch::new_with_size(
                            join_schema.clone(),
                            join_series,
                            left_rb.len(),
                        )?;
                        output_batches.push(output_rb);
                    }

                    if output_batches.is_empty() {
                        let empty = RecordBatch::empty(Some(join_schema.clone()));
                        return Ok(Some(MicroPartition::new_loaded(
                            join_schema,
                            Arc::new(vec![empty]),
                            None,
                        )));
                    }

                    // Return output_batches directly instead of concat-ing into one large
                    // RecordBatch — MicroPartition supports multiple batches natively.
                    Ok(Some(MicroPartition::new_loaded(
                        join_schema,
                        Arc::new(output_batches),
                        None,
                    )))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "AsofJoin (sorted)".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::AsofJoin
    }

    fn multiline_display(&self) -> Vec<String> {
        vec!["AsofJoin (sorted)".to_string()]
    }

    fn needs_probe_finalization(&self) -> bool {
        true
    }

    fn max_probe_concurrency(&self) -> usize {
        1
    }
}
