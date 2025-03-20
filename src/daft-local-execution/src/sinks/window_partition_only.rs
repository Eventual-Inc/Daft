use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{prelude::SchemaRef, series::Series};
use daft_dsl::{resolved_col, Column, Expr, ExprRef, ResolvedColumn};
use daft_micropartition::MicroPartition;
use daft_physical_plan::extract_agg_expr;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, NUM_CPUS};

/// State for window partition operations
enum WindowPartitionOnlyState {
    Accumulating {
        inner_states: Vec<Vec<MicroPartition>>,
    },
    Done,
}

impl WindowPartitionOnlyState {
    fn new(num_partitions: usize) -> Self {
        let inner_states = (0..num_partitions).map(|_| Vec::new()).collect();
        Self::Accumulating { inner_states }
    }

    fn push(
        &mut self,
        input: Arc<MicroPartition>,
        params: &WindowPartitionOnlyParams,
    ) -> DaftResult<()> {
        match self {
            Self::Accumulating { inner_states } => {
                let partitioned =
                    input.partition_by_hash(params.partition_by.as_slice(), inner_states.len())?;

                for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
                    state.push(p);
                }
                Ok(())
            }
            Self::Done => Err(common_error::DaftError::ValueError(
                "WindowPartitionOnlySink should be in Accumulating state".to_string(),
            )),
        }
    }

    fn finalize(&mut self) -> Vec<Vec<MicroPartition>> {
        match self {
            Self::Accumulating { inner_states } => {
                let res = std::mem::take(inner_states);
                *self = Self::Done;
                res
            }
            Self::Done => Vec::new(),
        }
    }
}

impl BlockingSinkState for WindowPartitionOnlyState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Parameters for window partition operations
struct WindowPartitionOnlyParams {
    original_aggregations: Vec<ExprRef>,
    partition_by: Vec<ExprRef>,
    partial_agg_exprs: Vec<ExprRef>,
    final_agg_exprs: Vec<ExprRef>,
    final_projections: Vec<ExprRef>,
    window_column_names: Vec<String>,
}

/// Window function implementation
pub struct WindowPartitionOnlySink {
    window_partition_only_params: Arc<WindowPartitionOnlyParams>,
}

impl WindowPartitionOnlySink {
    pub fn new(
        aggregations: &[ExprRef],
        partition_by: &[ExprRef],
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        let aggregations = aggregations
            .iter()
            .map(|expr| match expr.as_ref() {
                Expr::Function { func, inputs: _ } => {
                    if let daft_dsl::functions::FunctionExpr::Window(window_func) = func {
                        extract_agg_expr(&window_func.expr)
                    } else {
                        extract_agg_expr(expr)
                    }
                }
                _ => extract_agg_expr(expr),
            })
            .collect::<DaftResult<Vec<_>>>()?;

        let (partial_aggs, final_aggs, final_projections) =
            daft_physical_plan::populate_aggregation_stages(&aggregations, schema, partition_by);

        let window_column_names = (0..aggregations.len())
            .map(|i| format!("window_{}", i))
            .collect();

        let partial_agg_exprs = partial_aggs
            .into_values()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect();

        let final_agg_exprs = final_aggs
            .into_values()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect();

        Ok(Self {
            window_partition_only_params: Arc::new(WindowPartitionOnlyParams {
                original_aggregations: aggregations
                    .into_iter()
                    .map(|e| Arc::new(Expr::Agg(e)))
                    .collect(),
                partition_by: partition_by.to_vec(),
                partial_agg_exprs,
                final_agg_exprs,
                final_projections,
                window_column_names,
            }),
        })
    }

    fn num_partitions(&self) -> usize {
        *NUM_CPUS
    }
}

impl BlockingSink for WindowPartitionOnlySink {
    #[instrument(skip_all, name = "WindowPartitionOnlySink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        let params = self.window_partition_only_params.clone();
        spawner
            .spawn(
                async move {
                    let agg_state = state
                        .as_any_mut()
                        .downcast_mut::<WindowPartitionOnlyState>()
                        .expect("WindowPartitionOnlySink should have WindowPartitionOnlyState");

                    agg_state.push(input, &params)?;
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "WindowPartitionOnlySink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.window_partition_only_params.clone();
        let num_partitions = self.num_partitions();
        spawner
            .spawn(
                async move {
                    // Extract state values from each state
                    let mut state_iters = states
                        .into_iter()
                        .map(|mut state| {
                            state
                                .as_any_mut()
                                .downcast_mut::<WindowPartitionOnlyState>()
                                .expect(
                                    "WindowPartitionOnlySink should have WindowPartitionOnlyState",
                                )
                                .finalize()
                                .into_iter()
                        })
                        .collect::<Vec<_>>();

                    // Process each partition in parallel
                    let mut per_partition_finalize_tasks = tokio::task::JoinSet::new();
                    for _ in 0..num_partitions {
                        // Get the next state for each task
                        let per_partition_state = state_iters
                            .iter_mut()
                            .filter_map(|state| state.next())
                            .collect::<Vec<_>>();

                        let params = params.clone();
                        per_partition_finalize_tasks.spawn(async move {
                            // Skip if no data
                            let partitions: Vec<MicroPartition> =
                                per_partition_state.into_iter().flatten().collect();

                            if partitions.is_empty() {
                                return Ok(None);
                            }

                            // Concatenate partitions
                            let original_data = MicroPartition::concat(&partitions)?;
                            let original_tables = original_data.get_tables()?;
                            if original_tables.is_empty() {
                                return Ok(None);
                            }

                            // Compute aggregations
                            let aggregated = if !params.partial_agg_exprs.is_empty() {
                                // Multi-stage aggregation (for complex aggs like mean)
                                let partial = original_data
                                    .agg(&params.partial_agg_exprs, &params.partition_by)?;
                                if !params.final_agg_exprs.is_empty() {
                                    partial.agg(&params.final_agg_exprs, &params.partition_by)?
                                } else {
                                    partial
                                }
                            } else {
                                // Simple aggregation
                                original_data
                                    .agg(&params.original_aggregations, &params.partition_by)?
                            };

                            // Apply final projections
                            let final_projected =
                                aggregated.eval_expression_list(&params.final_projections)?;

                            // Create projection expressions
                            let mut window_projection_exprs = Vec::with_capacity(
                                params.partition_by.len() + params.window_column_names.len(),
                            );

                            // Add partition columns
                            for i in 0..params.partition_by.len() {
                                if let Some(field_name) =
                                    final_projected.schema().fields.keys().nth(i)
                                {
                                    window_projection_exprs.push(resolved_col(field_name.as_str()));
                                }
                            }

                            // Add window aggregation columns
                            let partition_col_offset = params.partition_by.len();
                            for (i, window_name) in params.window_column_names.iter().enumerate() {
                                let agg_idx = i + partition_col_offset;
                                if agg_idx < final_projected.schema().fields.len() {
                                    if let Some(agg_col_name) =
                                        final_projected.schema().fields.keys().nth(agg_idx)
                                    {
                                        window_projection_exprs.push(
                                            resolved_col(agg_col_name.as_str())
                                                .alias(window_name.as_str()),
                                        );
                                    }
                                }
                            }

                            if window_projection_exprs.is_empty() {
                                return Ok(None);
                            }

                            // Apply projections to rename columns
                            let renamed_aggs =
                                final_projected.eval_expression_list(&window_projection_exprs)?;
                            let agg_tables = renamed_aggs.get_tables()?;
                            if agg_tables.is_empty() {
                                return Ok(None);
                            }

                            let agg_table = &agg_tables.as_ref()[0];

                            // Extract partition column names
                            let partition_col_names: Vec<String> = params
                                .partition_by
                                .iter()
                                .filter_map(|expr| {
                                    if let Expr::Column(col) = expr.as_ref() {
                                        Some(match col {
                                            Column::Resolved(ResolvedColumn::Basic(name)) => {
                                                name.as_ref().to_string()
                                            }
                                            Column::Resolved(ResolvedColumn::JoinSide(name, _)) => {
                                                name.as_ref().to_string()
                                            }
                                            Column::Resolved(ResolvedColumn::OuterRef(field)) => {
                                                field.name.to_string()
                                            }
                                            Column::Unresolved(unresolved) => {
                                                unresolved.name.to_string()
                                            }
                                        })
                                    } else {
                                        None
                                    }
                                })
                                .collect();

                            // Build lookup dictionary
                            let mut agg_dict = std::collections::HashMap::new();
                            for row_idx in 0..agg_table.len() {
                                // Create key from partition columns
                                let key_parts: Vec<String> = partition_col_names
                                    .iter()
                                    .filter_map(|col_name| {
                                        agg_table.get_column(col_name).ok().and_then(|col| {
                                            col.slice(row_idx, row_idx + 1)
                                                .ok()
                                                .map(|value| format!("{:?}", value))
                                        })
                                    })
                                    .collect();

                                if key_parts.len() == partition_col_names.len() {
                                    agg_dict.insert(key_parts.join("|"), row_idx);
                                }
                            }

                            // Process each record batch
                            let mut processed_tables = Vec::with_capacity(original_tables.len());
                            for original_batch in original_tables.iter() {
                                if original_batch.is_empty() {
                                    continue;
                                }

                                // Process rows in the batch
                                let mut rows_with_aggs = Vec::with_capacity(original_batch.len());

                                for row_idx in 0..original_batch.len() {
                                    // Extract partition values for this row
                                    let key_parts: Vec<String> = partition_col_names
                                        .iter()
                                        .filter_map(|col_name| {
                                            original_batch.get_column(col_name).ok().and_then(
                                                |col| {
                                                    col.slice(row_idx, row_idx + 1)
                                                        .ok()
                                                        .map(|value| format!("{:?}", value))
                                                },
                                            )
                                        })
                                        .collect();

                                    // Look up the aggregation for this row
                                    if key_parts.len() == partition_col_names.len() {
                                        let key = key_parts.join("|");
                                        rows_with_aggs.push((row_idx, agg_dict.get(&key).copied()));
                                    } else {
                                        rows_with_aggs.push((row_idx, None));
                                    }
                                }

                                // Create result columns
                                let mut result_columns = Vec::with_capacity(
                                    original_batch.num_columns() + params.window_column_names.len(),
                                );

                                // Add original columns
                                for col_idx in 0..original_batch.num_columns() {
                                    if let Ok(col) = original_batch.get_column_by_index(col_idx) {
                                        result_columns.push(col.clone());
                                    }
                                }

                                // Add window columns
                                for window_name in &params.window_column_names {
                                    if let Ok(agg_col) = agg_table.get_column(window_name) {
                                        // Create window column values for each row
                                        let mut values = Vec::with_capacity(original_batch.len());

                                        for (_, agg_row_idx) in &rows_with_aggs {
                                            if let Some(idx) = agg_row_idx {
                                                if let Ok(value) = agg_col.slice(*idx, *idx + 1) {
                                                    values.push(value);
                                                }
                                            } else if let Ok(null_value) = agg_col.slice(0, 0) {
                                                values.push(null_value);
                                            }
                                        }

                                        // Concatenate values into a window column
                                        if !values.is_empty() {
                                            let values_ref: Vec<&Series> = values.iter().collect();
                                            if let Ok(combined) =
                                                Series::concat(values_ref.as_slice())
                                            {
                                                result_columns.push(combined.rename(window_name));
                                            }
                                        }
                                    }
                                }

                                // Create result table
                                if !result_columns.is_empty() {
                                    if let Ok(result_table) =
                                        RecordBatch::from_nonempty_columns(result_columns)
                                    {
                                        processed_tables.push(result_table);
                                    }
                                }
                            }

                            if processed_tables.is_empty() {
                                return Ok(None);
                            }

                            Ok(Some(MicroPartition::new_loaded(
                                processed_tables[0].schema.clone(),
                                Arc::new(processed_tables),
                                None,
                            )))
                        });
                    }

                    // Collect and combine results
                    let results: Vec<_> = per_partition_finalize_tasks
                        .join_all()
                        .await
                        .into_iter()
                        .collect::<DaftResult<Vec<_>>>()?
                        .into_iter()
                        .flatten()
                        .collect();

                    if results.is_empty() {
                        return Ok(None);
                    }

                    Ok(Some(Arc::new(MicroPartition::concat(&results)?)))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "WindowPartitionOnly"
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!(
                "WindowPartitionOnly: {}",
                self.window_partition_only_params
                    .original_aggregations
                    .iter()
                    .map(|e| e.to_string())
                    .join(", ")
            ),
            format!(
                "Partition by: {}",
                self.window_partition_only_params
                    .partition_by
                    .iter()
                    .map(|e| e.to_string())
                    .join(", ")
            ),
        ]
    }

    fn max_concurrency(&self) -> usize {
        *NUM_CPUS
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(WindowPartitionOnlyState::new(
            self.num_partitions(),
        )))
    }
}
