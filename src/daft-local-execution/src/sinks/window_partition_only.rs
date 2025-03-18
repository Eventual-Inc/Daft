use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{join::JoinType, prelude::SchemaRef};
use daft_dsl::{resolved_col, Expr, ExprRef};
use daft_micropartition::MicroPartition;
use daft_physical_plan::extract_agg_expr;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, NUM_CPUS};

enum WindowPartitionOnlyState {
    Accumulating {
        inner_states: Vec<Vec<MicroPartition>>,
    },
    Done,
}

impl WindowPartitionOnlyState {
    fn new(num_partitions: usize) -> Self {
        let inner_states = (0..num_partitions).map(|_| Vec::new()).collect::<Vec<_>>();
        Self::Accumulating { inner_states }
    }

    fn push(
        &mut self,
        input: Arc<MicroPartition>,
        params: &WindowPartitionOnlyParams,
    ) -> DaftResult<()> {
        let Self::Accumulating {
            ref mut inner_states,
        } = self
        else {
            panic!("WindowPartitionOnlySink should be in Accumulating state");
        };

        let partitioned =
            input.partition_by_hash(params.partition_by.as_slice(), inner_states.len())?;
        for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
            state.push(p);
        }
        Ok(())
    }

    fn finalize(&mut self) -> Vec<Vec<MicroPartition>> {
        let res = if let Self::Accumulating {
            ref mut inner_states,
            ..
        } = self
        {
            std::mem::take(inner_states)
        } else {
            panic!("WindowPartitionOnlySink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

impl BlockingSinkState for WindowPartitionOnlyState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct WindowPartitionOnlyParams {
    // Original aggregation expressions
    original_aggregations: Vec<ExprRef>,
    // Partition by expressions
    partition_by: Vec<ExprRef>,
    // First stage aggregation expressions
    partial_agg_exprs: Vec<ExprRef>,
    // Second stage aggregation expressions
    final_agg_exprs: Vec<ExprRef>,
    // Column name mappings for window results
    window_column_names: Vec<String>,
}

pub struct WindowPartitionOnlySink {
    window_partition_only_params: Arc<WindowPartitionOnlyParams>,
}

impl WindowPartitionOnlySink {
    pub fn new(
        aggregations: &[ExprRef],
        partition_by: &[ExprRef],
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        // Extract aggregation expressions from window functions
        let aggregations = aggregations
            .iter()
            .map(|expr| {
                // Check if this is a window function (Function::Window variant)
                match expr.as_ref() {
                    Expr::Function { func, inputs: _ } => {
                        // If this is a window function, extract the inner aggregation
                        if let daft_dsl::functions::FunctionExpr::Window(window_func) = func {
                            // Extract the inner expression (which should be an aggregation)
                            extract_agg_expr(&window_func.expr)
                        } else {
                            // For regular functions, try to extract the aggregation normally
                            extract_agg_expr(expr)
                        }
                    }
                    // For non-function expressions, use the standard extraction
                    _ => extract_agg_expr(expr),
                }
            })
            .collect::<DaftResult<Vec<_>>>()?;

        // Use the same multi-stage approach as grouped aggregates
        let (partial_aggs, final_aggs, _final_projections) =
            daft_physical_plan::populate_aggregation_stages(&aggregations, schema, partition_by);

        // Generate window column names based on the window function count
        let window_column_names = (0..aggregations.len())
            .map(|i| format!("window_{}", i))
            .collect::<Vec<_>>();

        // Convert first stage aggregations to expressions
        let partial_agg_exprs = partial_aggs
            .into_values()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect::<Vec<_>>();

        // Convert second stage aggregations to expressions
        let final_agg_exprs = final_aggs
            .into_values()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect::<Vec<_>>();

        Ok(Self {
            window_partition_only_params: Arc::new(WindowPartitionOnlyParams {
                original_aggregations: aggregations
                    .into_iter()
                    .map(|e| Arc::new(Expr::Agg(e)))
                    .collect(),
                partition_by: partition_by.to_vec(),
                partial_agg_exprs,
                final_agg_exprs,
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

                    let mut per_partition_finalize_tasks = tokio::task::JoinSet::new();
                    for _ in 0..num_partitions {
                        let per_partition_state = state_iters
                            .iter_mut()
                            .map(|state| {
                                state.next().expect(
                                    "WindowPartitionOnlyState should have Vec<MicroPartition>",
                                )
                            })
                            .collect::<Vec<_>>();
                        let params = params.clone();
                        per_partition_finalize_tasks.spawn(async move {
                            // Skip empty partitions
                            if per_partition_state.is_empty() {
                                return Ok(None);
                            }

                            // Concatenate all micropartitions for this partition
                            let partitions: Vec<MicroPartition> =
                                per_partition_state.into_iter().flatten().collect();
                            if partitions.is_empty() {
                                return Ok(None);
                            }

                            // Get the original data
                            let original_data = MicroPartition::concat(&partitions)?;

                            // Compute the aggregate values per partition
                            let aggregated = if !params.partial_agg_exprs.is_empty() {
                                // First apply partial aggregations
                                let partial = original_data
                                    .agg(&params.partial_agg_exprs, &params.partition_by)?;

                                // Then apply final aggregations if needed
                                if !params.final_agg_exprs.is_empty() {
                                    partial.agg(&params.final_agg_exprs, &params.partition_by)?
                                } else {
                                    partial
                                }
                            } else {
                                // Apply direct aggregations
                                original_data
                                    .agg(&params.original_aggregations, &params.partition_by)?
                            };

                            // Rename the aggregated columns to the desired window function names
                            let mut window_projection_exprs = Vec::new();

                            // First add the partition columns (needed for join)
                            for (i, _) in params.partition_by.iter().enumerate() {
                                let field_name = aggregated
                                    .schema()
                                    .fields
                                    .keys()
                                    .nth(i)
                                    .unwrap()
                                    .to_string();
                                window_projection_exprs.push(resolved_col(field_name));
                            }

                            // Then add the aggregate columns with their window names
                            let num_partition_cols = params.partition_by.len();
                            for (i, window_name) in params.window_column_names.iter().enumerate() {
                                if i < aggregated.schema().fields.len() - num_partition_cols {
                                    let agg_col_name = aggregated
                                        .schema()
                                        .fields
                                        .keys()
                                        .nth(num_partition_cols + i)
                                        .unwrap()
                                        .to_string();
                                    window_projection_exprs.push(
                                        resolved_col(agg_col_name).alias(window_name.clone()),
                                    );
                                }
                            }

                            // Create the renamed aggregated result
                            let renamed_aggs =
                                aggregated.eval_expression_list(&window_projection_exprs)?;

                            // Join the aggregated values back to the original data
                            // This preserves all original rows while adding the aggregated values
                            let joined = original_data.hash_join(
                                &renamed_aggs,
                                &params.partition_by, // Join on partition columns (left)
                                &params
                                    .partition_by
                                    .iter()
                                    .take(num_partition_cols)
                                    .cloned()
                                    .collect::<Vec<_>>(), // Join on partition columns (right)
                                None,
                                JoinType::Inner,
                            )?;

                            Ok(Some(joined))
                        });
                    }

                    // Collect results from all partitions
                    let results = per_partition_finalize_tasks
                        .join_all()
                        .await
                        .into_iter()
                        .collect::<DaftResult<Vec<_>>>()?
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>();

                    if results.is_empty() {
                        return Ok(None);
                    }

                    // Combine all partition results
                    let concated = MicroPartition::concat(&results)?;
                    Ok(Some(Arc::new(concated)))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "WindowPartitionOnly"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!(
            "WindowPartitionOnly: {}",
            self.window_partition_only_params
                .original_aggregations
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        display.push(format!(
            "Partition by: {}",
            self.window_partition_only_params
                .partition_by
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        display
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
