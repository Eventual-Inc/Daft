use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
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
    Accumulating(Vec<Arc<MicroPartition>>),
    Done,
}

impl WindowPartitionOnlyState {
    fn push(&mut self, input: Arc<MicroPartition>) {
        if let Self::Accumulating(ref mut partitions) = self {
            partitions.push(input);
        } else {
            panic!("WindowPartitionOnlySink should be in Accumulating state");
        }
    }

    fn finalize(&mut self) -> Vec<Arc<MicroPartition>> {
        let res = if let Self::Accumulating(ref mut partitions) = self {
            std::mem::take(partitions)
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
    sink_agg_exprs: Vec<ExprRef>,
    finalize_agg_exprs: Vec<ExprRef>,
    final_projections: Vec<ExprRef>,
    partition_by: Vec<ExprRef>,
    original_schema: SchemaRef,
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
        let aggregations = aggregations
            .iter()
            .map(extract_agg_expr)
            .collect::<DaftResult<Vec<_>>>()?;

        let (sink_aggs, finalize_aggs, final_projections) =
            daft_physical_plan::populate_aggregation_stages(&aggregations, schema, partition_by);

        let sink_agg_exprs = sink_aggs
            .values()
            .cloned()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect::<Vec<_>>();

        let finalize_agg_exprs = finalize_aggs
            .values()
            .cloned()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect::<Vec<_>>();

        Ok(Self {
            window_partition_only_params: Arc::new(WindowPartitionOnlyParams {
                sink_agg_exprs,
                finalize_agg_exprs,
                final_projections,
                partition_by: partition_by.to_vec(),
                original_schema: schema.clone(),
            }),
        })
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
        // Store the original input data for later processing
        spawner
            .spawn(
                async move {
                    let window_state = state
                        .as_any_mut()
                        .downcast_mut::<WindowPartitionOnlyState>()
                        .expect("WindowPartitionOnlySink should have WindowPartitionOnlyState");

                    // Save the original input data
                    window_state.push(input);
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

        spawner
            .spawn(
                async move {
                    // Collect all the original input partitions
                    let all_parts =
                        states
                            .into_iter()
                            .flat_map(|mut state| {
                                state
                            .as_any_mut()
                            .downcast_mut::<WindowPartitionOnlyState>()
                            .expect("WindowPartitionOnlySink should have WindowPartitionOnlyState")
                            .finalize()
                            })
                            .collect::<Vec<_>>();

                    // Concatenate all input partitions to create a single dataset
                    let input_data = Arc::new(MicroPartition::concat(all_parts)?);

                    // Compute partition-based aggregations
                    let partition_aggs =
                        input_data.window_agg(&params.sink_agg_exprs, &params.partition_by)?;

                    // Apply finalize aggregations if needed
                    let finalized_aggs = if !params.finalize_agg_exprs.is_empty() {
                        partition_aggs.agg(&params.finalize_agg_exprs, &params.partition_by)?
                    } else {
                        partition_aggs
                    };

                    // Join the aggregated values back to the original data based on partition keys
                    let result = input_data.hash_join(
                        &finalized_aggs,
                        &params.partition_by[..], // Left join keys (original data)
                        &params.partition_by[..], // Right join keys (aggregated data)
                        None,                     // Use default null_equals_nulls
                        daft_core::join::JoinType::Inner, // Inner join since all rows should have a match
                    )?;

                    // Create a projection list that includes both original and aggregated columns
                    let mut all_projections = Vec::new();
                    let mut added_columns = std::collections::HashSet::new();

                    // Add all original columns from the expected schema
                    for field_name in params.original_schema.fields.keys() {
                        if result.schema().fields.contains_key(field_name)
                            && !added_columns.contains(field_name)
                        {
                            all_projections.push(resolved_col(field_name.clone()));
                            added_columns.insert(field_name.clone());
                        }
                    }

                    // Add the aggregation columns
                    for expr in &params.final_projections {
                        // Get the column name to check for duplicates
                        let expr_str = expr.to_string();
                        let is_duplicate = if expr_str.starts_with("col(") {
                            // For simple columns, extract the name from col(name)
                            let col_name =
                                expr_str.trim_start_matches("col(").trim_end_matches(')');
                            added_columns.contains(col_name)
                        } else {
                            false // For expressions like aliases, assume not duplicate
                        };

                        if !is_duplicate {
                            all_projections.push(expr.clone());
                        }
                    }

                    let final_result = result.eval_expression_list(&all_projections)?;

                    Ok(Some(Arc::new(final_result)))
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
                .sink_agg_exprs
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
        Ok(Box::new(WindowPartitionOnlyState::Accumulating(vec![])))
    }
}
