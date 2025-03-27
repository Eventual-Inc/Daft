use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::{Column, Expr, ExprRef, ResolvedColumn};
use daft_micropartition::MicroPartition;
use daft_physical_plan::extract_agg_expr;
use daft_recordbatch::RecordBatch;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, NUM_CPUS};

/// State for window partition-order operations
enum WindowPartitionOrderByState {
    Accumulating {
        inner_states: Vec<Vec<MicroPartition>>,
    },
    Done,
}

impl WindowPartitionOrderByState {
    fn new(num_partitions: usize) -> Self {
        let inner_states = (0..num_partitions).map(|_| Vec::new()).collect::<Vec<_>>();
        Self::Accumulating { inner_states }
    }

    fn push(
        &mut self,
        input: Arc<MicroPartition>,
        params: &WindowPartitionOrderByParams,
    ) -> DaftResult<()> {
        let Self::Accumulating {
            ref mut inner_states,
        } = self
        else {
            panic!("WindowPartitionOrderBySink should be in Accumulating state");
        };

        // Keep using partition_by_value as it's critical for window functions
        let (partitioned, _partition_values) = input.partition_by_value(&params.partition_by)?;

        for (partition_idx, mp) in partitioned.into_iter().enumerate() {
            if partition_idx >= inner_states.len() {
                inner_states.resize_with(partition_idx + 1, Vec::new);
            }
            inner_states[partition_idx].push(mp);
        }
        Ok(())
    }

    fn finalize(&mut self) -> Vec<Vec<MicroPartition>> {
        let res = if let Self::Accumulating {
            ref mut inner_states,
        } = self
        {
            std::mem::take(inner_states)
        } else {
            panic!("WindowPartitionOrderBySink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

/// Helper function to compute a deterministic hash for partition keys
#[allow(dead_code)]
fn compute_partition_key_hash(
    batch: &RecordBatch,
    partition_col_names: &[String],
    row_idx: usize,
) -> Option<u64> {
    let mut key_hasher = DefaultHasher::new();

    for col_name in partition_col_names {
        if let Ok(col) = batch.get_column(col_name) {
            if let Ok(value) = col.slice(row_idx, row_idx + 1) {
                // Use a stable string representation for hashing
                value.to_string().hash(&mut key_hasher);
            } else {
                return None;
            }
        } else {
            return None;
        }
    }

    let hash_value = key_hasher.finish();
    Some(hash_value)
}

impl BlockingSinkState for WindowPartitionOrderByState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Parameters for window partition-order operations
struct WindowPartitionOrderByParams {
    original_aggregations: Vec<ExprRef>,
    partition_by: Vec<ExprRef>,
    order_by: Vec<ExprRef>,
    ascending: Vec<bool>,
    partial_agg_exprs: Vec<ExprRef>,
    final_agg_exprs: Vec<ExprRef>,
    final_projections: Vec<ExprRef>,
    #[allow(dead_code)]
    window_column_names: Vec<String>,
    #[allow(dead_code)]
    window_column_mapping: Vec<(String, String)>,
}

pub struct WindowPartitionOrderBySink {
    window_partition_order_by_params: Arc<WindowPartitionOrderByParams>,
}

impl WindowPartitionOrderBySink {
    pub fn new(
        aggregations: &[ExprRef],
        partition_by: &[ExprRef],
        order_by: &[ExprRef],
        ascending: &[bool],
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        // Extract the aggregation expressions similar to window_partition_only
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

        let (partial_aggs, final_aggs, mut final_projections) =
            daft_physical_plan::populate_aggregation_stages(&aggregations, schema, partition_by);

        // For window functions, we need to ensure each output column has a unique name
        // These will be used as internal column names during processing
        let window_column_names = aggregations
            .iter()
            .enumerate()
            .map(|(i, agg_expr)| {
                // Use the aggregation operation type as part of the name
                let operation_name = match agg_expr {
                    daft_dsl::AggExpr::Sum(_) => "sum",
                    daft_dsl::AggExpr::Min(_) => "min",
                    daft_dsl::AggExpr::Max(_) => "max",
                    daft_dsl::AggExpr::Mean(_) => "mean",
                    daft_dsl::AggExpr::Count(_, _) => "count",
                    _ => "agg",
                };

                // Get the column being aggregated if applicable
                let col_name = match agg_expr {
                    daft_dsl::AggExpr::Sum(e)
                    | daft_dsl::AggExpr::Min(e)
                    | daft_dsl::AggExpr::Max(e)
                    | daft_dsl::AggExpr::Mean(e)
                    | daft_dsl::AggExpr::Count(e, _) => {
                        // Try to extract the column name from the expression
                        if let Ok(field) = e.to_field(schema) {
                            field.name
                        } else {
                            format!("expr_{}", i)
                        }
                    }
                    _ => format!("expr_{}", i),
                };

                // Create a unique name that includes the operation, column name, and index
                format!("{}_{}_window_{}", operation_name, col_name, i)
            })
            .collect::<Vec<_>>();

        // Determine the expected output column names for each window function
        // This maps from the expected window output column name to our internal window column name
        let mut window_column_mapping = Vec::new();

        // We need to extract what alias the user expects for each window function
        // Then make sure our final projection maps to these names
        for (i, _expr) in aggregations.iter().enumerate() {
            // The last column in the window output should correspond to the window function i
            let internal_name = &window_column_names[i];

            // The logical plan expects window_0, window_1, etc.
            let expected_name = format!("window_{}", i);

            window_column_mapping.push((expected_name, internal_name.clone()));
        }

        // Replace the final projections with ones that have our unique window column names as aliases
        // Skip the partition columns in final_projections
        let partition_col_count = partition_by.len();
        for (i, window_name) in window_column_names.iter().enumerate() {
            let idx = i + partition_col_count;
            if idx < final_projections.len() {
                // Replace the projection with an aliased version to ensure unique names during processing
                let proj = final_projections[idx].clone().alias(window_name.as_str());
                final_projections[idx] = proj;
            }
        }

        let partial_agg_exprs = partial_aggs
            .into_values()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect::<Vec<_>>();

        let final_agg_exprs = final_aggs
            .into_values()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect::<Vec<_>>();

        Ok(Self {
            window_partition_order_by_params: Arc::new(WindowPartitionOrderByParams {
                original_aggregations: aggregations
                    .into_iter()
                    .map(|e| Arc::new(Expr::Agg(e)))
                    .collect(),
                partition_by: partition_by.to_vec(),
                order_by: order_by.to_vec(),
                ascending: ascending.to_vec(),
                partial_agg_exprs,
                final_agg_exprs,
                final_projections,
                window_column_names,
                window_column_mapping,
            }),
        })
    }

    fn num_partitions(&self) -> usize {
        *NUM_CPUS
    }
}

/// Extract column names from expressions
#[allow(dead_code)]
fn extract_column_names(exprs: &[ExprRef]) -> Vec<String> {
    exprs
        .iter()
        .filter_map(|expr| {
            if let Expr::Column(col) = expr.as_ref() {
                Some(match col {
                    Column::Resolved(ResolvedColumn::Basic(name)) => name.as_ref().to_string(),
                    Column::Resolved(ResolvedColumn::JoinSide(name, _)) => {
                        name.as_ref().to_string()
                    }
                    _ => return None,
                })
            } else {
                None
            }
        })
        .collect()
}

impl BlockingSink for WindowPartitionOrderBySink {
    #[instrument(skip_all, name = "WindowPartitionOrderBySink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        let params = self.window_partition_order_by_params.clone();
        spawner
            .spawn(
                async move {
                    let agg_state = state
                        .as_any_mut()
                        .downcast_mut::<WindowPartitionOrderByState>()
                        .expect(
                            "WindowPartitionOrderBySink should have WindowPartitionOrderByState",
                        );

                    agg_state.push(input, &params)?;

                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "WindowPartitionOrderBySink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.window_partition_order_by_params.clone();
        let _num_partitions = self.num_partitions();

        spawner
            .spawn(
                async move {
                    // Collect all micropartitions by their partition index
                    let mut partitioned_data: Vec<Vec<MicroPartition>> = Vec::new();

                    // Process each state and collect its partitions
                    for mut state in states {
                        let state_partitions = state
                            .as_any_mut()
                            .downcast_mut::<WindowPartitionOrderByState>()
                            .expect("WindowPartitionOrderBySink should have WindowPartitionOrderByState")
                            .finalize();

                        // Ensure our partitioned_data vector is big enough
                        if partitioned_data.len() < state_partitions.len() {
                            partitioned_data.resize_with(state_partitions.len(), Vec::new);
                        }

                        // Add each partition's data to our collection
                        for (idx, partition) in state_partitions.into_iter().enumerate() {
                            partitioned_data[idx].extend(partition);
                        }
                    }

                    if partitioned_data.is_empty() {
                        return Ok(None);
                    }

                    // Now process each partition's data in parallel
                    let mut process_partition_tasks = tokio::task::JoinSet::<DaftResult<Option<Arc<MicroPartition>>>>::new();

                    for partition_micropartitions in partitioned_data {
                        if partition_micropartitions.is_empty() {
                            continue;
                        }

                        let params = params.clone();
                        process_partition_tasks.spawn(async move {
                            // Process this partition's data
                            let original_data = MicroPartition::concat(&partition_micropartitions)?;
                            let original_tables = original_data.get_tables()?;
                            if original_tables.is_empty() {
                                return Ok(None);
                            }

                            // For now, we use the same window function logic as partition-only
                            // In the future this will be enhanced to handle order-by
                            let final_projected = if !params.partial_agg_exprs.is_empty() {
                                let partial = original_data
                                    .agg(&params.partial_agg_exprs, &params.partition_by)?;

                                let aggregated = if !params.final_agg_exprs.is_empty() {
                                    partial.agg(&params.final_agg_exprs, &params.partition_by)?
                                } else {
                                    partial
                                };

                                aggregated.eval_expression_list(&params.final_projections)?
                            } else {
                                let aggregated = original_data
                                    .agg(&params.original_aggregations, &params.partition_by)?;
                                aggregated.eval_expression_list(&params.final_projections)?
                            };

                            Ok(Some(Arc::new(final_projected)))
                        });
                    }

                    // Collect all the results
                    let mut results = Vec::new();
                    while let Some(result) = process_partition_tasks.join_next().await {
                        match result {
                            Ok(Ok(Some(mp))) => results.push(mp),
                            Ok(Ok(None)) => {}
                            Ok(Err(e)) => return Err(e),
                            Err(e) => {
                                return Err(common_error::DaftError::ComputeError(
                                    format!("Error joining task: {}", e),
                                ))
                            }
                        }
                    }

                    if results.is_empty() {
                        Ok(None)
                    } else if results.len() == 1 {
                        Ok(Some(results.pop().unwrap()))
                    } else {
                        match MicroPartition::concat(&results) {
                            Ok(mp) => Ok(Some(Arc::new(mp))),
                            Err(e) => Err(e),
                        }
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "WindowPartitionOrderBySink"
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![
            format!("WindowPartitionOrderBy"),
            format!(
                "  partition_by: {:?}",
                self.window_partition_order_by_params.partition_by
            ),
            format!(
                "  order_by: {:?}",
                self.window_partition_order_by_params.order_by
            ),
            format!(
                "  ascending: {:?}",
                self.window_partition_order_by_params.ascending
            ),
        ]
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(WindowPartitionOrderByState::new(
            self.num_partitions(),
        )))
    }

    fn max_concurrency(&self) -> usize {
        self.num_partitions()
    }
}
