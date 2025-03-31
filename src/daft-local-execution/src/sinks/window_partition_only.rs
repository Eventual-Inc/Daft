use std::{collections::HashSet, sync::Arc};

use common_error::DaftResult;
use daft_core::prelude::{SchemaRef, Series};
use daft_dsl::{resolved_col, Expr, ExprRef};
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
    Accumulating(Vec<MicroPartition>),
    Done,
}

impl WindowPartitionOnlyState {
    fn new(_num_partitions: usize) -> Self {
        Self::Accumulating(Vec::new())
    }

    fn push(
        &mut self,
        input: Arc<MicroPartition>,
        params: &WindowPartitionOnlyParams,
    ) -> DaftResult<()> {
        if let Self::Accumulating(ref mut partitions) = self {
            // Keep using partition_by_value as it's critical for window functions
            let (mut partitioned, _partition_values) =
                input.partition_by_value(&params.partition_by)?;
            partitions.append(&mut partitioned);
            Ok(())
        } else {
            panic!("WindowPartitionOnlySink should be in Accumulating state");
        }
    }

    fn finalize(&mut self) -> Vec<MicroPartition> {
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

/// Parameters for window partition operations
struct WindowPartitionOnlyParams {
    original_aggregations: Vec<ExprRef>,
    partition_by: Vec<ExprRef>,
    partial_agg_exprs: Vec<ExprRef>,
    final_agg_exprs: Vec<ExprRef>,
    final_projections: Vec<ExprRef>,
    window_column_names: Vec<String>,
    window_column_mapping: Vec<(String, String)>,
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
            .map(extract_agg_expr)
            .collect::<DaftResult<Vec<_>>>()?;

        // Get partial and final aggregation expressions
        let (partial_aggs, final_aggs, mut final_projections) =
            daft_physical_plan::populate_aggregation_stages(&aggregations, schema, partition_by);

        // Create simple internal names for window columns
        let window_column_names = aggregations
            .iter()
            .enumerate()
            .map(|(i, _)| format!("window_col_{}", i))
            .collect::<Vec<_>>();

        // Map internal names to expected output names
        let window_column_mapping = window_column_names
            .iter()
            .enumerate()
            .map(|(i, internal_name)| (format!("window_{}", i), internal_name.clone()))
            .collect::<Vec<_>>();

        // Alias the projections to use our internal names
        let partition_col_count = partition_by.len();
        for (i, window_name) in window_column_names.iter().enumerate() {
            let idx = i + partition_col_count;
            if idx < final_projections.len() {
                final_projections[idx] = final_projections[idx].clone().alias(window_name.as_str());
            }
        }

        // Convert aggregations to expression references
        let partial_agg_exprs = partial_aggs
            .into_values()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect();

        let final_agg_exprs = final_aggs
            .into_values()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect();

        let original_aggregations = aggregations
            .into_iter()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect();

        // Create parameters struct
        let params = WindowPartitionOnlyParams {
            original_aggregations,
            partition_by: partition_by.to_vec(),
            partial_agg_exprs,
            final_agg_exprs,
            final_projections,
            window_column_names,
            window_column_mapping,
        };

        Ok(Self {
            window_partition_only_params: Arc::new(params),
        })
    }

    fn num_partitions(&self) -> usize {
        *NUM_CPUS
    }
}

/// Create window fields and batch from columns
fn create_window_batch(
    window_column_mapping: &[(String, String)],
    window_cols: &[Series],
    batch_len: usize,
) -> DaftResult<RecordBatch> {
    // Create renamed series that have the expected names (window_0, window_1, etc.)
    let renamed_window_cols = window_cols
        .iter()
        .zip(window_column_mapping.iter())
        .map(|(col, (expected_name, _))| col.rename(expected_name))
        .collect::<Vec<_>>();

    // Create schema directly from renamed series
    let fields = renamed_window_cols
        .iter()
        .map(|col| daft_core::prelude::Field::new(col.name(), col.data_type().clone()))
        .collect::<Vec<_>>();

    let window_schema = daft_core::prelude::Schema::new(fields)?;
    RecordBatch::new_with_size(Arc::new(window_schema), renamed_window_cols, batch_len)
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
        spawner
            .spawn(
                async move {
                    let mut all_partitions = Vec::new();
                    for mut state in states {
                        let state = state
                            .as_any_mut()
                            .downcast_mut::<WindowPartitionOnlyState>()
                            .expect("State type mismatch");
                        all_partitions.extend(state.finalize());
                    }

                    if all_partitions.is_empty() {
                        return Ok(None);
                    }

                    let mut processed_partitions = Vec::new();
                    for partition in all_partitions {
                        let original_tables = partition.get_tables()?;
                        if original_tables.is_empty() {
                            continue;
                        }

                        // Compute window functions for this partition
                        let final_projected = if !params.partial_agg_exprs.is_empty() {
                            let partial =
                                partition.agg(&params.partial_agg_exprs, &params.partition_by)?;

                            let aggregated = if !params.final_agg_exprs.is_empty() {
                                partial.agg(&params.final_agg_exprs, &params.partition_by)?
                            } else {
                                partial
                            };

                            aggregated.eval_expression_list(&params.final_projections)?
                        } else {
                            let aggregated = partition
                                .agg(&params.original_aggregations, &params.partition_by)?;
                            aggregated.eval_expression_list(&params.final_projections)?
                        };

                        // Create window projections
                        let mut window_projection_exprs = Vec::with_capacity(
                            params.partition_by.len() + params.window_column_names.len(),
                        );

                        // Add partition columns and window columns to projection expressions
                        window_projection_exprs.extend(params.partition_by.iter().cloned());
                        window_projection_exprs.extend(
                            params
                                .window_column_names
                                .iter()
                                .map(|name| resolved_col(name.as_str())),
                        );

                        if window_projection_exprs.is_empty() {
                            continue;
                        }

                        let renamed_aggs =
                            final_projected.eval_expression_list(&window_projection_exprs)?;
                        let agg_tables = renamed_aggs.get_tables()?;
                        if agg_tables.is_empty() {
                            continue;
                        }

                        let agg_table = &agg_tables.as_ref()[0];

                        let mut processed_tables = Vec::with_capacity(original_tables.len());
                        for original_batch in original_tables.iter() {
                            if original_batch.is_empty() {
                                continue;
                            }

                            // Use first row for broadcasting since all rows in a partition have the same values
                            let window_cols = params
                                .window_column_names
                                .iter()
                                .filter_map(|window_name| {
                                    agg_table.get_column(window_name.as_str()).ok().and_then(
                                        |col| {
                                            col.slice(0, 1)
                                                .and_then(|value| {
                                                    value.broadcast(original_batch.len())
                                                })
                                                .ok()
                                        },
                                    )
                                })
                                .collect::<Vec<_>>();

                            if !window_cols.is_empty() {
                                // Get original batch column names as a HashSet for efficient lookups
                                let original_col_names: HashSet<String> =
                                    original_batch.column_names().into_iter().collect();

                                // Filter out any window column mappings that would clash with original columns
                                let filtered_window_mapping: Vec<(String, String)> = params
                                    .window_column_mapping
                                    .iter()
                                    .filter(|(user_name, _)| {
                                        !original_col_names.contains(user_name)
                                    })
                                    .cloned()
                                    .collect();

                                // Create a set of indices to keep
                                let keep_indices: HashSet<usize> = filtered_window_mapping
                                    .iter()
                                    .filter_map(|(_, internal_name)| {
                                        params
                                            .window_column_names
                                            .iter()
                                            .position(|n| n == internal_name)
                                    })
                                    .collect();

                                // Filter the window columns
                                let filtered_window_cols: Vec<Series> = window_cols
                                    .into_iter()
                                    .enumerate()
                                    .filter_map(|(i, col)| {
                                        if keep_indices.contains(&i) {
                                            Some(col)
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();

                                let window_batch = create_window_batch(
                                    &filtered_window_mapping,
                                    &filtered_window_cols,
                                    original_batch.len(),
                                )?;

                                processed_tables.push(original_batch.union(&window_batch)?);
                            } else {
                                processed_tables.push(original_batch.clone());
                            }
                        }

                        if processed_tables.is_empty() {
                            continue;
                        }

                        processed_partitions.push(MicroPartition::new_loaded(
                            processed_tables[0].schema.clone(),
                            Arc::new(processed_tables),
                            None,
                        ));
                    }

                    if processed_partitions.is_empty() {
                        return Ok(None);
                    }

                    // Combine all processed partition results
                    Ok(Some(Arc::new(MicroPartition::concat(
                        &processed_partitions,
                    )?)))
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
