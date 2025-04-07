use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::{Schema, SchemaRef};
use daft_dsl::{resolved_col, Expr, ExprRef};
use daft_io::IOStatsContext;
use daft_micropartition::MicroPartition;
use daft_physical_plan::extract_agg_expr;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, NUM_CPUS};

#[derive(Default)]
struct SinglePartitionWindowState {
    partitions: Vec<Arc<MicroPartition>>,
}

enum WindowPartitionOnlyState {
    Accumulating {
        inner_states: Vec<Option<SinglePartitionWindowState>>,
    },
    Done,
}

impl WindowPartitionOnlyState {
    fn new(num_partitions: usize) -> Self {
        let inner_states = (0..num_partitions).map(|_| None).collect::<Vec<_>>();
        Self::Accumulating { inner_states }
    }

    fn push(&mut self, input: Arc<MicroPartition>, partition_by: &[ExprRef]) -> DaftResult<()> {
        if let Self::Accumulating {
            ref mut inner_states,
        } = self
        {
            let partitioned = input.partition_by_hash(partition_by, inner_states.len())?;
            for (p, state) in partitioned.into_iter().zip(inner_states.iter_mut()) {
                let state = state.get_or_insert_with(SinglePartitionWindowState::default);
                state.partitions.push(Arc::new(p));
            }
        } else {
            panic!("WindowPartitionOnlySink should be in Accumulating state");
        }
        Ok(())
    }

    fn finalize(&mut self) -> Vec<Option<SinglePartitionWindowState>> {
        let res = if let Self::Accumulating {
            ref mut inner_states,
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
                    let window_state = state
                        .as_any_mut()
                        .downcast_mut::<WindowPartitionOnlyState>()
                        .expect("WindowPartitionOnlySink should have WindowPartitionOnlyState");

                    window_state.push(input, &params.partition_by)?;
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
        println!("\n============ WINDOW PARTITION EXECUTION DETAILS ============");
        println!("Original schema: {:?}", params.original_schema);

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

                    let mut per_partition_tasks = tokio::task::JoinSet::new();

                    println!("\n----- INPUT PARTITIONS -----");
                    for _partition_idx in 0..num_partitions {
                        let per_partition_state = state_iters.iter_mut().map(|state| {
                            state.next().expect(
                                "WindowPartitionOnlyState should have SinglePartitionWindowState",
                            )
                        });

                        // Collect all partition data for this partition index
                        let all_partitions: Vec<Arc<MicroPartition>> = per_partition_state
                            .flatten()
                            .flat_map(|state| state.partitions)
                            .collect();

                        if all_partitions.is_empty() {
                            continue;
                        }

                        // Print details of partitions
                        for (i, part) in all_partitions.iter().enumerate() {
                            println!("Input partition #{} (size: {} rows):", i, part.len());
                            println!("Schema: {:?}", part.schema());

                            // Print sample data
                            let io_stats = IOStatsContext::new("print_partition_details");
                            if let Ok(tables) = part.concat_or_get(io_stats.clone()) {
                                if !tables.is_empty() {
                                    println!("{}", tables[0]);
                                }
                            }
                        }

                        let params = params.clone();
                        per_partition_tasks.spawn(async move {
                            if all_partitions.is_empty() {
                                return Ok(None);
                            }

                            // Concatenate all partitions for this partition index
                            let input_data = Arc::new(MicroPartition::concat(&all_partitions)?);
                            println!("\nConcatenated input data size: {} rows", input_data.len());

                            println!("\n----- WINDOW AGGREGATION -----");
                            println!("Partition expressions: {:?}", params.partition_by);
                            println!("Aggregation expressions: {:?}", params.sink_agg_exprs);

                            // Perform window aggregation on the concatenated data
                            let partition_aggs = input_data
                                .window_agg(&params.sink_agg_exprs, &params.partition_by)?;

                            // Print window aggregation results
                            println!(
                                "Window aggregation result (size: {} rows):",
                                partition_aggs.len()
                            );
                            println!("Schema: {:?}", partition_aggs.schema());

                            let io_stats = IOStatsContext::new("print_window_agg_details");
                            let agg_tables = partition_aggs.concat_or_get(io_stats.clone())?;
                            if !agg_tables.is_empty() {
                                println!("{}", agg_tables[0]);
                            }

                            println!("\n----- FINALIZATION AGGREGATION -----");
                            let finalized_aggs = if !params.finalize_agg_exprs.is_empty() {
                                println!(
                                    "Finalization expressions: {:?}",
                                    params.finalize_agg_exprs
                                );
                                let result = partition_aggs
                                    .agg(&params.finalize_agg_exprs, &params.partition_by)?;
                                println!("Finalization result (size: {} rows):", result.len());
                                println!("Schema: {:?}", result.schema());

                                let finalize_tables = result.concat_or_get(io_stats.clone())?;
                                if !finalize_tables.is_empty() {
                                    println!("{}", finalize_tables[0]);
                                }
                                result
                            } else {
                                println!("No finalization aggregation needed");
                                partition_aggs
                            };

                            println!("\n----- HASH JOIN -----");
                            println!("Joining on: {:?}", params.partition_by);
                            let result = input_data.hash_join(
                                &finalized_aggs,
                                &params.partition_by[..],
                                &params.partition_by[..],
                                None,
                                daft_core::join::JoinType::Inner,
                            )?;

                            println!("Join result (size: {} rows):", result.len());
                            println!("Schema: {:?}", result.schema());

                            let join_tables = result.concat_or_get(io_stats.clone())?;
                            if !join_tables.is_empty() {
                                println!("{}", join_tables[0]);
                            }

                            println!("\n----- FINAL PROJECTION -----");
                            let mut all_projections = Vec::new();
                            let mut added_columns = std::collections::HashSet::new();

                            for field_name in params.original_schema.fields.keys() {
                                if result.schema().fields.contains_key(field_name)
                                    && !added_columns.contains(field_name)
                                {
                                    println!("Adding column: {}", field_name);
                                    all_projections.push(resolved_col(field_name.clone()));
                                    added_columns.insert(field_name.clone());
                                }
                            }

                            for expr in &params.final_projections {
                                let expr_str = expr.to_string();
                                let is_duplicate = if expr_str.starts_with("col(") {
                                    let col_name =
                                        expr_str.trim_start_matches("col(").trim_end_matches(')');
                                    added_columns.contains(col_name)
                                } else {
                                    false
                                };

                                if !is_duplicate {
                                    println!("Adding expression: {}", expr_str);
                                    all_projections.push(expr.clone());
                                } else {
                                    println!("Skipping duplicate: {}", expr_str);
                                }
                            }

                            println!("Final projection list: {:?}", all_projections);
                            let final_result = result.eval_expression_list(&all_projections)?;

                            println!("\n----- FINAL RESULT -----");
                            println!("Final result (size: {} rows):", final_result.len());
                            println!("Schema: {:?}", final_result.schema());

                            let final_tables = final_result.concat_or_get(io_stats)?;
                            if !final_tables.is_empty() {
                                println!("{}", final_tables[0]);
                            }

                            Ok(Some(Arc::new(final_result)))
                        });
                    }

                    // Collect results from all partition tasks
                    let mut results = Vec::new();
                    while let Some(res) = per_partition_tasks.join_next().await {
                        match res {
                            Ok(Ok(Some(part))) => results.push(part),
                            Ok(Ok(None)) => {}
                            Ok(Err(e)) => return Err(e),
                            Err(e) => {
                                return Err(common_error::DaftError::ComputeError(format!(
                                    "Task join error: {}",
                                    e
                                )))
                            }
                        }
                    }

                    if results.is_empty() {
                        // Create an empty result with the correct schema
                        let schema_fields =
                            params.original_schema.fields.values().cloned().collect();
                        // Add fields for final projections
                        let empty_result =
                            MicroPartition::empty(Some(Arc::new(Schema::new(schema_fields)?)));
                        return Ok(Some(Arc::new(empty_result)));
                    }

                    // Concatenate all partition results
                    let final_result = MicroPartition::concat(&results)?;
                    println!("============ END WINDOW PARTITION EXECUTION DETAILS ============\n");
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
        Ok(Box::new(WindowPartitionOnlyState::new(
            self.num_partitions(),
        )))
    }
}
