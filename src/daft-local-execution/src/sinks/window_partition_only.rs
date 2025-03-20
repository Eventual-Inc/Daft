use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
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

        // Partition by value ensures unique partition keys are separated correctly
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
            .collect::<Vec<_>>();

        let partial_agg_exprs = partial_aggs
            .into_values()
            .map(|e| Arc::new(Expr::Agg(e)))
            .collect::<Vec<_>>();

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
                            .filter_map(|state| state.next())
                            .collect::<Vec<_>>();

                        let params = params.clone();
                        per_partition_finalize_tasks.spawn(async move {
                            let partitions = per_partition_state
                                .into_iter()
                                .flatten()
                                .collect::<Vec<_>>();
                            if partitions.is_empty() {
                                return Ok(None);
                            }

                            let original_data = MicroPartition::concat(&partitions)?;
                            let original_tables = original_data.get_tables()?;
                            if original_tables.is_empty() {
                                return Ok(None);
                            }

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

                            let mut window_projection_exprs = Vec::with_capacity(
                                params.partition_by.len() + params.window_column_names.len(),
                            );

                            for i in 0..params.partition_by.len() {
                                if let Some(field_name) =
                                    final_projected.schema().fields.keys().nth(i)
                                {
                                    window_projection_exprs.push(resolved_col(field_name.as_str()));
                                }
                            }

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

                            let renamed_aggs =
                                final_projected.eval_expression_list(&window_projection_exprs)?;
                            let agg_tables = renamed_aggs.get_tables()?;
                            if agg_tables.is_empty() {
                                return Ok(None);
                            }

                            let agg_table = &agg_tables.as_ref()[0];

                            let partition_col_names = params
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
                                .collect::<Vec<String>>();

                            let mut agg_dict = std::collections::HashMap::new();
                            for row_idx in 0..agg_table.len() {
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

                            let mut processed_tables = Vec::with_capacity(original_tables.len());
                            for original_batch in original_tables.iter() {
                                if original_batch.is_empty() {
                                    continue;
                                }

                                let key_parts: Vec<String> = partition_col_names
                                    .iter()
                                    .filter_map(|col_name| {
                                        original_batch.get_column(col_name).ok().and_then(|col| {
                                            col.slice(0, 1).ok().map(|value| format!("{:?}", value))
                                        })
                                    })
                                    .collect();

                                let row_idx = if key_parts.len() == partition_col_names.len() {
                                    agg_dict.get(&key_parts.join("|")).copied()
                                } else {
                                    None
                                };

                                let window_cols = params
                                    .window_column_names
                                    .iter()
                                    .filter_map(|window_name| {
                                        agg_table.get_column(window_name.as_str()).ok().and_then(
                                            |col| match row_idx {
                                                Some(idx) => col
                                                    .slice(idx, idx + 1)
                                                    .and_then(|value| {
                                                        value.broadcast(original_batch.len())
                                                    })
                                                    .ok(),
                                                None => col
                                                    .slice(0, 0)
                                                    .and_then(|null_col| {
                                                        null_col.broadcast(original_batch.len())
                                                    })
                                                    .ok(),
                                            },
                                        )
                                    })
                                    .collect::<Vec<_>>();

                                if !window_cols.is_empty() {
                                    let window_fields = params
                                        .window_column_names
                                        .iter()
                                        .zip(window_cols.iter())
                                        .map(|(name, col)| {
                                            daft_core::prelude::Field::new(
                                                name,
                                                col.data_type().clone(),
                                            )
                                        })
                                        .collect::<Vec<_>>();

                                    let window_schema =
                                        daft_core::prelude::Schema::new(window_fields)?;
                                    let window_batch = RecordBatch::new_with_size(
                                        Arc::new(window_schema),
                                        window_cols,
                                        original_batch.len(),
                                    )?;

                                    processed_tables.push(original_batch.union(&window_batch)?);
                                } else {
                                    processed_tables.push(original_batch.clone());
                                }
                            }

                            if processed_tables.is_empty() {
                                return Ok(None);
                            }

                            let result = MicroPartition::new_loaded(
                                processed_tables[0].schema.clone(),
                                Arc::new(processed_tables),
                                None,
                            );

                            Ok(Some(result))
                        });
                    }

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
