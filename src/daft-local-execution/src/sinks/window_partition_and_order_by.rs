use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::{IntoSeries, SchemaRef, UInt64Array};
use daft_dsl::{resolved_col, ExprRef, WindowExpr};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::{ExecutionTaskSpawner, NUM_CPUS};

#[derive(Default)]
struct SinglePartitionWindowState {
    partitions: Vec<RecordBatch>,
}

enum WindowPartitionAndOrderByState {
    Accumulating {
        inner_states: Vec<Option<SinglePartitionWindowState>>,
    },
    Done,
}

impl WindowPartitionAndOrderByState {
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
                for table in p.get_tables()?.iter() {
                    state.partitions.push(table.clone());
                }
            }
        } else {
            panic!("WindowPartitionAndOrderBySink should be in Accumulating state");
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
            panic!("WindowPartitionAndOrderBySink should be in Accumulating state");
        };
        *self = Self::Done;
        res
    }
}

impl BlockingSinkState for WindowPartitionAndOrderByState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct WindowPartitionAndOrderByParams {
    window_exprs: Vec<WindowExpr>,
    aliases: Vec<String>,
    partition_by: Vec<ExprRef>,
    order_by: Vec<ExprRef>,
    ascending: Vec<bool>,
    original_schema: SchemaRef,
}

pub struct WindowPartitionAndOrderBySink {
    window_partition_and_order_by_params: Arc<WindowPartitionAndOrderByParams>,
}

impl WindowPartitionAndOrderBySink {
    pub fn new(
        window_exprs: &[WindowExpr],
        aliases: &[String],
        partition_by: &[ExprRef],
        order_by: &[ExprRef],
        ascending: &[bool],
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        Ok(Self {
            window_partition_and_order_by_params: Arc::new(WindowPartitionAndOrderByParams {
                window_exprs: window_exprs.to_vec(),
                aliases: aliases.to_vec(),
                partition_by: partition_by.to_vec(),
                order_by: order_by.to_vec(),
                ascending: ascending.to_vec(),
                original_schema: schema.clone(),
            }),
        })
    }

    fn num_partitions(&self) -> usize {
        *NUM_CPUS
    }
}

impl BlockingSink for WindowPartitionAndOrderBySink {
    #[instrument(skip_all, name = "WindowPartitionAndOrderBySink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        let params = self.window_partition_and_order_by_params.clone();
        spawner
            .spawn(
                async move {
                    let window_state = state
                        .as_any_mut()
                        .downcast_mut::<WindowPartitionAndOrderByState>()
                        .expect("WindowPartitionAndOrderBySink should have WindowPartitionAndOrderByState");

                    window_state.push(input, &params.partition_by)?;
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "WindowPartitionAndOrderBySink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.window_partition_and_order_by_params.clone();
        let num_partitions = self.num_partitions();

        spawner
            .spawn(
                async move {
                    let mut state_iters = states
                        .into_iter()
                        .map(|mut state| {
                            state
                                .as_any_mut()
                                .downcast_mut::<WindowPartitionAndOrderByState>()
                                .expect(
                                    "WindowPartitionAndOrderBySink should have WindowPartitionAndOrderByState",
                                )
                                .finalize()
                                .into_iter()
                        })
                        .collect::<Vec<_>>();

                    let mut per_partition_tasks = tokio::task::JoinSet::new();

                    for _partition_idx in 0..num_partitions {
                        let per_partition_state = state_iters.iter_mut().map(|state| {
                            state.next().expect(
                                "WindowPartitionAndOrderByState should have SinglePartitionWindowState",
                            )
                        });

                        let all_partitions: Vec<RecordBatch> = per_partition_state
                            .flatten()
                            .flat_map(|state| state.partitions)
                            .collect();

                        if all_partitions.is_empty() {
                            continue;
                        }

                        let params = params.clone();
                        per_partition_tasks.spawn(async move {
                            // First concatenate all partitions
                            let input_data = RecordBatch::concat(&all_partitions)?;

                            // Sort the data by order_by expressions
                            let inverse_ascending = params.ascending.iter().map(|&b| !b).collect::<Vec<_>>(); // TODO: fix later
                            let sorted_data = input_data.sort(&params.order_by, &inverse_ascending, &[true])?;

                            // Process each window expression
                            let mut result = sorted_data;
                            for (window_expr, name) in params.window_exprs.iter().zip(params.aliases.iter()) {
                                match window_expr {
                                    WindowExpr::RowNumber() => {
                                        let row_numbers = (1..(result.num_rows() + 1) as u64).collect::<Vec<_>>();
                                        let row_number_series = UInt64Array::from((name.as_str(), row_numbers)).into_series();
                                        let row_number_batch = RecordBatch::from_nonempty_columns(vec![row_number_series])?;
                                        result = result.union(&row_number_batch)?;
                                    }
                                    WindowExpr::Agg(agg_expr) => {
                                        result = result.window_agg(&[agg_expr.clone()], &[name.clone()], &params.partition_by)?;
                                    }
                                }
                            }

                            // Project back to original schema plus window function results
                            let all_projections = params
                                .original_schema
                                .fields
                                .keys()
                                .map(|k| resolved_col(k.clone()))
                                .collect::<Vec<_>>();

                            let final_result = result.eval_expression_list(&all_projections)?;
                            Ok(final_result)
                        });
                    }

                    let results = per_partition_tasks
                        .join_all()
                        .await
                        .into_iter()
                        .collect::<DaftResult<Vec<_>>>()?;

                    if results.is_empty() {
                        let empty_result =
                            MicroPartition::empty(Some(params.original_schema.clone()));
                        return Ok(Some(Arc::new(empty_result)));
                    }

                    let final_result = MicroPartition::new_loaded(
                        params.original_schema.clone(),
                        results.into(),
                        None,
                    );
                    Ok(Some(Arc::new(final_result)))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "WindowPartitionAndOrderBy"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!(
            "WindowPartitionAndOrderBy: {}",
            self.window_partition_and_order_by_params
                .window_exprs
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        display.push(format!(
            "Partition by: {}",
            self.window_partition_and_order_by_params
                .partition_by
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        display.push(format!(
            "Order by: {}",
            self.window_partition_and_order_by_params
                .order_by
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
        Ok(Box::new(WindowPartitionAndOrderByState::new(
            self.num_partitions(),
        )))
    }
}
