use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::{resolved_col, AggExpr, ExprRef};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    BlockingSinkStatus,
};
use crate::ExecutionTaskSpawner;

#[derive(Default)]
struct SinglePartitionWindowState {
    partitions: Vec<RecordBatch>,
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
                for table in p.get_tables()?.iter() {
                    state.partitions.push(table.clone());
                }
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
    agg_exprs: Vec<AggExpr>,
    aliases: Vec<String>,
    partition_by: Vec<ExprRef>,
    original_schema: SchemaRef,
}

pub struct WindowPartitionOnlySink {
    window_partition_only_params: Arc<WindowPartitionOnlyParams>,
}

impl WindowPartitionOnlySink {
    pub fn new(
        agg_exprs: &[AggExpr],
        aliases: &[String],
        partition_by: &[ExprRef],
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        Ok(Self {
            window_partition_only_params: Arc::new(WindowPartitionOnlyParams {
                agg_exprs: agg_exprs.to_vec(),
                aliases: aliases.to_vec(),
                partition_by: partition_by.to_vec(),
                original_schema: schema.clone(),
            }),
        })
    }

    fn num_partitions(&self) -> usize {
        self.max_concurrency()
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

                    for _partition_idx in 0..num_partitions {
                        let per_partition_state = state_iters.iter_mut().map(|state| {
                            state.next().expect(
                                "WindowPartitionOnlyState should have SinglePartitionWindowState",
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
                            let input_data = RecordBatch::concat(&all_partitions)?;

                            let result = input_data.window_grouped_agg(
                                &params.agg_exprs,
                                &params.aliases,
                                &params.partition_by,
                            )?;
                            let all_projections = params
                                .original_schema
                                .field_names()
                                .map(resolved_col)
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
        "WindowPartitionOnly"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!(
            "WindowPartitionOnly: {}",
            self.window_partition_only_params
                .agg_exprs
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

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(WindowPartitionOnlyState::new(
            self.num_partitions(),
        )))
    }
}
