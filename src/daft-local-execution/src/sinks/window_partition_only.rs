use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::{BoundAggExpr, BoundExpr};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::{
    blocking_sink::{
        BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult,
        BlockingSinkSinkResult, BlockingSinkStatus,
    },
    window_base::{WindowBaseState, WindowSinkParams},
};
use crate::{ExecutionTaskSpawner, pipeline::NodeName};

struct WindowPartitionOnlyParams {
    agg_exprs: Vec<BoundAggExpr>,
    aliases: Vec<String>,
    partition_by: Vec<BoundExpr>,
    original_schema: SchemaRef,
}

impl WindowSinkParams for WindowPartitionOnlyParams {
    fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    fn partition_by(&self) -> &[BoundExpr] {
        &self.partition_by
    }

    fn name(&self) -> &'static str {
        "WindowPartitionOnly"
    }
}

pub struct WindowPartitionOnlySink {
    window_partition_only_params: Arc<WindowPartitionOnlyParams>,
}

impl WindowPartitionOnlySink {
    pub fn new(
        agg_exprs: &[BoundAggExpr],
        aliases: &[String],
        partition_by: &[BoundExpr],
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
    type State = WindowBaseState;

    #[instrument(skip_all, name = "WindowPartitionOnlySink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let params = self.window_partition_only_params.clone();
        let sink_name = params.name().to_string();
        spawner
            .spawn(
                async move {
                    state.push(input, params.partition_by(), &sink_name)?;
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                Span::current(),
            )
            .into()
    }

    #[instrument(skip_all, name = "WindowPartitionOnlySink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let params = self.window_partition_only_params.clone();
        let num_partitions = self.num_partitions();

        spawner
            .spawn(
                async move {
                    let mut state_iters = states
                        .into_iter()
                        .map(|mut state| state.finalize(params.name()).into_iter())
                        .collect::<Vec<_>>();

                    let mut per_partition_tasks = tokio::task::JoinSet::new();

                    for _partition_idx in 0..num_partitions {
                        let per_partition_state = state_iters.iter_mut().map(|state| {
                            state
                                .next()
                                .expect("WindowBaseState should have SinglePartitionWindowState")
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

                            Ok(result)
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
                        return Ok(BlockingSinkFinalizeOutput::Finished(vec![Arc::new(
                            empty_result,
                        )]));
                    }

                    let final_result = MicroPartition::new_loaded(
                        params.original_schema.clone(),
                        results.into(),
                        None,
                    );

                    Ok(BlockingSinkFinalizeOutput::Finished(vec![Arc::new(
                        final_result,
                    )]))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "WindowPartitionOnly".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::WindowPartitionOnly
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

    fn make_state(&self) -> DaftResult<Self::State> {
        WindowBaseState::make_base_state(self.num_partitions())
    }
}
