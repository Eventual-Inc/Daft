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
        BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    },
    window_base::{
        WindowBaseState, WindowSinkParams, finalize_partitioned_windows, window_bucket_budget,
        window_spill_dirs,
    },
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    spill::SpillConfig,
};

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
    spill_config: Option<SpillConfig>,
    budget_per_bucket: usize,
}

impl WindowPartitionOnlySink {
    pub fn new(
        agg_exprs: &[BoundAggExpr],
        aliases: &[String],
        partition_by: &[BoundExpr],
        schema: &SchemaRef,
        spill_config: Option<SpillConfig>,
    ) -> DaftResult<Self> {
        let budget_per_bucket = window_bucket_budget(&spill_config);
        Ok(Self {
            window_partition_only_params: Arc::new(WindowPartitionOnlyParams {
                agg_exprs: agg_exprs.to_vec(),
                aliases: aliases.to_vec(),
                partition_by: partition_by.to_vec(),
                original_schema: schema.clone(),
            }),
            spill_config,
            budget_per_bucket,
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
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let params = self.window_partition_only_params.clone();
        let sink_name = params.name().to_string();
        spawner
            .spawn(
                async move {
                    state.push(input, params.partition_by(), &sink_name)?;
                    Ok(state)
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
    ) -> BlockingSinkFinalizeResult {
        let params = self.window_partition_only_params.clone();
        let num_partitions = self.num_partitions();
        let schema = params.original_schema.clone();

        let compute = move |all_partitions: Vec<RecordBatch>| -> DaftResult<RecordBatch> {
            let input_data = RecordBatch::concat(&all_partitions)?;
            input_data.window_grouped_agg(&params.agg_exprs, &params.aliases, &params.partition_by)
        };

        spawner
            .spawn(
                finalize_partitioned_windows(states, num_partitions, schema, compute),
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "WindowPartitionOnly".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Window
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

    fn make_state(&self, _input_id: InputId) -> DaftResult<Self::State> {
        WindowBaseState::make_base_state(
            self.num_partitions(),
            window_spill_dirs(&self.spill_config),
            self.budget_per_bucket,
        )
    }
}
