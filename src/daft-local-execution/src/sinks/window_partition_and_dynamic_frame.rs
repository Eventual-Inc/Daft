use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_core::prelude::*;
use daft_dsl::{
    WindowFrame,
    expr::bound_expr::{BoundExpr, BoundWindowExpr},
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{Span, instrument};

use super::{
    blocking_sink::{
        BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    },
    window_base::{
        WindowBaseState, WindowSinkParams, finalize_partitioned_windows, partition_into_groups,
        sort_and_materialize_groups, window_bucket_budget, window_spill_dirs,
    },
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::{InputId, NodeName},
    spill::SpillConfig,
};

struct WindowPartitionAndDynamicFrameParams {
    window_exprs: Vec<BoundWindowExpr>,
    min_periods: usize,
    aliases: Vec<String>,
    partition_by: Vec<BoundExpr>,
    order_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    frame: WindowFrame,
    original_schema: SchemaRef,
}

impl WindowSinkParams for WindowPartitionAndDynamicFrameParams {
    fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    fn partition_by(&self) -> &[BoundExpr] {
        &self.partition_by
    }

    fn name(&self) -> &'static str {
        "WindowPartitionAndDynamicFrame"
    }
}

pub struct WindowPartitionAndDynamicFrameSink {
    window_partition_and_dynamic_frame_params: Arc<WindowPartitionAndDynamicFrameParams>,
    spill_config: Option<SpillConfig>,
    budget_per_bucket: usize,
}

impl WindowPartitionAndDynamicFrameSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        window_exprs: &[BoundWindowExpr],
        min_periods: usize,
        aliases: &[String],
        partition_by: &[BoundExpr],
        order_by: &[BoundExpr],
        descending: &[bool],
        nulls_first: &[bool],
        frame: &WindowFrame,
        schema: &SchemaRef,
        spill_config: Option<SpillConfig>,
    ) -> DaftResult<Self> {
        let budget_per_bucket = window_bucket_budget(&spill_config);
        Ok(Self {
            window_partition_and_dynamic_frame_params: Arc::new(
                WindowPartitionAndDynamicFrameParams {
                    window_exprs: window_exprs.to_vec(),
                    min_periods,
                    aliases: aliases.to_vec(),
                    partition_by: partition_by.to_vec(),
                    order_by: order_by.to_vec(),
                    descending: descending.to_vec(),
                    nulls_first: nulls_first.to_vec(),
                    frame: frame.clone(),
                    original_schema: schema.clone(),
                },
            ),
            spill_config,
            budget_per_bucket,
        })
    }

    fn num_partitions(&self) -> usize {
        self.max_concurrency()
    }
}

impl BlockingSink for WindowPartitionAndDynamicFrameSink {
    type State = WindowBaseState;

    #[instrument(skip_all, name = "WindowPartitionAndDynamicFrameSink::sink")]
    fn sink(
        &self,
        input: MicroPartition,
        mut state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let params = self.window_partition_and_dynamic_frame_params.clone();
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

    #[instrument(skip_all, name = "WindowPartitionAndDynamicFrameSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.window_partition_and_dynamic_frame_params.clone();
        let num_partitions = self.num_partitions();

        if params.partition_by.is_empty() {
            return Err(DaftError::ValueError(
                "Partition by cannot be empty for window functions".into(),
            ))
            .into();
        }

        let schema = params.original_schema.clone();
        let compute = move |all_partitions: Vec<RecordBatch>| -> DaftResult<RecordBatch> {
            let groups = partition_into_groups(&all_partitions, &params.partition_by)?;
            let full_data = RecordBatch::concat(&all_partitions)?;
            let partitions = sort_and_materialize_groups(
                groups,
                full_data,
                &params.order_by,
                &params.descending,
                &params.nulls_first,
            )?;

            if partitions.is_empty() {
                return Ok(RecordBatch::empty(Some(params.original_schema.clone())));
            }

            let grouped_results: Vec<RecordBatch> = partitions
                .into_iter()
                .map(|partition| -> DaftResult<RecordBatch> {
                    let new_cols: Vec<Series> = params
                        .window_exprs
                        .iter()
                        .zip(params.aliases.iter())
                        .map(|(window_expr, name)| -> DaftResult<Series> {
                            let dtype =
                                window_expr.as_ref().to_field(&params.original_schema)?.dtype;
                            partition.window_agg_dynamic_frame_col(
                                name,
                                window_expr,
                                &params.order_by,
                                &params.descending,
                                params.min_periods,
                                &dtype,
                                &params.frame,
                            )
                        })
                        .collect::<DaftResult<_>>()?;

                    if new_cols.is_empty() {
                        Ok(partition)
                    } else {
                        partition.union(&RecordBatch::from_nonempty_columns(new_cols)?)
                    }
                })
                .collect::<DaftResult<_>>()?;

            RecordBatch::concat(&grouped_results)
        };

        spawner
            .spawn(
                finalize_partitioned_windows(states, num_partitions, schema, compute),
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "WindowPartitionAndDynamicFrame".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::Window
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!(
            "WindowPartitionAndDynamicFrame: {}",
            self.window_partition_and_dynamic_frame_params
                .window_exprs
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        display.push(format!(
            "Partition by: {}",
            self.window_partition_and_dynamic_frame_params
                .partition_by
                .iter()
                .map(|e| e.to_string())
                .join(", ")
        ));
        display.push(format!(
            "Order by: {}",
            self.window_partition_and_dynamic_frame_params
                .order_by
                .iter()
                .zip(
                    self.window_partition_and_dynamic_frame_params
                        .descending
                        .iter()
                )
                .zip(
                    self.window_partition_and_dynamic_frame_params
                        .nulls_first
                        .iter()
                )
                .map(|((e, d), n)| format!(
                    "{} {} {}",
                    e,
                    if *d { "desc" } else { "asc" },
                    if *n { "nulls first" } else { "nulls last" }
                ))
                .join(", ")
        ));
        display.push(format!(
            "Frame: {:?}",
            self.window_partition_and_dynamic_frame_params.frame
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
