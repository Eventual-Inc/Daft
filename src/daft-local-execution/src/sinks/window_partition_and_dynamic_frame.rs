use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{array::ops::IntoGroups, datatypes::UInt64Array, prelude::*};
use daft_dsl::{
    expr::bound_expr::{BoundAggExpr, BoundExpr},
    AggExpr, ExprRef, WindowFrame,
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use itertools::Itertools;
use tracing::{instrument, Span};

use super::{
    blocking_sink::{
        BlockingSink, BlockingSinkFinalizeResult, BlockingSinkSinkResult, BlockingSinkState,
    },
    window_base::{base_sink, WindowBaseState, WindowSinkParams},
};
use crate::ExecutionTaskSpawner;

struct WindowPartitionAndDynamicFrameParams {
    aggregations: Vec<AggExpr>,
    min_periods: usize,
    aliases: Vec<String>,
    partition_by: Vec<ExprRef>,
    order_by: Vec<ExprRef>,
    descending: Vec<bool>,
    frame: WindowFrame,
    original_schema: SchemaRef,
}

impl WindowSinkParams for WindowPartitionAndDynamicFrameParams {
    fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    fn partition_by(&self) -> &[ExprRef] {
        &self.partition_by
    }

    fn name(&self) -> &'static str {
        "WindowPartitionAndDynamicFrame"
    }
}

pub struct WindowPartitionAndDynamicFrameSink {
    window_partition_and_dynamic_frame_params: Arc<WindowPartitionAndDynamicFrameParams>,
}

impl WindowPartitionAndDynamicFrameSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        aggregations: &[AggExpr],
        min_periods: usize,
        aliases: &[String],
        partition_by: &[ExprRef],
        order_by: &[ExprRef],
        descending: &[bool],
        frame: &WindowFrame,
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        Ok(Self {
            window_partition_and_dynamic_frame_params: Arc::new(
                WindowPartitionAndDynamicFrameParams {
                    aggregations: aggregations.to_vec(),
                    min_periods,
                    aliases: aliases.to_vec(),
                    partition_by: partition_by.to_vec(),
                    order_by: order_by.to_vec(),
                    descending: descending.to_vec(),
                    frame: frame.clone(),
                    original_schema: schema.clone(),
                },
            ),
        })
    }

    fn num_partitions(&self) -> usize {
        self.max_concurrency()
    }
}

impl BlockingSink for WindowPartitionAndDynamicFrameSink {
    #[instrument(skip_all, name = "WindowPartitionAndDynamicFrameSink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        base_sink(
            self.window_partition_and_dynamic_frame_params.clone(),
            input,
            state,
            spawner,
        )
    }

    #[instrument(skip_all, name = "WindowPartitionAndDynamicFrameSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        let params = self.window_partition_and_dynamic_frame_params.clone();
        let num_partitions = self.num_partitions();

        spawner
            .spawn(
                async move {
                    let mut state_iters = states
                        .into_iter()
                        .map(|mut state| {
                            state
                                .as_any_mut()
                                .downcast_mut::<WindowBaseState>()
                                .expect(
                                    "WindowPartitionAndDynamicFrameSink should have WindowBaseState",
                                )
                                .finalize(params.name())
                                .into_iter()
                        })
                        .collect::<Vec<_>>();

                    let mut per_partition_tasks = tokio::task::JoinSet::new();

                    for _partition_idx in 0..num_partitions {
                        let per_partition_state = state_iters.iter_mut().map(|state| {
                            state.next().expect(
                                "WindowBaseState should have SinglePartitionWindowState",
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

                        let input_schema = &all_partitions[0].schema;

                        let partition_by = params.partition_by
                            .iter()
                            .map(|expr| BoundExpr::try_new(expr.clone(), input_schema))
                            .collect::<DaftResult<Vec<_>>>()?;

                        let order_by = params.order_by
                            .iter()
                            .map(|expr| BoundExpr::try_new(expr.clone(), input_schema))
                            .collect::<DaftResult<Vec<_>>>()?;

                        let aggregations = params.aggregations
                            .iter()
                            .map(|expr| BoundAggExpr::try_new(expr.clone(), input_schema))
                            .collect::<DaftResult<Vec<_>>>()?;

                        if partition_by.is_empty() {
                            return Err(DaftError::ValueError(
                                "Partition by cannot be empty for window functions".into(),
                            ));
                        }

                        per_partition_tasks.spawn(async move {
                            let input_data = RecordBatch::concat(&all_partitions)?;

                            if input_data.is_empty() {
                                return RecordBatch::empty(Some(params.original_schema.clone()));
                            }

                            let partitionby_table = input_data.eval_expression_list(&partition_by)?;
                            let (_, partitionvals_indices) = partitionby_table.make_groups()?;

                            let mut partitions = partitionvals_indices.iter().map(|indices| {
                                let indices_series = UInt64Array::from(("indices", indices.clone())).into_series();
                                input_data.take(&indices_series).unwrap()
                            }).collect::<Vec<_>>();

                            for partition in &mut partitions {
                                // Sort the partition by the order_by columns (default for nulls_first is to be same as descending)
                                *partition = partition.sort(&order_by, &params.descending, &params.descending)?;

                                for (agg_expr, name) in aggregations.iter().zip(params.aliases.iter()) {
                                    let dtype = agg_expr.as_ref().to_field(&params.original_schema)?.dtype;
                                    *partition = partition.window_agg_dynamic_frame(name.clone(), agg_expr, params.min_periods, &dtype, &params.frame)?;
                                }
                            }

                            let final_result = RecordBatch::concat(&partitions)?;
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
        "WindowPartitionAndDynamicFrame"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push(format!(
            "WindowPartitionAndDynamicFrame: {}",
            self.window_partition_and_dynamic_frame_params
                .aggregations
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
                .map(|e| e.to_string())
                .join(", ")
        ));
        display.push(format!(
            "Frame: {:?}",
            self.window_partition_and_dynamic_frame_params.frame
        ));
        display
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        WindowBaseState::make_base_state(self.num_partitions())
    }
}
