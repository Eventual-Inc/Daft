use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_core::{array::ops::IntoGroups, datatypes::UInt64Array, prelude::*};
use daft_dsl::{resolved_col, ExprRef, WindowExpr};
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

struct WindowPartitionAndOrderByParams {
    window_exprs: Vec<WindowExpr>,
    aliases: Vec<String>,
    partition_by: Vec<ExprRef>,
    order_by: Vec<ExprRef>,
    descending: Vec<bool>,
    original_schema: SchemaRef,
}

impl WindowSinkParams for WindowPartitionAndOrderByParams {
    fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    fn partition_by(&self) -> &[ExprRef] {
        &self.partition_by
    }

    fn name(&self) -> &'static str {
        "WindowPartitionAndOrderBy"
    }
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
        descending: &[bool],
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        Ok(Self {
            window_partition_and_order_by_params: Arc::new(WindowPartitionAndOrderByParams {
                window_exprs: window_exprs.to_vec(),
                aliases: aliases.to_vec(),
                partition_by: partition_by.to_vec(),
                order_by: order_by.to_vec(),
                descending: descending.to_vec(),
                original_schema: schema.clone(),
            }),
        })
    }

    fn num_partitions(&self) -> usize {
        self.max_concurrency()
    }
}

impl BlockingSink for WindowPartitionAndOrderBySink {
    #[instrument(skip_all, name = "WindowPartitionAndOrderBySink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        base_sink(
            self.window_partition_and_order_by_params.clone(),
            input,
            state,
            spawner,
        )
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
                                .downcast_mut::<WindowBaseState>()
                                .expect("WindowPartitionAndOrderBySink should have WindowBaseState")
                                .finalize(params.name())
                                .into_iter()
                        })
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

                        if params.partition_by.is_empty() {
                            return Err(DaftError::ValueError(
                                "Partition by cannot be empty for window functions".into(),
                            ));
                        }

                        per_partition_tasks.spawn(async move {
                            let input_data = RecordBatch::concat(&all_partitions)?;

                            if input_data.is_empty() {
                                return RecordBatch::empty(Some(params.original_schema.clone()));
                            }

                            let groupby_table =
                                input_data.eval_expression_list(&params.partition_by)?;
                            let (_, groupvals_indices) = groupby_table.make_groups()?;

                            let mut partitions = groupvals_indices
                                .iter()
                                .map(|indices| {
                                    let indices_series =
                                        UInt64Array::from(("indices", indices.clone()))
                                            .into_series();
                                    input_data.take(&indices_series).unwrap()
                                })
                                .collect::<Vec<_>>();

                            for partition in &mut partitions {
                                // Sort the partition by the order_by columns (default for nulls_first is to be same as descending)
                                *partition = partition.sort(
                                    &params.order_by,
                                    &params.descending,
                                    &params.descending,
                                )?;

                                for (window_expr, name) in
                                    params.window_exprs.iter().zip(params.aliases.iter())
                                {
                                    *partition = match window_expr {
                                        WindowExpr::Agg(agg_expr) => {
                                            partition.window_agg(agg_expr, name.clone())?
                                        }
                                        WindowExpr::RowNumber => {
                                            partition.window_row_number(name.clone())?
                                        }
                                        WindowExpr::Rank => partition.window_rank(
                                            name.clone(),
                                            &params.order_by,
                                            false,
                                        )?,
                                        WindowExpr::DenseRank => partition.window_rank(
                                            name.clone(),
                                            &params.order_by,
                                            true,
                                        )?,
                                        WindowExpr::Offset {
                                            input,
                                            offset,
                                            default,
                                        } => partition.window_offset(
                                            name.clone(),
                                            input.clone(),
                                            *offset,
                                            default.clone(),
                                        )?,
                                    }
                                }
                            }

                            let all_projections = params
                                .original_schema
                                .field_names()
                                .map(resolved_col)
                                .collect::<Vec<_>>();

                            let final_result = RecordBatch::concat(&partitions)?;
                            let final_result =
                                final_result.eval_expression_list(&all_projections)?;
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

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        WindowBaseState::make_base_state(self.num_partitions())
    }
}
