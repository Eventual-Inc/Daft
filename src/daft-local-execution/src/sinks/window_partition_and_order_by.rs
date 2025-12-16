use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use common_metrics::ops::NodeType;
use daft_core::{array::ops::IntoGroups, datatypes::UInt64Array, prelude::*};
use daft_dsl::{
    Expr, WindowExpr,
    expr::bound_expr::{BoundExpr, BoundWindowExpr},
};
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

struct WindowPartitionAndOrderByParams {
    window_exprs: Vec<BoundWindowExpr>,
    aliases: Vec<String>,
    partition_by: Vec<BoundExpr>,
    order_by: Vec<BoundExpr>,
    descending: Vec<bool>,
    nulls_first: Vec<bool>,
    original_schema: SchemaRef,
}

impl WindowSinkParams for WindowPartitionAndOrderByParams {
    fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    fn partition_by(&self) -> &[BoundExpr] {
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
        window_exprs: &[BoundWindowExpr],
        aliases: &[String],
        partition_by: &[BoundExpr],
        order_by: &[BoundExpr],
        descending: &[bool],
        nulls_first: &[bool],
        schema: &SchemaRef,
    ) -> DaftResult<Self> {
        Ok(Self {
            window_partition_and_order_by_params: Arc::new(WindowPartitionAndOrderByParams {
                window_exprs: window_exprs.to_vec(),
                aliases: aliases.to_vec(),
                partition_by: partition_by.to_vec(),
                order_by: order_by.to_vec(),
                descending: descending.to_vec(),
                nulls_first: nulls_first.to_vec(),
                original_schema: schema.clone(),
            }),
        })
    }

    fn num_partitions(&self) -> usize {
        self.max_concurrency()
    }
}

impl BlockingSink for WindowPartitionAndOrderBySink {
    type State = WindowBaseState;

    #[instrument(skip_all, name = "WindowPartitionAndOrderBySink::sink")]
    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        let params = self.window_partition_and_order_by_params.clone();
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

    #[instrument(skip_all, name = "WindowPartitionAndOrderBySink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        let params = self.window_partition_and_order_by_params.clone();
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

                        if params.partition_by.is_empty() {
                            return Err(DaftError::ValueError(
                                "Partition by cannot be empty for window functions".into(),
                            ));
                        }

                        per_partition_tasks.spawn(async move {
                            let input_data = RecordBatch::concat(&all_partitions)?;

                            if input_data.is_empty() {
                                return Ok(RecordBatch::empty(Some(
                                    params.original_schema.clone(),
                                )));
                            }

                            let groupby_table =
                                input_data.eval_expression_list(&params.partition_by)?;
                            let (_, groupvals_indices) = groupby_table.make_groups()?;

                            let mut partitions = groupvals_indices
                                .iter()
                                .map(|indices| {
                                    let indices_arr =
                                        UInt64Array::from(("indices", indices.clone()));
                                    input_data.take(&indices_arr).unwrap()
                                })
                                .collect::<Vec<_>>();

                            for partition in &mut partitions {
                                // Sort the partition by the order_by columns
                                *partition = partition.sort(
                                    &params.order_by,
                                    &params.descending,
                                    &params.nulls_first,
                                )?;

                                for (window_expr, name) in
                                    params.window_exprs.iter().zip(params.aliases.iter())
                                {
                                    *partition = match window_expr.as_ref() {
                                        WindowExpr::Agg(agg_expr) => {
                                            let new_col = partition
                                                .eval_expression(&BoundExpr::new_unchecked(
                                                    Arc::new(Expr::Agg(agg_expr.clone())),
                                                ))?
                                                .broadcast(partition.len())?
                                                .rename(name.clone());
                                            partition.append_column(
                                                params.original_schema.clone(),
                                                new_col,
                                            )?
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
                                            BoundExpr::new_unchecked(input.clone()),
                                            *offset,
                                            default.clone().map(BoundExpr::new_unchecked),
                                        )?,
                                    }
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
        "WindowPartitionAndOrderBy".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::WindowPartitionAndOrderBy
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
                .zip(self.window_partition_and_order_by_params.descending.iter())
                .zip(self.window_partition_and_order_by_params.nulls_first.iter())
                .map(|((e, d), n)| format!(
                    "{} {} {}",
                    e,
                    if *d { "desc" } else { "asc" },
                    if *n { "nulls first" } else { "nulls last" }
                ))
                .join(", ")
        ));
        display
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        WindowBaseState::make_base_state(self.num_partitions())
    }
}
