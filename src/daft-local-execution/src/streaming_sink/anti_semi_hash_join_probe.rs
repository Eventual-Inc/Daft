use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_core::{
    prelude::{SchemaRef, UInt64Array},
    series::IntoSeries,
};
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{ProbeState, RecordBatch};
use futures::{StreamExt, stream};
use itertools::Itertools;
use tracing::{Span, info_span, instrument};

use super::{
    base::{StreamingSink, StreamingSinkExecuteResult, StreamingSinkOutput},
    outer_hash_join_probe::IndexBitmapBuilder,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::NodeName,
    state_bridge::BroadcastStateBridgeRef,
    streaming_sink::base::{StreamingSinkFinalizeOutput, StreamingSinkFinalizeResult},
};

pub(crate) enum AntiSemiProbeState {
    Building(BroadcastStateBridgeRef<ProbeState>),
    Probing(Arc<ProbeState>, Option<IndexBitmapBuilder>),
}

impl AntiSemiProbeState {
    async fn get_or_await_probe_state(
        &mut self,
        needs_bitmap: bool,
    ) -> (&mut Arc<ProbeState>, &mut Option<IndexBitmapBuilder>) {
        if let Self::Building(bridge) = self {
            let probe_state = bridge.get_state().await;
            let builder = if needs_bitmap {
                Some(IndexBitmapBuilder::new(probe_state.get_record_batches()))
            } else {
                None
            };
            *self = Self::Probing(probe_state, builder);
        }
        match self {
            Self::Probing(probe_state, builder) => (probe_state, builder),
            _ => unreachable!(),
        }
    }
}

struct AntiSemiJoinParams {
    probe_on: Vec<BoundExpr>,
    is_semi: bool,
}

pub(crate) struct AntiSemiProbeSink {
    params: Arc<AntiSemiJoinParams>,
    output_schema: SchemaRef,
    probe_state_bridge: BroadcastStateBridgeRef<ProbeState>,
    build_on_left: bool,
}

impl AntiSemiProbeSink {
    pub fn new(
        probe_on: Vec<BoundExpr>,
        join_type: &JoinType,
        output_schema: &SchemaRef,
        probe_state_bridge: BroadcastStateBridgeRef<ProbeState>,
        build_on_left: bool,
    ) -> Self {
        Self {
            params: Arc::new(AntiSemiJoinParams {
                probe_on,
                is_semi: *join_type == JoinType::Semi,
            }),
            output_schema: output_schema.clone(),
            probe_state_bridge,
            build_on_left,
        }
    }

    // This function performs probing for anti-semi joins when the side to keep is the probe side, i.e. we built the probe table
    // on the right side and are streaming the left side.
    fn probe_anti_semi(
        probe_on: &[BoundExpr],
        probe_state: &Arc<ProbeState>,
        input: &Arc<MicroPartition>,
        is_semi: bool,
    ) -> DaftResult<Arc<MicroPartition>> {
        let input_tables = input.record_batches();
        let mut input_idxs = vec![vec![]; input_tables.len()];
        for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
            let join_keys = table.eval_expression_list(probe_on)?;
            let iter = probe_state.probe_exists(&join_keys)?;

            for (probe_row_idx, matched) in iter.enumerate() {
                // 1. If this is a semi join, we keep the row if it matches.
                // 2. If this is an anti join, we keep the row if it doesn't match.
                match (is_semi, matched) {
                    (true, true) | (false, false) => {
                        input_idxs[probe_side_table_idx].push(probe_row_idx as u64);
                    }
                    _ => {}
                }
            }
        }
        let probe_side_tables = input_idxs
            .into_iter()
            .zip(input_tables.iter())
            .map(|(idxs, table)| {
                let idxs_arr = UInt64Array::from(("idxs", idxs));
                table.take(&idxs_arr)
            })
            .collect::<DaftResult<Vec<_>>>()?;
        Ok(Arc::new(MicroPartition::new_loaded(
            probe_side_tables[0].schema.clone(),
            Arc::new(probe_side_tables),
            None,
        )))
    }

    // This function performs the anti semi join when the side to keep is the build side, i.e. we built the probe table
    // on the left side and we are streaming the right side. In this case, we use a bitmap index to track matches, and only
    // emit a final result at the end.
    fn probe_anti_semi_with_bitmap(
        probe_on: &[BoundExpr],
        probe_state: &Arc<ProbeState>,
        bitmap_builder: &mut IndexBitmapBuilder,
        input: &Arc<MicroPartition>,
    ) -> DaftResult<()> {
        let _loop = info_span!("AntiSemiOperator::eval_and_probe").entered();
        for table in input.record_batches() {
            let join_keys = table.eval_expression_list(probe_on)?;
            let idx_iter = probe_state.probe_indices(&join_keys)?;

            for inner_iter in idx_iter.flatten() {
                for (build_table_idx, build_row_idx) in inner_iter {
                    bitmap_builder.mark_used(build_table_idx as usize, build_row_idx as usize);
                }
            }
        }
        Ok(())
    }

    // Finalize the anti/semi join where we have a bitmap index, i.e. left side builds.
    async fn finalize_anti_semi(
        mut states: Vec<AntiSemiProbeState>,
        is_semi: bool,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let mut states_iter = states.iter_mut();
        let first_state = states_iter
            .next()
            .expect("at least one state should be present");
        let (first_probe_state, first_bitmap_builder) =
            first_state.get_or_await_probe_state(true).await;
        let tables = first_probe_state.get_record_batches();
        let first_bitmap = first_bitmap_builder
            .take()
            .expect("bitmap should be set")
            .build();

        let mut merged_bitmap = {
            let bitmaps = stream::once(async move { first_bitmap })
                .chain(stream::iter(states_iter).then(|s| async move {
                    s.get_or_await_probe_state(true)
                        .await
                        .1
                        .take()
                        .expect("bitmap should be set")
                        .build()
                }))
                .collect::<Vec<_>>()
                .await;

            bitmaps.into_iter().fold(None, |acc, x| match acc {
                None => Some(x),
                Some(acc) => Some(acc.merge(&x)),
            })
        }
        .expect("at least one bitmap should be present");

        // The bitmap marks matched rows as 0, so we need to negate it if we are doing semi join, i.e. the matched rows become 1 so that
        // we can we can keep them in the final result.
        if is_semi {
            merged_bitmap = merged_bitmap.negate();
        }

        let leftovers = merged_bitmap
            .convert_to_boolean_arrays()
            .zip(tables.iter())
            .map(|(bitmap, table)| table.mask_filter(&bitmap.into_series()))
            .collect::<DaftResult<Vec<_>>>()?;
        let build_side_table = RecordBatch::concat(&leftovers)?;

        Ok(Some(Arc::new(MicroPartition::new_loaded(
            build_side_table.schema.clone(),
            Arc::new(vec![build_side_table]),
            None,
        ))))
    }
}

impl StreamingSink for AntiSemiProbeSink {
    type State = AntiSemiProbeState;
    type BatchingStrategy = crate::dynamic_batching::StaticBatchingStrategy;
    #[instrument(skip_all, name = "AntiSemiProbeSink::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: AntiSemiProbeState,
        task_spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult<Self> {
        if input.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
            return Ok((state, StreamingSinkOutput::NeedMoreInput(Some(empty)))).into();
        }

        let params = self.params.clone();
        let build_on_left = self.build_on_left;
        task_spawner
            .spawn(
                async move {
                    let (ps, bitmap_builder) = state.get_or_await_probe_state(build_on_left).await;

                    if let Some(bm_builder) = bitmap_builder {
                        Self::probe_anti_semi_with_bitmap(
                            &params.probe_on,
                            ps,
                            bm_builder,
                            &input,
                        )?;
                        Ok((state, StreamingSinkOutput::NeedMoreInput(None)))
                    } else {
                        let res =
                            Self::probe_anti_semi(&params.probe_on, ps, &input, params.is_semi);
                        Ok((state, StreamingSinkOutput::NeedMoreInput(Some(res?))))
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "AntiSemiHashJoinProbe".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::AntiSemiHashJoinProbe
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        if self.params.is_semi {
            res.push(format!(
                "SemiHashJoinProbe: {}",
                self.params
                    .probe_on
                    .iter()
                    .map(|e| e.to_string())
                    .join(", ")
            ));
        } else {
            res.push(format!(
                "AntiHashJoinProbe: {}",
                self.params
                    .probe_on
                    .iter()
                    .map(|e| e.to_string())
                    .join(", ")
            ));
        }
        res.push(format!("Build on left: {}", self.build_on_left));
        res
    }

    #[instrument(skip_all, name = "AntiSemiProbeSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkFinalizeResult<Self> {
        if self.build_on_left {
            let is_semi = self.params.is_semi;
            task_spawner
                .spawn(
                    async move {
                        let output = Self::finalize_anti_semi(states, is_semi).await?;
                        Ok(StreamingSinkFinalizeOutput::Finished(output))
                    },
                    Span::current(),
                )
                .into()
        } else {
            Ok(StreamingSinkFinalizeOutput::Finished(None)).into()
        }
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        Ok(AntiSemiProbeState::Building(
            self.probe_state_bridge.clone(),
        ))
    }

    fn max_concurrency(&self) -> usize {
        common_runtime::get_compute_pool_num_threads()
    }
    fn batching_strategy(&self) -> Self::BatchingStrategy {
        crate::dynamic_batching::StaticBatchingStrategy::new(
            self.morsel_size_requirement().unwrap_or_default(),
        )
    }
}
