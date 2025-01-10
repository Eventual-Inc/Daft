use std::sync::Arc;

use common_error::DaftResult;
use daft_core::{prelude::SchemaRef, series::IntoSeries};
use daft_dsl::ExprRef;
use daft_logical_plan::JoinType;
use daft_micropartition::MicroPartition;
use daft_table::{GrowableTable, ProbeState, Probeable, Table};
use futures::{stream, StreamExt};
use itertools::Itertools;
use tracing::{info_span, instrument, Span};

use super::{
    outer_hash_join_probe::IndexBitmapBuilder,
    streaming_sink::{
        StreamingSink, StreamingSinkExecuteResult, StreamingSinkOutput, StreamingSinkState,
    },
};
use crate::{
    dispatcher::{DispatchSpawner, RoundRobinDispatcher, UnorderedDispatcher},
    state_bridge::BroadcastStateBridgeRef,
    ExecutionRuntimeContext, ExecutionTaskSpawner,
};

enum AntiSemiProbeState {
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
                Some(IndexBitmapBuilder::new(probe_state.get_tables()))
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

impl StreamingSinkState for AntiSemiProbeState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct AntiSemiJoinParams {
    probe_on: Vec<ExprRef>,
    is_semi: bool,
}

pub(crate) struct AntiSemiProbeSink {
    params: Arc<AntiSemiJoinParams>,
    output_schema: SchemaRef,
    probe_state_bridge: BroadcastStateBridgeRef<ProbeState>,
    build_on_left: bool,
}

impl AntiSemiProbeSink {
    const DEFAULT_GROWABLE_SIZE: usize = 20;

    pub fn new(
        probe_on: Vec<ExprRef>,
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
        probe_on: &[ExprRef],
        probe_set: &Arc<dyn Probeable>,
        input: &Arc<MicroPartition>,
        is_semi: bool,
    ) -> DaftResult<Arc<MicroPartition>> {
        let _growables = info_span!("AntiSemiOperator::build_growables").entered();

        let input_tables = input.get_tables()?;

        let mut probe_side_growable = GrowableTable::new(
            &input_tables.iter().collect::<Vec<_>>(),
            false,
            Self::DEFAULT_GROWABLE_SIZE,
        )?;

        drop(_growables);
        {
            let _loop = info_span!("AntiSemiOperator::eval_and_probe").entered();
            for (probe_side_table_idx, table) in input_tables.iter().enumerate() {
                let join_keys = table.eval_expression_list(probe_on)?;
                let iter = probe_set.probe_exists(&join_keys)?;

                for (probe_row_idx, matched) in iter.enumerate() {
                    // 1. If this is a semi join, we keep the row if it matches.
                    // 2. If this is an anti join, we keep the row if it doesn't match.
                    match (is_semi, matched) {
                        (true, true) | (false, false) => {
                            probe_side_growable.extend(probe_side_table_idx, probe_row_idx, 1);
                        }
                        _ => {}
                    }
                }
            }
        }
        let probe_side_table = probe_side_growable.build()?;
        Ok(Arc::new(MicroPartition::new_loaded(
            probe_side_table.schema.clone(),
            Arc::new(vec![probe_side_table]),
            None,
        )))
    }

    // This function performs the anti semi join when the side to keep is the build side, i.e. we built the probe table
    // on the left side and we are streaming the right side. In this case, we use a bitmap index to track matches, and only
    // emit a final result at the end.
    fn probe_anti_semi_with_bitmap(
        probe_on: &[ExprRef],
        probe_set: &Arc<dyn Probeable>,
        bitmap_builder: &mut IndexBitmapBuilder,
        input: &Arc<MicroPartition>,
    ) -> DaftResult<()> {
        let input_tables = input.get_tables()?;

        let _loop = info_span!("AntiSemiOperator::eval_and_probe").entered();
        for table in input_tables.iter() {
            let join_keys = table.eval_expression_list(probe_on)?;
            let idx_mapper = probe_set.probe_indices(&join_keys)?;

            for inner_iter in idx_mapper.make_iter().flatten() {
                for (build_side_table_idx, build_row_idx) in inner_iter {
                    bitmap_builder.mark_used(build_side_table_idx as usize, build_row_idx as usize);
                }
            }
        }
        Ok(())
    }

    // Finalize the anti/semi join where we have a bitmap index, i.e. left side builds.
    async fn finalize_anti_semi(
        mut states: Vec<Box<dyn StreamingSinkState>>,
        is_semi: bool,
    ) -> DaftResult<Option<Arc<MicroPartition>>> {
        let mut states_iter = states.iter_mut();
        let first_state = states_iter
            .next()
            .expect("at least one state should be present")
            .as_any_mut()
            .downcast_mut::<AntiSemiProbeState>()
            .expect("state should be AntiSemiProbeState");
        let (first_probe_state, first_bitmap_builder) =
            first_state.get_or_await_probe_state(true).await;
        let tables = first_probe_state.get_tables();
        let first_bitmap = first_bitmap_builder
            .take()
            .expect("bitmap should be set")
            .build();

        let mut merged_bitmap = {
            let bitmaps = stream::once(async move { first_bitmap })
                .chain(stream::iter(states_iter).then(|s| async move {
                    let state = s
                        .as_any_mut()
                        .downcast_mut::<AntiSemiProbeState>()
                        .expect("state should be AntiSemiProbeState");
                    state
                        .get_or_await_probe_state(true)
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

        let build_side_table = Table::concat(&leftovers)?;
        Ok(Some(Arc::new(MicroPartition::new_loaded(
            build_side_table.schema.clone(),
            Arc::new(vec![build_side_table]),
            None,
        ))))
    }
}

impl StreamingSink for AntiSemiProbeSink {
    #[instrument(skip_all, name = "AntiSemiProbeSink::execute")]
    fn execute(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn StreamingSinkState>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> StreamingSinkExecuteResult {
        if input.is_empty() {
            let empty = Arc::new(MicroPartition::empty(Some(self.output_schema.clone())));
            return Ok((state, StreamingSinkOutput::NeedMoreInput(Some(empty)))).into();
        }

        let params = self.params.clone();
        let build_on_left = self.build_on_left;
        task_spawner
            .spawn(
                async move {
                    let probe_state = state
                        .as_any_mut()
                        .downcast_mut::<AntiSemiProbeState>()
                        .expect("AntiSemiProbeState should be used with AntiSemiProbeSink");
                    let (ps, bitmap_builder) =
                        probe_state.get_or_await_probe_state(build_on_left).await;
                    if let Some(bm_builder) = bitmap_builder {
                        Self::probe_anti_semi_with_bitmap(
                            &params.probe_on,
                            ps.get_probeable(),
                            bm_builder,
                            &input,
                        )?;
                        Ok((state, StreamingSinkOutput::NeedMoreInput(None)))
                    } else {
                        let res = Self::probe_anti_semi(
                            &params.probe_on,
                            ps.get_probeable(),
                            &input,
                            params.is_semi,
                        );
                        Ok((state, StreamingSinkOutput::NeedMoreInput(Some(res?))))
                    }
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> &'static str {
        "AntiSemiHashJoinProbe"
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
        states: Vec<Box<dyn super::streaming_sink::StreamingSinkState>>,
        task_spawner: &ExecutionTaskSpawner,
    ) -> super::streaming_sink::StreamingSinkFinalizeResult {
        if self.build_on_left {
            let is_semi = self.params.is_semi;
            task_spawner
                .spawn(
                    async move { Self::finalize_anti_semi(states, is_semi).await },
                    Span::current(),
                )
                .into()
        } else {
            Ok(None).into()
        }
    }

    fn make_state(&self) -> Box<dyn StreamingSinkState> {
        Box::new(AntiSemiProbeState::Building(
            self.probe_state_bridge.clone(),
        ))
    }

    fn dispatch_spawner(
        &self,
        runtime_handle: &ExecutionRuntimeContext,
        maintain_order: bool,
    ) -> Arc<dyn DispatchSpawner> {
        if maintain_order {
            Arc::new(RoundRobinDispatcher::new(Some(
                runtime_handle.default_morsel_size(),
            )))
        } else {
            Arc::new(UnorderedDispatcher::new(Some(
                runtime_handle.default_morsel_size(),
            )))
        }
    }
}
