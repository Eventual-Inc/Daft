use std::sync::Arc;

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{make_probeable_builder, ProbeState, ProbeableBuilder, RecordBatch};
use itertools::Itertools;
use tracing::{info_span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkState, BlockingSinkStatus,
};
use crate::{state_bridge::BroadcastStateBridgeRef, ExecutionTaskSpawner};

enum ProbeTableState {
    Building {
        probe_table_builder: Option<Box<dyn ProbeableBuilder>>,
        projection: Vec<BoundExpr>,
        tables: Vec<RecordBatch>,
    },
    Done,
}

impl ProbeTableState {
    fn new(
        key_schema: &SchemaRef,
        projection: Vec<BoundExpr>,
        nulls_equal_aware: Option<&Vec<bool>>,
        track_indices: bool,
    ) -> DaftResult<Self> {
        Ok(Self::Building {
            probe_table_builder: Some(make_probeable_builder(
                key_schema.clone(),
                nulls_equal_aware,
                track_indices,
            )?),
            projection,
            tables: Vec::new(),
        })
    }

    fn add_tables(&mut self, input: &Arc<MicroPartition>) -> DaftResult<()> {
        if let Self::Building {
            ref mut probe_table_builder,
            projection,
            tables,
        } = self
        {
            let probe_table_builder = probe_table_builder.as_mut().unwrap();
            let input_tables = input.get_tables()?;
            if input_tables.is_empty() {
                tables.push(RecordBatch::empty(Some(input.schema()))?);
                return Ok(());
            }
            for table in input_tables.iter() {
                tables.push(table.clone());
                let join_keys = table.eval_expression_list(projection)?;

                probe_table_builder.add_table(&join_keys)?;
            }
            Ok(())
        } else {
            panic!("add_tables can only be used during the Building Phase")
        }
    }
    fn finalize(&mut self) -> ProbeState {
        if let Self::Building {
            probe_table_builder,
            tables,
            ..
        } = self
        {
            let ptb = std::mem::take(probe_table_builder).expect("should be set in building mode");
            let pt = ptb.build();

            let ps = ProbeState::new(pt, tables.clone().into());
            *self = Self::Done;
            ps
        } else {
            panic!("finalize can only be used during the Building Phase")
        }
    }
}

impl BlockingSinkState for ProbeTableState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub struct HashJoinBuildSink {
    key_schema: SchemaRef,
    projection: Vec<BoundExpr>,
    nulls_equal_aware: Option<Vec<bool>>,
    track_indices: bool,
    probe_state_bridge: BroadcastStateBridgeRef<ProbeState>,
}

impl HashJoinBuildSink {
    pub(crate) fn new(
        key_schema: SchemaRef,
        projection: Vec<BoundExpr>,
        nulls_equal_aware: Option<Vec<bool>>,
        track_indices: bool,
        probe_state_bridge: BroadcastStateBridgeRef<ProbeState>,
    ) -> DaftResult<Self> {
        Ok(Self {
            key_schema,
            projection,
            nulls_equal_aware,
            track_indices,
            probe_state_bridge,
        })
    }
}

impl BlockingSink for HashJoinBuildSink {
    fn name(&self) -> &'static str {
        "HashJoinBuild"
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push("HashJoinBuild:".to_string());
        display.push(format!("Track Indices: {}", self.track_indices));
        display.push(format!("Key Schema: {}", self.key_schema.short_string()));
        if let Some(null_equals_nulls) = &self.nulls_equal_aware {
            display.push(format!(
                "Null equals Nulls = [{}]",
                null_equals_nulls.iter().map(|b| b.to_string()).join(", ")
            ));
        }
        display
    }

    fn sink(
        &self,
        input: Arc<MicroPartition>,
        mut state: Box<dyn BlockingSinkState>,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult {
        spawner
            .spawn(
                async move {
                    let probe_table_state: &mut ProbeTableState = state
                        .as_any_mut()
                        .downcast_mut::<ProbeTableState>()
                        .expect("HashJoinBuildSink should have ProbeTableState");
                    probe_table_state.add_tables(&input)?;
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                info_span!("HashJoinBuildSink::sink"),
            )
            .into()
    }

    #[instrument(skip_all, name = "HashJoinBuildSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Box<dyn BlockingSinkState>>,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult {
        assert_eq!(states.len(), 1);
        let mut state = states.into_iter().next().unwrap();
        let probe_table_state = state
            .as_any_mut()
            .downcast_mut::<ProbeTableState>()
            .expect("State type mismatch");
        let finalized_probe_state = probe_table_state.finalize();
        self.probe_state_bridge
            .set_state(finalized_probe_state.into());
        Ok(BlockingSinkFinalizeOutput::Finished(vec![])).into()
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn make_state(&self) -> DaftResult<Box<dyn BlockingSinkState>> {
        Ok(Box::new(ProbeTableState::new(
            &self.key_schema,
            self.projection.clone(),
            self.nulls_equal_aware.as_ref(),
            self.track_indices,
        )?))
    }
}
