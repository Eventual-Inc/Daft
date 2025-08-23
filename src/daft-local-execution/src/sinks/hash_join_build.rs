use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use common_error::DaftResult;
use common_metrics::{snapshot, Stat, StatSnapshotSend};
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{make_probeable_builder, ProbeState, ProbeableBuilder, RecordBatch};
use itertools::Itertools;
use tracing::{info_span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkStatus,
};
use crate::{
    ops::NodeType,
    pipeline::NodeName,
    runtime_stats::{RuntimeStats, CPU_US_KEY, ROWS_RECEIVED_KEY},
    state_bridge::BroadcastStateBridgeRef,
    ExecutionTaskSpawner,
};

pub(crate) enum ProbeTableState {
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
                let empty_table = RecordBatch::empty(Some(input.schema()));
                let join_keys = empty_table.eval_expression_list(projection)?;
                probe_table_builder.add_table(&join_keys)?;
                tables.push(empty_table);
            } else {
                for table in input_tables.iter() {
                    tables.push(table.clone());
                    let join_keys = table.eval_expression_list(projection)?;

                    probe_table_builder.add_table(&join_keys)?;
                }
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

#[derive(Default)]
struct HashJoinBuildRuntimeStats {
    cpu_us: AtomicU64,
    rows_received: AtomicU64,
}

impl RuntimeStats for HashJoinBuildRuntimeStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshotSend {
        snapshot![
            CPU_US_KEY; Stat::Duration(Duration::from_micros(self.cpu_us.load(ordering))),
            ROWS_RECEIVED_KEY; Stat::Count(self.rows_received.load(ordering)),
        ]
    }

    fn add_rows_received(&self, rows: u64) {
        self.rows_received.fetch_add(rows, Ordering::Relaxed);
    }

    fn add_rows_emitted(&self, _: u64) {
        unreachable!("HashJoinBuildSink shouldn't emit rows")
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.fetch_add(cpu_us, Ordering::Relaxed);
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
    type State = ProbeTableState;

    fn name(&self) -> NodeName {
        "HashJoinBuild".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::HashJoinBuild
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
        mut state: Self::State,
        spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkSinkResult<Self> {
        spawner
            .spawn(
                async move {
                    state.add_tables(&input)?;
                    Ok(BlockingSinkStatus::NeedMoreInput(state))
                },
                info_span!("HashJoinBuildSink::sink"),
            )
            .into()
    }

    #[instrument(skip_all, name = "HashJoinBuildSink::finalize")]
    fn finalize(
        &self,
        states: Vec<Self::State>,
        _spawner: &ExecutionTaskSpawner,
    ) -> BlockingSinkFinalizeResult<Self> {
        assert_eq!(states.len(), 1);
        let mut state = states.into_iter().next().unwrap();
        let finalized_probe_state = state.finalize();
        self.probe_state_bridge
            .set_state(finalized_probe_state.into());
        Ok(BlockingSinkFinalizeOutput::Finished(vec![])).into()
    }

    fn max_concurrency(&self) -> usize {
        1
    }

    fn make_state(&self) -> DaftResult<Self::State> {
        ProbeTableState::new(
            &self.key_schema,
            self.projection.clone(),
            self.nulls_equal_aware.as_ref(),
            self.track_indices,
        )
    }

    fn make_runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        Arc::new(HashJoinBuildRuntimeStats::default())
    }

    fn morsel_size_requirement(&self) -> Option<crate::pipeline::MorselSizeRequirement> {
        None
    }
}
