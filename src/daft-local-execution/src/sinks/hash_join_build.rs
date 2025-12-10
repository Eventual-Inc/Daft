use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use common_error::DaftResult;
use common_metrics::{CPU_US_KEY, Stat, StatSnapshot, ops::NodeType, snapshot};
use daft_core::prelude::SchemaRef;
use daft_dsl::expr::bound_expr::BoundExpr;
use daft_micropartition::MicroPartition;
use daft_recordbatch::{ProbeState, ProbeableBuilder, RecordBatch, make_probeable_builder};
use itertools::Itertools;
use opentelemetry::{KeyValue, global};
use tracing::{info_span, instrument};

use super::blocking_sink::{
    BlockingSink, BlockingSinkFinalizeOutput, BlockingSinkFinalizeResult, BlockingSinkSinkResult,
    BlockingSinkStatus,
};
use crate::{
    ExecutionTaskSpawner,
    pipeline::NodeName,
    runtime_stats::{Counter, RuntimeStats},
    state_bridge::BroadcastStateBridgeRef,
};

pub(crate) struct ProbeTableState {
    probe_table_builder: Box<dyn ProbeableBuilder>,
    projection: Vec<BoundExpr>,
    tables: Vec<RecordBatch>,
}

impl ProbeTableState {
    fn new(
        key_schema: &SchemaRef,
        projection: Vec<BoundExpr>,
        nulls_equal_aware: Option<&Vec<bool>>,
        track_indices: bool,
    ) -> DaftResult<Self> {
        Ok(Self {
            probe_table_builder: make_probeable_builder(
                key_schema.clone(),
                nulls_equal_aware,
                track_indices,
            )?,
            projection,
            tables: Vec::new(),
        })
    }

    fn add_tables(&mut self, input: &Arc<MicroPartition>) -> DaftResult<()> {
        let input_tables = input.record_batches();
        if input_tables.is_empty() {
            let empty_table = RecordBatch::empty(Some(input.schema()));
            let join_keys = empty_table.eval_expression_list(&self.projection)?;
            self.probe_table_builder.add_table(&join_keys)?;
            self.tables.push(empty_table);
        } else {
            for table in input_tables {
                self.tables.push(table.clone());
                let join_keys = table.eval_expression_list(&self.projection)?;

                self.probe_table_builder.add_table(&join_keys)?;
            }
        }
        Ok(())
    }
    fn finalize(self) -> ProbeState {
        let pt = self.probe_table_builder.build();
        ProbeState::new(pt, self.tables)
    }
}

struct HashJoinBuildRuntimeStats {
    cpu_us: Counter,
    rows_in: Counter,

    node_kv: Vec<KeyValue>,
}
impl HashJoinBuildRuntimeStats {
    pub fn new(id: usize) -> Self {
        let meter = global::meter("daft.local.node_stats");
        let node_kv = vec![KeyValue::new("node_id", id.to_string())];
        Self {
            cpu_us: Counter::new(&meter, "cpu_us".into(), None),
            rows_in: Counter::new(&meter, "rows_in".into(), None),
            node_kv,
        }
    }
}

impl RuntimeStats for HashJoinBuildRuntimeStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        snapshot![
            CPU_US_KEY; Stat::Duration(Duration::from_micros(self.cpu_us.load(ordering))),
            "rows inserted"; Stat::Count(self.rows_in.load(ordering)),
        ]
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    fn add_rows_out(&self, _: u64) {
        unreachable!("HashJoinBuildSink shouldn't emit rows")
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.add(cpu_us, self.node_kv.as_slice());
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
        "Hash Join Build".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::HashJoinBuild
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut display = vec![];
        display.push("Hash Join Build:".to_string());
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
        let state = states.into_iter().next().unwrap();
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

    fn make_runtime_stats(&self, id: usize) -> Arc<dyn RuntimeStats> {
        Arc::new(HashJoinBuildRuntimeStats::new(id))
    }

    fn morsel_size_requirement(&self) -> Option<crate::pipeline::MorselSizeRequirement> {
        None
    }
}
