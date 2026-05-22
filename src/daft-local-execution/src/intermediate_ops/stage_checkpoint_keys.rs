use std::sync::{Arc, atomic::Ordering};

use common_checkpoint_config::CheckpointIdMap;
use common_error::DaftResult;
use common_metrics::{
    Counter, Meter, StatSnapshot,
    ops::{NodeInfo, NodeType},
    snapshot::StageCheckpointKeysSnapshot,
};
use daft_checkpoint::CheckpointStoreRef;
use daft_dsl::{Expr, expr::bound_expr::BoundExpr};
use daft_micropartition::MicroPartition;
use opentelemetry::KeyValue;
use tracing::{Span, instrument};

use super::intermediate_op::{IntermediateOpExecuteResult, IntermediateOperator};
use crate::{
    ExecutionTaskSpawner,
    dynamic_batching::StaticBatchingStrategy,
    pipeline::{InputId, MorselSizeRequirement, NodeName},
    runtime_stats::RuntimeStats,
};

pub struct StageCheckpointKeysStats {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    bytes_in: Counter,
    bytes_out: Counter,
    num_tasks: Counter,
    keys_staged: Counter,
    node_kv: Vec<KeyValue>,
}

impl StageCheckpointKeysStats {
    pub fn add_keys_staged(&self, keys: u64) {
        self.keys_staged.add(keys, self.node_kv.as_slice());
    }
}

impl RuntimeStats for StageCheckpointKeysStats {
    fn new(meter: &Meter, node_info: &NodeInfo) -> Self {
        let node_kv = node_info.to_key_values();

        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            bytes_in: meter.bytes_in_metric(),
            bytes_out: meter.bytes_out_metric(),
            num_tasks: meter.num_tasks_metric(),
            keys_staged: meter.u64_counter("keys_staged"),
            node_kv,
        }
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        StatSnapshot::StageCheckpointKeys(StageCheckpointKeysSnapshot {
            cpu_us: self.duration_us.load(ordering),
            rows_in: self.rows_in.load(ordering),
            rows_out: self.rows_out.load(ordering),
            keys_staged: self.keys_staged.load(ordering),
            bytes_in: self.bytes_in.load(ordering),
            bytes_out: self.bytes_out.load(ordering),
            num_tasks: self.num_tasks.load(ordering),
        })
    }

    fn add_rows_in(&self, rows: u64) {
        self.rows_in.add(rows, self.node_kv.as_slice());
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.add(rows, self.node_kv.as_slice());
    }

    fn add_duration_us(&self, cpu_us: u64) {
        self.duration_us.add(cpu_us, self.node_kv.as_slice());
    }

    fn add_bytes_in(&self, bytes: u64) {
        self.bytes_in.add(bytes, self.node_kv.as_slice());
    }

    fn add_bytes_out(&self, bytes: u64) {
        self.bytes_out.add(bytes, self.node_kv.as_slice());
    }

    fn increment_num_tasks(&self) {
        self.num_tasks.add(1, self.node_kv.as_slice());
    }
}

/// Checkpoint operator that records which source keys are being processed.
///
/// Extracts the key column from each morsel, stages it to the
/// CheckpointStore, and passes the morsel through unchanged.
/// Uses `CheckpointIdMap` to derive a per-input `CheckpointId` so that
/// tasks sharing a pipeline each checkpoint their own entry.
pub struct StageCheckpointKeysOperator {
    key_expr: BoundExpr,
    store: CheckpointStoreRef,
    id_map: CheckpointIdMap,
}

impl StageCheckpointKeysOperator {
    pub fn new(key_expr: BoundExpr, store: CheckpointStoreRef, id_map: CheckpointIdMap) -> Self {
        // Checkpoint keys must be column references only — no computed expressions.
        assert!(
            matches!(key_expr.inner().as_ref(), Expr::Column(..)),
            "checkpoint key must be a column reference, got: {key_expr}"
        );
        Self {
            key_expr,
            store,
            id_map,
        }
    }
}

impl IntermediateOperator for StageCheckpointKeysOperator {
    type State = ();
    type Stats = StageCheckpointKeysStats;
    type BatchingStrategy = StaticBatchingStrategy;

    #[instrument(skip_all, name = "StageCheckpointKeysOperator::execute")]
    fn execute(
        &self,
        input: MicroPartition,
        state: Self::State,
        runtime_stats: Arc<Self::Stats>,
        task_spawner: &ExecutionTaskSpawner,
        input_id: InputId,
    ) -> IntermediateOpExecuteResult<Self> {
        let key_expr = self.key_expr.clone();
        let store = self.store.clone();
        let checkpoint_id = self.id_map.get_or_generate(input_id);

        task_spawner
            .spawn(
                async move {
                    for rb in input.record_batches() {
                        let key_series = rb.eval_expression(&key_expr)?;
                        let key_count = key_series.len() as u64;
                        store.stage_keys(&checkpoint_id, key_series).await?;
                        runtime_stats.add_keys_staged(key_count);
                    }
                    Ok((state, input))
                },
                Span::current(),
            )
            .into()
    }

    fn name(&self) -> NodeName {
        "StageCheckpointKeys".into()
    }

    fn op_type(&self) -> NodeType {
        NodeType::StageCheckpointKeys
    }

    fn multiline_display(&self) -> Vec<String> {
        vec![format!("StageCheckpointKeys: key={}", self.key_expr)]
    }

    fn make_state(&self) -> Self::State {}

    fn batching_strategy(&self) -> DaftResult<Self::BatchingStrategy> {
        Ok(StaticBatchingStrategy::new(MorselSizeRequirement::default()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use common_metrics::{Meter, ops::NodeInfo};

    use super::StageCheckpointKeysStats;
    use crate::runtime_stats::RuntimeStats;

    fn node_info_from_id(id: usize) -> NodeInfo {
        NodeInfo {
            id,
            ..Default::default()
        }
    }

    #[test]
    fn keys_staged_accumulates() {
        let stats =
            StageCheckpointKeysStats::new(&Meter::test_scope("test_stats"), &node_info_from_id(1));

        stats.add_keys_staged(100);
        stats.add_keys_staged(50);

        assert_eq!(stats.keys_staged.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn snapshot_includes_keys_staged() {
        let stats =
            StageCheckpointKeysStats::new(&Meter::test_scope("test_stats"), &node_info_from_id(2));

        stats.add_rows_in(200);
        stats.add_rows_out(200);
        stats.add_keys_staged(200);

        let snapshot = stats.build_snapshot(Ordering::Relaxed);
        match snapshot {
            common_metrics::StatSnapshot::StageCheckpointKeys(s) => {
                assert_eq!(s.rows_in, 200);
                assert_eq!(s.rows_out, 200);
                assert_eq!(s.keys_staged, 200);
            }
            other => panic!("expected StageCheckpointKeys snapshot, got {:?}", other),
        }
    }
}
