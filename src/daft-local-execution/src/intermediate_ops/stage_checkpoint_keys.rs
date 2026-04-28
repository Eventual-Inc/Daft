use std::sync::Arc;

use common_checkpoint_config::CheckpointIdMap;
use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_checkpoint::CheckpointStoreRef;
use daft_dsl::{Expr, expr::bound_expr::BoundExpr};
use daft_micropartition::MicroPartition;
use tracing::{Span, instrument};

use super::intermediate_op::{IntermediateOpExecuteResult, IntermediateOperator};
use crate::{
    ExecutionTaskSpawner,
    dynamic_batching::StaticBatchingStrategy,
    pipeline::{InputId, MorselSizeRequirement, NodeName},
};

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
    type BatchingStrategy = StaticBatchingStrategy;

    #[instrument(skip_all, name = "StageCheckpointKeysOperator::execute")]
    fn execute(
        &self,
        input: MicroPartition,
        state: Self::State,
        _runtime_stats: Arc<Self::Stats>,
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
                        store.stage_keys(&checkpoint_id, key_series).await?;
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
