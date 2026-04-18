use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::NodeType;
use daft_checkpoint::{CheckpointId, CheckpointStoreRef};
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
#[allow(dead_code)]
pub struct StageCheckpointKeysOperator {
    key_expr: BoundExpr,
    store: CheckpointStoreRef,
    checkpoint_id: CheckpointId,
}

impl StageCheckpointKeysOperator {
    #[allow(dead_code)]
    pub fn new(
        key_expr: BoundExpr,
        store: CheckpointStoreRef,
        checkpoint_id: CheckpointId,
    ) -> Self {
        // Checkpoint keys must be column references only — no computed expressions.
        assert!(
            matches!(key_expr.inner().as_ref(), Expr::Column(..)),
            "checkpoint key must be a column reference, got: {key_expr}"
        );
        Self {
            key_expr,
            store,
            checkpoint_id,
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
        _input_id: InputId,
    ) -> IntermediateOpExecuteResult<Self> {
        let key_expr = self.key_expr.clone();
        let store = self.store.clone();
        let checkpoint_id = self.checkpoint_id.clone();

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
