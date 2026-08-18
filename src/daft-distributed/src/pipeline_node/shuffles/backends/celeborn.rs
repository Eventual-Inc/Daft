use common_error::DaftResult;
use daft_local_plan::{
    CelebornShuffleReadInput, LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend,
};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{NodeID, PipelineNodeImpl},
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

/// Read-stage spec carrying the metadata needed to construct reduce tasks.
///
/// For Celeborn this is intentionally lightweight: the Celeborn cluster itself
/// owns the partition location index, so reduce tasks only need to know the
/// shuffle id; the per-task `partition_idx` is
/// supplied separately through the task `Input::CelebornShuffle` channel (see
/// `emit_read_tasks`).
pub(super) struct CelebornShuffleReadSpec {
    pub(crate) shuffle_id: u64,
    pub(crate) num_mappers: u32,
}

/// Emit one reduce task per partition. Each task runs a local
/// `LocalPhysicalPlan::shuffle_read` whose backend is
/// `ShuffleReadBackend::Celeborn`. The target `partition_idx` is attached as a
/// `CelebornShuffleReadInput` so the worker-side `ShuffleReadSource` knows which
/// reduce partition to call `CelebornClient::read_partition` for.
pub(crate) async fn emit_read_tasks(
    node_id: NodeID,
    schema: SchemaRef,
    num_partitions: usize,
    read_spec: CelebornShuffleReadSpec,
    node: &dyn PipelineNodeImpl,
    result_tx: Sender<SwordfishTaskBuilder>,
) -> DaftResult<()> {
    emit_read_tasks_with(
        node_id,
        schema,
        num_partitions,
        read_spec,
        node,
        result_tx,
        |_, plan| Ok(plan),
    )
    .await
}

pub(crate) async fn emit_read_tasks_with<F>(
    node_id: NodeID,
    schema: SchemaRef,
    num_partitions: usize,
    read_spec: CelebornShuffleReadSpec,
    node: &dyn PipelineNodeImpl,
    result_tx: Sender<SwordfishTaskBuilder>,
    mut wrap_plan: F,
) -> DaftResult<()>
where
    F: FnMut(
        usize,
        daft_local_plan::LocalPhysicalPlanRef,
    ) -> DaftResult<daft_local_plan::LocalPhysicalPlanRef>,
{
    for partition_idx in 0..num_partitions {
        let shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
            node_id,
            schema.clone(),
            ShuffleReadBackend::Celeborn {
                shuffle_id: read_spec.shuffle_id,
                num_mappers: read_spec.num_mappers,
            },
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)),
        );
        let plan = wrap_plan(partition_idx, shuffle_read_plan)?;

        let task = SwordfishTaskBuilder::new(plan, node, node_id)
            .with_celeborn_shuffle_reads(node_id, vec![CelebornShuffleReadInput { partition_idx }]);

        let _ = result_tx.send(task).await;
    }

    Ok(())
}
