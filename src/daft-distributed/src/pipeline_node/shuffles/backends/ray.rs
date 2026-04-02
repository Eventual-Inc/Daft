use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{NodeID, PipelineNodeImpl},
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

pub(crate) async fn emit_read_tasks(
    node_id: NodeID,
    schema: SchemaRef,
    partition_groups: Vec<Vec<PartitionRef>>,
    node: &dyn PipelineNodeImpl,
    result_tx: Sender<SwordfishTaskBuilder>,
) -> DaftResult<()> {
    for partition_group in partition_groups {
        let shuffle_read = LocalPhysicalPlan::shuffle_read(
            node_id,
            schema.clone(),
            ShuffleReadBackend::Ray,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)),
        );

        let builder = SwordfishTaskBuilder::new(shuffle_read, node, node_id)
            .with_psets(node_id, partition_group);

        let _ = result_tx.send(builder).await;
    }

    Ok(())
}
