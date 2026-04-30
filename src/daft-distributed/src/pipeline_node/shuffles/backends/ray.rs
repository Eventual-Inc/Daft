use daft_common::error::DaftResult;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{MaterializedOutput, NodeID, PipelineNodeImpl},
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

pub(crate) async fn emit_read_tasks(
    node_id: NodeID,
    schema: SchemaRef,
    partition_groups: Vec<Vec<MaterializedOutput>>,
    node: &dyn PipelineNodeImpl,
    result_tx: Sender<SwordfishTaskBuilder>,
) -> DaftResult<()> {
    for partition_group in partition_groups {
        let psets = partition_group
            .into_iter()
            .flat_map(|output| output.into_inner().0)
            .collect::<Vec<_>>();

        let shuffle_read = LocalPhysicalPlan::shuffle_read(
            node_id,
            schema.clone(),
            ShuffleReadBackend::Ray,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)),
        );

        let builder =
            SwordfishTaskBuilder::new(shuffle_read, node, node_id).with_psets(node_id, psets);

        let _ = result_tx.send(builder).await;
    }

    Ok(())
}
