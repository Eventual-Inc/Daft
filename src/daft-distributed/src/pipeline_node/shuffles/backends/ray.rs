use common_error::DaftResult;
use daft_local_plan::{
    LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef, ShuffleReadBackend,
};
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
    wrap_plan: &mut (
             dyn FnMut(usize, LocalPhysicalPlanRef) -> DaftResult<LocalPhysicalPlanRef> + Send
         ),
) -> DaftResult<()> {
    for (partition_idx, partition_group) in partition_groups.into_iter().enumerate() {
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
        let plan = wrap_plan(partition_idx, shuffle_read)?;

        let builder = SwordfishTaskBuilder::new(plan, node, node_id).with_psets(node_id, psets);

        let _ = result_tx.send(builder).await;
    }

    Ok(())
}
