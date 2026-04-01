use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
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
        let total_size_bytes = partition_group
            .iter()
            .map(|p| p.size_bytes())
            .sum::<usize>();
        let in_memory_scan = LocalPhysicalPlan::in_memory_scan(
            node_id,
            schema.clone(),
            total_size_bytes,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)),
        );

        let builder = SwordfishTaskBuilder::new(in_memory_scan, node, node_id)
            .with_psets(node_id, partition_group);

        let _ = result_tx.send(builder).await;
    }

    Ok(())
}
