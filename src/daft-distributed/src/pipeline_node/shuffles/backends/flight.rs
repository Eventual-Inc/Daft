use common_error::DaftResult;
use daft_local_plan::{
    FlightShuffleReadInput, LocalNodeContext, LocalPhysicalPlan, PyFlightPartitionRef,
    ShuffleReadBackend,
};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{MaterializedOutput, NodeID, PipelineNodeImpl},
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

#[derive(Clone, Default)]
pub(crate) struct FlightShuffleBackendConfig {
    pub(crate) shuffle_id: u64,
    pub(crate) shuffle_dirs: Vec<String>,
    pub(crate) compression: Option<String>,
}

pub(crate) fn register_cleanup(
    backend: &FlightShuffleBackendConfig,
    plan_context: &mut PlanExecutionContext,
) {
    let shuffle_dirs_to_register: Vec<String> = backend
        .shuffle_dirs
        .iter()
        .map(|base_dir| format!("{}/daft_shuffle/{}", base_dir, backend.shuffle_id))
        .collect();
    plan_context.register_flight_shuffle_cleanup(backend.shuffle_id, shuffle_dirs_to_register);
}

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
            .map(|partition| {
                partition
                    .as_any()
                    .downcast_ref::<PyFlightPartitionRef>()
                    .expect("expected flight partition ref")
                    .inner
                    .clone()
            })
            .collect::<Vec<_>>();

        let shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
            node_id,
            schema.clone(),
            ShuffleReadBackend::Flight,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)),
        );

        let task = SwordfishTaskBuilder::new(shuffle_read_plan, node, node_id)
            .with_flight_shuffle_reads(node_id, vec![FlightShuffleReadInput { refs: psets }]);

        let _ = result_tx.send(task).await;
    }

    Ok(())
}
