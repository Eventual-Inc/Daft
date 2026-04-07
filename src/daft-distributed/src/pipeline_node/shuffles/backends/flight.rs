use common_error::DaftResult;
use daft_local_plan::{
    FlightShufflePartitionRef, FlightShuffleReadInput, LocalNodeContext, LocalPhysicalPlan,
    ShuffleReadBackend,
};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{NodeID, PipelineNodeImpl},
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

#[derive(Clone)]
pub(crate) struct FlightShuffleBackendConfig {
    pub(crate) shuffle_id: u64,
    pub(crate) shuffle_dirs: Vec<String>,
    pub(crate) compression: Option<String>,
}

pub(crate) struct FlightShuffleReadSpec {
    shuffle_id: u64,
    partition_groups: Vec<Vec<FlightShufflePartitionRef>>,
}

pub(crate) fn register_cleanup(
    backend: &FlightShuffleBackendConfig,
    plan_context: &mut PlanExecutionContext,
) {
    plan_context.register_flight_shuffle_id(backend.shuffle_id);
    let shuffle_dirs_to_register: Vec<String> = backend
        .shuffle_dirs
        .iter()
        .map(|base_dir| format!("{}/daft_shuffle/{}", base_dir, backend.shuffle_id))
        .collect();
    plan_context.register_shuffle_dirs(shuffle_dirs_to_register);
}

pub(crate) fn read_spec_from_partition_groups(
    backend: &FlightShuffleBackendConfig,
    partition_groups: Vec<Vec<FlightShufflePartitionRef>>,
) -> FlightShuffleReadSpec {
    FlightShuffleReadSpec {
        shuffle_id: backend.shuffle_id,
        partition_groups,
    }
}

pub(crate) async fn emit_read_tasks(
    node_id: NodeID,
    schema: SchemaRef,
    read_spec: FlightShuffleReadSpec,
    node: &dyn PipelineNodeImpl,
    result_tx: Sender<SwordfishTaskBuilder>,
) -> DaftResult<()> {
    for partition_group in read_spec.partition_groups {
        let shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
            node_id,
            schema.clone(),
            ShuffleReadBackend::Flight {
                shuffle_id: read_spec.shuffle_id,
            },
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)),
        );

        let task = SwordfishTaskBuilder::new(shuffle_read_plan, node, node_id)
            .with_flight_shuffle_reads(
                node_id,
                vec![FlightShuffleReadInput {
                    refs: partition_group,
                }],
            );

        let _ = result_tx.send(task).await;
    }

    Ok(())
}
