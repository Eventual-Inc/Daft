use std::collections::HashMap;

use common_error::DaftResult;
use daft_local_plan::{
    FlightShuffleReadInput, LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend,
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
pub(crate) struct FlightDistributedExchangeConfig {
    pub(crate) exchange_id: u64,
    pub(crate) shuffle_dirs: Vec<String>,
    pub(crate) compression: Option<String>,
}

pub(crate) struct FlightReadSpec {
    exchange_id: u64,
    server_cache_mapping: HashMap<String, Vec<u32>>,
}

pub(crate) fn register_cleanup(
    backend: &FlightDistributedExchangeConfig,
    plan_context: &mut PlanExecutionContext,
) {
    let shuffle_dirs_to_register: Vec<String> = backend
        .shuffle_dirs
        .iter()
        .map(|base_dir| format!("{}/daft_shuffle/{}", base_dir, backend.exchange_id))
        .collect();
    plan_context.register_shuffle_dirs(shuffle_dirs_to_register);
}

pub(crate) fn read_spec_from_server_cache_mapping(
    backend: &FlightDistributedExchangeConfig,
    server_cache_mapping: HashMap<String, Vec<u32>>,
) -> FlightReadSpec {
    FlightReadSpec {
        exchange_id: backend.exchange_id,
        server_cache_mapping,
    }
}

pub(crate) async fn emit_read_tasks(
    node_id: NodeID,
    schema: SchemaRef,
    num_partitions: usize,
    read_spec: FlightReadSpec,
    node: &dyn PipelineNodeImpl,
    result_tx: Sender<SwordfishTaskBuilder>,
) -> DaftResult<()> {
    for partition_idx in 0..num_partitions {
        let shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
            node_id,
            schema.clone(),
            ShuffleReadBackend::Flight {
                shuffle_id: read_spec.exchange_id,
                server_cache_mapping: read_spec.server_cache_mapping.clone(),
            },
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)),
        );

        let task = SwordfishTaskBuilder::new(shuffle_read_plan, node, node_id)
            .with_flight_shuffle_reads(node_id, vec![FlightShuffleReadInput { partition_idx }]);

        let _ = result_tx.send(task).await;
    }

    Ok(())
}
