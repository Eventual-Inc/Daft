use std::collections::HashSet;

use common_error::DaftResult;
use daft_local_plan::{
    FlightShuffleReadInput, LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend,
};
use daft_logical_plan::stats::StatsState;
use daft_partition_refs::FlightPartitionRef;
use daft_schema::schema::SchemaRef;
use daft_shuffles::client::FlightClientManager;

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
    plan_context.register_shuffle_dirs(shuffle_dirs_to_register);
}

/// Orchestrator-side seal: tell every Flight server that participated in this
/// shuffle to consolidate its per-task entry groups into one file per partition.
/// Called once per shuffle, after the producer stage's `transpose` completes
/// (so we have the full ref set) and before any read task is emitted.
///
/// We extract the unique `server_address` set from the materialized refs and
/// fan out `seal_shuffle` gRPC calls in parallel. The shuffle_id we pass in
/// directly — every ref carries it but the backend config does too, so we
/// avoid digging it out of the refs.
pub(crate) async fn seal(
    shuffle_id: u64,
    partition_groups: &[Vec<MaterializedOutput>],
) -> DaftResult<()> {
    let mut server_addresses: HashSet<String> = HashSet::new();
    for group in partition_groups {
        for output in group {
            for partition in output.partitions() {
                if let Some(flight_ref) =
                    partition.as_any().downcast_ref::<FlightPartitionRef>()
                {
                    server_addresses.insert(flight_ref.server_address.clone());
                }
            }
        }
    }

    if server_addresses.is_empty() {
        return Ok(());
    }

    let client_manager = FlightClientManager::new();
    let seal_futs = server_addresses.into_iter().map(|addr| {
        let cm = client_manager.clone();
        async move { cm.seal_shuffle(&addr, shuffle_id).await }
    });
    // `try_join_all` returns the first error if any seal fails. We could
    // tolerate partial failure here (per-group seal is best-effort) but a
    // failed seal means some read paths get the slow M-opens layout; safer
    // to surface the error to the orchestrator and let it decide.
    futures::future::try_join_all(seal_futs).await?;
    Ok(())
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
                    .downcast_ref::<FlightPartitionRef>()
                    .expect("expected flight partition ref")
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
            .with_flight_shuffle_reads(node_id, vec![FlightShuffleReadInput::from_refs(psets)]);

        let _ = result_tx.send(task).await;
    }

    Ok(())
}
