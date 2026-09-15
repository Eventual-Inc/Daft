use std::{collections::BTreeMap, sync::Arc};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_local_plan::{
    FlightShuffleReadInput, LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend,
};
use daft_logical_plan::stats::StatsState;
use daft_partition_refs::FlightPartitionRef;
use daft_schema::schema::SchemaRef;
use futures::{Stream, StreamExt};

use crate::{
    pipeline_node::{MaterializedOutput, NodeID, PipelineNodeImpl},
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

pub(crate) fn register_cleanup(
    shuffle_id: u64,
    shuffle_dirs: &[String],
    plan_context: &mut PlanExecutionContext,
) {
    let shuffle_dirs_to_register: Vec<String> = shuffle_dirs
        .iter()
        .map(|base_dir| format!("{}/daft_shuffle/{}", base_dir, shuffle_id))
        .collect();
    plan_context.register_shuffle_dirs(shuffle_dirs_to_register);
}

/// Whether `partition` came out of a flight write, i.e. whether the flight read
/// path can address it.
pub(crate) fn is_flight_ref(partition: &PartitionRef) -> bool {
    partition.as_any().is::<FlightPartitionRef>()
}

/// View `partition` as a `FlightPartitionRef`.
///
/// Returns an error rather than panicking: the refs reaching the flight read path
/// come from whatever the upstream stage materialized, and mixing in a plain
/// in-memory ref is a bug in the calling node, not something to abort the process
/// over. The panic this replaces surfaced in Python as a bare
/// `RayTaskError(DaftCoreException)` with no indication of which node was at fault.
fn as_flight_ref(partition: &PartitionRef) -> DaftResult<&FlightPartitionRef> {
    partition
        .as_any()
        .downcast_ref::<FlightPartitionRef>()
        .ok_or_else(|| {
            DaftError::InternalError(
                "Flight shuffle read expected a flight partition ref, got a partition ref that \
                 did not come from a flight write."
                    .to_string(),
            )
        })
}

/// `partition_ref_id` layout: `(input_id << 32) | partition_idx`.
fn input_id_from_ref(flight_ref: &FlightPartitionRef) -> u32 {
    (flight_ref.partition_ref_id >> 32) as u32
}

/// `partition_ref_id` layout: `(input_id << 32) | partition_idx`.
fn partition_idx_from_ref(flight_ref: &FlightPartitionRef) -> u32 {
    (flight_ref.partition_ref_id & 0xFFFF_FFFF) as u32
}

/// Fold a stream of flight-shuffle map outputs into one read input per partition.
///
/// Each map task emits one `FlightPartitionRef` per output partition, so collecting them
/// all (as the generic transpose does) holds O(map_tasks x num_partitions) refs on the
/// coordinator — e.g. 10k map tasks x 8k partitions is ~82M refs, tens of GB of heap.
/// But the refs are structured (`partition_ref_id = (input_id << 32) | partition_idx`,
/// one per partition per map input), so the matrix is recoverable from just the set of
/// input ids per server — O(map_tasks) total, shared across all partitions via `Arc`.
/// The reduce side reconstructs the exact refs, issuing the same requests as if the
/// full matrix had been kept, so retried map tasks' stale registrations are never
/// addressed.
pub(crate) async fn fold_outputs_from_stream(
    mut materialized_stream: impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin,
    num_partitions: usize,
    shuffle_id: u64,
) -> DaftResult<Vec<FlightShuffleReadInput>> {
    let mut inputs_by_server: BTreeMap<String, Vec<u32>> = BTreeMap::new();

    while let Some(output) = materialized_stream.next().await {
        // A map output is one input's writes on one server: its refs all share
        // (server_address, input_id), one ref per partition. So the first ref
        // identifies the whole output.
        let Some(partition) = output?.into_inner().0.into_iter().next() else {
            continue;
        };
        let flight_ref = as_flight_ref(&partition)?;
        inputs_by_server
            .entry(flight_ref.server_address.clone())
            .or_default()
            .push(input_id_from_ref(flight_ref));
    }

    let inputs_by_server = Arc::new(inputs_by_server);
    Ok((0..num_partitions)
        .map(|partition_idx| FlightShuffleReadInput {
            shuffle_id,
            partition_idx: partition_idx as u32,
            inputs_by_server: inputs_by_server.clone(),
        })
        .collect())
}

/// Express an arbitrary set of flight partition refs as read inputs, grouped by
/// (shuffle, partition idx).
pub(crate) fn read_inputs_from_refs(
    partition_refs: Vec<PartitionRef>,
) -> DaftResult<Vec<FlightShuffleReadInput>> {
    let mut groups: BTreeMap<(u64, u32), BTreeMap<String, Vec<u32>>> = BTreeMap::new();
    for partition in partition_refs {
        let flight_ref = as_flight_ref(&partition)?;
        groups
            .entry((flight_ref.shuffle_id, partition_idx_from_ref(flight_ref)))
            .or_default()
            .entry(flight_ref.server_address.clone())
            .or_default()
            .push(input_id_from_ref(flight_ref));
    }

    Ok(groups
        .into_iter()
        .map(
            |((shuffle_id, partition_idx), inputs_by_server)| FlightShuffleReadInput {
                shuffle_id,
                partition_idx,
                inputs_by_server: Arc::new(inputs_by_server),
            },
        )
        .collect())
}

pub(crate) async fn emit_read_tasks(
    node_id: NodeID,
    schema: SchemaRef,
    read_inputs: Vec<FlightShuffleReadInput>,
    node: &dyn PipelineNodeImpl,
    result_tx: Sender<SwordfishTaskBuilder>,
) -> DaftResult<()> {
    for read_input in read_inputs {
        // Fresh plan per task: `SwordfishTaskBuilder::build` mutates the plan in
        // place and requires sole ownership of its Arc.
        let shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
            node_id,
            schema.clone(),
            ShuffleReadBackend::Flight,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)),
        );
        let task = SwordfishTaskBuilder::new(shuffle_read_plan, node, node_id)
            .with_flight_shuffle_reads(node_id, vec![read_input]);

        let _ = result_tx.send(task).await;
    }

    Ok(())
}
