use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_local_plan::{
    FlightMapOutput, FlightShuffleReadInput, LocalNodeContext, LocalPhysicalPlan,
    ShuffleReadBackend,
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
    shared_root: Option<&str>,
    plan_context: &mut PlanExecutionContext,
) {
    let shuffle_dirs_to_register: Vec<String> = shuffle_dirs
        .iter()
        .map(|base_dir| format!("{}/daft_shuffle/{}", base_dir, shuffle_id))
        .collect();
    plan_context.register_shuffle_dirs(shuffle_dirs_to_register);

    // Registered separately because a shared directory is one tree visible to
    // every node, not one tree per node: fanning the same delete out to the whole
    // cluster would have every worker racing to remove the same files.
    if let Some(shared_root) = shared_root {
        plan_context.register_shared_shuffle_dirs(vec![daft_shuffles::store::shared_shuffle_dir(
            shared_root,
            shuffle_id,
        )]);
    }
}

/// `partition_ref_id` layout: `(input_id << 32) | partition_idx`.
fn input_id_from_ref(flight_ref: &FlightPartitionRef) -> u32 {
    (flight_ref.partition_ref_id >> 32) as u32
}

/// `partition_ref_id` layout: `(input_id << 32) | partition_idx`.
fn partition_idx_from_ref(flight_ref: &FlightPartitionRef) -> u32 {
    (flight_ref.partition_ref_id & 0xFFFF_FFFF) as u32
}

fn map_output_from_ref(flight_ref: &FlightPartitionRef) -> FlightMapOutput {
    FlightMapOutput {
        input_id: input_id_from_ref(flight_ref),
        attempt: flight_ref.attempt,
    }
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
/// full matrix had been kept. Each output is recorded with the attempt that
/// produced it, so a stale registration or file left by another attempt of the
/// same task is never addressed.
///
/// Exactly one output per map input is accepted. The dispatcher delivers one
/// result per task, so a second output for an input cannot happen in normal
/// operation; if it ever did, folding both in would have every reducer read that
/// input twice. That is a wrong answer, so it is refused rather than tolerated.
pub(crate) async fn fold_outputs_from_stream(
    mut materialized_stream: impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin,
    num_partitions: usize,
    shuffle_id: u64,
    shared_root: Option<&str>,
) -> DaftResult<Vec<FlightShuffleReadInput>> {
    let mut inputs_by_server: BTreeMap<String, Vec<FlightMapOutput>> = BTreeMap::new();
    let mut seen_inputs: HashSet<u32> = HashSet::new();

    while let Some(output) = materialized_stream.next().await {
        // A map output is one input's writes on one server: its refs all share
        // (server_address, input_id, attempt), one ref per partition. So the
        // first ref identifies the whole output.
        let Some(partition) = output?.into_inner().0.into_iter().next() else {
            continue;
        };
        let flight_ref = partition
            .as_any()
            .downcast_ref::<FlightPartitionRef>()
            .expect("expected flight partition ref");
        let map_output = map_output_from_ref(flight_ref);
        if !seen_inputs.insert(map_output.input_id) {
            return Err(DaftError::InternalError(format!(
                "shuffle {} received two outputs for map input {} (second from {} attempt {:#x}); \
                 refusing to fold both, which would read that input twice",
                shuffle_id, map_output.input_id, flight_ref.server_address, map_output.attempt
            )));
        }
        inputs_by_server
            .entry(flight_ref.server_address.clone())
            .or_default()
            .push(map_output);
    }

    let inputs_by_server = Arc::new(inputs_by_server);
    let shared_root: Option<Arc<str>> = shared_root.map(Arc::from);
    Ok((0..num_partitions)
        .map(|partition_idx| FlightShuffleReadInput {
            shuffle_id,
            partition_idx: partition_idx as u32,
            inputs_by_server: inputs_by_server.clone(),
            shared_root: shared_root.clone(),
        })
        .collect())
}

/// Express an arbitrary set of flight partition refs as read inputs, grouped by
/// (shuffle, partition idx).
pub(crate) fn read_inputs_from_refs(
    partition_refs: Vec<PartitionRef>,
    shared_root: Option<&str>,
) -> Vec<FlightShuffleReadInput> {
    let mut groups: BTreeMap<(u64, u32), BTreeMap<String, Vec<FlightMapOutput>>> = BTreeMap::new();
    for partition in partition_refs {
        let flight_ref = partition
            .as_any()
            .downcast_ref::<FlightPartitionRef>()
            .expect("expected flight partition ref");
        groups
            .entry((flight_ref.shuffle_id, partition_idx_from_ref(flight_ref)))
            .or_default()
            .entry(flight_ref.server_address.clone())
            .or_default()
            .push(map_output_from_ref(flight_ref));
    }

    let shared_root: Option<Arc<str>> = shared_root.map(Arc::from);
    groups
        .into_iter()
        .map(
            |((shuffle_id, partition_idx), inputs_by_server)| FlightShuffleReadInput {
                shuffle_id,
                partition_idx,
                inputs_by_server: Arc::new(inputs_by_server),
                shared_root: shared_root.clone(),
            },
        )
        .collect()
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
