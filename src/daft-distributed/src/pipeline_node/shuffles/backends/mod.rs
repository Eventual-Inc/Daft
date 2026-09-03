use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_local_plan::{
    LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef, ShuffleBackend, ShuffleReadBackend,
};
use daft_logical_plan::stats::StatsState;
use daft_partition_refs::FlightPartitionRef;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{MaterializedOutput, NodeID, PipelineNodeContext, PipelineNodeImpl},
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

mod flight;
mod ray;

/// Mint the identity under which one shuffle's files and registrations live.
///
/// Random rather than derived from `(query_idx, node_id)`: those counters are
/// local to one driver process, so two drivers sharing a cluster — or a shared
/// filesystem — would produce the same id for their first query's first shuffle
/// and then write into, and clean up, each other's directories. Sixty-four random
/// bits make that collision negligible. The id is logged against the plan
/// coordinates it stands for so a directory on disk can still be traced back.
fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    let shuffle_id = rand::random::<u64>();
    tracing::info!(
        shuffle_id = shuffle_id,
        query_idx = context.query_idx,
        node_id = context.node_id,
        "Assigned flight shuffle id"
    );
    shuffle_id
}

/// Which map-side writer a shuffle node uses.
///
/// This decides whether shared placement is available, because only the combined
/// file carries an index a peer can resolve byte ranges from. The per-partition
/// layout is addressable only through the writing worker's in-memory cache, so
/// putting it on a shared mount would buy nothing and would leave the read side
/// looking for files in the wrong place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShuffleWriteKind {
    /// One combined, self-indexing file per map task (`RepartitionWrite`).
    CombinedFile,
    /// One directory per output partition (`GatherWrite`, `IntoPartitions`).
    PerPartition,
}

/// A shuffle node's resolved backend: the plan-level [`ShuffleBackend`] with this
/// node's `shuffle_id` stamped in, plus the node handles task building needs.
#[derive(Clone)]
pub(crate) struct ShuffleContext {
    backend: ShuffleBackend,
    schema: SchemaRef,
    node_id: NodeID,
}

impl ShuffleContext {
    pub(crate) fn new(
        context: &PipelineNodeContext,
        schema: SchemaRef,
        backend: ShuffleBackend,
        write_kind: ShuffleWriteKind,
    ) -> Self {
        Self {
            schema,
            node_id: context.node_id,
            backend: match backend {
                ShuffleBackend::Ray => ShuffleBackend::Ray,
                ShuffleBackend::Flight {
                    shuffle_dirs,
                    compression,
                    shared,
                    ..
                } => ShuffleBackend::Flight {
                    shuffle_id: make_shuffle_id(context),
                    shuffle_dirs,
                    compression,
                    // Dropped here rather than at each call site so the write and
                    // read halves cannot disagree: both read `shared` back off
                    // this one resolved backend.
                    shared: match write_kind {
                        ShuffleWriteKind::CombinedFile => shared,
                        ShuffleWriteKind::PerPartition => None,
                    },
                },
            },
        }
    }

    pub(crate) fn backend(&self) -> &ShuffleBackend {
        &self.backend
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn node_id(&self) -> NodeID {
        self.node_id
    }

    pub(crate) fn register_cleanup(&self, plan_context: &mut PlanExecutionContext) {
        match &self.backend {
            ShuffleBackend::Ray => {}
            ShuffleBackend::Flight {
                shuffle_id,
                shuffle_dirs,
                shared,
                ..
            } => {
                flight::register_cleanup(
                    *shuffle_id,
                    shuffle_dirs,
                    shared.as_ref().map(|s| s.root.as_str()),
                    plan_context,
                );
            }
        }
    }

    /// Build a `SwordfishTaskBuilder` whose plan reads from already-materialized
    /// partition refs (`in_memory_scan` for Ray refs, `shuffle_read(Flight)` for
    /// Flight refs) and then applies `wrap_plan` on top. The partition refs are
    /// attached to the task via the ref-appropriate API (`with_psets` /
    /// `with_flight_shuffle_reads`).
    ///
    /// The choice follows the refs themselves, not this node's configured
    /// backend. A node configured for Flight is only guaranteed Flight refs from
    /// its *own* map side; refs handed in from a child task — a sort's output,
    /// say, or a random shuffle's reduce output — are ordinary in-memory
    /// partitions no matter what the shuffle algorithm is, and reading them
    /// through the Flight path would be a type confusion.
    pub(crate) fn build_refs_task_builder<F>(
        &self,
        partition_refs: Vec<PartitionRef>,
        node: &dyn PipelineNodeImpl,
        wrap_plan: F,
    ) -> DaftResult<SwordfishTaskBuilder>
    where
        F: FnOnce(LocalPhysicalPlanRef) -> LocalPhysicalPlanRef,
    {
        let node_id = self.node_id;
        let num_flight = partition_refs
            .iter()
            .filter(|p| p.as_any().is::<FlightPartitionRef>())
            .count();
        if num_flight != 0 && num_flight != partition_refs.len() {
            return Err(DaftError::InternalError(format!(
                "shuffle node {} was handed {} Flight refs mixed with {} in-memory refs; \
                 a read task can only consume one kind",
                node_id,
                num_flight,
                partition_refs.len() - num_flight
            )));
        }
        let all_flight = num_flight != 0 && num_flight == partition_refs.len();

        if all_flight {
            let read_inputs =
                flight::read_inputs_from_refs(partition_refs, self.backend.shared_root());
            let shuffle_read = LocalPhysicalPlan::shuffle_read(
                node_id,
                self.schema.clone(),
                ShuffleReadBackend::Flight,
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            );
            let plan = wrap_plan(shuffle_read);
            Ok(SwordfishTaskBuilder::new(plan, node, node_id)
                .with_flight_shuffle_reads(node_id, read_inputs))
        } else {
            let total_size_bytes = partition_refs.iter().map(|p| p.size_bytes()).sum::<usize>();
            let in_memory_scan = LocalPhysicalPlan::in_memory_scan(
                node_id,
                self.schema.clone(),
                total_size_bytes,
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            );
            let plan = wrap_plan(in_memory_scan);
            Ok(SwordfishTaskBuilder::new(plan, node, node_id).with_psets(node_id, partition_refs))
        }
    }

    /// Group a stream of map-task outputs into per-partition read tasks.
    ///
    /// The Ray backend transposes the full (tasks x partitions) matrix of object refs.
    /// The flight backend folds the stream into per-server map-input lists shared by
    /// all reduce tasks, keeping coordinator memory O(map_tasks + partitions) instead
    /// of O(map_tasks x partitions).
    pub(crate) async fn emit_read_tasks_from_stream(
        &self,
        materialized_stream: impl futures::Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin,
        num_partitions: usize,
        node: &dyn PipelineNodeImpl,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        match &self.backend {
            ShuffleBackend::Ray => {
                let partition_groups =
                    crate::utils::transpose::transpose_materialized_outputs_from_stream(
                        materialized_stream,
                        num_partitions,
                    )
                    .await?;
                ray::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    partition_groups,
                    node,
                    result_tx,
                )
                .await
            }
            ShuffleBackend::Flight { shuffle_id, .. } => {
                let read_inputs = flight::fold_outputs_from_stream(
                    materialized_stream,
                    num_partitions,
                    *shuffle_id,
                    self.backend.shared_root(),
                )
                .await?;
                flight::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    read_inputs,
                    node,
                    result_tx,
                )
                .await
            }
        }
    }
}
