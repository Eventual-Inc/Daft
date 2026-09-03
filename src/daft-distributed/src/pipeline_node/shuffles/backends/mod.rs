use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_local_plan::{
    LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef, ShuffleBackend, ShuffleReadBackend,
};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{MaterializedOutput, NodeID, PipelineNodeContext, PipelineNodeImpl},
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

mod flight;
mod ray;

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    ((context.query_idx as u64) << 32) | (context.node_id as u64)
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
    ) -> Self {
        Self {
            schema,
            node_id: context.node_id,
            backend: match backend {
                ShuffleBackend::Ray => ShuffleBackend::Ray,
                ShuffleBackend::Flight {
                    shuffle_dirs,
                    compression,
                    ..
                } => ShuffleBackend::Flight {
                    shuffle_id: make_shuffle_id(context),
                    shuffle_dirs,
                    compression,
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
                ..
            } => {
                flight::register_cleanup(*shuffle_id, shuffle_dirs, plan_context);
            }
        }
    }

    /// Build a `SwordfishTaskBuilder` whose plan reads from already-materialized
    /// partition refs (`in_memory_scan` for plain refs, `shuffle_read(Flight)` for
    /// flight refs) and then applies `wrap_plan` on top. The partition refs are
    /// attached to the task via the matching API (`with_psets` /
    /// `with_flight_shuffle_reads`).
    ///
    /// The read path is chosen from what the refs *are*, not from what the backend
    /// is configured to be. A flight-configured node can legitimately hold plain
    /// in-memory refs: only refs produced by a flight write (`gather_write`,
    /// `repartition_write`, a Flight-backed local `into_partitions`) are addressable
    /// over flight, and a node that materializes its child's output directly — as
    /// `IntoPartitionsNode`'s coalesce branch does — gets ordinary Ray object refs.
    /// Dispatching on the config instead used to hand those to the flight reader and
    /// panic on the downcast.
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
        let num_flight_refs = partition_refs
            .iter()
            .filter(|p| flight::is_flight_ref(p))
            .count();
        // Empty falls to the in-memory path: there is nothing to read back over
        // flight, and `in_memory_scan` with no psets yields an empty partition.
        if num_flight_refs == 0 {
            let total_size_bytes = partition_refs.iter().map(|p| p.size_bytes()).sum::<usize>();
            let in_memory_scan = LocalPhysicalPlan::in_memory_scan(
                node_id,
                self.schema.clone(),
                total_size_bytes,
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            );
            let plan = wrap_plan(in_memory_scan);
            return Ok(
                SwordfishTaskBuilder::new(plan, node, node_id).with_psets(node_id, partition_refs)
            );
        }
        if num_flight_refs != partition_refs.len() {
            return Err(DaftError::InternalError(format!(
                "Shuffle read for node {} got a mix of flight and in-memory partition refs \
                 ({} of {} are flight refs); a stage's outputs must be all one or the other.",
                node_id,
                num_flight_refs,
                partition_refs.len(),
            )));
        }

        let read_inputs = flight::read_inputs_from_refs(partition_refs)?;
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
