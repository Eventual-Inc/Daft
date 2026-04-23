use std::sync::Arc;

use common_error::DaftResult;
use daft_local_plan::{
    GatherWriteBackend, LocalNodeContext, LocalPhysicalPlan, RepartitionWriteBackend,
};
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{
        MaterializedOutput, NodeID, PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
    },
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

mod flight;
mod ray;

pub(crate) use flight::FlightShuffleBackendConfig;

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    ((context.query_idx as u64) << 32) | (context.node_id as u64)
}

#[derive(Clone)]
pub(crate) enum DistributedShuffleBackend {
    Ray,
    Flight(FlightShuffleBackendConfig),
}

pub(crate) struct ShuffleBackend {
    backend: DistributedShuffleBackend,
    schema: SchemaRef,
    node_id: NodeID,
}

impl ShuffleBackend {
    pub(crate) fn new(
        context: &PipelineNodeContext,
        schema: SchemaRef,
        backend: DistributedShuffleBackend,
    ) -> Self {
        Self {
            schema,
            node_id: context.node_id,
            backend: match backend {
                DistributedShuffleBackend::Ray => DistributedShuffleBackend::Ray,
                DistributedShuffleBackend::Flight(backend) => {
                    DistributedShuffleBackend::Flight(FlightShuffleBackendConfig {
                        shuffle_id: make_shuffle_id(context),
                        shuffle_dirs: backend.shuffle_dirs,
                        compression: backend.compression,
                    })
                }
            },
        }
    }

    pub(crate) fn backend(&self) -> &DistributedShuffleBackend {
        &self.backend
    }

    pub(crate) fn register_cleanup(&self, plan_context: &mut PlanExecutionContext) {
        match &self.backend {
            DistributedShuffleBackend::Ray => {}
            DistributedShuffleBackend::Flight(backend) => {
                flight::register_cleanup(backend, plan_context);
            }
        }
    }

    pub(crate) fn build_repartition_write_stage(
        &self,
        producer: Arc<dyn PipelineNodeImpl>,
        input_node: TaskBuilderStream,
        num_partitions: usize,
        repartition_spec: RepartitionSpec,
    ) -> TaskBuilderStream {
        let schema = self.schema.clone();
        let node_id = self.node_id;
        let repartition_backend = match self.backend.clone() {
            DistributedShuffleBackend::Ray => RepartitionWriteBackend::Ray,
            DistributedShuffleBackend::Flight(backend) => RepartitionWriteBackend::Flight {
                shuffle_id: backend.shuffle_id,
                shuffle_dirs: backend.shuffle_dirs.clone(),
                compression: backend.compression,
            },
        };
        input_node.pipeline_instruction(producer, move |input| {
            LocalPhysicalPlan::repartition_write(
                input,
                num_partitions,
                schema.clone(),
                repartition_backend.clone(),
                repartition_spec.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            )
        })
    }

    pub(crate) fn build_gather_write_stage(
        &self,
        producer: Arc<dyn PipelineNodeImpl>,
        input_node: TaskBuilderStream,
    ) -> TaskBuilderStream {
        let schema = self.schema.clone();
        let node_id = self.node_id;
        let gather_backend = match self.backend.clone() {
            DistributedShuffleBackend::Ray => GatherWriteBackend::Ray,
            DistributedShuffleBackend::Flight(backend) => GatherWriteBackend::Flight {
                shuffle_id: backend.shuffle_id,
                shuffle_dirs: backend.shuffle_dirs.clone(),
                compression: backend.compression,
            },
        };
        input_node.pipeline_instruction(producer, move |input| {
            LocalPhysicalPlan::gather_write(
                input,
                schema.clone(),
                gather_backend.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            )
        })
    }

    pub(crate) async fn emit_read_tasks(
        &self,
        partition_groups: Vec<Vec<MaterializedOutput>>,
        node: &dyn PipelineNodeImpl,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        match &self.backend {
            DistributedShuffleBackend::Ray => {
                ray::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    partition_groups,
                    node,
                    result_tx,
                )
                .await
            }
            DistributedShuffleBackend::Flight(_) => {
                flight::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    partition_groups,
                    node,
                    result_tx,
                )
                .await
            }
        }
    }
}
