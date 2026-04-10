use std::sync::Arc;

use common_error::DaftResult;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, RepartitionWriteBackend};
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{NodeID, PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream},
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

mod flight;
mod ray;

pub(crate) use flight::FlightShuffleBackendConfig;
use flight::FlightShuffleReadSpec;

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    ((context.query_idx as u64) << 32) | (context.node_id as u64)
}

#[derive(Clone)]
pub(crate) enum DistributedShuffleBackend {
    Ray,
    Flight(FlightShuffleBackendConfig),
}

pub(crate) struct ShuffleBackendWriteConfig {
    pub(crate) input_node: TaskBuilderStream,
    pub(crate) producer: Arc<dyn PipelineNodeImpl>,
    pub(crate) backend: RepartitionWriteBackend,
    pub(crate) repartition_spec: RepartitionSpec,
}

pub(crate) struct ShuffleBackend {
    backend: DistributedShuffleBackend,
    schema: SchemaRef,
    num_partitions: usize,
    node_id: NodeID,
}

impl ShuffleBackend {
    pub(crate) fn new(
        context: &PipelineNodeContext,
        schema: SchemaRef,
        num_partitions: usize,
        backend: DistributedShuffleBackend,
    ) -> Self {
        Self {
            schema,
            num_partitions,
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

    pub(crate) fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    pub(crate) fn register_cleanup(&self, plan_context: &mut PlanExecutionContext) {
        match &self.backend {
            DistributedShuffleBackend::Ray => {}
            DistributedShuffleBackend::Flight(backend) => {
                flight::register_cleanup(backend, plan_context);
            }
        }
    }

    pub(crate) fn build_write_stage(&self, config: ShuffleBackendWriteConfig) -> TaskBuilderStream {
        let num_partitions = self.num_partitions;
        let schema = self.schema.clone();
        let node_id = self.node_id;
        config
            .input_node
            .pipeline_instruction(config.producer, move |input| {
                LocalPhysicalPlan::repartition_write(
                    input,
                    num_partitions,
                    schema.clone(),
                    config.backend.clone(),
                    config.repartition_spec.clone(),
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(node_id as usize)),
                )
            })
    }

    pub(crate) async fn emit_read_tasks(
        &self,
        read_spec: ShuffleBackendReadSpec,
        node: &dyn PipelineNodeImpl,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        match (&self.backend, read_spec) {
            (DistributedShuffleBackend::Ray, ShuffleBackendReadSpec::Ray { partition_groups }) => {
                ray::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    partition_groups,
                    node,
                    result_tx,
                )
                .await
            }
            (
                DistributedShuffleBackend::Flight(_),
                ShuffleBackendReadSpec::Flight { partition_groups },
            ) => {
                let read_spec: FlightShuffleReadSpec =
                    flight::read_spec_from_partition_groups(partition_groups);
                flight::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    read_spec,
                    node,
                    result_tx,
                )
                .await
            }
            (DistributedShuffleBackend::Ray, ShuffleBackendReadSpec::Flight { .. }) => {
                Err(common_error::DaftError::InternalError(
                    "Expected Ray shuffle read spec".to_string(),
                ))
            }
            (DistributedShuffleBackend::Flight(_), ShuffleBackendReadSpec::Ray { .. }) => {
                Err(common_error::DaftError::InternalError(
                    "Expected Flight shuffle read spec".to_string(),
                ))
            }
        }
    }
}

pub(crate) enum ShuffleBackendReadSpec {
    Ray {
        partition_groups: Vec<Vec<common_partitioning::PartitionRef>>,
    },
    Flight {
        partition_groups: Vec<Vec<daft_local_plan::FlightShufflePartitionRef>>,
    },
}
