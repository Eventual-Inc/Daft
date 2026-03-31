use std::sync::Arc;

use common_error::DaftResult;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleWriteBackend};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{NodeID, PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream, TaskOutput},
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

mod flight;
mod ray;

pub(crate) use flight::FlightDistributedExchangeConfig;
use flight::FlightReadSpec;
pub(crate) use ray::ray_partition_groups_from_outputs;

fn make_exchange_id(context: &PipelineNodeContext) -> u64 {
    ((context.query_idx as u64) << 32) | (context.node_id as u64)
}

#[derive(Clone)]
pub(crate) enum DistributedExchangeBackend {
    Ray,
    Flight(FlightDistributedExchangeConfig),
}

pub(crate) struct ExchangeWriteConfig {
    pub(crate) input_node: TaskBuilderStream,
    pub(crate) producer: Arc<dyn PipelineNodeImpl>,
    pub(crate) backend: ShuffleWriteBackend,
}

pub(crate) struct ExchangeBackend {
    backend: DistributedExchangeBackend,
    schema: SchemaRef,
    num_partitions: usize,
    node_id: NodeID,
}

impl ExchangeBackend {
    pub(crate) fn new(
        context: &PipelineNodeContext,
        schema: SchemaRef,
        num_partitions: usize,
        backend: DistributedExchangeBackend,
    ) -> Self {
        Self {
            schema,
            num_partitions,
            node_id: context.node_id,
            backend: match backend {
                DistributedExchangeBackend::Ray => DistributedExchangeBackend::Ray,
                DistributedExchangeBackend::Flight(backend) => {
                    DistributedExchangeBackend::Flight(FlightDistributedExchangeConfig {
                        exchange_id: make_exchange_id(context),
                        shuffle_dirs: backend.shuffle_dirs,
                        compression: backend.compression,
                    })
                }
            },
        }
    }

    pub(crate) fn backend(&self) -> &DistributedExchangeBackend {
        &self.backend
    }

    pub(crate) fn register_cleanup(&self, plan_context: &mut PlanExecutionContext) {
        match &self.backend {
            DistributedExchangeBackend::Ray => {}
            DistributedExchangeBackend::Flight(backend) => {
                flight::register_cleanup(backend, plan_context);
            }
        }
    }

    pub(crate) fn build_write_stage(&self, config: ExchangeWriteConfig) -> TaskBuilderStream {
        let num_partitions = self.num_partitions;
        let schema = self.schema.clone();
        let node_id = self.node_id;
        config
            .input_node
            .pipeline_instruction(config.producer, move |input| {
                LocalPhysicalPlan::shuffle_write(
                    input,
                    num_partitions,
                    schema.clone(),
                    config.backend.clone(),
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(node_id as usize)),
                )
            })
    }

    pub(crate) async fn emit_read_tasks(
        &self,
        outputs: Vec<TaskOutput>,
        node: &dyn PipelineNodeImpl,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        match &self.backend {
            DistributedExchangeBackend::Ray => {
                let partition_groups =
                    ray::ray_partition_groups_from_outputs(outputs, self.num_partitions)?;
                ray::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    partition_groups,
                    node,
                    result_tx,
                )
                .await
            }
            DistributedExchangeBackend::Flight(backend) => {
                let read_spec: FlightReadSpec = flight::read_spec_from_outputs(backend, outputs)?;
                flight::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    self.num_partitions,
                    read_spec,
                    node,
                    result_tx,
                )
                .await
            }
        }
    }
}
