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

pub(crate) use flight::FlightDistributedExchangeConfig;
use flight::FlightReadSpec;

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
    pub(crate) backend: RepartitionWriteBackend,
    pub(crate) repartition_spec: RepartitionSpec,
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

    pub(crate) fn num_partitions(&self) -> usize {
        self.num_partitions
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
        read_spec: ExchangeReadSpec,
        node: &dyn PipelineNodeImpl,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        match (&self.backend, read_spec) {
            (DistributedExchangeBackend::Ray, ExchangeReadSpec::Ray { partition_groups }) => {
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
                DistributedExchangeBackend::Flight(backend),
                ExchangeReadSpec::Flight {
                    server_cache_mapping,
                },
            ) => {
                let read_spec: FlightReadSpec =
                    flight::read_spec_from_server_cache_mapping(backend, server_cache_mapping);
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
            (DistributedExchangeBackend::Ray, ExchangeReadSpec::Flight { .. }) => {
                Err(common_error::DaftError::InternalError(
                    "Expected Ray exchange read spec".to_string(),
                ))
            }
            (DistributedExchangeBackend::Flight(_), ExchangeReadSpec::Ray { .. }) => {
                Err(common_error::DaftError::InternalError(
                    "Expected Flight exchange read spec".to_string(),
                ))
            }
        }
    }
}

pub(crate) enum ExchangeReadSpec {
    Ray {
        partition_groups: Vec<Vec<common_partitioning::PartitionRef>>,
    },
    Flight {
        server_cache_mapping: std::collections::HashMap<String, Vec<u32>>,
    },
}
