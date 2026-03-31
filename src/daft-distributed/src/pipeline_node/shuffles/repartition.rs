use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_local_plan::ShuffleWriteBackend;
use daft_logical_plan::partitioning::RepartitionSpec;
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl,
        TaskBuilderStream,
        exchanges::{DistributedExchangeBackend, ExchangeBackend, ExchangeWriteConfig},
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::channel::{Sender, create_channel},
};

pub(crate) struct RepartitionNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    repartition_spec: RepartitionSpec,
    exchange_backend: ExchangeBackend,
    child: DistributedPipelineNode,
}

impl RepartitionNode {
    const NODE_NAME: &'static str = "Repartition";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        num_partitions: usize,
        backend: DistributedExchangeBackend,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::Repartition,
            NodeCategory::BlockingSink,
        );
        let config = PipelineNodeConfig::new(
            schema.clone(),
            plan_config.config.clone(),
            repartition_spec
                .to_clustering_spec(child.config().clustering_spec.num_partitions())
                .into(),
        );

        Self {
            config,
            context: context.clone(),
            repartition_spec,
            exchange_backend: ExchangeBackend::new(&context, schema, num_partitions, backend),
            child,
        }
    }

    async fn execution_loop(
        self: Arc<Self>,
        local_shuffle_write_node: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let outputs = local_shuffle_write_node
            .task_outputs(
                scheduler_handle.clone(),
                self.context.query_idx,
                task_id_counter,
            )
            .try_collect::<Vec<_>>()
            .await?;

        self.exchange_backend
            .emit_read_tasks(outputs, self.as_ref(), result_tx)
            .await
    }
}

impl PipelineNodeImpl for RepartitionNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);
        let self_arc = self.clone();
        self.exchange_backend.register_cleanup(plan_context);
        let local_shuffle_write_node =
            self.exchange_backend
                .build_write_stage(ExchangeWriteConfig {
                    input_node,
                    producer: self.clone(),
                    backend: match self.exchange_backend.backend() {
                        DistributedExchangeBackend::Ray => ShuffleWriteBackend::Ray {
                            repartition_spec: self.repartition_spec.clone(),
                        },
                        DistributedExchangeBackend::Flight(backend) => {
                            ShuffleWriteBackend::Flight {
                                shuffle_id: backend.exchange_id,
                                shuffle_dirs: backend.shuffle_dirs.clone(),
                                compression: backend.compression.clone(),
                                repartition_spec: self.repartition_spec.clone(),
                            }
                        }
                    },
                });

        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = plan_context.task_id_counter();
        let scheduler_handle = plan_context.scheduler_handle();

        let execution = async move {
            self_arc
                .execution_loop(
                    local_shuffle_write_node,
                    task_id_counter,
                    result_tx,
                    scheduler_handle,
                )
                .await
        };

        plan_context.spawn(execution);
        TaskBuilderStream::from(result_rx)
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let backend_name = match self.exchange_backend.backend() {
            DistributedExchangeBackend::Ray => "RayShuffle",
            DistributedExchangeBackend::Flight(_) => "FlightShuffle",
        };
        let mut res = vec![format!(
            "{backend_name}: {}",
            self.repartition_spec.var_name()
        )];
        res.extend(self.repartition_spec.multiline_display());
        res
    }
}
