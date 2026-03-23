use std::{collections::HashSet, sync::Arc};

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, ShuffleReadBackend, ShuffleWriteBackend};
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, PipelineNodeConfig, PipelineNodeContext, PipelineNodeImpl,
        ShufflePartitionRef, TaskBuilderStream, TaskOutput,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::channel::{Sender, create_channel},
};

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    ((context.query_idx as u64) << 32) | (context.node_id as u64)
}

pub(crate) struct FlightShuffleNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    shuffle_id: u64,
    repartition_spec: RepartitionSpec,
    num_partitions: usize,
    shuffle_dirs: Vec<String>,
    compression: Option<String>,
    child: DistributedPipelineNode,
}

impl FlightShuffleNode {
    const NODE_NAME: &'static str = "FlightShuffle";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        repartition_spec: RepartitionSpec,
        schema: SchemaRef,
        num_partitions: usize,
        shuffle_dirs: Vec<String>,
        compression: Option<String>,
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
        let shuffle_id = make_shuffle_id(&context);
        let config = PipelineNodeConfig::new(
            schema,
            plan_config.config.clone(),
            repartition_spec
                .to_clustering_spec(child.config().clustering_spec.num_partitions())
                .into(),
        );

        Self {
            config,
            context,
            shuffle_id,
            repartition_spec,
            num_partitions,
            shuffle_dirs,
            compression,
            child,
        }
    }

    // Async execution to handle flight shuffle write and read operations
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

        let mut server_cache_mapping: std::collections::HashMap<String, HashSet<u32>> =
            std::collections::HashMap::new();
        for output in outputs {
            let TaskOutput::ShuffleWrite(output) = output else {
                return Err(common_error::DaftError::InternalError(
                    "Expected shuffle write task output for Flight shuffle write stage".to_string(),
                ));
            };
            for partition in output.partitions {
                match partition {
                    ShufflePartitionRef::Flight(partition) => {
                        server_cache_mapping
                            .entry(partition.server_address)
                            .or_default()
                            .insert(partition.cache_id);
                    }
                }
            }
        }

        for partition_idx in 0..self.num_partitions {
            let shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
                partition_idx,
                self.config.schema.clone(),
                ShuffleReadBackend::Flight {
                    shuffle_id: self.shuffle_id,
                    server_cache_mapping: server_cache_mapping
                        .iter()
                        .map(|(server, cache_ids)| {
                            (server.clone(), cache_ids.iter().copied().collect())
                        })
                        .collect(),
                },
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self.context.node_id as usize)),
            );

            let task = SwordfishTaskBuilder::new(shuffle_read_plan, self.as_ref(), self.node_id());

            let _ = result_tx.send(task).await;
        }

        Ok(())
    }
}

impl PipelineNodeImpl for FlightShuffleNode {
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

        // Register shuffle directories for cleanup when the plan completes
        let shuffle_dirs_to_register: Vec<String> = self
            .shuffle_dirs
            .iter()
            .map(|base_dir| format!("{}/daft_shuffle/{}", base_dir, self.shuffle_id))
            .collect();
        plan_context.register_shuffle_dirs(shuffle_dirs_to_register);

        let partition_by = match &self.repartition_spec {
            RepartitionSpec::Hash(hash_spec) => Some(hash_spec.by.clone()),
            RepartitionSpec::Random(_) => None,
            RepartitionSpec::Range(_) => {
                unreachable!("Range repartition is not supported for flight shuffle")
            }
        };
        let local_shuffle_write_node =
            input_node.pipeline_instruction(self.clone(), move |input| {
                LocalPhysicalPlan::shuffle_write(
                    input,
                    partition_by.clone(),
                    self.num_partitions,
                    self.config.schema.clone(),
                    ShuffleWriteBackend::Flight {
                        shuffle_id: self.shuffle_id,
                        shuffle_dirs: self.shuffle_dirs.clone(),
                        compression: self.compression.clone(),
                    },
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(self.context.node_id as usize)),
                )
            });

        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = plan_context.task_id_counter();
        let scheduler_handle = plan_context.scheduler_handle();

        let flight_shuffle_execution = async move {
            self_arc
                .execution_loop(
                    local_shuffle_write_node,
                    task_id_counter,
                    result_tx,
                    scheduler_handle,
                )
                .await
        };

        plan_context.spawn(flight_shuffle_execution);
        TaskBuilderStream::from(result_rx)
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec![format!(
            "FlightShuffle: {}",
            self.repartition_spec.var_name()
        )];
        res.extend(self.repartition_spec.multiline_display());
        res
    }
}
