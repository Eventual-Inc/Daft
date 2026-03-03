use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::RepartitionSpec, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::TryStreamExt;

use crate::{
    pipeline_node::{
        DistributedPipelineNode, MaterializedOutput, NodeID, NodeName, PipelineNodeConfig,
        PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream,
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SchedulingStrategy, SwordfishTask, SwordfishTaskBuilder, TaskID},
        worker::WorkerId,
    },
    utils::channel::{Receiver, Sender, create_channel},
};

fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    ((context.query_idx as u64) << 32) | (context.node_id as u64)
}

pub(crate) struct PreShuffleMergeFlightNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    pre_shuffle_merge_threshold: usize,
    shuffle_id: u64,
    _repartition_spec: RepartitionSpec,
    num_partitions: usize,
    shuffle_dirs: Vec<String>,
    compression: Option<String>,
    child: DistributedPipelineNode,
}

impl PreShuffleMergeFlightNode {
    const NODE_NAME: NodeName = "PreShuffleMergeFlight";

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        pre_shuffle_merge_threshold: usize,
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
            Self::NODE_NAME,
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
            pre_shuffle_merge_threshold,
            shuffle_id,
            _repartition_spec: repartition_spec,
            num_partitions,
            shuffle_dirs,
            compression,
            child,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }
}

impl PipelineNodeImpl for PreShuffleMergeFlightNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![self.child.clone()]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        vec![
            format!("Pre-Shuffle Merge Flight"),
            format!("Threshold: {}", self.pre_shuffle_merge_threshold),
            format!("Partitions: {}", self.num_partitions),
        ]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let input_node = self.child.clone().produce_tasks(plan_context);

        let shuffle_dirs_to_register: Vec<String> = self
            .shuffle_dirs
            .iter()
            .map(|base_dir| format!("{}/daft_shuffle/{}", base_dir, self.shuffle_id))
            .collect();
        plan_context.register_shuffle_dirs(shuffle_dirs_to_register);

        let (write_tx, write_rx) = create_channel(256);
        let (result_tx, result_rx) = create_channel(1);

        let task_id_counter = plan_context.task_id_counter();
        let scheduler_handle = plan_context.scheduler_handle();

        let task_id_counter_read = task_id_counter.clone();
        let scheduler_handle_read = scheduler_handle.clone();

        let self_merge = self.clone();
        let merge_execution = async move {
            self_merge
                .execute_merge(
                    input_node,
                    task_id_counter,
                    write_tx,
                    scheduler_handle,
                )
                .await
        };

        let self_read = self.clone();
        let read_phase_execution = async move {
            self_read
                .execute_read_phase(
                    write_rx,
                    task_id_counter_read,
                    result_tx,
                    scheduler_handle_read,
                )
                .await
        };

        plan_context.spawn(merge_execution);
        plan_context.spawn(read_phase_execution);
        TaskBuilderStream::from(result_rx)
    }
}

impl PreShuffleMergeFlightNode {
    async fn execute_merge(
        self: Arc<Self>,
        input_stream: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        write_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let mut materialized_stream =
            input_stream.materialize(scheduler_handle, self.context.query_idx, task_id_counter);

        let mut worker_buckets: HashMap<WorkerId, Vec<MaterializedOutput>> = HashMap::new();

        while let Some(output) = materialized_stream.try_next().await? {
            let worker_id = output.worker_id().clone();
            let bucket = worker_buckets.entry(worker_id.clone()).or_default();
            bucket.push(output);

            if bucket
                .iter()
                .map(|output| output.size_bytes())
                .sum::<usize>()
                >= self.pre_shuffle_merge_threshold
            {
                if let Some(materialized_outputs) = worker_buckets.remove(&worker_id) {
                    let (in_memory_scan, psets) =
                        MaterializedOutput::into_in_memory_scan_with_psets(
                            materialized_outputs,
                            self.config.schema.clone(),
                            self.node_id(),
                        );
                    let plan = LocalPhysicalPlan::flight_gather_write(
                        in_memory_scan,
                        self.config.schema.clone(),
                        self.shuffle_id,
                        self.shuffle_dirs.clone(),
                        self.compression.clone(),
                        StatsState::NotMaterialized,
                        LocalNodeContext::new(Some(self.context.node_id as usize)),
                    );
                    let builder = SwordfishTaskBuilder::new(plan, self.as_ref())
                        .with_psets(self.node_id(), psets)
                        .with_strategy(Some(SchedulingStrategy::WorkerAffinity {
                            worker_id,
                            soft: false,
                        }));

                    if write_tx.send(builder).await.is_err() {
                        break;
                    }
                }
            }
        }

        for (worker_id, materialized_outputs) in worker_buckets {
            if !materialized_outputs.is_empty() {
                let (in_memory_scan, psets) = MaterializedOutput::into_in_memory_scan_with_psets(
                    materialized_outputs,
                    self.config.schema.clone(),
                    self.node_id(),
                );
                let plan = LocalPhysicalPlan::flight_gather_write(
                    in_memory_scan,
                    self.config.schema.clone(),
                    self.shuffle_id,
                    self.shuffle_dirs.clone(),
                    self.compression.clone(),
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(self.context.node_id as usize)),
                );
                let builder = SwordfishTaskBuilder::new(plan, self.as_ref())
                    .with_psets(self.node_id(), psets)
                    .with_strategy(Some(SchedulingStrategy::WorkerAffinity {
                        worker_id,
                        soft: false,
                    }));

                if write_tx.send(builder).await.is_err() {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn execute_read_phase(
        self: Arc<Self>,
        write_rx: Receiver<SwordfishTaskBuilder>,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        let write_stream = TaskBuilderStream::from(write_rx);
        let outputs = write_stream
            .materialize(scheduler_handle, self.context.query_idx, task_id_counter)
            .try_collect::<Vec<_>>()
            .await?;

        let mut server_cache_mapping: HashMap<String, Vec<TaskID>> = HashMap::new();
        for output in &outputs {
            let server_address = output.ip_address().clone();
            let task_id = output.task_id();
            server_cache_mapping
                .entry(server_address)
                .or_default()
                .push(task_id);
        }

        for partition_idx in 0..self.num_partitions {
            let flight_shuffle_read_plan = LocalPhysicalPlan::flight_shuffle_read_with_cache_ids(
                self.shuffle_id,
                partition_idx,
                server_cache_mapping.clone(),
                self.config.schema.clone(),
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(self.context.node_id as usize)),
            );
            let task = SwordfishTaskBuilder::new(flight_shuffle_read_plan, self.as_ref());
            if result_tx.send(task).await.is_err() {
                break;
            }
        }

        Ok(())
    }
}
