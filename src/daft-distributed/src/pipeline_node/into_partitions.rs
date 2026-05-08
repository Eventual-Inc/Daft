use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::ops::{NodeCategory, NodeType};
use common_runtime::OrderedJoinSet;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{partitioning::UnknownClusteringConfig, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::StreamExt;

use super::{PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, PipelineNodeConfig, PipelineNodeContext,
        shuffles::backends::{DistributedShuffleBackend, ShuffleBackend},
    },
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
    },
    utils::channel::{Sender, create_channel},
};

#[derive(Clone)]
pub(crate) struct IntoPartitionsNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    num_partitions: usize,
    shuffle_backend: ShuffleBackend,
    child: DistributedPipelineNode,
}

impl IntoPartitionsNode {
    const NODE_NAME: &'static str = "IntoPartitions";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        num_partitions: usize,
        schema: SchemaRef,
        backend: DistributedShuffleBackend,
        child: DistributedPipelineNode,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Arc::from(Self::NODE_NAME),
            NodeType::IntoPartitions,
            NodeCategory::BlockingSink,
        );
        let config = PipelineNodeConfig::new(
            schema.clone(),
            plan_config.config.clone(),
            Arc::new(UnknownClusteringConfig::new(num_partitions).into()),
        );
        let shuffle_backend = ShuffleBackend::new(&context, schema, backend);

        Self {
            config,
            context,
            num_partitions,
            shuffle_backend,
            child,
        }
    }

    async fn coalesce_tasks(
        self: Arc<Self>,
        builders: Vec<SwordfishTaskBuilder>,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
        task_id_counter: &TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        assert!(
            builders.len() >= self.num_partitions,
            "Cannot coalesce from {} to {} partitions.",
            builders.len(),
            self.num_partitions
        );

        // Coalesce partitions evenly with remainder handling
        // Example: 10 inputs, 3 partitions = 4, 3, 3

        // Base inputs per partition: 10 / 3 = 3 (all tasks get at least 3 inputs)
        let base_inputs_per_partition = builders.len() / self.num_partitions;
        // Remainder: 10 % 3 = 1 (one task gets an extra input)
        let num_partitions_with_extra_input = builders.len() % self.num_partitions;

        let mut tasks_per_partition = Vec::new();

        let mut builder_iter = builders.into_iter();
        for partition_idx in 0..self.num_partitions {
            let mut chunk_size = base_inputs_per_partition;
            // This partition needs an extra input, i.e. partition_idx == 0 and remainder == 1
            if partition_idx < num_partitions_with_extra_input {
                chunk_size += 1;
            }

            // Build and submit all the tasks for this partition
            let submitted_tasks = builder_iter
                .by_ref()
                .take(chunk_size)
                .map(|builder| {
                    let submittable_task = builder.build(self.context.query_idx, task_id_counter);
                    submittable_task.submit(scheduler_handle)
                })
                .collect::<DaftResult<Vec<_>>>()?;
            tasks_per_partition.push(submitted_tasks);
        }

        let mut output_futures = OrderedJoinSet::new();
        for tasks in tasks_per_partition {
            output_futures.spawn(async move {
                let materialized_output = futures::future::try_join_all(tasks)
                    .await?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();
                DaftResult::Ok(materialized_output)
            });
        }

        while let Some(result) = output_futures.join_next().await {
            // Collect all the outputs from this task and coalesce them into a single task.
            let materialized_outputs = result??;
            let partition_refs = materialized_outputs
                .into_iter()
                .flat_map(|output| output.into_inner().0)
                .collect::<Vec<_>>();

            let node_id = self.node_id();
            let shuffle_backend = self.shuffle_backend.local_shuffle_backend();
            let builder = self.shuffle_backend.build_refs_task_builder(
                partition_refs,
                self.as_ref(),
                move |input| {
                    LocalPhysicalPlan::into_partitions(
                        input,
                        1,
                        shuffle_backend,
                        StatsState::NotMaterialized,
                        LocalNodeContext::new(Some(node_id as usize)),
                    )
                },
            );
            if result_tx.send(builder).await.is_err() {
                break;
            }
        }

        Ok(())
    }

    async fn split_tasks(
        self: Arc<Self>,
        builders: Vec<SwordfishTaskBuilder>,
        scheduler_handle: &SchedulerHandle<SwordfishTask>,
        task_id_counter: &TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        assert!(
            builders.len() <= self.num_partitions,
            "Cannot split from {} to {} partitions.",
            builders.len(),
            self.num_partitions
        );
        // Split partitions evenly with remainder handling
        // Example: 3 inputs, 10 partitions = 4, 3, 3

        // Base outputs per partition: 10 / 3 = 3 (all partitions will split into at least 3 outputs)
        let base_splits_per_partition = self.num_partitions / builders.len();
        // Remainder: 10 % 3 = 1 (one partition will split into 4 outputs)
        let num_partitions_with_extra_output = self.num_partitions % builders.len();

        let mut submitted_tasks = Vec::new();

        for (input_partition_idx, builder) in builders.into_iter().enumerate() {
            let mut num_outputs = base_splits_per_partition;
            // This partition will split into one more output, i.e. input_partition_idx == 0 and remainder == 1
            if input_partition_idx < num_partitions_with_extra_output {
                num_outputs += 1;
            }
            let into_partitions_builder = builder.map_plan(self.as_ref(), |plan| {
                LocalPhysicalPlan::into_partitions(
                    plan,
                    num_outputs,
                    self.shuffle_backend.local_shuffle_backend(),
                    StatsState::NotMaterialized,
                    LocalNodeContext::new(Some(self.node_id() as usize)),
                )
            });
            // Build and submit
            let submittable_task =
                into_partitions_builder.build(self.context.query_idx, task_id_counter);
            let submitted_task = submittable_task.submit(scheduler_handle)?;
            submitted_tasks.push(submitted_task);
        }

        let mut output_futures = OrderedJoinSet::new();
        for task in submitted_tasks {
            output_futures.spawn(task);
        }

        // Collect all the outputs and emit a new pass-through task for each output ref.
        while let Some(result) = output_futures.join_next().await {
            let materialized_outputs = result??;
            if let Some(output) = materialized_outputs {
                for output in output.split_into_materialized_outputs() {
                    let partition_refs = output.into_inner().0;
                    let builder = self.shuffle_backend.build_refs_task_builder(
                        partition_refs,
                        self.as_ref(),
                        |input| input,
                    );
                    if result_tx.send(builder).await.is_err() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn execute_into_partitions(
        self: Arc<Self>,
        input_stream: TaskBuilderStream,
        task_id_counter: TaskIDCounter,
        result_tx: Sender<SwordfishTaskBuilder>,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> DaftResult<()> {
        // Collect all input builders without materializing to count them
        let input_builders: Vec<SwordfishTaskBuilder> = input_stream.collect().await;
        let num_input_tasks = input_builders.len();

        match num_input_tasks.cmp(&self.num_partitions) {
            std::cmp::Ordering::Equal => {
                if self
                    .config
                    .execution_config
                    .enable_scan_task_split_and_merge
                {
                    let node_id = self.node_id();
                    for builder in input_builders {
                        let builder = builder.map_plan(self.as_ref(), |plan| {
                            LocalPhysicalPlan::into_partitions(
                                plan,
                                1,
                                self.shuffle_backend.local_shuffle_backend(),
                                StatsState::NotMaterialized,
                                LocalNodeContext::new(Some(node_id as usize)),
                            )
                        });
                        let _ = result_tx.send(builder).await;
                    }
                } else {
                    for builder in input_builders {
                        let _ = result_tx.send(builder).await;
                    }
                }
            }
            std::cmp::Ordering::Greater => {
                // Too many tasks - coalesce
                self.coalesce_tasks(
                    input_builders,
                    &scheduler_handle,
                    &task_id_counter,
                    result_tx,
                )
                .await?;
            }
            std::cmp::Ordering::Less => {
                // Too few tasks - split
                self.split_tasks(
                    input_builders,
                    &scheduler_handle,
                    &task_id_counter,
                    result_tx,
                )
                .await?;
            }
        }
        Ok(())
    }
}

impl PipelineNodeImpl for IntoPartitionsNode {
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
        let backend_name = match self.shuffle_backend.backend() {
            DistributedShuffleBackend::Ray => "Ray",
            DistributedShuffleBackend::Flight(_) => "Flight",
        };
        vec![
            format!("IntoPartitions({})", backend_name),
            format!("Num partitions = {}", self.num_partitions),
        ]
    }

    fn produce_tasks(
        self: Arc<Self>,
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        self.shuffle_backend.register_cleanup(plan_context);
        let input_stream = self.child.clone().produce_tasks(plan_context);
        let (result_tx, result_rx) = create_channel(1);

        plan_context.spawn(self.execute_into_partitions(
            input_stream,
            plan_context.task_id_counter(),
            result_tx,
            plan_context.scheduler_handle(),
        ));

        TaskBuilderStream::from(result_rx)
    }
}
