use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{
    ascii::fmt_tree_gitstyle,
    mermaid::{MermaidDisplayVisitor, SubgraphOptions},
    tree::TreeDisplay,
    DisplayLevel,
};
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{partitioning::ClusteringSpecRef, stats::StatsState, InMemoryInfo};
use daft_schema::schema::SchemaRef;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use materialize::{materialize_all_pipeline_outputs, materialize_running_pipeline_outputs};

use crate::{
    plan::PlanID,
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask, SubmittedTask},
        task::{SchedulingStrategy, SwordfishTask, Task, TaskContext},
        worker::WorkerId,
    },
    stage::{StageConfig, StageExecutionContext, StageID},
    utils::channel::{create_channel, Receiver, ReceiverStream},
};

#[cfg(feature = "python")]
mod actor_udf;
mod aggregate;
mod concat;
mod distinct;
mod explode;
mod filter;
mod gather;
mod hash_join;
mod in_memory_source;
mod limit;
pub(crate) mod materialize;
mod monotonically_increasing_id;
mod project;
mod repartition;
mod sample;
mod scan_source;
mod sink;
mod sort;
mod top_n;
mod translate;
mod unpivot;
mod window;

pub(crate) use translate::logical_plan_to_pipeline_node;
pub(crate) type NodeID = u32;
pub(crate) type NodeName = &'static str;

/// The materialized output of a completed pipeline node.
/// Contains both the partition data as well as metadata about the partition.
/// Right now, the only metadata is the worker id that has it so we can try
/// to schedule follow-up pipeline nodes on the same worker.
#[derive(Clone, Debug)]
pub(crate) struct MaterializedOutput {
    partition: Vec<PartitionRef>,
    worker_id: WorkerId,
}

impl MaterializedOutput {
    pub fn new(partition: Vec<PartitionRef>, worker_id: WorkerId) -> Self {
        Self {
            partition,
            worker_id,
        }
    }

    pub fn partitions(&self) -> &[PartitionRef] {
        &self.partition
    }

    #[allow(dead_code)]
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn into_inner(self) -> (Vec<PartitionRef>, WorkerId) {
        (self.partition, self.worker_id)
    }

    pub fn split_into_materialized_outputs(&self) -> Vec<Self> {
        self.partition
            .iter()
            .map(|partition| Self::new(vec![partition.clone()], self.worker_id.clone()))
            .collect()
    }

    pub fn num_rows(&self) -> DaftResult<usize> {
        self.partition
            .iter()
            .map(|partition| partition.num_rows())
            .sum()
    }

    pub fn size_bytes(&self) -> DaftResult<usize> {
        self.partition
            .iter()
            .map(|partition| partition.size_bytes().map(|size| size.unwrap_or(0)))
            .sum()
    }
}

#[derive(Debug)]
pub(crate) enum PipelineOutput<T: Task> {
    Materialized(MaterializedOutput),
    Task(SubmittableTask<T>),
    #[allow(dead_code)]
    Running(SubmittedTask),
}

pub(super) struct PipelineNodeConfig {
    pub schema: SchemaRef,
    pub execution_config: Arc<DaftExecutionConfig>,
    pub clustering_spec: ClusteringSpecRef,
}

impl PipelineNodeConfig {
    pub fn new(
        schema: SchemaRef,
        execution_config: Arc<DaftExecutionConfig>,
        clustering_spec: ClusteringSpecRef,
    ) -> Self {
        Self {
            schema,
            execution_config,
            clustering_spec,
        }
    }
}

pub(super) struct PipelineNodeContext {
    pub plan_id: PlanID,
    pub stage_id: StageID,
    pub node_id: NodeID,
    pub node_name: NodeName,
    pub child_ids: Vec<NodeID>,
    pub child_names: Vec<NodeName>,
    pub logical_node_id: Option<NodeID>,
}

impl PipelineNodeContext {
    pub fn new(
        stage_config: &StageConfig,
        node_id: NodeID,
        node_name: NodeName,
        child_ids: Vec<NodeID>,
        child_names: Vec<NodeName>,
        logical_node_id: Option<NodeID>,
    ) -> Self {
        Self {
            plan_id: stage_config.plan_id,
            stage_id: stage_config.stage_id,
            node_id,
            node_name,
            child_ids,
            child_names,
            logical_node_id,
        }
    }

    pub fn to_hashmap(&self) -> HashMap<String, String> {
        HashMap::from([
            ("plan_id".to_string(), self.plan_id.to_string()),
            ("stage_id".to_string(), self.stage_id.to_string()),
            ("node_id".to_string(), self.node_id.to_string()),
            ("node_name".to_string(), self.node_name.to_string()),
            ("child_ids".to_string(), self.child_ids.iter().join(",")),
            ("child_names".to_string(), self.child_names.iter().join(",")),
            (
                "logical_node_id".to_string(),
                self.logical_node_id.unwrap_or(0).to_string(),
            ),
        ])
    }
}

pub(crate) trait DistributedPipelineNode: Send + Sync + TreeDisplay {
    fn context(&self) -> &PipelineNodeContext;
    fn config(&self) -> &PipelineNodeConfig;
    #[allow(dead_code)]
    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>>;
    fn start(self: Arc<Self>, stage_context: &mut StageExecutionContext) -> RunningPipelineNode;
    fn as_tree_display(&self) -> &dyn TreeDisplay;
    fn name(&self) -> NodeName {
        self.context().node_name
    }
    #[allow(dead_code)]
    fn plan_id(&self) -> PlanID {
        self.context().plan_id
    }
    #[allow(dead_code)]
    fn stage_id(&self) -> StageID {
        self.context().stage_id
    }
    fn node_id(&self) -> NodeID {
        self.context().node_id
    }
}

/// Visualize a distributed pipeline as Mermaid markdown
pub fn viz_distributed_pipeline_mermaid(
    root: &dyn DistributedPipelineNode,
    display_type: DisplayLevel,
    bottom_up: bool,
    subgraph_options: Option<SubgraphOptions>,
) -> String {
    let mut output = String::new();
    let mut visitor =
        MermaidDisplayVisitor::new(&mut output, display_type, bottom_up, subgraph_options);
    visitor.fmt(root.as_tree_display()).unwrap();
    output
}

/// Visualize a distributed pipeline as ASCII text
pub fn viz_distributed_pipeline_ascii(root: &dyn DistributedPipelineNode, simple: bool) -> String {
    let mut s = String::new();
    let level = if simple {
        DisplayLevel::Compact
    } else {
        DisplayLevel::Default
    };
    fmt_tree_gitstyle(root.as_tree_display(), 0, &mut s, level).unwrap();
    s
}

#[derive(Debug)]
pub(crate) struct RunningPipelineNode {
    result_receiver: Receiver<PipelineOutput<SwordfishTask>>,
}

impl RunningPipelineNode {
    fn new(result_receiver: Receiver<PipelineOutput<SwordfishTask>>) -> Self {
        Self { result_receiver }
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Receiver<PipelineOutput<SwordfishTask>> {
        self.result_receiver
    }

    pub fn materialize(
        self,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin + 'static {
        let stream = self.into_stream().map(Ok);
        materialize_all_pipeline_outputs(stream, scheduler_handle)
    }

    pub fn materialize_running(
        self,
    ) -> impl Stream<Item = DaftResult<PipelineOutput<SwordfishTask>>> + Send + Unpin + 'static
    {
        let stream = self.into_stream().map(Ok);
        materialize_running_pipeline_outputs(stream)
    }

    pub fn into_stream(
        self,
    ) -> impl Stream<Item = PipelineOutput<SwordfishTask>> + Send + Unpin + 'static {
        ReceiverStream::new(self.result_receiver)
    }

    pub fn pipeline_instruction<F>(
        self,
        stage_context: &mut StageExecutionContext,
        node: Arc<dyn DistributedPipelineNode>,
        plan_builder: F,
    ) -> Self
    where
        F: Fn(LocalPhysicalPlanRef) -> DaftResult<LocalPhysicalPlanRef> + Send + Sync + 'static,
    {
        let (result_tx, result_rx) = create_channel(1);
        let task_id_counter = stage_context.task_id_counter();
        let execution_loop = async move {
            let mut task_or_partition_ref_stream = self.materialize_running();

            while let Some(pipeline_result) = task_or_partition_ref_stream.next().await {
                let pipeline_output = pipeline_result?;
                match pipeline_output {
                    PipelineOutput::Running(_) => {
                        unreachable!("All running tasks should be materialized before this point")
                    }
                    PipelineOutput::Materialized(materialized_output) => {
                        // make new task for this partition ref
                        let task = make_new_task_from_materialized_outputs(
                            TaskContext::from((node.context(), task_id_counter.next())),
                            vec![materialized_output],
                            &node,
                            &plan_builder,
                        )?;
                        if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                            break;
                        }
                    }
                    PipelineOutput::Task(task) => {
                        // append plan to this task
                        let task = append_plan_to_existing_task(
                            TaskContext::from((node.context(), task_id_counter.next())),
                            task,
                            &node,
                            &plan_builder,
                        )?;
                        if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                            break;
                        }
                    }
                }
            }
            Ok::<(), common_error::DaftError>(())
        };

        stage_context.spawn(execution_loop);
        Self::new(result_rx)
    }
}

fn make_new_task_from_materialized_outputs<F>(
    task_context: TaskContext,
    materialized_outputs: Vec<MaterializedOutput>,
    node: &Arc<dyn DistributedPipelineNode>,
    plan_builder: &F,
) -> DaftResult<SubmittableTask<SwordfishTask>>
where
    F: Fn(LocalPhysicalPlanRef) -> DaftResult<LocalPhysicalPlanRef> + Send + Sync + 'static,
{
    let num_partitions = materialized_outputs.len();
    let mut total_size_bytes = 0;
    let mut total_num_rows = 0;
    let mut partition_refs = vec![];
    let mut worker_id_counts: HashMap<WorkerId, usize> = HashMap::new();

    for materialized_output in materialized_outputs {
        total_size_bytes += materialized_output.size_bytes()?;
        total_num_rows += materialized_output.num_rows()?;
        let (output_refs, worker_id) = materialized_output.into_inner();
        partition_refs.extend(output_refs);
        let count = worker_id_counts.entry(worker_id.clone()).or_insert(0);
        *count += 1;
    }

    let worker_id = worker_id_counts
        .iter()
        .max_by_key(|(_, count)| *count)
        .map(|(worker_id, _)| worker_id.clone())
        .unwrap_or_default();

    let info = InMemoryInfo::new(
        node.config().schema.clone(),
        node.context().node_id.to_string(),
        None,
        num_partitions,
        total_size_bytes,
        total_num_rows,
        None,
        None,
    );
    let in_memory_source_plan =
        LocalPhysicalPlan::in_memory_scan(info, StatsState::NotMaterialized);
    let plan = plan_builder(in_memory_source_plan)?;
    let psets = HashMap::from([(node.node_id().to_string(), partition_refs)]);

    let task = SwordfishTask::new(
        task_context,
        plan,
        node.config().execution_config.clone(),
        psets,
        SchedulingStrategy::WorkerAffinity {
            worker_id,
            soft: true,
        },
        node.context().to_hashmap(),
    );
    Ok(SubmittableTask::new(task))
}

fn make_in_memory_scan_from_materialized_outputs(
    task_context: TaskContext,
    materialized_outputs: Vec<MaterializedOutput>,
    node: &Arc<dyn DistributedPipelineNode>,
) -> DaftResult<SubmittableTask<SwordfishTask>> {
    make_new_task_from_materialized_outputs(task_context, materialized_outputs, node, &|input| {
        Ok(input)
    })
}

fn append_plan_to_existing_task<F>(
    task_context: TaskContext,
    submittable_task: SubmittableTask<SwordfishTask>,
    node: &Arc<dyn DistributedPipelineNode>,
    plan_builder: &F,
) -> DaftResult<SubmittableTask<SwordfishTask>>
where
    F: Fn(LocalPhysicalPlanRef) -> DaftResult<LocalPhysicalPlanRef> + Send + Sync + 'static,
{
    let plan = submittable_task.task().plan();
    let new_plan = plan_builder(plan)?;
    let scheduling_strategy = submittable_task.task().strategy().clone();
    let psets = submittable_task.task().psets().clone();
    let config = submittable_task.task().config().clone();

    let task = submittable_task.with_new_task(SwordfishTask::new(
        task_context,
        new_plan,
        config,
        psets,
        scheduling_strategy,
        node.context().to_hashmap(),
    ));
    Ok(task)
}
