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
use daft_logical_plan::{stats::StatsState, InMemoryInfo};
use daft_schema::schema::SchemaRef;
use futures::{Stream, StreamExt};
use materialize::{materialize_all_pipeline_outputs, materialize_running_pipeline_outputs};

use crate::{
    plan::PlanID,
    scheduling::{
        scheduler::{SchedulerHandle, SubmittableTask, SubmittedTask},
        task::{SchedulingStrategy, SwordfishTask, Task},
        worker::WorkerId,
    },
    stage::{StageContext, StageID},
    utils::channel::{create_channel, Receiver, ReceiverStream},
};

#[cfg(feature = "python")]
mod actor_udf;
mod explode;
mod filter;
mod in_memory_source;
mod limit;
pub(crate) mod materialize;
mod project;
mod sample;
mod scan_source;
mod sink;
mod translate;
mod unpivot;

pub(crate) use translate::logical_plan_to_pipeline_node;
pub(crate) type NodeID = usize;

/// The materialized output of a completed pipeline node.
/// Contains both the partition data as well as metadata about the partition.
/// Right now, the only metadata is the worker id that has it so we can try
/// to schedule follow-up pipeline nodes on the same worker.
#[derive(Clone, Debug)]
pub(crate) struct MaterializedOutput {
    partition: PartitionRef,
    worker_id: WorkerId,
}

impl MaterializedOutput {
    #[allow(dead_code)]
    pub fn new(partition: PartitionRef, worker_id: WorkerId) -> Self {
        Self {
            partition,
            worker_id,
        }
    }

    pub fn partition(&self) -> &PartitionRef {
        &self.partition
    }

    #[allow(dead_code)]
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn into_inner(self) -> (PartitionRef, WorkerId) {
        (self.partition, self.worker_id)
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum PipelineOutput<T: Task> {
    Materialized(MaterializedOutput),
    Task(SubmittableTask<T>),
    Running(SubmittedTask),
}

#[allow(dead_code)]
pub(crate) trait DistributedPipelineNode: Send + Sync {
    fn name(&self) -> &'static str;
    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>>;
    fn start(self: Arc<Self>, stage_context: &mut StageContext) -> RunningPipelineNode;
    fn plan_id(&self) -> &PlanID;
    fn stage_id(&self) -> &StageID;
    fn node_id(&self) -> &NodeID;
    fn as_tree_display(&self) -> &dyn TreeDisplay;
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

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct RunningPipelineNode {
    result_receiver: Receiver<PipelineOutput<SwordfishTask>>,
}

impl RunningPipelineNode {
    #[allow(dead_code)]
    fn new(result_receiver: Receiver<PipelineOutput<SwordfishTask>>) -> Self {
        Self { result_receiver }
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Receiver<PipelineOutput<SwordfishTask>> {
        self.result_receiver
    }

    #[allow(dead_code)]
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
        stage_context: &mut StageContext,
        config: Arc<DaftExecutionConfig>,
        node_id: NodeID,
        schema: SchemaRef,
        context: HashMap<String, String>,
        plan_builder: F,
    ) -> Self
    where
        F: Fn(LocalPhysicalPlanRef) -> DaftResult<LocalPhysicalPlanRef> + Send + Sync + 'static,
    {
        let (result_tx, result_rx) = create_channel(1);

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
                        let task = make_new_task_from_materialized_output(
                            materialized_output,
                            context.clone(),
                            config.clone(),
                            node_id,
                            schema.clone(),
                            &plan_builder,
                        )?;
                        if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                            break;
                        }
                    }
                    PipelineOutput::Task(task) => {
                        // append plan to this task
                        let task =
                            append_plan_to_existing_task(task, context.clone(), &plan_builder)?;
                        if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                            break;
                        }
                    }
                }
            }
            Ok::<(), common_error::DaftError>(())
        };

        stage_context.joinset.spawn(execution_loop);
        Self::new(result_rx)
    }
}

fn make_new_task_from_materialized_output<F>(
    materialized_output: MaterializedOutput,
    context: HashMap<String, String>,
    config: Arc<DaftExecutionConfig>,
    node_id: NodeID,
    schema: SchemaRef,
    plan_builder: &F,
) -> DaftResult<SubmittableTask<SwordfishTask>>
where
    F: Fn(LocalPhysicalPlanRef) -> DaftResult<LocalPhysicalPlanRef>,
{
    let (partition_ref, worker_id) = materialized_output.into_inner();

    let info = InMemoryInfo::new(
        schema,
        node_id.to_string(),
        None,
        1,
        partition_ref
            .size_bytes()?
            .expect("The size of the materialized output should be known"),
        partition_ref.num_rows()?,
        None,
        None,
    );
    let in_memory_source_plan =
        LocalPhysicalPlan::in_memory_scan(info, StatsState::NotMaterialized);
    let plan = plan_builder(in_memory_source_plan)?;
    let psets = HashMap::from([(node_id.to_string(), vec![partition_ref])]);

    let task = SwordfishTask::new(
        plan,
        config,
        psets,
        SchedulingStrategy::WorkerAffinity {
            worker_id,
            soft: false,
        },
        context,
        node_id,
    );
    Ok(SubmittableTask::new(task))
}

fn append_plan_to_existing_task<F>(
    submittable_task: SubmittableTask<SwordfishTask>,
    context: HashMap<String, String>,
    plan_builder: &F,
) -> DaftResult<SubmittableTask<SwordfishTask>>
where
    F: Fn(LocalPhysicalPlanRef) -> DaftResult<LocalPhysicalPlanRef>,
{
    let plan = submittable_task.task().plan();
    let new_plan = plan_builder(plan)?;
    let scheduling_strategy = submittable_task.task().strategy().clone();
    let psets = submittable_task.task().psets().clone();
    let config = submittable_task.task().config().clone();
    let task_priority = submittable_task.task().task_priority();

    let task = submittable_task.with_new_task(SwordfishTask::new(
        new_plan,
        config,
        psets,
        scheduling_strategy,
        context,
        task_priority,
    ));
    Ok(task)
}
