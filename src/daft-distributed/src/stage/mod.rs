use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::LogicalPlanRef;
use futures::Stream;

use crate::{
    pipeline_node::{self, RunningPipelineNode},
    runtime::JoinSet,
    scheduling::{
        dispatcher::{TaskDispatcher, TaskDispatcherHandle},
        worker::WorkerManagerFactory,
    },
};

pub(crate) struct Stage {
    logical_plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
}

impl Stage {
    fn new(logical_plan: LogicalPlanRef, config: Arc<DaftExecutionConfig>) -> Self {
        Self {
            logical_plan,
            config,
        }
    }
}

impl Stage {
    pub(crate) fn run_stage(
        self,
        psets: HashMap<String, Vec<PartitionRef>>,
        worker_manager_factory: Box<dyn WorkerManagerFactory>,
    ) -> DaftResult<RunningStage> {
        let mut pipeline_node =
            pipeline_node::logical_plan_to_pipeline_node(self.logical_plan, self.config, psets)?;
        let mut stage_context = StageContext::try_new(worker_manager_factory)?;
        let running_node = pipeline_node.start(&mut stage_context);
        let running_stage = RunningStage::new(running_node, stage_context);
        Ok(running_stage)
    }
}

#[allow(dead_code)]
pub(crate) struct RunningStage {
    running_node: RunningPipelineNode,
    stage_context: StageContext,
}

impl RunningStage {
    fn new(running_node: RunningPipelineNode, stage_context: StageContext) -> Self {
        Self {
            running_node,
            stage_context,
        }
    }
}

impl Stream for RunningStage {
    type Item = DaftResult<PartitionRef>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!("Implement stream for running stage");
    }
}

pub(crate) struct StageContext {
    pub task_dispatcher_handle: TaskDispatcherHandle,
    pub joinset: JoinSet<DaftResult<()>>,
}

impl StageContext {
    fn try_new(worker_manager_factory: Box<dyn WorkerManagerFactory>) -> DaftResult<Self> {
        let worker_manager = worker_manager_factory.create_worker_manager()?;
        let task_dispatcher = TaskDispatcher::new(worker_manager);
        let mut joinset = JoinSet::new();
        let task_dispatcher_handle =
            TaskDispatcher::spawn_task_dispatcher(task_dispatcher, &mut joinset);
        Ok(Self {
            task_dispatcher_handle,
            joinset,
        })
    }
}

#[allow(dead_code)]
pub(crate) fn split_at_stage_boundary(
    plan: &LogicalPlanRef,
    config: &Arc<DaftExecutionConfig>,
) -> DaftResult<(Stage, Option<LogicalPlanRef>)> {
    struct StageBoundarySplitter {
        next_stage: Option<Stage>,
        _config: Arc<DaftExecutionConfig>,
    }

    impl TreeNodeRewriter for StageBoundarySplitter {
        type Node = LogicalPlanRef;

        fn f_down(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            Ok(Transformed::no(node))
        }

        fn f_up(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            // TODO: Implement stage boundary splitting. Stage boundaries will be defined by the presence of a repartition, or the root of the plan.
            // If it is the root of the plan, we will return a collect stage.
            // If it is a repartition, we will return a shuffle map stage.
            Ok(Transformed::no(node))
        }
    }

    let mut splitter = StageBoundarySplitter {
        next_stage: None,
        _config: config.clone(),
    };

    let transformed = plan.clone().rewrite(&mut splitter)?;

    if let Some(next_stage) = splitter.next_stage {
        Ok((next_stage, Some(transformed.data)))
    } else {
        // make collect stage
        let plan = transformed.data;
        let stage = Stage::new(plan, config.clone());
        Ok((stage, None))
    }
}
