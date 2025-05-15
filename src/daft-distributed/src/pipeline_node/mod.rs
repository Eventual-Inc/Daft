use std::{
    collections::HashMap,
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use collect::CollectNode;
use common_daft_config::DaftExecutionConfig;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_scan_info::{Pushdowns, ScanTaskLikeRef};
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::{
    ops::Source, source_info::PlaceHolderInfo, ClusteringSpec, InMemoryInfo, LogicalPlan,
    LogicalPlanRef, SourceInfo,
};
use futures::Stream;
use limit::LimitNode;
use serde::{Deserialize, Serialize};
use translate::translate_logical_plan_to_pipeline_plan;

use crate::{
    scheduling::{
        dispatcher::SubmittedTask,
        task::{SwordfishTask, Task},
        worker::WorkerId,
    },
    stage::StageContext,
    utils::channel::Receiver,
};

mod collect;
mod limit;
pub(crate) mod materialize;
mod translate;

#[typetag::serde(tag = "type")]
pub(crate) trait DistributedPipelineNode: Send + Sync + Debug {
    fn as_tree_display(&self) -> &dyn TreeDisplay;
    fn name(&self) -> &'static str;
    fn children(&self) -> Vec<&dyn DistributedPipelineNode>;
    fn start(
        &self,
        stage_context: &mut StageContext,
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    ) -> RunningPipelineNode<SwordfishTask>;
}

#[allow(dead_code)]
pub(crate) struct RunningPipelineNode<T: Task> {
    result_receiver: Receiver<PipelineOutput<T>>,
}

impl<T: Task> RunningPipelineNode<T> {
    #[allow(dead_code)]
    fn new(result_receiver: Receiver<PipelineOutput<T>>) -> Self {
        Self { result_receiver }
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Receiver<PipelineOutput<T>> {
        self.result_receiver
    }
}

impl<T: Task> Stream for RunningPipelineNode<T> {
    type Item = DaftResult<PipelineOutput<T>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.result_receiver.poll_recv(cx) {
            Poll::Ready(Some(result)) => Poll::Ready(Some(Ok(result))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MaterializedOutput {
    partition: PartitionRef,
    worker_id: WorkerId,
}

impl MaterializedOutput {
    pub fn new(partition: PartitionRef, worker_id: WorkerId) -> Self {
        Self {
            partition,
            worker_id,
        }
    }

    pub fn partition(&self) -> &PartitionRef {
        &self.partition
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn into_inner(self) -> (PartitionRef, WorkerId) {
        (self.partition, self.worker_id)
    }
}

#[derive(Debug)]
pub(crate) enum PipelineOutput<T: Task> {
    Materialized(MaterializedOutput),
    Task(T),
    Running(SubmittedTask),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
enum PipelineInput {
    InMemorySource {
        info: InMemoryInfo,
    },
    ScanTasks {
        pushdowns: Pushdowns,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    },
    Intermediate,
}

#[allow(dead_code)]
pub(crate) fn logical_plan_to_pipeline_node(
    plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
) -> DaftResult<Box<dyn DistributedPipelineNode>> {
    struct PipelineNodeBoundarySplitter {
        root: LogicalPlanRef,
        current_nodes: Vec<Box<dyn DistributedPipelineNode>>,
        config: Arc<DaftExecutionConfig>,
        node_id_counter: usize,
    }

    impl PipelineNodeBoundarySplitter {
        fn get_next_node_id(&mut self) -> usize {
            let node_id = self.node_id_counter;
            self.node_id_counter += 1;
            node_id
        }
    }

    impl TreeNodeRewriter for PipelineNodeBoundarySplitter {
        type Node = LogicalPlanRef;

        fn f_down(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            Ok(Transformed::no(node))
        }

        fn f_up(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            match node.as_ref() {
                LogicalPlan::Limit(limit) => {
                    let input_nodes = std::mem::take(&mut self.current_nodes);
                    let pipeline_plan = translate_logical_plan_to_pipeline_plan(node.clone())?;
                    let collect_node = Box::new(CollectNode::new(
                        self.get_next_node_id(),
                        self.config.clone(),
                        pipeline_plan,
                        input_nodes,
                    ));
                    self.current_nodes = vec![Box::new(LimitNode::new(
                        self.get_next_node_id(),
                        limit.limit as usize,
                        node.schema().clone(),
                        self.config.clone(),
                        collect_node,
                    ))];
                    // Here we will have to return a placeholder, essentially cutting off the plan
                    let placeholder = PlaceHolderInfo::new(
                        node.schema().clone(),
                        ClusteringSpec::default().into(),
                    );
                    Ok(Transformed::yes(
                        LogicalPlan::Source(Source::new(
                            node.schema().clone(),
                            SourceInfo::PlaceHolder(placeholder).into(),
                        ))
                        .into(),
                    ))
                }
                _ => Ok(Transformed::no(node)),
            }
        }
    }

    let mut splitter = PipelineNodeBoundarySplitter {
        root: plan.clone(),
        current_nodes: vec![],
        config,
        node_id_counter: 0,
    };

    let transformed = plan.rewrite(&mut splitter)?;
    match transformed.data.as_ref() {
        LogicalPlan::Source(source)
            if matches!(source.source_info.as_ref(), SourceInfo::PlaceHolder(_)) =>
        {
            assert!(splitter.current_nodes.len() == 1);
            Ok(splitter
                .current_nodes
                .pop()
                .expect("Expected exactly one node"))
        }
        _ => {
            let logical_plan = transformed.data;
            let pipeline_plan = translate_logical_plan_to_pipeline_plan(logical_plan)?;
            let input_nodes = std::mem::take(&mut splitter.current_nodes);
            let collect_node = Box::new(CollectNode::new(
                splitter.get_next_node_id(),
                splitter.config.clone(),
                pipeline_plan,
                input_nodes,
            ));
            Ok(collect_node)
        }
    }
}
