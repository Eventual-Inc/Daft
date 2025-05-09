use std::{
    collections::HashMap,
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
    ops::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan, LogicalPlanRef,
    SourceInfo,
};
use futures::Stream;
use limit::LimitNode;
use translate::translate_logical_plan_to_pipeline_plan;

use crate::{
    channel::Receiver,
    scheduling::{dispatcher::SubmittedTask, task::SwordfishTask},
    stage::StageContext,
};

mod collect;
mod limit;
pub(crate) mod materialize;
mod translate;

pub(crate) trait DistributedPipelineNode: Send + Sync {
    fn as_tree_display(&self) -> &dyn TreeDisplay;
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
    #[allow(dead_code)]
    fn children(&self) -> Vec<&dyn DistributedPipelineNode>;
    #[allow(dead_code)]
    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode;
}

#[allow(dead_code)]
pub(crate) struct RunningPipelineNode {
    result_receiver: Receiver<PipelineOutput>,
}

impl RunningPipelineNode {
    #[allow(dead_code)]
    fn new(result_receiver: Receiver<PipelineOutput>) -> Self {
        Self { result_receiver }
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Receiver<PipelineOutput> {
        self.result_receiver
    }
}

impl Stream for RunningPipelineNode {
    type Item = DaftResult<PipelineOutput>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.result_receiver.poll_recv(cx) {
            Poll::Ready(Some(result)) => Poll::Ready(Some(Ok(result))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[allow(dead_code)]
pub(crate) enum PipelineOutput {
    Materialized(PartitionRef),
    Task(SwordfishTask),
    Running(SubmittedTask),
}

#[derive(Clone)]
enum PipelineInput {
    InMemorySource {
        cache_key: String,
        partition_refs: Vec<PartitionRef>,
    },
    ScanTasks {
        #[allow(dead_code)]
        source_id: usize,
        pushdowns: Pushdowns,
        scan_tasks: Arc<Vec<ScanTaskLikeRef>>,
    },
    Intermediate,
}

#[allow(dead_code)]
pub(crate) fn logical_plan_to_pipeline_node(
    plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
    psets: HashMap<String, Vec<PartitionRef>>,
    stage_context: &mut StageContext,
) -> DaftResult<Box<dyn DistributedPipelineNode>> {
    struct PipelineNodeBoundarySplitter<'a> {
        root: LogicalPlanRef,
        psets: HashMap<String, Vec<PartitionRef>>,
        current_nodes: Vec<Box<dyn DistributedPipelineNode>>,
        config: Arc<DaftExecutionConfig>,
        stage_context: &'a mut StageContext,
    }

    impl<'a> TreeNodeRewriter for PipelineNodeBoundarySplitter<'a> {
        type Node = LogicalPlanRef;

        fn f_down(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            Ok(Transformed::no(node))
        }

        fn f_up(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            let is_root = Arc::ptr_eq(&node, &self.root);
            println!("node: {:?}, is_root: {:?}", node.as_ref().name(), is_root);
            match node.as_ref() {
                LogicalPlan::Limit(limit) => {
                    let input_nodes = std::mem::take(&mut self.current_nodes);
                    let pipeline_plan = translate_logical_plan_to_pipeline_plan(
                        &node,
                        &self.config,
                        &mut self.psets,
                    )?;
                    let collect_node = Box::new(CollectNode::new(pipeline_plan, input_nodes));
                    self.current_nodes = vec![Box::new(LimitNode::new(
                        self.stage_context.get_node_id(),
                        limit.limit as usize,
                        node.schema().clone(),
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
                // _ if is_root => {
                //     let pipeline_plan = translate_logical_plan_to_pipeline_plan(
                //         &node,
                //         &self.config,
                //         &mut self.psets,
                //     )?;
                //     let input_nodes = std::mem::take(&mut self.current_nodes);
                //     println!("input_nodes: {:?}", input_nodes.len());
                //     self.current_nodes =
                //         vec![Box::new(CollectNode::new(pipeline_plan, input_nodes))];
                //     Ok(Transformed::no(node))
                // }
                _ => Ok(Transformed::no(node)),
            }
        }
    }

    let mut splitter = PipelineNodeBoundarySplitter {
        root: plan.clone(),
        current_nodes: vec![],
        psets,
        config,
        stage_context,
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
            let pipeline_plan = translate_logical_plan_to_pipeline_plan(
                &logical_plan,
                &splitter.config,
                &mut splitter.psets,
            )?;
            let input_nodes = std::mem::take(&mut splitter.current_nodes);
            let collect_node = Box::new(CollectNode::new(pipeline_plan, input_nodes));
            Ok(collect_node)
        }
    }
}
