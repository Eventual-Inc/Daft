use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use collect::CollectNode;
use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::{LogicalPlan, LogicalPlanRef};
use futures::Stream;
use limit::LimitNode;
use translate::translate_pipeline_plan_to_local_physical_plans;

use crate::{
    channel::Receiver,
    scheduling::task::{SwordfishTask, SwordfishTaskResultHandle},
    stage::StageContext,
};

mod collect;
mod limit;
mod translate;

pub(crate) trait DistributedPipelineNode: Send + Sync {
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
    #[allow(dead_code)]
    fn children(&self) -> Vec<&dyn DistributedPipelineNode>;
    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode;
}

#[allow(dead_code)]
pub(crate) struct RunningPipelineNode {
    result_receiver: Receiver<PipelineOutput>,
}

impl RunningPipelineNode {
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

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!("Implement stream for running pipeline node");
    }
}

#[allow(dead_code)]
pub(crate) enum PipelineOutput {
    Materialized(PartitionRef),
    Task(SwordfishTask),
    Running(Box<dyn SwordfishTaskResultHandle>),
}

pub(crate) fn logical_plan_to_pipeline_node(
    plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
    psets: HashMap<String, Vec<PartitionRef>>,
) -> DaftResult<Box<dyn DistributedPipelineNode>> {
    struct PipelineNodeBoundarySplitter {
        root: LogicalPlanRef,
        psets: HashMap<String, Vec<PartitionRef>>,
        current_nodes: Vec<Box<dyn DistributedPipelineNode>>,
        config: Arc<DaftExecutionConfig>,
    }

    impl TreeNodeRewriter for PipelineNodeBoundarySplitter {
        type Node = LogicalPlanRef;

        fn f_down(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            Ok(Transformed::no(node))
        }

        fn f_up(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            let is_root = Arc::ptr_eq(&node, &self.root);
            match node.as_ref() {
                LogicalPlan::Limit(limit) => {
                    let input_nodes = std::mem::take(&mut self.current_nodes);
                    let translated_local_physical_plans =
                        translate_pipeline_plan_to_local_physical_plans(
                            node.clone(),
                            &self.config,
                        )?;
                    self.current_nodes = vec![Box::new(LimitNode::new(
                        limit.limit as usize,
                        translated_local_physical_plans,
                        input_nodes,
                        std::mem::take(&mut self.psets),
                    ))];
                    // Here we will have to return a placeholder, essentially cutting off the plan
                    todo!("Implement pipeline node boundary splitter for limit");
                }
                _ if is_root => {
                    let input_nodes = std::mem::take(&mut self.current_nodes);
                    let translated_local_physical_plans =
                        translate_pipeline_plan_to_local_physical_plans(
                            node.clone(),
                            &self.config,
                        )?;
                    self.current_nodes = vec![Box::new(CollectNode::new(
                        translated_local_physical_plans,
                        input_nodes,
                        std::mem::take(&mut self.psets),
                    ))];
                    Ok(Transformed::no(node))
                }
                _ => Ok(Transformed::no(node)),
            }
        }
    }

    let mut splitter = PipelineNodeBoundarySplitter {
        root: plan.clone(),
        current_nodes: vec![],
        psets,
        config,
    };

    let _transformed = plan.rewrite(&mut splitter)?;
    assert!(splitter.current_nodes.len() == 1);
    Ok(splitter
        .current_nodes
        .pop()
        .expect("Expected exactly one node"))
}
