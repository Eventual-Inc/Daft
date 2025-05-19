use std::{collections::HashMap, sync::Arc};

use collect::CollectNode;
use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::{LogicalPlan, LogicalPlanRef};
use futures::{Stream, StreamExt};
use limit::LimitNode;
use materialize::materialize_all_pipeline_outputs;
use translate::translate_pipeline_plan_to_local_physical_plans;

use crate::{
    scheduling::{
        scheduler::{SchedulerHandle, SubmittedTask},
        task::SwordfishTask,
    },
    stage::StageContext,
    utils::channel::{Receiver, ReceiverStream},
};

mod collect;
mod limit;
pub(crate) mod materialize;
mod translate;

pub(crate) trait DistributedPipelineNode: Send + Sync {
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

    #[allow(dead_code)]
    pub fn materialize(
        self,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
    ) -> impl Stream<Item = DaftResult<PartitionRef>> + Send + Unpin + 'static {
        let stream = self.into_stream().map(Ok);
        materialize_all_pipeline_outputs(stream, scheduler_handle)
    }

    pub fn into_stream(self) -> impl Stream<Item = PipelineOutput> + Send + Unpin + 'static {
        ReceiverStream::new(self.result_receiver)
    }
}

#[allow(dead_code)]
pub(crate) enum PipelineOutput {
    Materialized(PartitionRef),
    Task(SwordfishTask),
    Running(SubmittedTask),
}

#[allow(dead_code)]
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
                    todo!("FLOTILLA_MS1: Implement pipeline node boundary splitter for limit");
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
