use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use collect::CollectProgram;
use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::{LogicalPlan, LogicalPlanRef};
use futures::Stream;
use limit::LimitProgram;
use translate::translate_program_plan_to_local_physical_plans;

use crate::{channel::Receiver, stage::StageContext};

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

pub(crate) struct RunningPipelineNode {
    result_receiver: Receiver<PartitionRef>,
}

impl RunningPipelineNode {
    fn new(result_receiver: Receiver<PartitionRef>) -> Self {
        Self { result_receiver }
    }

    pub fn into_inner(self) -> Receiver<PartitionRef> {
        self.result_receiver
    }
}

impl Stream for RunningPipelineNode {
    type Item = DaftResult<PartitionRef>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!("Implement stream for running pipeline node");
    }
}

pub(crate) fn logical_plan_to_program(
    plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
    psets: HashMap<String, Vec<PartitionRef>>,
) -> DaftResult<Box<dyn DistributedPipelineNode>> {
    struct ProgramBoundarySplitter {
        root: LogicalPlanRef,
        psets: HashMap<String, Vec<PartitionRef>>,
        current_programs: Vec<Box<dyn DistributedPipelineNode>>,
        config: Arc<DaftExecutionConfig>,
    }

    impl TreeNodeRewriter for ProgramBoundarySplitter {
        type Node = LogicalPlanRef;

        fn f_down(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            Ok(Transformed::no(node))
        }

        fn f_up(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            let is_root = Arc::ptr_eq(&node, &self.root);
            match node.as_ref() {
                LogicalPlan::Limit(limit) => {
                    let input_programs = std::mem::take(&mut self.current_programs);
                    let translated_local_physical_plans =
                        translate_program_plan_to_local_physical_plans(node.clone(), &self.config)?;
                    self.current_programs = vec![Box::new(LimitProgram::new(
                        limit.limit as usize,
                        translated_local_physical_plans,
                        input_programs,
                        std::mem::take(&mut self.psets),
                    ))];
                    // Here we will have to return a placeholder, essentially cutting off the plan
                    todo!("Implement program boundary splitter for limit");
                }
                _ if is_root => {
                    let input_programs = std::mem::take(&mut self.current_programs);
                    let translated_local_physical_plans =
                        translate_program_plan_to_local_physical_plans(node.clone(), &self.config)?;
                    self.current_programs = vec![Box::new(CollectProgram::new(
                        translated_local_physical_plans,
                        input_programs,
                        std::mem::take(&mut self.psets),
                    ))];
                    Ok(Transformed::no(node))
                }
                _ => Ok(Transformed::no(node)),
            }
        }
    }

    let mut splitter = ProgramBoundarySplitter {
        root: plan.clone(),
        current_programs: vec![],
        psets,
        config,
    };

    let _transformed = plan.rewrite(&mut splitter)?;
    assert!(splitter.current_programs.len() == 1);
    Ok(splitter
        .current_programs
        .pop()
        .expect("Expected exactly one program"))
}
