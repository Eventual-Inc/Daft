use std::{collections::HashMap, sync::Arc};

use actor_pool_project::ActorPoolProjectProgram;
use collect::CollectProgram;
use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::{LogicalPlan, LogicalPlanRef};
use limit::LimitProgram;

use crate::scheduling::dispatcher::TaskDispatcherHandle;

mod actor_pool_project;
mod collect;
mod limit;
mod task_producer;

#[allow(dead_code)]
pub enum Program {
    Collect(CollectProgram),
    Limit(LimitProgram),
    ActorPoolProject(ActorPoolProjectProgram),
}

impl Program {
    pub fn spawn_program(
        self,
        _task_dispatcher_handle: TaskDispatcherHandle,
        _config: Arc<DaftExecutionConfig>,
        _psets: HashMap<String, Vec<PartitionRef>>,
        _joinset: &mut tokio::task::JoinSet<DaftResult<()>>,
        _next_receiver: Option<tokio::sync::mpsc::Receiver<PartitionRef>>,
    ) -> tokio::sync::mpsc::Receiver<PartitionRef> {
        todo!()
    }
}

pub fn logical_plan_to_programs(plan: LogicalPlanRef) -> DaftResult<Vec<Program>> {
    struct ProgramBoundarySplitter {
        root: LogicalPlanRef,
        programs: Vec<Program>,
    }

    impl TreeNodeRewriter for ProgramBoundarySplitter {
        type Node = LogicalPlanRef;

        fn f_down(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            Ok(Transformed::no(node))
        }

        fn f_up(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            let is_root = Arc::ptr_eq(&node, &self.root);
            match node.as_ref() {
                LogicalPlan::Limit(_limit) => todo!(),
                LogicalPlan::ActorPoolProject(_actor_pool_project) => todo!(),
                _ if is_root => {
                    self.programs
                        .push(Program::Collect(CollectProgram::new(node.clone())));
                    Ok(Transformed::no(node))
                }
                _ => Ok(Transformed::no(node)),
            }
        }
    }

    let mut splitter = ProgramBoundarySplitter {
        root: plan.clone(),
        programs: vec![],
    };

    let _transformed = plan.rewrite(&mut splitter)?;
    Ok(splitter.programs)
}
