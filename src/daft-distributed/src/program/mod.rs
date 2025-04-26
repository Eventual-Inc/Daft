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
use daft_local_plan::LocalPhysicalPlanRef;
use daft_logical_plan::{LogicalPlan, LogicalPlanRef};
use futures::Stream;
use limit::LimitProgram;
use translate::translate_program_plan_to_local_physical_plans;

use crate::{channel::Receiver, stage::StageContext};

mod collect;
mod limit;
mod translate;

pub(crate) struct ProgramContext {
    local_physical_plans: Vec<LocalPhysicalPlanRef>,
    input_programs: Vec<Program>,
    input_psets: HashMap<String, Vec<PartitionRef>>,
}

impl ProgramContext {
    fn new(
        local_physical_plans: Vec<LocalPhysicalPlanRef>,
        input_programs: Vec<Program>,
        input_psets: HashMap<String, Vec<PartitionRef>>,
    ) -> Self {
        // We must have at least one local physical plan
        assert!(!local_physical_plans.is_empty());

        if !input_programs.is_empty() {
            // If we have an input program, we must have exactly one local physical plan
            assert!(local_physical_plans.len() == 1);
            // If we have an input program, we must have no input psets
            assert!(input_psets.is_empty());
        }
        Self {
            local_physical_plans,
            input_programs,
            input_psets,
        }
    }
}

// A program creates tasks from a logical plan and submits them to the task dispatcher.
#[allow(dead_code)]
pub enum Program {
    Collect(CollectProgram),
    Limit(LimitProgram),
}

// program . call run = running program. running program implements stream or something.
impl Program {
    // Spawn the tasks of a program onto the joinset, and return a receiver to receive the results of the program.
    pub fn run_program(self, stage_context: &mut StageContext) -> RunningProgram {
        match self {
            Self::Collect(collect_program) => collect_program.run_program(stage_context),
            Self::Limit(limit_program) => limit_program.run_program(stage_context),
        }
    }
}

pub struct RunningProgram {
    result_receiver: Receiver<PartitionRef>,
}

impl RunningProgram {
    pub fn new(result_receiver: Receiver<PartitionRef>) -> Self {
        Self { result_receiver }
    }

    pub fn into_inner(self) -> Receiver<PartitionRef> {
        self.result_receiver
    }
}

impl Stream for RunningProgram {
    type Item = DaftResult<PartitionRef>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub fn logical_plan_to_program(
    plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
    psets: HashMap<String, Vec<PartitionRef>>,
) -> DaftResult<Program> {
    struct ProgramBoundarySplitter {
        root: LogicalPlanRef,
        psets: HashMap<String, Vec<PartitionRef>>,
        current_programs: Vec<Program>,
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
                    assert!(input_programs.len() == 1);
                    let translated_local_physical_plans =
                        translate_program_plan_to_local_physical_plans(node.clone(), &self.config)?;
                    let program_context = ProgramContext::new(
                        translated_local_physical_plans,
                        input_programs,
                        std::mem::take(&mut self.psets),
                    );
                    self.current_programs = vec![Program::Limit(LimitProgram::new(
                        limit.limit as usize,
                        program_context,
                    ))];
                    // Here we will have to return a placeholder, essentially cutting off the plan
                    todo!()
                }
                _ if is_root => {
                    let input_programs = std::mem::take(&mut self.current_programs);
                    let translated_local_physical_plans =
                        translate_program_plan_to_local_physical_plans(node.clone(), &self.config)?;
                    let program_context = ProgramContext::new(
                        translated_local_physical_plans,
                        input_programs,
                        std::mem::take(&mut self.psets),
                    );
                    self.current_programs =
                        vec![Program::Collect(CollectProgram::new(program_context))];
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
    Ok(splitter.current_programs.pop().unwrap())
}
