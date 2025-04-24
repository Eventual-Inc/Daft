use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_local_plan::LocalPhysicalPlanRef;
use daft_logical_plan::{
    ops::Source, source_info::PlaceHolderInfo, ClusteringSpec, LogicalPlan, LogicalPlanRef,
    SourceInfo,
};
use futures::{Stream, StreamExt};

use crate::{
    dispatcher::TaskDispatcherHandle, operator::Operator,
    translate::translate_logical_plan_to_local_physical_plans,
};

pub enum PlanProducer {
    SourcePlan(SourcePlanProducer),
    IntermediatePlan(IntermediatePlanProducer),
}

impl PlanProducer {
    pub fn new(
        plan: LogicalPlanRef,
        input_rx: Option<tokio::sync::mpsc::Receiver<PartitionRef>>,
    ) -> Self {
        match input_rx {
            Some(rx) => Self::IntermediatePlan(IntermediatePlanProducer::new(plan, rx)),
            None => Self::SourcePlan(SourcePlanProducer::new(plan)),
        }
    }
}

impl Stream for PlanProducer {
    type Item = DaftResult<LocalPhysicalPlanRef>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            Self::SourcePlan(producer) => producer.poll_next_unpin(cx),
            Self::IntermediatePlan(producer) => producer.poll_next_unpin(cx),
        }
    }
}
pub struct SourcePlanProducer {
    local_physical_plans: VecDeque<LocalPhysicalPlanRef>,
}

impl SourcePlanProducer {
    pub fn new(plan: LogicalPlanRef) -> Self {
        // VecDeque::from<Vec<_>> is guaranteed to be O(1) and not re-allocate / allocate new memory
        let local_physical_plans = VecDeque::from(
            translate_logical_plan_to_local_physical_plans(
                plan,
                Arc::new(DaftExecutionConfig::default()),
            )
            .unwrap(),
        );
        Self {
            local_physical_plans,
        }
    }
}

impl Stream for SourcePlanProducer {
    type Item = DaftResult<LocalPhysicalPlanRef>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Just yield the local physical plans, in order
        if let Some(local_physical_plan) = self.local_physical_plans.pop_front() {
            return Poll::Ready(Some(Ok(local_physical_plan)));
        }
        Poll::Ready(None)
    }
}

pub struct IntermediatePlanProducer {
    plan: LogicalPlanRef,
    input_rx: tokio::sync::mpsc::Receiver<PartitionRef>,
}

impl IntermediatePlanProducer {
    pub fn new(plan: LogicalPlanRef, input_rx: tokio::sync::mpsc::Receiver<PartitionRef>) -> Self {
        Self { plan, input_rx }
    }
}

impl Stream for IntermediatePlanProducer {
    type Item = DaftResult<LocalPhysicalPlanRef>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

#[derive(Debug)]
pub enum Program {
    Collect(CollectProgram),
    Limit(LimitProgram),
    ActorPoolProject(ActorPoolProjectProgram),
}

impl Program {
    pub fn spawn_program(
        self,
        task_dispatcher_handle: TaskDispatcherHandle,
        config: Arc<DaftExecutionConfig>,
        psets: HashMap<String, Vec<PartitionRef>>,
        joinset: &mut tokio::task::JoinSet<DaftResult<()>>,
        next_receiver: Option<tokio::sync::mpsc::Receiver<PartitionRef>>,
    ) -> tokio::sync::mpsc::Receiver<PartitionRef> {
        match self {
            Program::Collect(program) => program.spawn_program(
                task_dispatcher_handle,
                config,
                psets,
                next_receiver,
                joinset,
            ),
            Program::Limit(program) => todo!(),
            Program::ActorPoolProject(program) => todo!(),
        }
    }
}
#[derive(Debug)]
pub struct CollectProgram {
    pub plan: LogicalPlanRef,
}

impl CollectProgram {
    pub fn new(plan: LogicalPlanRef) -> Self {
        Self { plan }
    }

    pub fn spawn_program(
        self,
        task_dispatcher_handle: TaskDispatcherHandle,
        config: Arc<DaftExecutionConfig>,
        psets: HashMap<String, Vec<PartitionRef>>,
        input_rx: Option<tokio::sync::mpsc::Receiver<PartitionRef>>,
        joinset: &mut tokio::task::JoinSet<DaftResult<()>>,
    ) -> tokio::sync::mpsc::Receiver<PartitionRef> {
        let plan_producer = PlanProducer::new(self.plan, input_rx);
        let (result_tx, result_rx) = tokio::sync::mpsc::channel(1);
        let operator = Operator::new(
            plan_producer,
            task_dispatcher_handle,
            config,
            result_tx,
            psets,
        );
        joinset.spawn(operator.run());
        result_rx
    }
}

#[derive(Debug)]
pub struct LimitProgram {
    pub limit: i64,
    pub eager: bool,
    pub plan: LogicalPlanRef,
}

impl LimitProgram {
    pub fn new(limit: i64, eager: bool, plan: LogicalPlanRef) -> Self {
        Self { limit, eager, plan }
    }
}

#[derive(Debug)]
pub struct ActorPoolProjectProgram {}

pub fn logical_plan_to_programs(plan: LogicalPlanRef) -> DaftResult<Vec<Program>> {
    struct ProgramBoundarySplitter {
        root: LogicalPlanRef,
        programs: Vec<Program>,
        config: Arc<DaftExecutionConfig>,
    }

    impl TreeNodeRewriter for ProgramBoundarySplitter {
        type Node = LogicalPlanRef;

        fn f_down(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            Ok(Transformed::no(node))
        }

        fn f_up(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            let schema = node.schema().clone();
            let is_root = Arc::ptr_eq(&node, &self.root);
            match node.as_ref() {
                LogicalPlan::Limit(limit) => {
                    self.programs.push(Program::Limit(LimitProgram::new(
                        limit.limit,
                        limit.eager,
                        node,
                    )));
                    let ph = PlaceHolderInfo::new(schema.clone(), ClusteringSpec::default().into());
                    let new_scan = LogicalPlan::Source(Source::new(
                        schema,
                        SourceInfo::PlaceHolder(ph).into(),
                    ));

                    Ok(Transformed::yes(new_scan.into()))
                }
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
        config: Arc::new(DaftExecutionConfig::default()),
    };

    let _transformed = plan.rewrite(&mut splitter)?;
    Ok(splitter.programs)
}
