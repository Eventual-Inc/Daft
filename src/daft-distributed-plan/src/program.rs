use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_local_plan::{InMemoryScan, Limit, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{
    ops::Source,
    source_info::PlaceHolderInfo,
    stats::{ApproxStats, PlanStats, StatsState},
    ClusteringSpec, InMemoryInfo, LogicalPlan, LogicalPlanRef, SourceInfo,
};
use daft_schema::schema::SchemaRef;
use futures::{Stream, StreamExt};

use crate::{
    dispatcher::TaskDispatcherHandle, operator::Operator, task::SwordfishTask,
    translate::translate_logical_plan_to_local_physical_plans,
};

pub enum TaskProducer {
    SourceTask(SourceTaskProducer),
    IntermediateTask(IntermediateTaskProducer),
}

impl TaskProducer {
    pub fn new(
        plan: LogicalPlanRef,
        input_rx: Option<tokio::sync::mpsc::Receiver<PartitionRef>>,
        psets: HashMap<String, Vec<PartitionRef>>,
        daft_execution_config: Arc<DaftExecutionConfig>,
    ) -> Self {
        match input_rx {
            Some(rx) => Self::IntermediateTask(IntermediateTaskProducer::new(
                plan,
                rx,
                daft_execution_config,
            )),
            None => Self::SourceTask(SourceTaskProducer::new(plan, psets, daft_execution_config)),
        }
    }
}

impl Stream for TaskProducer {
    type Item = DaftResult<SwordfishTask>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            Self::SourceTask(producer) => producer.poll_next_unpin(cx),
            Self::IntermediateTask(producer) => producer.poll_next_unpin(cx),
        }
    }
}
pub struct SourceTaskProducer {
    local_physical_plans: VecDeque<LocalPhysicalPlanRef>,
    psets: HashMap<String, Vec<PartitionRef>>,
    daft_execution_config: Arc<DaftExecutionConfig>,
}

impl SourceTaskProducer {
    pub fn new(
        plan: LogicalPlanRef,
        psets: HashMap<String, Vec<PartitionRef>>,
        daft_execution_config: Arc<DaftExecutionConfig>,
    ) -> Self {
        // VecDeque::from<Vec<_>> is guaranteed to be O(1) and not re-allocate / allocate new memory
        let local_physical_plans = VecDeque::from(
            translate_logical_plan_to_local_physical_plans(plan, &daft_execution_config).unwrap(),
        );
        Self {
            local_physical_plans,
            psets,
            daft_execution_config,
        }
    }
}

impl Stream for SourceTaskProducer {
    type Item = DaftResult<SwordfishTask>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Just yield the local physical plans, in order
        if let Some(local_physical_plan) = self.local_physical_plans.pop_front() {
            let task = SwordfishTask::new(
                local_physical_plan,
                self.daft_execution_config.clone(),
                std::mem::take(&mut self.psets),
            );
            return Poll::Ready(Some(Ok(task)));
        }
        Poll::Ready(None)
    }
}

pub struct IntermediateTaskProducer {
    plan: LogicalPlanRef,
    input_rx: tokio::sync::mpsc::Receiver<PartitionRef>,
    daft_execution_config: Arc<DaftExecutionConfig>,
}

impl IntermediateTaskProducer {
    pub fn new(
        plan: LogicalPlanRef,
        input_rx: tokio::sync::mpsc::Receiver<PartitionRef>,
        daft_execution_config: Arc<DaftExecutionConfig>,
    ) -> Self {
        Self {
            plan,
            input_rx,
            daft_execution_config,
        }
    }
}

impl Stream for IntermediateTaskProducer {
    type Item = DaftResult<SwordfishTask>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

#[derive(Debug)]
pub enum Program {
    Collect(CollectProgram),
    Limit(LimitProgram),
    #[allow(dead_code)]
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
            Program::Limit(program) => program.spawn_program(
                task_dispatcher_handle,
                config,
                psets,
                next_receiver,
                joinset,
            ),
            Program::ActorPoolProject(_program) => todo!(),
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
        let task_producer = TaskProducer::new(self.plan, input_rx, psets, config);
        let (result_tx, result_rx) = tokio::sync::mpsc::channel(1);
        let operator = Operator::new(task_producer, task_dispatcher_handle, result_tx);
        joinset.spawn(operator.run());
        result_rx
    }
}

#[derive(Debug)]
pub struct LimitProgram {
    pub limit: usize,
    schema: SchemaRef,
}

impl LimitProgram {
    pub fn new(limit: usize, schema: SchemaRef) -> Self {
        Self { limit, schema }
    }

    pub fn spawn_program(
        self,
        task_dispatcher_handle: TaskDispatcherHandle,
        config: Arc<DaftExecutionConfig>,
        mut psets: HashMap<String, Vec<PartitionRef>>,
        input_rx: Option<tokio::sync::mpsc::Receiver<PartitionRef>>,
        joinset: &mut tokio::task::JoinSet<DaftResult<()>>,
    ) -> tokio::sync::mpsc::Receiver<PartitionRef> {
        let (result_tx, result_rx) = tokio::sync::mpsc::channel(1);
        let limit = self.limit;
        joinset.spawn(async move {
            let mut count = 0;
            let mut input_rx = input_rx.expect("LimitProgram requires an input");
            let mut leftover = None;
            while let Some(result) = input_rx.recv().await {
                let num_rows = result.num_rows()?;
                count += num_rows;
                match count.cmp(&limit) {
                    Ordering::Less | Ordering::Equal => {
                        if let Err(_) = result_tx.send(result).await {
                            break;
                        }
                    }
                    Ordering::Greater => {
                        // We have more rows than the limit so we need to dispatch a new task to handle the remaining rows
                        leftover = Some(result);
                        break;
                    }
                }
            }
            // drop the input_rx so that in flight tasks can be cancelled
            if let Some(leftover) = leftover {
                drop(input_rx);
                // now we dispatch a new task with new limit
                let leftover_size_bytes = leftover
                    .size_bytes()?
                    .expect("Leftover size bytes should be Some");
                let leftover_num_rows = leftover.num_rows()?;
                let new_limit = limit - count;
                let in_memory_info = InMemoryInfo::new(
                    self.schema.clone(),
                    "limit".to_string(),
                    None,
                    1,
                    leftover_size_bytes,
                    leftover_num_rows,
                    None,
                    None,
                );
                let in_memory_scan = LocalPhysicalPlan::InMemoryScan(InMemoryScan {
                    info: in_memory_info,
                    stats_state: StatsState::Materialized(
                        PlanStats::new(ApproxStats {
                            num_rows: leftover_num_rows,
                            size_bytes: leftover_size_bytes,
                            acc_selectivity: 1.0,
                        })
                        .into(),
                    ),
                });
                let limit = LocalPhysicalPlan::Limit(Limit {
                    input: in_memory_scan.into(),
                    num_rows: new_limit as i64,
                    schema: self.schema.clone(),
                    stats_state: StatsState::Materialized(
                        PlanStats::new(ApproxStats {
                            num_rows: new_limit,
                            size_bytes: new_limit / leftover_num_rows * leftover_size_bytes,
                            acc_selectivity: new_limit as f64 / leftover_num_rows as f64,
                        })
                        .into(),
                    ),
                });
                psets.insert("limit".to_string(), vec![leftover]);
                let new_task = SwordfishTask::new(limit.into(), config, psets);
                if let Ok(result_rx) = task_dispatcher_handle.submit_task(new_task).await {
                    if let Some(result) = result_rx.await {
                        let _ = result_tx.send(result?).await;
                    }
                }
            }
            Ok(())
        });
        result_rx
    }
}

#[derive(Debug)]
pub struct ActorPoolProjectProgram {}

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
            let schema = node.schema().clone();
            let is_root = Arc::ptr_eq(&node, &self.root);
            match node.as_ref() {
                LogicalPlan::Limit(limit) => {
                    // Make a collect program with the limit. These will be task-local limits.
                    let collect = Program::Collect(CollectProgram::new(node.clone()));
                    self.programs.push(collect);
                    // Make a limit program with the limit. This will be the global limit.
                    self.programs.push(Program::Limit(LimitProgram::new(
                        limit.limit as usize,
                        schema.clone(),
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
    };

    let _transformed = plan.rewrite(&mut splitter)?;
    Ok(splitter.programs)
}
