use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlanRef;
use daft_logical_plan::LogicalPlanRef;
use futures::{Stream, StreamExt};

use super::translate::translate_program_plan_to_local_physical_plans;
use crate::{channel::Receiver, scheduling::task::SwordfishTask};

// A task producer creates tasks from a logical plan and produces a stream of tasks.
#[allow(dead_code)]
pub enum TaskProducer {
    // A source task producer simply translates a logical plan into a stream of tasks.
    SourceTask(SourceTaskProducer),
    // An intermediate task producer takes an input receiver and a logical plan, and produces a stream of tasks.
    // It is used for intermediate programs where the previous program's output is an input to the next program.
    IntermediateTask(IntermediateTaskProducer),
}

impl TaskProducer {
    #[allow(dead_code)]
    pub fn new(
        plan: LogicalPlanRef,
        input_rx: Option<Receiver<PartitionRef>>,
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
    #[allow(dead_code)]
    pub fn new(
        plan: LogicalPlanRef,
        psets: HashMap<String, Vec<PartitionRef>>,
        daft_execution_config: Arc<DaftExecutionConfig>,
    ) -> Self {
        // VecDeque::from<Vec<_>> is guaranteed to be O(1) and not re-allocate / allocate new memory
        let local_physical_plans = VecDeque::from(
            translate_program_plan_to_local_physical_plans(plan, &daft_execution_config).unwrap(),
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
    _plan: LogicalPlanRef,
    _input_rx: Receiver<PartitionRef>,
    _daft_execution_config: Arc<DaftExecutionConfig>,
}

impl IntermediateTaskProducer {
    #[allow(dead_code)]
    pub fn new(
        plan: LogicalPlanRef,
        input_rx: Receiver<PartitionRef>,
        daft_execution_config: Arc<DaftExecutionConfig>,
    ) -> Self {
        Self {
            _plan: plan,
            _input_rx: input_rx,
            _daft_execution_config: daft_execution_config,
        }
    }
}

impl Stream for IntermediateTaskProducer {
    type Item = DaftResult<SwordfishTask>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
