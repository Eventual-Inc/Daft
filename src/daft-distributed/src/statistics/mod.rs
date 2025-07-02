use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use common_error::DaftResult;
use daft_logical_plan::LogicalPlanRef;

use crate::scheduling::task::{TaskContext, TaskName, TaskStatus};

pub mod http_subscriber;
pub use http_subscriber::{HttpSubscriber, QueryGraph, QueryGraphNode, MetricDisplayInformation};

#[derive(Debug, Clone)]
pub struct PlanState {
    pub plan_id: u32,
    pub logical_plan: LogicalPlanRef,
    pub description: String,
}

#[derive(Debug, Clone)]
pub struct TaskState {
    pub name: TaskName,
    pub status: TaskExecutionStatus,
    pub pending: u32,
    pub completed: u32,
    pub canceled: u32,
    pub failed: u32,
    pub total: u32,
}

#[derive(Debug, Clone)]
pub enum TaskExecutionStatus {
    Created,
    Running,
    Completed,
    Failed,
    Canceled,
}

#[allow(clippy::enum_variant_names)]
pub(crate) enum StatisticsEvent {
    SubmittedTask {
        context: TaskContext,
        name: TaskName,
    },
    #[allow(dead_code)]
    ScheduledTask {
        context: TaskContext,
    },
    TaskStarted {
        context: TaskContext,
    },
    FinishedTask {
        context: TaskContext,
    },
    #[allow(dead_code)]
    FailedTask {
        context: TaskContext,
        reason: String,
    },
    CancelledTask {
        context: TaskContext,
    },
    PlanStarted {
        plan_id: u32,
        description: String,
    },
    PlanFinished {
        plan_id: u32,
        description: String,
    },
}

impl From<(TaskContext, &DaftResult<TaskStatus>)> for StatisticsEvent {
    fn from((context, result): (TaskContext, &DaftResult<TaskStatus>)) -> Self {
        match result {
            Ok(status) => match status {
                TaskStatus::Success { .. } => Self::FinishedTask { context },
                TaskStatus::Failed { error, .. } => Self::FailedTask {
                    context,
                    reason: error.to_string(),
                },
                TaskStatus::Cancelled => Self::CancelledTask { context },
                TaskStatus::WorkerDied => Self::FailedTask {
                    context,
                    reason: "Worker died".to_string(),
                },
                TaskStatus::WorkerUnavailable => Self::FailedTask {
                    context,
                    reason: "Worker unavailable".to_string(),
                },
            },
            Err(e) => Self::FailedTask {
                context,
                reason: e.to_string(),
            },
        }
    }
}

pub trait StatisticsSubscriber: Send + Sync + 'static {
    fn handle_event(
        &self,
        event: &StatisticsEvent,
        plans: &HashMap<u32, PlanState>,
        tasks: &HashMap<TaskContext, TaskState>,
    ) -> DaftResult<()>;
}

pub type StatisticsManagerRef = Arc<StatisticsManager>;

#[derive(Default)]
pub struct StatisticsManager {
    subscribers: Vec<Box<dyn StatisticsSubscriber>>,
    plans: Mutex<HashMap<u32, PlanState>>,
    tasks: Mutex<HashMap<TaskContext, TaskState>>,
}

impl StatisticsManager {
    pub fn new(subscribers: Vec<Box<dyn StatisticsSubscriber>>) -> StatisticsManagerRef {
        Arc::new(Self {
            subscribers,
            plans: Mutex::new(HashMap::new()),
            tasks: Mutex::new(HashMap::new()),
        })
    }

    pub fn register_plan(&self, plan_id: u32, logical_plan: LogicalPlanRef, description: String) -> DaftResult<()> {
        let mut plans = self.plans.lock().unwrap();
        plans.insert(plan_id, PlanState {
            plan_id,
            logical_plan,
            description,
        });
        Ok(())
    }

    pub fn handle_event(&self, event: StatisticsEvent) -> DaftResult<()> {
        // Update internal state based on event
        self.update_state(&event)?;

        // Get current state snapshots
        let plans = self.plans.lock().unwrap().clone();
        let tasks = self.tasks.lock().unwrap().clone();

        // Notify all subscribers
        for subscriber in &self.subscribers {
            subscriber.handle_event(&event, &plans, &tasks)?;
        }
        Ok(())
    }

    fn update_state(&self, event: &StatisticsEvent) -> DaftResult<()> {
        let mut tasks = self.tasks.lock().unwrap();

        match event {
            StatisticsEvent::SubmittedTask { context, name } => {
                let task_state = tasks.entry(*context).or_insert_with(|| TaskState {
                    name: name.clone(),
                    status: TaskExecutionStatus::Created,
                    pending: 0,
                    completed: 0,
                    canceled: 0,
                    failed: 0,
                    total: 0,
                });
                task_state.total += 1;
            }
            StatisticsEvent::ScheduledTask { context } => {
                if let Some(task_state) = tasks.get_mut(context) {
                    task_state.status = TaskExecutionStatus::Running;
                    task_state.pending += 1;
                }
            }
            StatisticsEvent::TaskStarted { context } => {
                if let Some(task_state) = tasks.get_mut(context) {
                    task_state.status = TaskExecutionStatus::Running;
                }
            }
            StatisticsEvent::FinishedTask { context } => {
                if let Some(task_state) = tasks.get_mut(context) {
                    task_state.status = TaskExecutionStatus::Completed;
                    if task_state.pending > 0 {
                        task_state.pending -= 1;
                    }
                    task_state.completed += 1;
                }
            }
            StatisticsEvent::FailedTask { context, .. } => {
                if let Some(task_state) = tasks.get_mut(context) {
                    task_state.status = TaskExecutionStatus::Failed;
                    if task_state.pending > 0 {
                        task_state.pending -= 1;
                    }
                    task_state.failed += 1;
                }
            }
            StatisticsEvent::CancelledTask { context } => {
                if let Some(task_state) = tasks.get_mut(context) {
                    task_state.status = TaskExecutionStatus::Canceled;
                    if task_state.pending > 0 {
                        task_state.pending -= 1;
                    }
                    task_state.canceled += 1;
                }
            }
            StatisticsEvent::PlanStarted { .. } | StatisticsEvent::PlanFinished { .. } => {
                // Plan-level events don't update task state
            }
        }

        Ok(())
    }
}
