use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use daft_logical_plan::LogicalPlanRef;

use crate::scheduling::task::{TaskContext, TaskName, TaskStatus};

pub mod http_subscriber;
pub use http_subscriber::HttpSubscriber;

#[cfg(test)]
mod http_subscriber_test;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PlanState {
    pub plan_id: usize,
    pub query_id: String,
    pub logical_plan: LogicalPlanRef,
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

#[allow(dead_code)]
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
    FinishedTask {
        context: TaskContext,
    },
    TaskStarted {
        context: TaskContext,
    },
    TaskFailed {
        context: TaskContext,
        error: String,
    },
    TaskCanceled {
        context: TaskContext,
    },
    PlanStarted {
        plan_id: u32,
        plan_description: String,
    },
    PlanFinished {
        plan_id: u32,
    },
}

impl From<(TaskContext, &DaftResult<TaskStatus>)> for StatisticsEvent {
    fn from((context, result): (TaskContext, &DaftResult<TaskStatus>)) -> Self {
        match result {
            Ok(status) => match status {
                TaskStatus::Success { .. } => Self::FinishedTask { context },
                TaskStatus::Failed { error, .. } => Self::TaskFailed {
                    context,
                    error: error.to_string(),
                },
                TaskStatus::Cancelled => Self::TaskCanceled { context },
                TaskStatus::WorkerDied => Self::TaskFailed {
                    context,
                    error: "Worker died".to_string(),
                },
                TaskStatus::WorkerUnavailable => Self::TaskFailed {
                    context,
                    error: "Worker unavailable".to_string(),
                },
            },
            Err(e) => Self::TaskFailed {
                context,
                error: e.to_string(),
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

    pub fn register_plan(
        &self,
        plan_id: u32,
        query_id: String,
        logical_plan: LogicalPlanRef,
    ) -> DaftResult<()> {
        let mut plans = self.plans.lock().unwrap();
        plans.insert(
            plan_id,
            PlanState {
                plan_id: plan_id as usize,
                query_id,
                logical_plan,
            },
        );
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
            StatisticsEvent::TaskFailed { context, .. } => {
                if let Some(task_state) = tasks.get_mut(context) {
                    task_state.status = TaskExecutionStatus::Failed;
                    if task_state.pending > 0 {
                        task_state.pending -= 1;
                    }
                    task_state.failed += 1;
                }
            }
            StatisticsEvent::TaskCanceled { context } => {
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
