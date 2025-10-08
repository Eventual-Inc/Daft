pub mod progress_bar;

use std::sync::{Arc, Mutex};

use common_metrics::QueryID;
use common_error::DaftResult;
use daft_context::{Subscribers, Subscriber as _};
use futures::future;

use crate::{
    scheduling::task::{TaskContext, TaskName, TaskStatus},
};

pub use progress_bar::ProgressBar;


#[derive(Debug, Clone)]
#[allow(dead_code)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum TaskEvent {
    TaskSubmitted {
        context: TaskContext,
        name: TaskName,
    },

    TaskScheduled {
        context: TaskContext,
    },
    TaskCompleted {
        context: TaskContext,
    },
    TaskStarted {
        context: TaskContext,
    },
    TaskFailed {
        context: TaskContext,
        reason: String,
    },
    TaskCancelled {
        context: TaskContext,
    },
}

impl From<(TaskContext, &DaftResult<TaskStatus>)> for TaskEvent {
    fn from((context, task_result): (TaskContext, &DaftResult<TaskStatus>)) -> Self {
        match task_result {
            Ok(task_status) => match task_status {
                TaskStatus::Success { .. } => Self::TaskCompleted { context },
                TaskStatus::Failed { error } => Self::TaskFailed {
                    context,
                    reason: error.to_string(),
                },
                TaskStatus::Cancelled => Self::TaskCancelled { context },
                TaskStatus::WorkerDied => Self::TaskFailed {
                    context,
                    reason: "Worker died".to_string(),
                },
                TaskStatus::WorkerUnavailable => Self::TaskFailed {
                    context,
                    reason: "Worker unavailable".to_string(),
                },
            },
            Err(error) => Self::TaskFailed {
                context,
                reason: error.to_string(),
            },
        }
    }
}

pub type StatisticsManagerRef = Arc<StatisticsManager>;

#[derive(Default)]
pub struct StatisticsManager {
    /// Ray progress bar
    pbar: Mutex<Option<ProgressBar>>,
    /// Global subscribers
    subscribers: Vec<Arc<Subscribers>>,
}

impl StatisticsManager {
    pub fn new(pbar: Option<ProgressBar>, subscribers: Vec<Arc<Subscribers>>) -> StatisticsManagerRef {

        Arc::new(Self {
            pbar: Mutex::new(pbar),
            subscribers,
        })
    }

    pub fn handle_event(&self, event: TaskEvent) -> DaftResult<()> {
        let mut pbar = self.pbar.lock().unwrap();
        if let Some(pbar) = pbar.as_mut() {
            pbar.handle_event(&event)?;
        }
        Ok(())
    }

    pub fn handle_start(&self, query_id: QueryID) -> DaftResult<()> {
        for subscriber in self.subscribers.iter() {
            subscriber.on_exec_start(query_id.clone(), &[])?;
        }
        Ok(())
    }

    pub async fn handle_finish(&self, query_id: QueryID) -> DaftResult<()> {
        future::try_join_all(self.subscribers.iter().map(|subscriber| subscriber.on_exec_end(query_id.clone()))).await?;
        Ok(())
    }
}
