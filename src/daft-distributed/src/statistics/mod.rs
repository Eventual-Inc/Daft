use std::sync::Arc;

use common_error::DaftResult;

use crate::scheduling::task::{TaskID, TaskName};

pub(crate) enum StatisticsEvent {
    TaskSubmitted {
        task_id: TaskID,
        task_name: TaskName,
    },
    TaskScheduled {
        task_id: TaskID,
    },
    TaskFinished {
        task_id: TaskID,
    },
}

pub(crate) trait StatisticsSubscriber: Send + Sync + 'static {
    fn handle_event(&self, event: &StatisticsEvent) -> DaftResult<()>;
}

pub(crate) type StatisticsManagerRef = Arc<StatisticsManager>;

#[derive(Default)]
pub(crate) struct StatisticsManager {
    subscribers: Vec<Box<dyn StatisticsSubscriber>>,
}

impl StatisticsManager {
    pub fn new(subscribers: Vec<Box<dyn StatisticsSubscriber>>) -> StatisticsManagerRef {
        Arc::new(Self { subscribers })
    }

    pub fn handle_event(&self, event: StatisticsEvent) -> DaftResult<()> {
        for subscriber in &self.subscribers {
            subscriber.handle_event(&event)?;
        }
        Ok(())
    }
}
