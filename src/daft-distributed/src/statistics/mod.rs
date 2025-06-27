use std::sync::Arc;

use common_error::DaftResult;

use crate::{
    plan::PlanID,
    scheduling::task::{TaskContext, TaskName},
};

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
    #[allow(dead_code)]
    PlanStarted {
        plan_id: PlanID,
    },
    PlanFinished {
        plan_id: PlanID,
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
