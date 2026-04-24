use std::sync::Arc;

use common_error::DaftResult;
use common_metrics::QueryID;
use daft_context::{
    DaftContext, get_context,
    subscribers::{
        event_header,
        events::{Event, TaskEndEvent, TaskMeta, TaskOutcome, TaskSubmitEvent},
    },
};

use crate::{
    scheduling::task::TaskContext,
    statistics::{StatisticsSubscriber, TaskEvent},
};

pub fn task_events_enabled() -> bool {
    if let Ok(val) = std::env::var("DAFT_TASK_EVENTS_ENABLED") {
        matches!(val.trim().to_lowercase().as_str(), "1" | "true")
    } else {
        false // Disabled by default; enable with DAFT_TASK_EVENTS_ENABLED=true
    }
}

pub(crate) struct TaskLifecycleEventSubscriber {
    context: DaftContext,
    query_id: QueryID,
}

impl TaskLifecycleEventSubscriber {
    pub fn new(query_id: QueryID) -> Self {
        let context = get_context();
        Self { context, query_id }
    }

    fn dispatch_event(&self, event: &Event) -> DaftResult<()> {
        self.context.notify(event)
    }
}

impl StatisticsSubscriber for TaskLifecycleEventSubscriber {
    fn handle_event(&mut self, event: &TaskEvent) -> DaftResult<()> {
        match event {
            TaskEvent::Submitted { context, name } => {
                let submit_event = Event::TaskSubmit(TaskSubmitEvent {
                    header: event_header(self.query_id.clone()),
                    task: task_meta_from_context(context, Some(name.clone())),
                });
                self.dispatch_event(&submit_event)
            }
            TaskEvent::Completed {
                context,
                stats,
                worker_id,
            } => {
                let end_event = Event::TaskEnd(TaskEndEvent {
                    header: event_header(self.query_id.clone()),
                    task: task_meta_from_context(context, None),
                    worker_id: Some(worker_id.clone()),
                    outcome: TaskOutcome::Success,
                    stats: stats.nodes.clone(),
                });
                self.dispatch_event(&end_event)
            }
            TaskEvent::Failed {
                context,
                reason,
                worker_id,
                retryable,
            } => {
                if *retryable {
                    Ok(())
                } else {
                    let end_event = Event::TaskEnd(TaskEndEvent {
                        header: event_header(self.query_id.clone()),
                        task: task_meta_from_context(context, None),
                        worker_id: worker_id.clone(),
                        outcome: TaskOutcome::Failed {
                            message: reason.into(),
                        },
                        stats: vec![],
                    });
                    self.dispatch_event(&end_event)
                }
            }
            TaskEvent::Cancelled { context } => {
                let end_event = Event::TaskEnd(TaskEndEvent {
                    header: event_header(self.query_id.clone()),
                    task: task_meta_from_context(context, None),
                    worker_id: None,
                    outcome: TaskOutcome::Cancelled,
                    stats: vec![],
                });
                self.dispatch_event(&end_event)
            }
            _ => Ok(()),
        }
    }
}

fn task_meta_from_context(context: &TaskContext, name: Option<String>) -> Arc<TaskMeta> {
    let meta = TaskMeta {
        id: context.task_id,
        origin_node_id: context.last_node_id,
        node_ids: context.node_ids.clone(),
        plan_fingerprint: context.plan_fingerprint,
        name: name.map(Arc::from),
    };
    Arc::new(meta)
}
