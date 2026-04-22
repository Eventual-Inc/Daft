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

#[allow(dead_code)]
pub(crate) struct TaskLifecycleEventSubscriber {
    context: DaftContext,
    query_id: QueryID,
}

#[allow(dead_code)]
impl TaskLifecycleEventSubscriber {
    pub fn new(query_id: QueryID) -> Self {
        let context = get_context();
        Self { context, query_id }
    }

    fn dispatch_event(&self, event: &Event) -> DaftResult<()> {
        self.context.notify_event(event)
    }
}

impl StatisticsSubscriber for TaskLifecycleEventSubscriber {
    fn handle_event(&mut self, event: &TaskEvent) -> DaftResult<()> {
        match event {
            TaskEvent::Submitted { context, .. } => {
                let submit_event = Event::TaskSubmit(TaskSubmitEvent {
                    header: event_header(self.query_id.clone()),
                    task: task_meta_from_context(context),
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
                    task: task_meta_from_context(context),
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
                        task: task_meta_from_context(context),
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
                    task: task_meta_from_context(context),
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

#[allow(dead_code)]
fn task_meta_from_context(context: &TaskContext) -> Arc<TaskMeta> {
    let meta = TaskMeta {
        id: context.task_id,
        node_ids: context.node_ids.clone(),
    };
    Arc::new(meta)
}
