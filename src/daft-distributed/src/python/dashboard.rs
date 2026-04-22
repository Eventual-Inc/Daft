use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::{
    QueryID, Stat, TASK_ACTIVE_KEY, TASK_CANCELLED_KEY, TASK_COMPLETED_KEY, TASK_FAILED_KEY,
    snapshot::StatSnapshotImpl,
};
use daft_context::get_context;

use crate::{
    pipeline_node::NodeID,
    scheduling::task::TaskContext,
    statistics::{StatisticsSubscriber, TaskEvent, stats::RuntimeNodeManager},
};

pub struct DashboardStatisticsSubscriber {
    query_id: QueryID,
    runtime_node_managers: Option<Arc<HashMap<NodeID, RuntimeNodeManager>>>,
    started_operators: Mutex<HashSet<usize>>,
    initialized_subscriber: Mutex<bool>,
}

impl DashboardStatisticsSubscriber {
    pub fn new(query_id: QueryID) -> Self {
        Self {
            query_id,
            runtime_node_managers: None,
            started_operators: Mutex::new(HashSet::new()),
            initialized_subscriber: Mutex::new(false),
        }
    }

    /// Emit the latest per-operator stats (snapshot metrics + task lifecycle
    /// counters) for every manager whose node participated in `task_ctx`.
    fn emit_stats_for(&self, task_ctx: &TaskContext) {
        let Some(managers) = &self.runtime_node_managers else {
            return;
        };
        let relevant_stats = managers
            .values()
            .map(|mgr| {
                let (info, snapshot) = mgr.export_snapshot();
                let mut stats = snapshot.to_stats();
                stats
                    .0
                    .push((TASK_ACTIVE_KEY.into(), Stat::Count(mgr.active_task_count())));
                stats.0.push((
                    TASK_COMPLETED_KEY.into(),
                    Stat::Count(mgr.completed_task_count()),
                ));
                stats
                    .0
                    .push((TASK_FAILED_KEY.into(), Stat::Count(mgr.failed_task_count())));
                stats.0.push((
                    TASK_CANCELLED_KEY.into(),
                    Stat::Count(mgr.cancelled_task_count()),
                ));
                // use `id` here because it's a distributed node
                // these nodes do not have an `origin_node_id`
                (info.id, stats)
            })
            .filter(|(node_id, _)| task_ctx.node_ids.contains(&(*node_id as u32)))
            .collect::<Vec<_>>();

        if !relevant_stats.is_empty()
            && let Err(e) =
                get_context().notify_exec_emit_stats(self.query_id.clone(), relevant_stats)
        {
            tracing::error!("Failed to notify exec emit stats: {}", e);
        }
    }
}

impl StatisticsSubscriber for DashboardStatisticsSubscriber {
    fn set_runtime_node_managers(&mut self, managers: Arc<HashMap<NodeID, RuntimeNodeManager>>) {
        self.runtime_node_managers = Some(managers);
    }

    fn handle_event(&mut self, event: &TaskEvent) -> DaftResult<()> {
        // Skip all dashboard functionality when RAY_DISABLE_DASHBOARD=1
        if std::env::var("RAY_DISABLE_DASHBOARD").as_deref() == Ok("1") {
            return Ok(());
        }

        // Only process events if dashboard URL is configured
        let should_notify = std::env::var("DAFT_DASHBOARD_URL").is_ok();
        if !should_notify {
            return Ok(());
        }

        // Initialize dashboard subscriber if needed
        let context = get_context();
        let should_initialize = {
            let init = self.initialized_subscriber.lock().unwrap();
            !*init
        };

        if should_initialize {
            match daft_context::subscribers::dashboard::DashboardSubscriber::try_new() {
                Ok(Some(sub)) => {
                    context.attach_subscriber("_dashboard".to_string(), Arc::new(sub));
                    let mut init = self.initialized_subscriber.lock().unwrap();
                    *init = true;
                }
                Ok(None) | Err(_) => {
                    // Mark as initialized to avoid repeated attempts
                    let mut init = self.initialized_subscriber.lock().unwrap();
                    *init = true;
                }
            }
        }

        // Send dashboard notifications. Lifecycle counters (task.active,
        // task.completed, task.failed, task.cancelled) are updated on the
        // manager by StatisticsManager before this subscriber runs, so we
        // emit stats for every event that changes a counter — otherwise
        // `task.active` wouldn't be visible until the first task ends.
        match event {
            TaskEvent::Submitted {
                context: task_ctx, ..
            } => {
                // Notify about newly started operators, avoiding duplicate notifications
                let mut started = self.started_operators.lock().unwrap();

                for node_id in &task_ctx.node_ids {
                    let node_id = *node_id as usize;
                    if started.insert(node_id)
                        // if insert returned false, short-circuit will skip notify
                        && let Err(e) =
                            context.notify_exec_operator_start(self.query_id.clone(), node_id)
                    {
                        tracing::error!("Failed to notify exec operator start: {}", e);
                    }
                }
            }
            TaskEvent::Scheduled { context: task_ctx }
            | TaskEvent::Completed {
                context: task_ctx, ..
            }
            | TaskEvent::Failed {
                context: task_ctx, ..
            }
            | TaskEvent::Cancelled { context: task_ctx } => {
                self.emit_stats_for(task_ctx);
            }
        }
        Ok(())
    }
}
