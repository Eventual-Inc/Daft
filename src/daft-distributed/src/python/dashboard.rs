use std::{
    collections::HashMap,
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
    initialized_subscriber: Mutex<bool>,
    /// Whether a dashboard was configured when this subscriber was created.
    /// Resolved once so task events do not hit the filesystem each time.
    dashboard_configured: bool,
}

impl DashboardStatisticsSubscriber {
    pub fn new(query_id: QueryID) -> Self {
        // Only process events if a dashboard is configured, either explicitly
        // or through the URL published by `daft dashboard start`.
        let dashboard_configured =
            daft_context::subscribers::dashboard::resolve_dashboard_url().is_some();
        Self {
            query_id,
            runtime_node_managers: None,
            initialized_subscriber: Mutex::new(false),
            dashboard_configured,
        }
    }

    /// Emit the latest per-operator stats (snapshot metrics + task lifecycle
    /// counters) for every manager whose node participated in `task_ctx`.
    fn emit_stats_for(&self, task_ctx: &TaskContext) {
        let Some(managers) = &self.runtime_node_managers else {
            return;
        };
        // Filter by participating node first so we only snapshot + build
        // Stats for managers this task actually touched — O(participating)
        // rather than O(all managers) per event.
        let relevant_stats = managers
            .values()
            .filter(|mgr| task_ctx.node_ids.contains(&(mgr.node_id() as u32)))
            .map(|mgr| {
                let (info, snapshot) = mgr.export_snapshot();
                let mut stats = snapshot.to_stats();
                // `task.count` is already in `stats` via `snapshot.to_stats()`:
                // it's the per-operator attributed work counter (incremented
                // once per distributed task whose worker snapshots matched
                // this node's origin). The four `task.*` counters appended
                // below are distinct — they are scheduler-lifecycle state
                // on the manager, incremented for every task whose
                // `context.node_ids` includes this node regardless of origin
                // attribution. So `task.completed >= task.count` at any
                // given node.
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

        // Only process events if a dashboard was configured at startup.
        if !self.dashboard_configured {
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
        // OperatorStart / OperatorEnd are emitted centrally by
        // `StatisticsManager` (independent of dashboard configuration),
        // so this subscriber only needs to push stats snapshots.
        match event {
            TaskEvent::Submitted {
                context: task_ctx, ..
            }
            | TaskEvent::Scheduled {
                context: task_ctx, ..
            }
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
