use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::{QueryID, snapshot::StatSnapshotImpl};
use daft_context::get_context;

use crate::{
    pipeline_node::NodeID,
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

        // Track started operators
        match event {
            TaskEvent::Submitted {
                context: task_ctx, ..
            } => {
                let mut started = self.started_operators.lock().unwrap();
                for node_id in &task_ctx.node_ids {
                    let node_id = *node_id as usize;
                    if !started.contains(&node_id) {
                        started.insert(node_id);
                    }
                }
            }
            TaskEvent::Completed {
                context: task_ctx, ..
            } => {
                let mut started = self.started_operators.lock().unwrap();
                for node_id in &task_ctx.node_ids {
                    let node_id = *node_id as usize;
                    if !started.contains(&node_id) {
                        started.insert(node_id);
                    }
                }
            }
            _ => {}
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

        // Send dashboard notifications
        match event {
            TaskEvent::Submitted {
                context: task_ctx, ..
            } => {
                // Notify about newly started operators
                let node_ids_to_notify = {
                    let started = self.started_operators.lock().unwrap();
                    task_ctx
                        .node_ids
                        .iter()
                        .map(|id| *id as usize)
                        .filter(|id| started.contains(id))
                        .collect::<Vec<_>>()
                };

                for node_id in node_ids_to_notify {
                    if let Err(e) =
                        context.notify_exec_operator_start(self.query_id.clone(), node_id)
                    {
                        tracing::error!("Failed to notify exec operator start: {}", e);
                    }
                }
            }
            TaskEvent::Completed { .. } => {
                // Read smart-aggregated stats from RuntimeNodeManagers
                // (already updated by StatisticsManager before this subscriber runs)
                if let Some(managers) = &self.runtime_node_managers {
                    let all_stats = managers
                        .values()
                        .flat_map(|mgr| {
                            let (info, snapshot) = mgr.export_snapshot();
                            let mut entries = vec![(info.node_origin_id, snapshot.to_stats())];
                            // Include per-phase stats with synthetic node IDs
                            for (phase_id, phase_snapshot) in mgr.export_phase_snapshots() {
                                entries.push((phase_id, phase_snapshot.to_stats()));
                            }
                            entries
                        })
                        .collect::<Vec<_>>();

                    if !all_stats.is_empty()
                        && let Err(e) =
                            context.notify_exec_emit_stats(self.query_id.clone(), all_stats)
                    {
                        tracing::error!("Failed to notify exec emit stats: {}", e);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}
