use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::{
    QueryID, ROWS_IN_KEY, ROWS_OUT_KEY, Stat, Stats, TASK_DURATION_KEY, snapshot::StatSnapshotImpl,
};
use daft_context::get_context;

use crate::statistics::{StatisticsSubscriber, TaskEvent};

pub struct DashboardStatisticsSubscriber {
    query_id: QueryID,
    operator_stats: Mutex<HashMap<usize, HashMap<Arc<str>, Stat>>>,
    started_operators: Mutex<HashSet<usize>>,
    initialized_subscriber: Mutex<bool>,
}

impl DashboardStatisticsSubscriber {
    pub fn new(query_id: QueryID) -> Self {
        Self {
            query_id,
            operator_stats: Mutex::new(HashMap::new()),
            started_operators: Mutex::new(HashSet::new()),
            initialized_subscriber: Mutex::new(false),
        }
    }
}

impl StatisticsSubscriber for DashboardStatisticsSubscriber {
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

        // Accumulate statistics first
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
            TaskEvent::Completed { stats, .. } => {
                let mut accumulated = self.operator_stats.lock().unwrap();
                let mut started = self.started_operators.lock().unwrap();

                for (node_info, task_stats) in &stats.nodes {
                    let node_id = node_info.id;
                    if !started.contains(&node_id) {
                        started.insert(node_id);
                    }

                    let entry = accumulated.entry(node_id).or_default();
                    let task_stats = task_stats.to_stats();
                    for (key, stat) in &task_stats.0 {
                        let mapped_key = match key.as_ref() {
                            "rows_in" => ROWS_IN_KEY,
                            "rows_out" => ROWS_OUT_KEY,
                            "duration_us" => TASK_DURATION_KEY,
                            _ => key.as_ref(),
                        };
                        let arc_key: Arc<str> = Arc::from(mapped_key);
                        match entry.entry(arc_key) {
                            std::collections::hash_map::Entry::Occupied(mut e) => {
                                let current_stat = e.get_mut();
                                match (current_stat, stat) {
                                    (Stat::Count(c1), Stat::Count(c2)) => *c1 += *c2,
                                    (Stat::Bytes(b1), Stat::Bytes(b2)) => *b1 += *b2,
                                    (Stat::Duration(d1), Stat::Duration(d2)) => *d1 += *d2,
                                    _ => {}
                                }
                            }
                            std::collections::hash_map::Entry::Vacant(e) => {
                                e.insert(stat.clone());
                            }
                        }
                    }
                }
            }
            _ => {
                // Only process submitted and completed events for now
            }
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
                    // Handle notifications non-blockingly
                    if let Err(e) =
                        context.notify_exec_operator_start(self.query_id.clone(), node_id)
                    {
                        tracing::error!("Failed to notify exec operator start: {}", e);
                    }
                }
            }
            TaskEvent::Completed { .. } => {
                // Send accumulated statistics to dashboard
                let all_stats = {
                    let accumulated = self.operator_stats.lock().unwrap();
                    accumulated
                        .iter()
                        .map(|(node_id, stats)| {
                            let snapshot = Stats(
                                stats
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect::<smallvec::SmallVec<[(Arc<str>, Stat); 3]>>(),
                            );
                            (*node_id, snapshot)
                        })
                        .collect::<Vec<(usize, Stats)>>()
                };

                // Send the stats notification
                if !all_stats.is_empty()
                    && let Err(e) = context.notify_exec_emit_stats(self.query_id.clone(), all_stats)
                {
                    tracing::error!("Failed to notify exec emit stats: {}", e);
                }
            }
            _ => {
                // For now, we only emit notifications for submitted and completed events
            }
        }
        Ok(())
    }
}
