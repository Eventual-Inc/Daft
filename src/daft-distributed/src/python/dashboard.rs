use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::{QueryID, Stat, Stats};
use common_metrics::snapshot::StatSnapshotImpl;
use daft_context::get_context;

use crate::statistics::{StatisticsSubscriber, TaskEvent};

pub struct DashboardStatisticsSubscriber {
    query_id: QueryID,
    // Accumulate stats per operator across tasks: NodeID -> {StatName -> Stat}
    operator_stats: Mutex<HashMap<usize, HashMap<Arc<str>, Stat>>>,
    // Track which operators have been notified as started
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
        let context = get_context();
        
        {
            let mut init = self.initialized_subscriber.lock().unwrap();
            if !*init {
                // Try to attach dashboard subscriber if not present
                // We use try_new to check env var and connectivity
                if let Ok(Some(sub)) = daft_context::subscribers::dashboard::DashboardSubscriber::try_new() {
                    context.attach_subscriber("_dashboard".to_string(), Arc::new(sub));
                    tracing::info!("DashboardStatisticsSubscriber attached DashboardSubscriber to context");
                    eprintln!("DashboardStatisticsSubscriber attached DashboardSubscriber to context");
                } else {
                    tracing::warn!("DashboardStatisticsSubscriber failed to create DashboardSubscriber or env var not set");
                    eprintln!("DashboardStatisticsSubscriber failed to create DashboardSubscriber or env var not set");
                }
                *init = true;
            }
        }

        match event {
            TaskEvent::Submitted {
                context: task_ctx, ..
            } => {
                let mut started = self.started_operators.lock().unwrap();
                for node_id in &task_ctx.node_ids {
                    let node_id = *node_id as usize;
                    if !started.contains(&node_id) {
                        context.notify_exec_operator_start(self.query_id.clone(), node_id)?;
                        started.insert(node_id);
                    }
                }
            }
            TaskEvent::Completed { stats, .. } => {
                let mut accumulated = self.operator_stats.lock().unwrap();
                let mut started = self.started_operators.lock().unwrap();

                println!("Dashboard received Completed event with stats: {:?}", stats);

                for (node_id, task_stats) in stats {
                    // Mark operator as started if not already done
                    if !started.contains(node_id) {
                        context.notify_exec_operator_start(self.query_id.clone(), *node_id)?;
                        started.insert(*node_id);
                    }

                    let entry = accumulated.entry(*node_id).or_default();
                    // Merge task_stats into entry
                    for (key, stat) in task_stats.to_stats().0.iter() {
                        let mapped_key = match key.as_ref() {
                            "rows_in" => "rows in",
                            "rows_out" => "rows out",
                            "cpu_us" => "cpu us",
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

                // Emit current total stats to Dashboard
                tracing::debug!("Dashboard emitting accumulated stats: {:?}", accumulated);
                let all_stats: Vec<(usize, Stats)> = accumulated
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
                    .collect();

                context.notify_exec_emit_stats(self.query_id.clone(), all_stats)?;
            }
            _ => {
                // For now, we only emit on task completion
            }
        }
        Ok(())
    }
}
