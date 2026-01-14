use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::{QueryID, Stat, StatSnapshot};
use daft_context::get_context;

use crate::statistics::{StatisticsSubscriber, TaskEvent};

pub struct DashboardStatisticsSubscriber {
    query_id: QueryID,
    // Accumulate stats per operator across tasks: NodeID -> {StatName -> Stat}
    operator_stats: Mutex<HashMap<usize, HashMap<Arc<str>, Stat>>>,
    // Track which operators have been notified as started
    started_operators: Mutex<HashSet<usize>>,
}

impl DashboardStatisticsSubscriber {
    pub fn new(query_id: QueryID) -> Self {
        Self {
            query_id,
            operator_stats: Mutex::new(HashMap::new()),
            started_operators: Mutex::new(HashSet::new()),
        }
    }
}

impl StatisticsSubscriber for DashboardStatisticsSubscriber {
    fn handle_event(&mut self, event: &TaskEvent) -> DaftResult<()> {
        let context = get_context();
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

                for (node_id, task_stats) in stats {
                    // Mark operator as started if not already done
                    if !started.contains(node_id) {
                        context.notify_exec_operator_start(self.query_id.clone(), *node_id)?;
                        started.insert(*node_id);
                    }

                    let entry = accumulated.entry(*node_id).or_default();
                    // Merge task_stats into entry
                    for (key, stat) in task_stats.iter() {
                        let mapped_key = match key {
                            "rows_in" => "rows in",
                            "rows_out" => "rows out",
                            "cpu_us" => "cpu us",
                            _ => key,
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
                let all_stats: Vec<(usize, StatSnapshot)> = accumulated
                    .iter()
                    .map(|(node_id, stats)| {
                        let snapshot = StatSnapshot(
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
