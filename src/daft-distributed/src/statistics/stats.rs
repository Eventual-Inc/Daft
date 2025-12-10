use std::collections::HashMap;

use common_error::DaftResult;
use common_metrics::{CPU_US_KEY, QueryID, ROWS_IN_KEY, ROWS_OUT_KEY, Stat, StatSnapshot};
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Meter, UpDownCounter},
};

use crate::{pipeline_node::NodeID, statistics::TaskEvent};

pub trait RuntimeStats: Send + Sync + 'static {
    fn handle_task_event(&self, event: &TaskEvent) -> DaftResult<()>;
}

#[allow(clippy::struct_field_names)]
pub struct DefaultRuntimeStats {
    node_id: NodeID,
    pub node_kv: Vec<KeyValue>,

    active_tasks: UpDownCounter<i64>,
    completed_tasks: Counter<u64>,
    failed_tasks: Counter<u64>,
    cancelled_tasks: Counter<u64>,

    completed_rows_in: Counter<u64>,
    completed_rows_out: Counter<u64>,
    completed_cpu_us: Counter<u64>,
}

impl DefaultRuntimeStats {
    pub fn new_impl(meter: &Meter, node_id: NodeID, query_id: QueryID) -> Self {
        let node_kv = vec![
            KeyValue::new("node_id", node_id.to_string()),
            KeyValue::new("query_id", query_id),
        ];
        Self {
            node_id,
            node_kv,
            active_tasks: meter
                .i64_up_down_counter("daft.distributed.node_stats.active_tasks")
                .build(),
            completed_tasks: meter
                .u64_counter("daft.distributed.node_stats.completed_tasks")
                .build(),
            failed_tasks: meter
                .u64_counter("daft.distributed.node_stats.failed_tasks")
                .build(),
            cancelled_tasks: meter
                .u64_counter("daft.distributed.node_stats.cancelled_tasks")
                .build(),
            completed_rows_in: meter
                .u64_counter("daft.distributed.node_stats.completed_rows_in")
                .build(),
            completed_rows_out: meter
                .u64_counter("daft.distributed.node_stats.completed_rows_out")
                .build(),
            completed_cpu_us: meter
                .u64_counter("daft.distributed.node_stats.completed_cpu_us")
                .build(),
        }
    }

    pub fn new(node_id: NodeID, query_id: QueryID) -> Self {
        Self::new_impl(
            &global::meter("daft.distributed.node_stats"),
            node_id,
            query_id,
        )
    }

    fn inc_active_tasks(&self) {
        self.active_tasks.add(1, self.node_kv.as_slice());
    }

    fn dec_active_tasks(&self) {
        self.active_tasks.add(-1, self.node_kv.as_slice());
    }

    fn handle_swordfish_node_stats(&self, stats: &StatSnapshot) {
        let stats = stats
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect::<HashMap<String, Stat>>();

        // Worker run-time
        let Some(Stat::Duration(cpu_us)) = stats.get(CPU_US_KEY) else {
            panic!("Worker run-time stat not found for node {}", self.node_id);
        };
        self.completed_cpu_us
            .add(cpu_us.as_micros() as u64, self.node_kv.as_slice());

        // Worker rows in
        // TODO: For source operators, use a special runtime stats
        if let Some(Stat::Count(rows_in)) = stats.get(ROWS_IN_KEY) {
            self.completed_rows_in
                .add(*rows_in, self.node_kv.as_slice());
        }

        // Worker rows out
        // TODO: For sink operators, use a special runtime stats
        if let Some(Stat::Count(rows_out)) = stats.get(ROWS_OUT_KEY) {
            self.completed_rows_out
                .add(*rows_out, self.node_kv.as_slice());
        }
    }
}

impl RuntimeStats for DefaultRuntimeStats {
    fn handle_task_event(&self, event: &TaskEvent) -> DaftResult<()> {
        match event {
            TaskEvent::Scheduled { .. } => {
                self.inc_active_tasks();
            }
            TaskEvent::Completed { stats, .. } => {
                self.dec_active_tasks();
                self.completed_tasks.add(1, self.node_kv.as_slice());

                // Iterate over all worker stats and extract ones relevant to this node
                for (stat_node_id, stats) in stats {
                    if *stat_node_id == (self.node_id as usize) {
                        self.handle_swordfish_node_stats(stats);
                    }
                }
            }
            TaskEvent::Failed { .. } => {
                self.dec_active_tasks();
                self.failed_tasks.add(1, self.node_kv.as_slice());
            }
            TaskEvent::Cancelled { .. } => {
                self.dec_active_tasks();
                self.cancelled_tasks.add(1, self.node_kv.as_slice());
            }
            TaskEvent::Submitted { .. } => (), // We don't track submitted tasks
        }

        Ok(())
    }
}
