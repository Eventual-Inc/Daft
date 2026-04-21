use std::sync::{Arc, atomic::Ordering};

use common_metrics::{
    Counter, Meter, StatSnapshot, TASK_ACTIVE_KEY, TASK_CANCELLED_KEY, TASK_COMPLETED_KEY,
    TASK_FAILED_KEY, UNIT_TASKS, UpDownCounter,
    ops::NodeInfo,
    snapshot::{DefaultSnapshot, StatSnapshotImpl as _},
};
use opentelemetry::KeyValue;

use crate::{
    pipeline_node::{PipelineNodeContext, metrics::key_values_from_context},
    statistics::TaskEvent,
};

pub trait RuntimeStats: Send + Sync + 'static {
    fn handle_worker_node_stats(&self, node_info: &NodeInfo, snapshot: &StatSnapshot);
    /// Returns the accumulated stats.
    fn export_snapshot(&self) -> StatSnapshot;
}
pub type RuntimeStatsRef = Arc<dyn RuntimeStats>;

pub struct RuntimeNodeManager {
    node_info: Arc<NodeInfo>,
    pub node_kv: Vec<KeyValue>,
    runtime_stats: RuntimeStatsRef,

    active_tasks: UpDownCounter,
    completed_tasks: Counter,
    failed_tasks: Counter,
    cancelled_tasks: Counter,
}

impl RuntimeNodeManager {
    pub fn new(meter: &Meter, runtime_stats: RuntimeStatsRef, node_info: Arc<NodeInfo>) -> Self {
        let node_kv = node_info.to_key_values();
        Self {
            node_info,
            node_kv,
            runtime_stats,
            active_tasks: meter.i64_up_down_counter(TASK_ACTIVE_KEY),
            completed_tasks: meter.u64_counter_with_desc_and_unit(
                TASK_COMPLETED_KEY,
                None,
                Some(UNIT_TASKS.into()),
            ),
            failed_tasks: meter.u64_counter_with_desc_and_unit(
                TASK_FAILED_KEY,
                None,
                Some(UNIT_TASKS.into()),
            ),
            cancelled_tasks: meter.u64_counter_with_desc_and_unit(
                TASK_CANCELLED_KEY,
                None,
                Some(UNIT_TASKS.into()),
            ),
        }
    }

    /// Returns the accumulated stats for this node as (NodeInfo, StatSnapshot) for export to the driver.
    pub fn export_snapshot(&self) -> (Arc<NodeInfo>, StatSnapshot) {
        (self.node_info.clone(), self.runtime_stats.export_snapshot())
    }

    fn dec_active_tasks(&self) {
        self.active_tasks.add(-1, self.node_kv.as_slice());
    }

    pub fn handle_task_event(&self, event: &TaskEvent) {
        match event {
            TaskEvent::Scheduled { .. } => {
                self.active_tasks.add(1, self.node_kv.as_slice());
            }
            TaskEvent::Completed { stats, .. } => {
                self.dec_active_tasks();
                self.completed_tasks.add(1, self.node_kv.as_slice());

                for (node_info, snapshot) in &stats.nodes {
                    // Local nodes are associated to this node through the node_origin_id
                    if let Some(node_origin_id) = node_info.node_origin_id {
                        if self.node_info.id == node_origin_id {
                            self.runtime_stats
                                .handle_worker_node_stats(node_info, snapshot);
                        }
                    } else {
                        tracing::debug!(
                            "local node stats missing `origin_node_id`, skipping attribution: {:?}",
                            node_info
                        );
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
    }
}

pub struct BaseCounters {
    duration_us: Counter,
    rows_in: Counter,
    rows_out: Counter,
    bytes_in: Counter,
    bytes_out: Counter,
    node_kv: Vec<KeyValue>,
}

impl BaseCounters {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        let node_kv = key_values_from_context(context);
        Self {
            duration_us: meter.duration_us_metric(),
            rows_in: meter.rows_in_metric(),
            rows_out: meter.rows_out_metric(),
            bytes_in: meter.bytes_in_metric(),
            bytes_out: meter.bytes_out_metric(),
            node_kv,
        }
    }

    pub fn add_duration_us(&self, v: u64) {
        self.duration_us.add(v, self.node_kv.as_slice());
    }

    pub fn add_rows_in(&self, v: u64) {
        self.rows_in.add(v, self.node_kv.as_slice());
    }

    pub fn add_rows_out(&self, v: u64) {
        self.rows_out.add(v, self.node_kv.as_slice());
    }

    pub fn add_bytes_in(&self, v: u64) {
        self.bytes_in.add(v, self.node_kv.as_slice());
    }

    pub fn add_bytes_out(&self, v: u64) {
        self.bytes_out.add(v, self.node_kv.as_slice());
    }

    pub fn export_default_snapshot(&self) -> StatSnapshot {
        StatSnapshot::Default(DefaultSnapshot {
            cpu_us: self.duration_us.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
        })
    }
}

pub struct DefaultRuntimeStats {
    base: BaseCounters,
}

impl DefaultRuntimeStats {
    pub fn new(meter: &Meter, context: &PipelineNodeContext) -> Self {
        Self {
            base: BaseCounters::new(meter, context),
        }
    }
}

impl RuntimeStats for DefaultRuntimeStats {
    fn handle_worker_node_stats(&self, _node_info: &NodeInfo, snapshot: &StatSnapshot) {
        self.base.add_duration_us(snapshot.duration_us());

        let StatSnapshot::Default(snapshot) = snapshot else {
            // TODO: Return immediately for now, but ideally should error
            return;
        };

        self.base.add_rows_in(snapshot.rows_in);
        self.base.add_rows_out(snapshot.rows_out);
        self.base.add_bytes_in(snapshot.bytes_in);
        self.base.add_bytes_out(snapshot.bytes_out);
    }

    fn export_snapshot(&self) -> StatSnapshot {
        self.base.export_default_snapshot()
    }
}

#[cfg(test)]
pub(super) mod tests {

    use std::{collections::HashMap, sync::Arc};

    use common_metrics::{
        Meter, NodeID, StatSnapshot,
        ops::{NodeCategory, NodeInfo, NodeType},
        snapshot::DefaultSnapshot,
    };
    use daft_local_plan::ExecutionStats;

    use super::{DefaultRuntimeStats, RuntimeNodeManager};
    use crate::{
        pipeline_node::PipelineNodeContext, scheduling::task::TaskContext, statistics::TaskEvent,
    };

    #[test]
    fn test_runtime_stats_origin_node_id() {
        struct Case {
            name: &'static str,
            node_id: usize,
            origin_node_id: usize,
            expected_cpu_us: u64,
            expected_rows_in: u64,
            expected_rows_out: u64,
            expected_bytes_in: u64,
            expected_bytes_out: u64,
        }

        let distributed_node_id: NodeID = 42;

        let cases = [
            Case {
                name: "matching",
                node_id: distributed_node_id,
                origin_node_id: distributed_node_id,
                expected_cpu_us: 100,
                expected_rows_in: 10,
                expected_rows_out: 5,
                expected_bytes_in: 1000,
                expected_bytes_out: 500,
            },
            Case {
                name: "non_matching",
                node_id: distributed_node_id,
                origin_node_id: 99,
                expected_cpu_us: 0,
                expected_rows_in: 0,
                expected_rows_out: 0,
                expected_bytes_in: 0,
                expected_bytes_out: 0,
            },
        ];

        for case in cases {
            let distributed_node_info = Arc::new(NodeInfo {
                id: case.node_id,
                name: "parent".into(),
                node_origin_id: None,
                node_type: NodeType::Filter,
                node_category: NodeCategory::Intermediate,
                node_phase: None,
                context: HashMap::new(),
            });

            let distributed_ctx = PipelineNodeContext::new(
                0,
                "q".into(),
                case.node_id as u32,
                "distributed-filter".into(),
                NodeType::Filter,
                NodeCategory::Intermediate,
            );

            let meter = Meter::test_scope("runtime-stats-test");
            let runtime_stats = Arc::new(DefaultRuntimeStats::new(&meter, &distributed_ctx));
            let manager = RuntimeNodeManager::new(&meter, runtime_stats, distributed_node_info);

            let local_node_info = Arc::new(NodeInfo {
                id: 7,
                node_origin_id: Some(case.origin_node_id),
                ..Default::default()
            });

            let local_snapshot = StatSnapshot::Default(DefaultSnapshot {
                cpu_us: 100,
                rows_in: 10,
                rows_out: 5,
                bytes_in: 1000,
                bytes_out: 500,
            });

            let event = TaskEvent::Completed {
                context: TaskContext::new(0, 42, 1, vec![42], 0),
                stats: ExecutionStats::new("".into(), vec![(local_node_info, local_snapshot)]),
            };

            manager.handle_task_event(&event);

            let (_info, actual) = manager.export_snapshot();
            let StatSnapshot::Default(got) = &actual else {
                panic!("{}: expected StatSnapshot::Default", case.name);
            };
            assert_eq!(got.cpu_us, case.expected_cpu_us, "{}: cpu_us", case.name);
            assert_eq!(got.rows_in, case.expected_rows_in, "{}: rows_in", case.name);
            assert_eq!(
                got.rows_out, case.expected_rows_out,
                "{}: rows_out",
                case.name
            );
            assert_eq!(
                got.bytes_in, case.expected_bytes_in,
                "{}: bytes_in",
                case.name
            );
            assert_eq!(
                got.bytes_out, case.expected_bytes_out,
                "{}: bytes_out",
                case.name
            );
        }
    }
}
