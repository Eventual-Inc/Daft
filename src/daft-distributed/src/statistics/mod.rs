pub(crate) mod stats;
pub(crate) mod task_lifecycle;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use common_error::DaftResult;
use common_metrics::{Meter, QueryID, ops::NodeInfo};
use common_treenode::{TreeNode, TreeNodeRecursion};
use daft_context::{
    get_context,
    subscribers::{
        Event, event_header,
        events::{OperatorEndEvent, OperatorMeta, OperatorStartEvent},
    },
};
use daft_local_plan::ExecutionStats;
pub use stats::RuntimeStats;

use crate::{
    pipeline_node::{DistributedPipelineNode, NodeID},
    scheduling::{
        task::{TaskContext, TaskMetadata, TaskName, TaskStatus},
        worker::WorkerId,
    },
    statistics::stats::RuntimeNodeManager,
};

const STATISTICS_LOG_TARGET: &str = "DaftStatisticsManager";

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum TaskEvent {
    Submitted {
        context: TaskContext,
        name: TaskName,
        metadata: TaskMetadata,
    },
    Scheduled {
        context: TaskContext,
    },
    Completed {
        context: TaskContext,
        stats: ExecutionStats,
        worker_id: WorkerId,
    },
    Failed {
        context: TaskContext,
        reason: String,
        worker_id: Option<WorkerId>,
        retryable: bool,
    },
    Cancelled {
        context: TaskContext,
    },
}

impl TaskEvent {
    pub fn new(context: TaskContext, result: &DaftResult<TaskStatus>, worker_id: WorkerId) -> Self {
        match result {
            Ok(task_status) => match task_status {
                TaskStatus::Success { stats, .. } => Self::Completed {
                    context,
                    stats: stats.clone(),
                    worker_id,
                },
                TaskStatus::Failed { error } => Self::Failed {
                    context,
                    reason: error.to_string(),
                    worker_id: Some(worker_id),
                    retryable: false,
                },
                TaskStatus::Cancelled => Self::Cancelled { context },
                // WorkerDied and WorkerUnavailable are the only
                // task statuses that get retried in the dispatcher
                // src/daft-distributed/src/scheduling/dispatcher.rs
                TaskStatus::WorkerDied => Self::Failed {
                    context,
                    reason: "Worker died".to_string(),
                    worker_id: Some(worker_id),
                    retryable: true,
                },
                TaskStatus::WorkerUnavailable => Self::Failed {
                    context,
                    reason: "Worker unavailable".to_string(),
                    worker_id: Some(worker_id),
                    retryable: true,
                },
            },
            Err(error) => Self::Failed {
                context,
                reason: error.to_string(),
                worker_id: Some(worker_id),
                retryable: false,
            },
        }
    }

    pub fn context(&self) -> &TaskContext {
        match self {
            Self::Submitted { context, .. } => context,
            Self::Scheduled { context, .. } => context,
            Self::Completed { context, .. } => context,
            Self::Failed { context, .. } => context,
            Self::Cancelled { context, .. } => context,
        }
    }
}

pub trait StatisticsSubscriber: Send + Sync + 'static {
    fn handle_event(&mut self, event: &TaskEvent) -> DaftResult<()>;

    /// Called once during [`StatisticsManager`] construction, before any events
    /// are dispatched. Provides shared access to the runtime node managers for
    /// subscribers that need aggregated stats.
    fn set_runtime_node_managers(&mut self, _managers: Arc<HashMap<NodeID, RuntimeNodeManager>>) {}
}

pub type StatisticsManagerRef = Arc<StatisticsManager>;

/// One side of an operator-lifecycle transition observed by the
/// `StatisticsManager`. Kept in a buffer so the driver process can drain
/// and re-dispatch them on its own `DaftContext` — Flotilla execution
/// runs inside a Ray actor, so events fired through the actor's local
/// context never reach driver-side subscribers without IPC.
#[derive(Debug, Clone)]
pub struct PendingOperatorEvent {
    pub kind: PendingOperatorKind,
    pub query_id: QueryID,
    pub node_id: NodeID,
    pub name: Arc<str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingOperatorKind {
    Start,
    End,
}

#[derive(Default)]
pub struct StatisticsManager {
    runtime_node_managers: Arc<HashMap<NodeID, RuntimeNodeManager>>,
    subscribers: Mutex<Vec<Box<dyn StatisticsSubscriber>>>,
    /// Query id used as the header on `OperatorStart` / `OperatorEnd`
    /// events. Empty for the `Default` impl used in tests that don't
    /// exercise the global event bus.
    query_id: QueryID,
    /// Buffered operator-lifecycle events for cross-process delivery to
    /// the driver. Drained by `take_pending_operator_events`.
    pending_operator_events: Mutex<Vec<PendingOperatorEvent>>,
}

impl StatisticsManager {
    pub fn from_pipeline_node(
        pipeline_node: &DistributedPipelineNode,
        mut subscribers: Vec<Box<dyn StatisticsSubscriber>>,
        meter: &Meter,
        query_id: QueryID,
    ) -> DaftResult<StatisticsManagerRef> {
        let mut runtime_node_managers = HashMap::new();
        pipeline_node.apply(|node| {
            let node_info = Arc::new(NodeInfo {
                name: node.name(),
                id: node.node_id() as usize,
                node_origin_id: None,
                node_type: node.context().node_type.clone(),
                node_category: node.context().node_category.clone(),
                node_phase: None,
                context: HashMap::new(),
            });
            runtime_node_managers.insert(
                node.node_id(),
                RuntimeNodeManager::new(meter, node.runtime_stats(), node_info),
            );
            Ok(TreeNodeRecursion::Continue)
        })?;

        let runtime_node_managers = Arc::new(runtime_node_managers);

        for subscriber in &mut subscribers {
            subscriber.set_runtime_node_managers(runtime_node_managers.clone());
        }

        Ok(Arc::new(Self {
            runtime_node_managers,
            subscribers: Mutex::new(subscribers),
            query_id,
            pending_operator_events: Mutex::new(Vec::new()),
        }))
    }

    /// Drain and return all operator-lifecycle events buffered since the
    /// last call. Used by the driver-side Python wrapper to relay events
    /// across the Ray actor boundary into the driver's `DaftContext`.
    pub fn take_pending_operator_events(&self) -> Vec<PendingOperatorEvent> {
        let mut buf = self.pending_operator_events.lock().unwrap();
        std::mem::take(&mut *buf)
    }

    pub fn handle_event(&self, event: TaskEvent) -> DaftResult<()> {
        // First, drive per-node lifecycle (Start/End) events. We do this
        // *before* `RuntimeNodeManager::handle_task_event` so that
        // `OperatorStart` is dispatched while subscribers see no task
        // counters yet, mirroring the local-execution ordering.
        match &event {
            TaskEvent::Submitted { context, .. } => {
                for node_id in &context.node_ids {
                    let mgr = self
                        .runtime_node_managers
                        .get(node_id)
                        .expect("No runtime stats found for node");
                    if mgr.on_task_submitted() {
                        self.dispatch_operator_start(mgr.node_info());
                    }
                }
            }
            TaskEvent::Completed { context, .. }
            | TaskEvent::Failed { context, .. }
            | TaskEvent::Cancelled { context } => {
                for node_id in &context.node_ids {
                    let mgr = self
                        .runtime_node_managers
                        .get(node_id)
                        .expect("No runtime stats found for node");
                    if mgr.on_task_finished() {
                        self.dispatch_operator_end(mgr.node_info());
                    }
                }
            }
            TaskEvent::Scheduled { .. } => {}
        }

        for node_id in &event.context().node_ids {
            let node_manager = self
                .runtime_node_managers
                .get(node_id)
                .expect("No runtime stats found for node");
            node_manager.handle_task_event(&event);
        }

        let mut subscribers = self.subscribers.lock().unwrap();
        for (i, subscriber) in subscribers.iter_mut().enumerate() {
            tracing::debug!(target: STATISTICS_LOG_TARGET, "StatisticsManager calling subscriber {}", i);
            subscriber.handle_event(&event)?;
        }
        Ok(())
    }

    /// Mark a distributed pipeline node as done producing tasks. Called
    /// when the `TaskBuilderStream` returned by that node's `produce_tasks`
    /// has been fully drained or dropped. If all in-flight tasks for the
    /// node have already finished, this fires `OperatorEnd`.
    pub fn notify_produce_complete(&self, node_id: NodeID) {
        let Some(mgr) = self.runtime_node_managers.get(&node_id) else {
            return;
        };
        if mgr.on_produce_complete() {
            self.dispatch_operator_end(mgr.node_info());
        }
    }

    /// Best-effort end-of-query flush: fire a synthetic `OperatorEnd` for
    /// every node that fired `OperatorStart` but hasn't yet emitted End.
    /// Use this on query teardown / shutdown paths where in-flight tasks
    /// may have been aborted without producing terminal `TaskEvent`s
    /// (which would have decremented `pending_tasks` and triggered End
    /// naturally). Idempotent — already-ended nodes are skipped.
    pub fn flush_started_operators(&self) {
        for mgr in self.runtime_node_managers.values() {
            if mgr.force_end() {
                self.dispatch_operator_end(mgr.node_info());
            }
        }
    }

    fn dispatch_operator_start(&self, node_info: &Arc<NodeInfo>) {
        let meta = Arc::new(OperatorMeta::from(node_info.as_ref()));
        // Fire on the local (actor-process) context so dashboard /
        // event-log subscribers running in the actor see the event.
        let event = Event::OperatorStart(OperatorStartEvent {
            header: event_header(self.query_id.clone()),
            operator: meta.clone(),
        });
        if let Err(e) = get_context().notify(&event) {
            tracing::error!("Failed to notify operator start: {}", e);
        }
        // Also buffer for the driver to drain and re-dispatch.
        self.pending_operator_events
            .lock()
            .unwrap()
            .push(PendingOperatorEvent {
                kind: PendingOperatorKind::Start,
                query_id: self.query_id.clone(),
                node_id: meta.node_id as NodeID,
                name: meta.name.clone(),
            });
    }

    fn dispatch_operator_end(&self, node_info: &Arc<NodeInfo>) {
        let meta = Arc::new(OperatorMeta::from(node_info.as_ref()));
        let event = Event::OperatorEnd(OperatorEndEvent {
            header: event_header(self.query_id.clone()),
            operator: meta.clone(),
        });
        if let Err(e) = get_context().notify(&event) {
            tracing::error!("Failed to notify operator end: {}", e);
        }
        self.pending_operator_events
            .lock()
            .unwrap()
            .push(PendingOperatorEvent {
                kind: PendingOperatorKind::End,
                query_id: self.query_id.clone(),
                node_id: meta.node_id as NodeID,
                name: meta.name.clone(),
            });
    }

    /// Collects accumulated stats from each node manager and returns them as an
    /// ExecutionEngineFinalResult for export to the driver (e.g. after the partition stream is done).
    pub fn export_metrics(&self) -> ExecutionStats {
        let nodes: Vec<(Arc<NodeInfo>, _)> = self
            .runtime_node_managers
            .values()
            .map(RuntimeNodeManager::export_snapshot)
            .collect();
        ExecutionStats::new("".into(), nodes)
    }
}
