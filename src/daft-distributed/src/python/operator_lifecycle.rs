use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};

use common_error::DaftResult;

use crate::{
    pipeline_node::NodeID,
    statistics::{StatisticsSubscriber, TaskEvent},
};

/// A driver-bound notification describing an operator lifecycle transition
/// observed in the actor that runs the distributed plan. Drained over Ray
/// so the driver can dispatch on its own subscriber list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OperatorLifecycleEvent {
    Start { node_id: NodeID },
    End { node_id: NodeID },
}

#[derive(Default, Debug)]
struct LifecycleState {
    /// Nodes that have had at least one task submitted (already emitted Start).
    started: HashSet<NodeID>,
    /// Nodes for which we've already emitted End — guards against re-emit
    /// if a stale task completion races finalize().
    ended: HashSet<NodeID>,
    /// In-flight task count keyed by node id. A task contributes to every node
    /// listed in its `node_ids` vector.
    active_counts: HashMap<NodeID, u32>,
    /// FIFO of events the driver hasn't drained yet.
    pending_events: VecDeque<OperatorLifecycleEvent>,
}

/// Shared queue of operator lifecycle events. The subscriber writes; the
/// Python-facing partition stream drains. Cheap to clone (Arc).
#[derive(Clone, Default)]
pub(crate) struct OperatorLifecycleQueue {
    state: Arc<Mutex<LifecycleState>>,
}

impl OperatorLifecycleQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn drain(&self) -> Vec<OperatorLifecycleEvent> {
        let mut state = self.state.lock().unwrap();
        state.pending_events.drain(..).collect()
    }

    /// Emit OperatorEnd for every node that started but hasn't ended yet. Call
    /// this once the distributed plan is fully drained — usually right after
    /// the partition stream returns its final partition.
    pub fn finalize_remaining(&self) {
        let mut state = self.state.lock().unwrap();
        let to_end: Vec<NodeID> = state
            .started
            .iter()
            .filter(|n| !state.ended.contains(n))
            .copied()
            .collect();
        for node_id in to_end {
            state
                .pending_events
                .push_back(OperatorLifecycleEvent::End { node_id });
            state.ended.insert(node_id);
        }
    }

    fn record_submitted(&self, node_ids: &[NodeID]) {
        let mut state = self.state.lock().unwrap();
        for &node_id in node_ids {
            *state.active_counts.entry(node_id).or_insert(0) += 1;
            if state.started.insert(node_id) {
                state
                    .pending_events
                    .push_back(OperatorLifecycleEvent::Start { node_id });
            }
        }
    }

    fn record_terminal(&self, node_ids: &[NodeID]) {
        let mut state = self.state.lock().unwrap();
        for &node_id in node_ids {
            if let Some(c) = state.active_counts.get_mut(&node_id) {
                *c = c.saturating_sub(1);
            }
        }
    }
}

/// Subscribes to scheduler task events and translates them into a stream of
/// operator lifecycle events for the driver. Start fires on the first
/// submission for a node; End is deferred to `finalize_remaining` so that
/// transient `active == 0` valleys (e.g. between stages) don't produce
/// spurious End→Start oscillation.
pub(crate) struct OperatorLifecycleEventSubscriber {
    queue: OperatorLifecycleQueue,
}

impl OperatorLifecycleEventSubscriber {
    pub fn new(queue: OperatorLifecycleQueue) -> Self {
        Self { queue }
    }
}

impl StatisticsSubscriber for OperatorLifecycleEventSubscriber {
    fn handle_event(&mut self, event: &TaskEvent) -> DaftResult<()> {
        let context = event.context();
        match event {
            TaskEvent::Submitted { .. } => {
                self.queue.record_submitted(&context.node_ids);
            }
            TaskEvent::Completed { .. }
            | TaskEvent::Failed { .. }
            | TaskEvent::Cancelled { .. } => {
                self.queue.record_terminal(&context.node_ids);
            }
            TaskEvent::Scheduled { .. } => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use daft_local_plan::ExecutionStats;

    use super::*;
    use crate::scheduling::task::TaskContext;

    fn submitted(node_ids: Vec<NodeID>) -> TaskEvent {
        TaskEvent::Submitted {
            context: TaskContext::new(0, *node_ids.last().unwrap_or(&0), 0, node_ids, 0),
            name: "t".into(),
            metadata: Default::default(),
        }
    }

    fn completed(node_ids: Vec<NodeID>) -> TaskEvent {
        TaskEvent::Completed {
            context: TaskContext::new(0, *node_ids.last().unwrap_or(&0), 0, node_ids, 0),
            stats: ExecutionStats::new("q".into(), vec![]),
            worker_id: "w".into(),
        }
    }

    #[test]
    fn start_fires_once_per_node_on_first_submission() {
        let queue = OperatorLifecycleQueue::new();
        let mut sub = OperatorLifecycleEventSubscriber::new(queue.clone());

        sub.handle_event(&submitted(vec![1, 2])).unwrap();
        sub.handle_event(&submitted(vec![1, 2])).unwrap(); // dup

        let drained = queue.drain();
        assert_eq!(drained.len(), 2);
        assert!(matches!(drained[0], OperatorLifecycleEvent::Start { node_id: 1 }));
        assert!(matches!(drained[1], OperatorLifecycleEvent::Start { node_id: 2 }));
    }

    #[test]
    fn finalize_emits_end_for_started_nodes() {
        let queue = OperatorLifecycleQueue::new();
        let mut sub = OperatorLifecycleEventSubscriber::new(queue.clone());

        sub.handle_event(&submitted(vec![1, 2])).unwrap();
        sub.handle_event(&completed(vec![1, 2])).unwrap();
        let _ = queue.drain(); // discard Start events

        queue.finalize_remaining();
        let drained = queue.drain();
        assert_eq!(drained.len(), 2);
        assert!(matches!(drained[0], OperatorLifecycleEvent::End { .. }));
        assert!(matches!(drained[1], OperatorLifecycleEvent::End { .. }));

        // Idempotent
        queue.finalize_remaining();
        assert!(queue.drain().is_empty());
    }
}
