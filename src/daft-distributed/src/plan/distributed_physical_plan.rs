//! The *actual* distributed physical plan for a Flotilla query.
//!
//! Unlike `DistributedPipeline` (which only describes the tree of pipeline nodes
//! the planner produced), `DistributedPhysicalPlan` is the aggregate of the
//! `LocalPhysicalPlan`s each pipeline node emits at runtime via
//! `produce_tasks()`. It is populated as execution proceeds and becomes
//! authoritative only once the query has finished.
//!
//! Entries are keyed by `(node_chain, plan_fingerprint)`, where `node_chain` is
//! the ordered list of distributed pipeline node IDs whose operators
//! contributed to the local plan (leaf source -> terminal producer). Two tasks
//! with the same key carry functionally identical local plans and are
//! collapsed into a single entry with an occurrence count.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use daft_local_plan::{Input, LocalPhysicalPlanRef, SourceId};
use serde::Serialize;

use crate::{
    pipeline_node::{NodeID, PlanFingerprint},
    scheduling::task::SwordfishTaskBuilder,
};

/// Summary of a single input source used by a local plan.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum InputSummary {
    ScanTasks { count: usize },
    GlobPaths { count: usize },
    FlightShuffle { count: usize },
    InMemory { count: usize },
}

impl From<&Input> for InputSummary {
    fn from(input: &Input) -> Self {
        match input {
            Input::ScanTasks(v) => Self::ScanTasks { count: v.len() },
            Input::GlobPaths(v) => Self::GlobPaths { count: v.len() },
            Input::FlightShuffle(v) => Self::FlightShuffle { count: v.len() },
            Input::InMemory(v) => Self::InMemory { count: v.len() },
        }
    }
}

/// One group of tasks that share an identical local plan shape, produced by
/// the same chain of distributed pipeline nodes.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DistributedPhysicalPlanEntry {
    /// Distributed pipeline node IDs whose operators composed this local plan.
    /// First entry is the deepest source; last entry is the terminal pipeline
    /// node that actually emitted the task. These are stable node IDs that
    /// match those used in the `DistributedPipeline` tree visualization.
    pub node_chain: Vec<NodeID>,
    /// Fingerprint identifying tasks with functionally identical plans. Pre-
    /// `build()`, so it does not fold in `query_idx`.
    pub plan_fingerprint: PlanFingerprint,
    /// Number of tasks with this exact (node_chain, plan_fingerprint) pair.
    pub count: usize,
    /// The local physical plan tree. `LocalPhysicalPlan` derives Serialize, so
    /// the frontend can render it directly.
    pub local_plan: LocalPhysicalPlanRef,
    /// Summary of task inputs per source id.
    pub inputs: HashMap<SourceId, InputSummary>,
    /// Number of partitions per in-memory source id.
    pub psets: HashMap<SourceId, usize>,
}

/// Aggregate of all local plans produced during execution of a Flotilla query.
/// Serialized to JSON for the dashboard.
#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct DistributedPhysicalPlan {
    pub entries: Vec<DistributedPhysicalPlanEntry>,
}

/// Thread-safe collector that observes each `SwordfishTaskBuilder` as it is
/// produced and accumulates it into a `DistributedPhysicalPlan`.
#[derive(Clone, Default)]
pub(crate) struct DistributedPhysicalPlanCollector {
    inner: Arc<Mutex<DistributedPhysicalPlan>>,
}

impl DistributedPhysicalPlanCollector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a builder without consuming it. Called once per task produced by
    /// the distributed pipeline, before the builder is turned into a
    /// `SubmittableTask`.
    pub fn observe(&self, builder: &SwordfishTaskBuilder) {
        let key_chain = builder.pending_node_ids();
        let key_fp = builder.plan_fingerprint();
        let mut inner = self.inner.lock().expect("collector poisoned");
        if let Some(entry) = inner.entries.iter_mut().find(|e| {
            e.plan_fingerprint == key_fp && e.node_chain.as_slice() == key_chain
        }) {
            entry.count += 1;
            return;
        }
        let inputs = builder
            .inputs()
            .iter()
            .map(|(src, input)| (*src, InputSummary::from(input)))
            .collect();
        let psets = builder
            .psets()
            .iter()
            .map(|(src, parts)| (*src, parts.len()))
            .collect();
        inner.entries.push(DistributedPhysicalPlanEntry {
            node_chain: key_chain.to_vec(),
            plan_fingerprint: key_fp,
            count: 1,
            local_plan: builder.plan().clone(),
            inputs,
            psets,
        });
    }

    /// Take a snapshot of the accumulated plan.
    pub fn snapshot(&self) -> DistributedPhysicalPlan {
        self.inner.lock().expect("collector poisoned").clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;

    use super::*;
    use crate::pipeline_node::tests::{MockNode, make_source_stream};

    /// Producing three tasks from the same MockNode (identical fingerprint +
    /// node chain) should collapse into one entry with count 3.
    #[tokio::test]
    async fn collapses_tasks_with_identical_fingerprint_and_chain() {
        let source = Arc::new(MockNode::new(42));
        let collector = DistributedPhysicalPlanCollector::new();
        let stream = make_source_stream(&source, 3);
        let collector_clone = collector.clone();
        let _fps: Vec<_> = stream
            .map(move |b| {
                collector_clone.observe(&b);
                b.fingerprint()
            })
            .collect()
            .await;
        let snap = collector.snapshot();
        assert_eq!(snap.entries.len(), 1);
        assert_eq!(snap.entries[0].count, 3);
        assert_eq!(snap.entries[0].node_chain, vec![42]);
    }

    /// Distinct fingerprints (via extend_fingerprint) should produce distinct
    /// entries keyed on (node_chain, fingerprint).
    #[tokio::test]
    async fn distinct_fingerprints_stay_separate() {
        let source = Arc::new(MockNode::new(1));
        let collector = DistributedPhysicalPlanCollector::new();
        let counter = std::sync::atomic::AtomicU32::new(0);
        let collector_clone = collector.clone();
        let _: Vec<_> = make_source_stream(&source, 4)
            .map(move |b| {
                let id = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                b.extend_fingerprint(id)
            })
            .map(move |b| {
                collector_clone.observe(&b);
                b.fingerprint()
            })
            .collect()
            .await;
        let snap = collector.snapshot();
        assert_eq!(snap.entries.len(), 4);
        for e in &snap.entries {
            assert_eq!(e.count, 1);
            assert_eq!(e.node_chain, vec![1]);
        }
    }
}
