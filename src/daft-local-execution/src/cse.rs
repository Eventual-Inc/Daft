//! Common Subplan Elimination (CSE) execution-layer caching.
//!
//! When the logical optimizer detects identical subplans, it wraps them in
//! `CommonSubplan` nodes with a shared id. This module provides the runtime
//! machinery that computes such a subplan once (via [`CseCacheWriteNode`])
//! and replays the cached result for subsequent consumers (via
//! [`CseCacheReadNode`]).
//!
//! ## Architecture
//!
//! ```text
//!                       ┌──────────────────┐
//!                       │  CseSharedCache  │
//!                       │  buffer: Vec<M>  │
//!                       │  done:  AtomicBool
//!                       │  notify: Notify  │
//!                       └───┬──────────▲───┘
//!                    push /  │          │  wait_done + replay
//!              mark_done /   │          │
//!     ┌──────────────────┐   │  ┌───────┴──────────┐
//!     │ CseCacheWriteNode │──┘  │ CseCacheReadNode │
//!     │ (transparent      │     │ (synthetic       │
//!     │  passthrough)     │     │  source)         │
//!     └──────────────────┘     └──────────────────┘
//! ```

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use common_display::tree::TreeDisplay;
use common_metrics::ops::{NodeCategory, NodeInfo, NodeType};
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartitionRef;
use tokio::sync::{Mutex, Notify};

use crate::{
    ExecutionRuntimeContext,
    pipeline::{BuilderContext, MorselSizeRequirement, PipelineMessage, PipelineNode},
};

// ---------------------------------------------------------------------------
// CseSharedCache
// ---------------------------------------------------------------------------

/// Shared cache between a [`CseCacheWriteNode`] and one or more
/// [`CseCacheReadNode`]s.
///
/// The writer buffers morsels via [`push_morsel`](Self::push_morsel) and
/// signals completion with [`mark_done`](Self::mark_done).  Readers call
/// [`wait_done`](Self::wait_done) and then [`replay`](Self::replay).
pub(crate) struct CseSharedCache {
    buffer: Mutex<Vec<MicroPartitionRef>>,
    notify: Notify,
    done: AtomicBool,
    cse_id: usize,
}

impl CseSharedCache {
    pub fn new(cse_id: usize) -> Arc<Self> {
        Arc::new(Self {
            buffer: Mutex::new(Vec::new()),
            notify: Notify::new(),
            done: AtomicBool::new(false),
            cse_id,
        })
    }

    pub fn cse_id(&self) -> usize {
        self.cse_id
    }

    /// Append a morsel to the shared buffer (called by the writer).
    pub async fn push_morsel(&self, morsel: MicroPartitionRef) {
        self.buffer.lock().await.push(morsel);
    }

    /// Signal that the writer is done producing morsels.
    ///
    /// Idempotent — safe to call multiple times.
    pub fn mark_done(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Wait until the writer has signalled completion.
    ///
    /// Returns immediately if [`mark_done`](Self::mark_done) has already been
    /// called.
    pub async fn wait_done(&self) {
        if self.done.load(Ordering::Acquire) {
            return;
        }
        loop {
            self.notify.notified().await;
            if self.done.load(Ordering::Acquire) {
                return;
            }
        }
    }

    /// Take a snapshot of all buffered morsels.
    ///
    /// Callers should invoke [`wait_done`](Self::wait_done) first to ensure
    /// the buffer is complete.
    pub async fn replay(&self) -> Vec<MicroPartitionRef> {
        self.buffer.lock().await.clone()
    }
}

// ---------------------------------------------------------------------------
// CseCacheWriteNode
// ---------------------------------------------------------------------------

/// Pipeline node that wraps a child and transparently forwards its output
/// downstream while simultaneously buffering morsels into a [`CseSharedCache`].
///
/// When the child emits a `Flush`, this node forwards the flush and signals
/// completion so that any [`CseCacheReadNode`] sharing the same cache can
/// replay the buffered data.
pub(crate) struct CseCacheWriteNode {
    child: Box<dyn PipelineNode>,
    shared: Arc<CseSharedCache>,
    node_info: Arc<NodeInfo>,
    plan_stats: StatsState,
    morsel_size_requirement: MorselSizeRequirement,
}

impl CseCacheWriteNode {
    pub fn new(
        child: Box<dyn PipelineNode>,
        shared: Arc<CseSharedCache>,
        plan_stats: StatsState,
        ctx: &BuilderContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = "CseCacheWrite".into();
        let info = ctx.next_node_info(
            name,
            NodeType::CseCacheWrite,
            NodeCategory::Intermediate,
            context,
        );
        Self {
            child,
            shared,
            node_info: Arc::new(info),
            plan_stats,
            morsel_size_requirement: MorselSizeRequirement::default(),
        }
    }

    pub fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

impl TreeDisplay for CseCacheWriteNode {
    fn id(&self) -> String {
        self.node_id().to_string()
    }

    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        let cse_id = self.shared.cse_id();
        match level {
            common_display::DisplayLevel::Compact => {
                writeln!(display, "CseCacheWrite(id={cse_id})").unwrap();
            }
            _ => {
                writeln!(display, "CseCacheWrite(id={cse_id})").unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {stats}").unwrap();
                }
            }
        }
        display
    }

    fn repr_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.node_id(),
            "category": "Intermediate",
            "type": "CseCacheWrite",
            "name": self.name(),
            "cse_id": self.shared.cse_id(),
        })
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![self.child.as_tree_display()]
    }
}

impl PipelineNode for CseCacheWriteNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![self.child.as_ref()]
    }

    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
        vec![&self.child]
    }

    fn name(&self) -> Arc<str> {
        self.node_info.name.clone()
    }

    fn propagate_morsel_size_requirement(
        &mut self,
        downstream_requirement: MorselSizeRequirement,
        default_requirement: MorselSizeRequirement,
    ) {
        self.morsel_size_requirement = downstream_requirement;
        self.child
            .propagate_morsel_size_requirement(downstream_requirement, default_requirement);
    }

    fn start(
        self: Box<Self>,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<crate::channel::Receiver<PipelineMessage>> {
        let node_id = self.node_id();
        let name = self.name();
        let shared = self.shared.clone();

        let mut child_receiver = self.child.start(maintain_order, runtime_handle)?;
        let (dest_tx, dest_rx) = crate::channel::create_channel(1);

        let stats_manager = runtime_handle.stats_manager();
        runtime_handle.spawn(
            async move {
                // Guard: ensure mark_done is called even on panic/error paths.
                struct DoneGuard(Arc<CseSharedCache>);
                impl Drop for DoneGuard {
                    fn drop(&mut self) {
                        self.0.mark_done();
                    }
                }
                let _guard = DoneGuard(shared.clone());

                stats_manager.activate_node(node_id);

                loop {
                    let msg = child_receiver.recv().await;
                    match msg {
                        Some(PipelineMessage::Morsel {
                            input_id,
                            partition,
                        }) => {
                            shared.push_morsel(Arc::new(partition.clone())).await;
                            if dest_tx
                                .send(PipelineMessage::Morsel {
                                    input_id,
                                    partition,
                                })
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Some(PipelineMessage::Flush(input_id)) => {
                            let _ = dest_tx.send(PipelineMessage::Flush(input_id)).await;
                            // DoneGuard calls mark_done() on drop.
                            break;
                        }
                        Some(PipelineMessage::FlightPartitionRef { .. }) => {
                            unreachable!(
                                "CseCacheWriteNode should not receive flight partition refs"
                            )
                        }
                        None => {
                            // Child closed without flush — DoneGuard calls
                            // mark_done() on drop.
                            break;
                        }
                    }
                }

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &name,
        );

        Ok(dest_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }

    fn node_id(&self) -> usize {
        self.node_info.id
    }

    fn node_info(&self) -> Arc<NodeInfo> {
        self.node_info.clone()
    }
}

// ---------------------------------------------------------------------------
// CseCacheReadNode
// ---------------------------------------------------------------------------

/// Source-like pipeline node that replays morsels buffered by a
/// [`CseCacheWriteNode`] sharing the same [`CseSharedCache`].
///
/// On [`start`](PipelineNode::start) it waits for the writer to signal
/// completion, then replays all buffered morsels downstream followed by
/// a `Flush`.  Morsels are always emitted with `input_id = 0` since this
/// is a synthetic source that does not receive injected inputs from the
/// runner.
pub(crate) struct CseCacheReadNode {
    shared: Arc<CseSharedCache>,
    node_info: Arc<NodeInfo>,
    plan_stats: StatsState,
}

impl CseCacheReadNode {
    pub fn new(
        shared: Arc<CseSharedCache>,
        _source_id: u32,
        plan_stats: StatsState,
        ctx: &BuilderContext,
        context: &LocalNodeContext,
    ) -> Self {
        let name: Arc<str> = "CseCacheRead".into();
        let info = ctx.next_node_info(name, NodeType::CseCacheRead, NodeCategory::Source, context);
        Self {
            shared,
            node_info: Arc::new(info),
            plan_stats,
        }
    }

    pub fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

impl TreeDisplay for CseCacheReadNode {
    fn id(&self) -> String {
        self.node_id().to_string()
    }

    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        let cse_id = self.shared.cse_id();
        match level {
            common_display::DisplayLevel::Compact => {
                writeln!(display, "CseCacheRead(id={cse_id})").unwrap();
            }
            _ => {
                writeln!(display, "CseCacheRead(id={cse_id})").unwrap();
                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {stats}").unwrap();
                }
            }
        }
        display
    }

    fn repr_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.node_id(),
            "category": "Source",
            "type": "CseCacheRead",
            "name": self.name(),
            "cse_id": self.shared.cse_id(),
        })
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }
}

impl PipelineNode for CseCacheReadNode {
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![]
    }

    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
        vec![]
    }

    fn name(&self) -> Arc<str> {
        self.node_info.name.clone()
    }

    fn propagate_morsel_size_requirement(
        &mut self,
        _downstream_requirement: MorselSizeRequirement,
        _default_requirement: MorselSizeRequirement,
    ) {
        // CseCacheRead replays pre-buffered morsels — morsel sizing
        // is determined by the writer, not by downstream requirements.
    }

    fn start(
        self: Box<Self>,
        _maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<crate::channel::Receiver<PipelineMessage>> {
        let node_id = self.node_id();
        let name = self.name();
        let shared = self.shared.clone();

        let (dest_tx, dest_rx) = crate::channel::create_channel(1);
        let stats_manager = runtime_handle.stats_manager();

        runtime_handle.spawn(
            async move {
                stats_manager.activate_node(node_id);

                shared.wait_done().await;

                let morsels = shared.replay().await;
                for morsel in morsels {
                    let owned = (*morsel).clone();
                    if dest_tx
                        .send(PipelineMessage::Morsel {
                            input_id: 0,
                            partition: owned,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }

                let _ = dest_tx.send(PipelineMessage::Flush(0)).await;

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &name,
        );

        Ok(dest_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }

    fn node_id(&self) -> usize {
        self.node_info.id
    }

    fn node_info(&self) -> Arc<NodeInfo> {
        self.node_info.clone()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_micropartition::MicroPartition;

    use super::*;

    fn make_morsel() -> MicroPartitionRef {
        Arc::new(MicroPartition::empty(None))
    }

    #[tokio::test]
    async fn test_mark_done_wakes_reader() {
        let cache = CseSharedCache::new(1);
        let cache_clone = cache.clone();

        let handle = tokio::spawn(async move {
            cache_clone.wait_done().await;
        });

        tokio::task::yield_now().await;

        cache.mark_done();
        handle
            .await
            .expect("reader should complete after mark_done");
    }

    #[tokio::test]
    async fn test_wait_done_returns_immediately_if_already_done() {
        let cache = CseSharedCache::new(1);
        cache.mark_done();
        cache.wait_done().await;
    }

    #[tokio::test]
    async fn test_push_then_replay() {
        let cache = CseSharedCache::new(1);
        let m1 = make_morsel();
        let m2 = make_morsel();

        cache.push_morsel(m1.clone()).await;
        cache.push_morsel(m2.clone()).await;
        cache.mark_done();

        let replayed = cache.replay().await;
        assert_eq!(replayed.len(), 2, "should replay all pushed morsels");
    }

    #[tokio::test]
    async fn test_multiple_readers_replay_same_data() {
        let cache = CseSharedCache::new(1);
        cache.push_morsel(make_morsel()).await;
        cache.push_morsel(make_morsel()).await;
        cache.mark_done();

        let r1 = cache.replay().await;
        let r2 = cache.replay().await;
        assert_eq!(r1.len(), r2.len());
        assert_eq!(r1.len(), 2);
    }

    #[tokio::test]
    async fn test_cse_id_is_preserved() {
        let cache = CseSharedCache::new(42);
        assert_eq!(cache.cse_id(), 42);
    }
}
