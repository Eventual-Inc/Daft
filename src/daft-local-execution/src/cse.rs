//! Common Subplan Elimination (CSE) execution-layer caching.
//!
//! When the logical optimizer detects identical subplans, it wraps them in
//! `CommonSubplan` nodes with a shared id. This module provides the runtime
//! machinery that computes such a subplan once (via [`CseCacheWriteNode`])
//! and streams the result to all consumers (via [`CseCacheReadNode`])
//! through per-reader unbounded mpsc channels.
//!
//! ## Architecture
//!
//! ```text
//!                       ┌──────────────────────┐
//!                       │    CseSharedCache    │
//!                       │  txs: Vec<Sender>    │
//!                       └───┬──────────────▲───┘
//!             fan-out to all /              │  register sender
//!                           │              │
//!     ┌──────────────────┐  │  ┌───────────┴──────────┐
//!     │ CseCacheWriteNode │──┘  │ CseCacheReadNode(s) │
//!     │ (transparent      │     │ (synthetic           │
//!     │  passthrough)     │     │  source(s))          │
//!     └──────────────────┘     └──────────────────────┘
//! ```
//!
//! Unlike the previous buffer-then-replay design:
//! - Zero pipeline stall: readers consume as the writer produces.
//! - No late-subscriber message loss: every reader registers its channel at
//!   pipeline-construction time (in [`CseCacheReadNode::new`]), strictly
//!   before any writer task can start fanning out.

use std::sync::{Arc, Mutex};

use common_display::tree::TreeDisplay;
use common_metrics::ops::{NodeCategory, NodeInfo, NodeType};
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;

use crate::{
    ExecutionRuntimeContext,
    pipeline::{BuilderContext, MorselSizeRequirement, PipelineMessage, PipelineNode},
};

// ---------------------------------------------------------------------------
// CseSharedCache
// ---------------------------------------------------------------------------

/// Fan-out cache: the writer sends each message to every registered reader
/// through an unbounded mpsc channel.  Readers register their own sender at
/// pipeline-construction time — before any writer task runs — so no message
/// can be published ahead of a registration (no late-subscriber loss) and
/// there is no pipeline stall.
pub(crate) struct CseSharedCache {
    txs: Mutex<Vec<tokio::sync::mpsc::UnboundedSender<PipelineMessage>>>,
    cse_id: usize,
}

impl CseSharedCache {
    pub fn new(cse_id: usize) -> Arc<Self> {
        Arc::new(Self {
            txs: Mutex::new(Vec::new()),
            cse_id,
        })
    }

    pub fn cse_id(&self) -> usize {
        self.cse_id
    }

    /// Send a message to all registered readers.  Returns immediately;
    /// unbounded channels never block the sender.
    pub fn fan_out(&self, msg: PipelineMessage) {
        let txs = self.txs.lock().unwrap();
        for tx in txs.iter() {
            let _ = tx.send(msg.clone());
        }
    }

    /// Register a reader and return its receiver.  The writer will fan out
    /// all messages to this channel.  Must be called before the writer task
    /// starts (i.e. at pipeline-construction time) so that no message is
    /// published before the registration.
    pub fn register_reader(&self) -> tokio::sync::mpsc::UnboundedReceiver<PipelineMessage> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.txs.lock().unwrap().push(tx);
        rx
    }
}

// ---------------------------------------------------------------------------
// CseCacheWriteNode
// ---------------------------------------------------------------------------

/// Pipeline node that wraps a child and transparently forwards its output
/// downstream while simultaneously publishing every message to all registered
/// [`CseCacheReadNode`]s via per-reader unbounded mpsc channels.
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
                stats_manager.activate_node(node_id);

                loop {
                    let msg = child_receiver.recv().await;
                    match msg {
                        Some(msg @ PipelineMessage::Morsel { .. })
                        | Some(msg @ PipelineMessage::Flush(_)) => {
                            shared.fan_out(msg.clone());
                            if dest_tx.send(msg).await.is_err() {
                                break;
                            }
                        }
                        Some(PipelineMessage::FlightPartitionRef { .. }) => {
                            unreachable!(
                                "CseCacheWriteNode should not receive flight partition refs"
                            )
                        }
                        None => break,
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

/// Source-like pipeline node that consumes messages from its dedicated
/// unbounded mpsc channel.
///
/// The channel is registered with the shared cache at construction time —
/// strictly before any [`CseCacheWriteNode`] task can run — so a writer that
/// finishes early can never publish past an unregistered reader.
pub(crate) struct CseCacheReadNode {
    shared: Arc<CseSharedCache>,
    rx: Option<tokio::sync::mpsc::UnboundedReceiver<PipelineMessage>>,
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
        // Register with the cache now, while the pipeline tree is still being
        // built and no writer task is running yet.
        let rx = shared.register_reader();
        Self {
            shared,
            rx: Some(rx),
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
        let mut this = self;
        let mut rx = this
            .rx
            .take()
            .expect("CseCacheReadNode can only be started once");

        let (dest_tx, dest_rx) = crate::channel::create_channel(1);
        let stats_manager = runtime_handle.stats_manager();

        runtime_handle.spawn(
            async move {
                stats_manager.activate_node(node_id);

                // Collect all messages first, then forward downstream.
                // This avoids interleaving CseCacheRead data with concurrent
                // CseCacheWrite data into the same downstream HashJoin —
                // which would deadlock when the join needs to build its
                // hash table from one side before processing the other.
                let mut buffer = Vec::new();
                while let Some(msg) = rx.recv().await {
                    let is_flush = matches!(msg, PipelineMessage::Flush(_));
                    buffer.push(msg);
                    if is_flush {
                        break;
                    }
                }

                for msg in buffer {
                    if dest_tx.send(msg).await.is_err() {
                        break;
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_and_receive() {
        let cache = CseSharedCache::new(1);
        let mut rx = cache.register_reader();

        use daft_micropartition::MicroPartition;
        let mp = MicroPartition::empty(None);
        let msg = PipelineMessage::Morsel {
            input_id: 0,
            partition: mp,
        };
        cache.fan_out(msg.clone());

        let received = rx.recv().await.unwrap();
        assert!(matches!(received, PipelineMessage::Morsel { .. }));
    }

    #[tokio::test]
    async fn test_multiple_readers() {
        let cache = CseSharedCache::new(1);
        let mut rx1 = cache.register_reader();
        let mut rx2 = cache.register_reader();

        use daft_micropartition::MicroPartition;
        let mp = MicroPartition::empty(None);
        cache.fan_out(PipelineMessage::Morsel {
            input_id: 0,
            partition: mp,
        });

        assert!(rx1.recv().await.is_some());
        assert!(rx2.recv().await.is_some());
    }

    #[tokio::test]
    async fn test_reader_registered_before_write_receives_all() {
        let cache = CseSharedCache::new(1);
        // Reader registers BEFORE writer sends anything
        let mut rx = cache.register_reader();

        use daft_micropartition::MicroPartition;
        let mp1 = MicroPartition::empty(None);
        let msg1 = PipelineMessage::Morsel {
            input_id: 0,
            partition: mp1,
        };
        cache.fan_out(msg1);
        cache.fan_out(PipelineMessage::Flush(0));

        assert!(rx.recv().await.is_some()); // Morsel
        assert!(rx.recv().await.is_some()); // Flush
        // Drop the cache so the registered sender is released and the
        // channel closes; otherwise recv() would wait forever.
        drop(cache);
        assert!(rx.recv().await.is_none()); // closed
    }

    #[tokio::test]
    async fn test_cse_id_is_preserved() {
        let cache = CseSharedCache::new(42);
        assert_eq!(cache.cse_id(), 42);
    }
}
