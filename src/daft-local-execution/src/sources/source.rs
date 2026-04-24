use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use async_trait::async_trait;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::{
    Counter, Meter, SpillReporter, StatSnapshot,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::{SourceSnapshot, SpillSource},
};
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_local_plan::{InputId, LocalNodeContext};
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use dashmap::DashMap;
// MicroPartition is used in PipelineMessage
use futures::{StreamExt, stream::BoxStream};
use opentelemetry::KeyValue;
use snafu::ResultExt;

use crate::{
    ExecutionRuntimeContext, PipelineExecutionSnafu,
    channel::create_channel,
    pipeline::{BuilderContext, MorselSizeRequirement, NodeName, PipelineMessage, PipelineNode},
    runtime_stats::{RuntimeStats, RuntimeStatsManagerHandle},
};

pub type SourceStream<'a> = BoxStream<'a, DaftResult<PipelineMessage>>;

/// Per-input stats handle bundling the counters that sources record against.
/// Cheap to clone; every holder of the same `InputStats` shares the same
/// underlying counters.
#[derive(Clone)]
pub(crate) struct InputStats {
    pub io_stats: IOStatsRef,
    pub spill: SpillReporter,
}

/// Per-input-id `InputStats` registry shared between a `Source` implementation
/// and the `SourceNode` that owns it. Sources resolve the right counters for
/// each morsel via `get_or_create`, and the matching per-input `SourceStats`
/// reads from the same entry — so `bytes_read` and spill metrics naturally
/// scope to one `input_id` rather than accumulating across every task that
/// runs on a reused pipeline.
///
/// The provider carries `meter` + `node_info` so `SpillReporter`s can be
/// lazily constructed per-input — spill counters only materialize for inputs
/// that actually spill.
#[derive(Clone)]
pub(crate) struct StatsProvider {
    inner: Arc<DashMap<InputId, InputStats>>,
    meter: Meter,
    node_info: Arc<NodeInfo>,
}

impl StatsProvider {
    pub(crate) fn new(meter: Meter, node_info: Arc<NodeInfo>) -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            meter,
            node_info,
        }
    }

    pub(crate) fn get_or_create(&self, input_id: InputId) -> InputStats {
        self.inner
            .entry(input_id)
            .or_insert_with(|| InputStats {
                io_stats: IOStatsRef::default(),
                spill: SpillReporter::new(&self.meter, &self.node_info, SpillSource::Native),
            })
            .clone()
    }
}

pub(crate) struct SourceStats {
    duration_us: Counter,
    rows_out: Counter,
    bytes_out: Counter,
    num_tasks: Counter,
    input_stats: InputStats,
    node_kv: Vec<KeyValue>,
}

impl SourceStats {
    fn with_input_stats(meter: &Meter, node_info: &NodeInfo, input_stats: InputStats) -> Self {
        Self {
            duration_us: meter.duration_us_metric(),
            rows_out: meter.rows_out_metric(),
            bytes_out: meter.bytes_out_metric(),
            num_tasks: meter.num_tasks_metric(),
            input_stats,
            node_kv: node_info.to_key_values(),
        }
    }
}

impl RuntimeStats for SourceStats {
    fn new(meter: &Meter, node_info: &NodeInfo) -> Self {
        let input_stats = InputStats {
            io_stats: IOStatsRef::default(),
            spill: SpillReporter::new(meter, node_info, SpillSource::Native),
        };
        Self::with_input_stats(meter, node_info, input_stats)
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        let cpu_us = self.duration_us.load(ordering);
        let rows_out = self.rows_out.load(ordering);
        let bytes_read = self.input_stats.io_stats.load_bytes_read() as u64;
        StatSnapshot::Source(SourceSnapshot {
            cpu_us,
            rows_out,
            bytes_read,
            bytes_out: self.bytes_out.load(ordering),
            num_tasks: self.num_tasks.load(ordering),
            spill: self.input_stats.spill.snapshot(ordering),
        })
    }

    fn add_rows_in(&self, _: u64) {
        unreachable!("Source Nodes shouldn't receive rows")
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.add(rows, self.node_kv.as_slice());
    }

    fn add_duration_us(&self, cpu_us: u64) {
        self.duration_us.add(cpu_us, self.node_kv.as_slice());
    }

    fn add_bytes_in(&self, _: u64) {
        unreachable!("Source Nodes shouldn't receive bytes")
    }

    fn add_bytes_out(&self, bytes: u64) {
        self.bytes_out.add(bytes, self.node_kv.as_slice());
    }

    fn increment_num_tasks(&self) {
        self.num_tasks.add(1, self.node_kv.as_slice());
    }
}

#[async_trait]
pub trait Source: Send + Sync {
    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn multiline_display(&self) -> Vec<String>;
    fn get_data(
        self: Box<Self>,
        maintain_order: bool,
        stats_provider: StatsProvider,
        chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>>;
    fn schema(&self) -> &SchemaRef;
}

pub(crate) struct SourceNode {
    source: Box<dyn Source>,
    meter: Meter,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
    morsel_size_requirement: MorselSizeRequirement,
}

impl SourceNode {
    pub fn new(
        source: Box<dyn Source>,
        plan_stats: StatsState,
        ctx: &BuilderContext,
        context: &LocalNodeContext,
    ) -> Self {
        let info = ctx.next_node_info(
            source.name().into(),
            source.op_type(),
            NodeCategory::Source,
            context,
        );
        Self {
            source,
            meter: ctx.meter.clone(),
            plan_stats,
            node_info: Arc::new(info),
            morsel_size_requirement: MorselSizeRequirement::default(),
        }
    }

    pub fn boxed(self) -> Box<dyn PipelineNode> {
        Box::new(self)
    }
}

impl TreeDisplay for SourceNode {
    fn id(&self) -> String {
        self.node_id().to_string()
    }

    fn display_as(&self, level: common_display::DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        use common_display::DisplayLevel;
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.source.name()).unwrap();
            }
            _ => {
                let multiline_display = self.source.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();

                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }
                writeln!(display, "Batch Size = {}", self.morsel_size_requirement).unwrap();
            }
        }
        display
    }

    fn repr_json(&self) -> serde_json::Value {
        let mut json = serde_json::json!({
            "id": self.node_id(),
            "category": "Source",
            "type": self.source.op_type().to_string(),
            "name": self.name(),
        });

        if let StatsState::Materialized(stats) = &self.plan_stats {
            json["approx_stats"] = serde_json::json!(stats);
        }

        json
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children()
            .iter()
            .map(|v| v.as_tree_display())
            .collect()
    }
}

impl PipelineNode for SourceNode {
    fn name(&self) -> Arc<str> {
        self.node_info.name.clone()
    }
    fn children(&self) -> Vec<&dyn PipelineNode> {
        vec![]
    }
    fn boxed_children(&self) -> Vec<&Box<dyn PipelineNode>> {
        vec![]
    }
    fn propagate_morsel_size_requirement(
        &mut self,
        downstream_requirement: MorselSizeRequirement,
        _default_morsel_size: MorselSizeRequirement,
    ) {
        self.morsel_size_requirement = downstream_requirement;
    }

    fn start(
        self: Box<Self>,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<crate::channel::Receiver<PipelineMessage>> {
        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();
        let schema = self.source.schema().clone();
        let name = self.name();
        let meter = self.meter.clone();
        let node_info = self.node_info.clone();
        let stats_provider = StatsProvider::new(meter.clone(), node_info.clone());

        let (destination_sender, destination_receiver) = create_channel(1);
        let chunk_size = match self.morsel_size_requirement {
            MorselSizeRequirement::Strict(size) => size,
            MorselSizeRequirement::Flexible(_, upper) => upper,
        };

        let mut source_stream = self
            .source
            .get_data(maintain_order, stats_provider.clone(), chunk_size.into())
            .with_context(|_| PipelineExecutionSnafu {
                node_name: name.to_string(),
            })?;

        runtime_handle.spawn(
            async move {
                let mut has_data = false;
                let mut per_input_stats: HashMap<InputId, Arc<SourceStats>> = HashMap::new();
                stats_manager.activate_node(node_id);

                let mut source_started = Instant::now();
                loop {
                    let next = source_stream.next().await;
                    let elapsed = source_started.elapsed().as_micros() as u64;
                    let Some(msg) = next else {
                        break;
                    };
                    has_data = true;
                    let msg = msg?;
                    match &msg {
                        PipelineMessage::Morsel {
                            input_id,
                            partition,
                        } => {
                            let stats = get_or_create_source_stats(
                                &mut per_input_stats,
                                *input_id,
                                &meter,
                                &node_info,
                                &stats_provider,
                                &stats_manager,
                                node_id,
                            );
                            stats.add_duration_us(elapsed);
                            stats.add_rows_out(partition.len() as u64);
                            stats.add_bytes_out(partition.size_bytes() as u64);
                            stats.increment_num_tasks();
                        }
                        PipelineMessage::Flush(input_id) => {
                            if let Some(stats) = per_input_stats.get(input_id) {
                                stats.add_duration_us(elapsed);
                            }
                            per_input_stats.remove(input_id);
                        }
                        PipelineMessage::FlightPartitionRef { .. } => {
                            unreachable!(
                                "SourceNode should not receive flight partition refs from child"
                            )
                        }
                    }
                    if destination_sender.send(msg).await.is_err() {
                        break;
                    }
                    source_started = Instant::now();
                }

                if !has_data {
                    let empty = MicroPartition::empty(Some(schema.clone()));
                    let stats = get_or_create_source_stats(
                        &mut per_input_stats,
                        0,
                        &meter,
                        &node_info,
                        &stats_provider,
                        &stats_manager,
                        node_id,
                    );
                    stats.add_rows_out(0);
                    let _ = destination_sender
                        .send(PipelineMessage::Morsel {
                            input_id: 0,
                            partition: empty,
                        })
                        .await;
                }

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &name,
        );
        Ok(destination_receiver)
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

fn get_or_create_source_stats(
    per_input_stats: &mut HashMap<InputId, Arc<SourceStats>>,
    input_id: InputId,
    meter: &Meter,
    node_info: &NodeInfo,
    stats_provider: &StatsProvider,
    stats_manager: &RuntimeStatsManagerHandle,
    node_id: usize,
) -> Arc<SourceStats> {
    per_input_stats
        .entry(input_id)
        .or_insert_with(|| {
            let input_stats = stats_provider.get_or_create(input_id);
            let stats = Arc::new(SourceStats::with_input_stats(meter, node_info, input_stats));
            stats_manager.register_runtime_stats(node_id, input_id, stats.clone());
            stats
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use common_metrics::{Meter, ops::NodeInfo};

    use super::*;
    use crate::runtime_stats::RuntimeStats;

    fn make_provider(name: &'static str) -> StatsProvider {
        StatsProvider::new(Meter::test_scope(name), Arc::new(NodeInfo::default()))
    }

    #[test]
    fn provider_returns_same_io_stats_for_same_input_id() {
        let provider = make_provider("provider_returns_same_io_stats_for_same_input_id");
        let a = provider.get_or_create(5);
        let b = provider.get_or_create(5);
        assert!(Arc::ptr_eq(&a.io_stats, &b.io_stats));
    }

    #[test]
    fn provider_returns_distinct_io_stats_for_distinct_input_ids() {
        let provider = make_provider("provider_returns_distinct_io_stats_for_distinct_input_ids");
        let a = provider.get_or_create(5);
        let b = provider.get_or_create(6);
        assert!(!Arc::ptr_eq(&a.io_stats, &b.io_stats));
    }

    #[test]
    fn provider_clone_shares_underlying_map() {
        let provider_a = make_provider("provider_clone_shares_underlying_map");
        let provider_b = provider_a.clone();
        let from_a = provider_a.get_or_create(1);
        let from_b = provider_b.get_or_create(1);
        assert!(Arc::ptr_eq(&from_a.io_stats, &from_b.io_stats));
    }

    /// Regression test for the bytes.read N× inflation bug.
    ///
    /// Before this refactor every `SourceStats` on a reused worker pipeline
    /// shared one `IOStatsRef`, so each per-input snapshot reported the
    /// shared counter's cumulative value. Across N tasks, the driver's
    /// `Counter::add` accumulated those cumulatives → N×/triangular
    /// inflation (500× observed in practice).
    ///
    /// Post-refactor each per-input `SourceStats` resolves its own
    /// `InputStats` via the provider, so bytes recorded against one
    /// `input_id` must not surface in another `input_id`'s snapshot.
    #[test]
    fn per_input_source_stats_are_isolated() {
        let meter = Meter::test_scope("per_input_source_stats_are_isolated");
        let node_info = NodeInfo::default();
        let provider = StatsProvider::new(meter.clone(), Arc::new(node_info.clone()));

        let stats_a = SourceStats::with_input_stats(&meter, &node_info, provider.get_or_create(1));
        let stats_b = SourceStats::with_input_stats(&meter, &node_info, provider.get_or_create(2));
        let stats_c = SourceStats::with_input_stats(&meter, &node_info, provider.get_or_create(3));

        // Each input "reads" 100 MB independently.
        provider
            .get_or_create(1)
            .io_stats
            .mark_bytes_read(100_000_000);
        provider
            .get_or_create(2)
            .io_stats
            .mark_bytes_read(100_000_000);
        provider
            .get_or_create(3)
            .io_stats
            .mark_bytes_read(100_000_000);

        // Without the fix each snapshot would report the cumulative
        // 300M (shared counter's current value).
        for stats in [&stats_a, &stats_b, &stats_c] {
            let StatSnapshot::Source(snapshot) = stats.flush() else {
                panic!("expected Source snapshot");
            };
            assert_eq!(snapshot.bytes_read, 100_000_000);
        }
    }
}
