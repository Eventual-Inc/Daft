use std::sync::{Arc, atomic::Ordering};

use async_trait::async_trait;
use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::{
    CPU_US_KEY, Counter, ROWS_OUT_KEY, StatSnapshot,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot::{SourceSnapshot, StatSnapshotImpl},
};
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_local_plan::LocalNodeContext;
use daft_logical_plan::stats::StatsState;
// MicroPartition is used in PipelineMessage
use futures::{StreamExt, stream::BoxStream};
use opentelemetry::{KeyValue, global};
use snafu::ResultExt;

use crate::{
    ExecutionRuntimeContext, PipelineExecutionSnafu,
    channel::create_channel,
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    pipeline_message::PipelineMessage,
    runtime_stats::RuntimeStats,
};

pub type SourceStream<'a> = BoxStream<'a, DaftResult<PipelineMessage>>;

pub(crate) struct SourceStats {
    cpu_us: Counter,
    rows_out: Counter,
    io_stats: IOStatsRef,

    node_kv: Vec<KeyValue>,
}

impl SourceStats {
    pub fn new(id: usize) -> Self {
        let meter = global::meter("daft.local.node_stats");
        let node_kv = vec![KeyValue::new("node_id", id.to_string())];

        Self {
            cpu_us: Counter::new(&meter, CPU_US_KEY, None),
            rows_out: Counter::new(&meter, ROWS_OUT_KEY, None),
            io_stats: IOStatsRef::default(),

            node_kv,
        }
    }
}

impl RuntimeStats for SourceStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshot {
        let cpu_us = self.cpu_us.load(ordering);
        let rows_out = self.rows_out.load(ordering);
        let bytes_read = self.io_stats.load_bytes_read() as u64;
        StatSnapshot::Source(SourceSnapshot {
            cpu_us,
            rows_out,
            bytes_read,
        })
    }

    fn add_rows_in(&self, _: u64) {
        unreachable!("Source Nodes shouldn't receive rows")
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.add(rows, self.node_kv.as_slice());
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.add(cpu_us, self.node_kv.as_slice());
    }
}

#[async_trait]
pub trait Source: Send + Sync {
    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn make_runtime_stats(&self, id: usize) -> Arc<SourceStats> {
        Arc::new(SourceStats::new(id))
    }
    fn multiline_display(&self) -> Vec<String>;
    fn get_data(
        &mut self,
        maintain_order: bool,
        io_stats: IOStatsRef,
        chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>>;
}

pub(crate) struct SourceNode {
    source: Box<dyn Source>,
    runtime_stats: Arc<SourceStats>,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
    morsel_size_requirement: MorselSizeRequirement,
}

impl SourceNode {
    pub fn new(
        source: Box<dyn Source>,
        plan_stats: StatsState,
        ctx: &RuntimeContext,
        context: &LocalNodeContext,
    ) -> Self {
        let info = ctx.next_node_info(
            source.name().into(),
            source.op_type(),
            NodeCategory::Source,
            context,
        );
        let runtime_stats = source.make_runtime_stats(info.id);
        Self {
            source,
            runtime_stats,
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
            level => {
                let multiline_display = self.source.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();

                if let StatsState::Materialized(stats) = &self.plan_stats {
                    writeln!(display, "Stats = {}", stats).unwrap();
                }
                writeln!(display, "Batch Size = {}", self.morsel_size_requirement).unwrap();

                if matches!(level, DisplayLevel::Verbose) {
                    let rt_result = self.runtime_stats.snapshot();

                    writeln!(display).unwrap();
                    for (name, value) in rt_result.to_stats() {
                        writeln!(display, "{} = {}", name.as_ref().capitalize(), value).unwrap();
                    }
                }
            }
        }
        display
    }

    fn repr_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.node_id(),
            "category": "Source",
            "type": self.source.op_type().to_string(),
            "name": self.name(),
        })
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
        &mut self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<crate::channel::Receiver<PipelineMessage>> {
        let io_stats = self.runtime_stats.io_stats.clone();
        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();

        let (destination_sender, destination_receiver) = create_channel(1);
        let chunk_size = match self.morsel_size_requirement {
            MorselSizeRequirement::Strict(size) => size,
            MorselSizeRequirement::Flexible(_, upper) => upper,
        };

        let mut source_stream = self
            .source
            .get_data(maintain_order, io_stats, chunk_size.into())
            .with_context(|_| PipelineExecutionSnafu {
                node_name: self.name().to_string(),
            })?;
        let runtime_stats = self.runtime_stats.clone();
        runtime_handle.spawn(
            async move {
                stats_manager.activate_node(node_id);

                while let Some(msg) = source_stream.next().await {
                    let msg = msg?;
                    // Track rows only for Morsel messages
                    if let PipelineMessage::Morsel { ref partition, .. } = msg {
                        runtime_stats.add_rows_out(partition.len() as u64);
                    }
                    if destination_sender.send(msg).await.is_err() {
                        break;
                    }
                }

                stats_manager.finalize_node(node_id);
                Ok(())
            },
            &self.name(),
        );
        Ok(destination_receiver)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }

    fn node_id(&self) -> usize {
        self.node_info.id
    }

    fn plan_id(&self) -> Arc<str> {
        Arc::from(self.node_info.context.get("plan_id").unwrap().clone())
    }

    fn node_info(&self) -> Arc<NodeInfo> {
        self.node_info.clone()
    }

    fn runtime_stats(&self) -> Arc<dyn RuntimeStats> {
        self.runtime_stats.clone()
    }
}
