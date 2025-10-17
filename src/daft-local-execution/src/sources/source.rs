use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use capitalize::Capitalize;
use common_display::tree::TreeDisplay;
use common_error::DaftResult;
use common_metrics::{
    Stat, StatSnapshotSend,
    ops::{NodeCategory, NodeInfo, NodeType},
    snapshot,
};
use daft_core::prelude::SchemaRef;
use daft_io::IOStatsRef;
use daft_logical_plan::stats::StatsState;
use daft_micropartition::MicroPartition;
use futures::{StreamExt, stream::BoxStream};

use crate::{
    ExecutionRuntimeContext,
    channel::{Receiver, create_channel},
    pipeline::{MorselSizeRequirement, NodeName, PipelineNode, RuntimeContext},
    runtime_stats::{CPU_US_KEY, CountingSender, ROWS_OUT_KEY, RuntimeStats},
};

pub type SourceStream<'a> = BoxStream<'a, DaftResult<Arc<MicroPartition>>>;

#[derive(Default)]
pub(crate) struct SourceStats {
    cpu_us: AtomicU64,
    rows_out: AtomicU64,
    io_stats: IOStatsRef,
}

impl RuntimeStats for SourceStats {
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn build_snapshot(&self, ordering: Ordering) -> StatSnapshotSend {
        snapshot![
            CPU_US_KEY; Stat::Duration(Duration::from_micros(self.cpu_us.load(ordering))),
            ROWS_OUT_KEY; Stat::Count(self.rows_out.load(ordering)),
            "bytes read"; Stat::Bytes(self.io_stats.load_bytes_read() as u64),
        ]
    }

    fn add_rows_in(&self, _: u64) {
        unreachable!("Source Nodes shouldn't receive rows")
    }

    fn add_rows_out(&self, rows: u64) {
        self.rows_out.fetch_add(rows, Ordering::Relaxed);
    }

    fn add_cpu_us(&self, cpu_us: u64) {
        self.cpu_us.fetch_add(cpu_us, Ordering::Relaxed);
    }
}

#[async_trait]
pub trait Source: Send + Sync {
    fn name(&self) -> NodeName;
    fn op_type(&self) -> NodeType;
    fn make_runtime_stats(&self) -> Arc<SourceStats> {
        Arc::new(SourceStats::default())
    }
    fn multiline_display(&self) -> Vec<String>;
    async fn get_data(
        &self,
        maintain_order: bool,
        io_stats: IOStatsRef,
        chunk_size: usize,
    ) -> DaftResult<SourceStream<'static>>;
    fn schema(&self) -> &SchemaRef;
}

pub(crate) struct SourceNode {
    source: Arc<dyn Source>,
    runtime_stats: Arc<SourceStats>,
    plan_stats: StatsState,
    node_info: Arc<NodeInfo>,
    morsel_size_requirement: MorselSizeRequirement,
}

impl SourceNode {
    pub fn new(
        source: Arc<dyn Source>,
        plan_stats: StatsState,
        ctx: &RuntimeContext,
        output_schema: SchemaRef,
    ) -> Self {
        let info = ctx.next_node_info(
            source.name().into(),
            source.op_type(),
            NodeCategory::Source,
            output_schema,
        );
        let runtime_stats = source.make_runtime_stats();
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
                    for (name, value) in rt_result {
                        writeln!(display, "{} = {}", name.capitalize(), value).unwrap();
                    }
                }
            }
        }
        display
    }

    fn repr_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id(),
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
        &self,
        maintain_order: bool,
        runtime_handle: &mut ExecutionRuntimeContext,
    ) -> crate::Result<Receiver<Arc<MicroPartition>>> {
        let source = self.source.clone();
        let io_stats = self.runtime_stats.io_stats.clone();
        let stats_manager = runtime_handle.stats_manager();
        let node_id = self.node_id();

        let (destination_sender, destination_receiver) = create_channel(0);
        let counting_sender = CountingSender::new(destination_sender, self.runtime_stats.clone());
        let chunk_size = match self.morsel_size_requirement {
            MorselSizeRequirement::Strict(size) => size,
            MorselSizeRequirement::Flexible(_, upper) => upper,
        };

        runtime_handle.spawn(
            async move {
                let mut has_data = false;
                let mut source_stream = source
                    .get_data(maintain_order, io_stats, chunk_size)
                    .await?;
                while let Some(part) = source_stream.next().await {
                    has_data = true;
                    stats_manager.activate_node(node_id);
                    if counting_sender.send(part?).await.is_err() {
                        stats_manager.finalize_node(node_id);
                        return Ok(());
                    }
                }
                if !has_data {
                    stats_manager.activate_node(node_id);
                    let empty = Arc::new(MicroPartition::empty(Some(source.schema().clone())));
                    let _ = counting_sender.send(empty).await;
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
