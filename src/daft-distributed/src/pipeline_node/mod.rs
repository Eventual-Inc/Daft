use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_daft_config::DaftExecutionConfig;
use common_display::{
    DisplayLevel,
    ascii::fmt_tree_gitstyle,
    mermaid::{MermaidDisplayVisitor, SubgraphOptions},
    tree::TreeDisplay,
};
use common_error::DaftResult;
use common_metrics::{
    QueryID,
    ops::{NodeCategory, NodeType},
};
use common_partitioning::PartitionRef;
use common_treenode::ConcreteTreeNode;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{InMemoryInfo, partitioning::ClusteringSpecRef, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{Stream, StreamExt, stream::BoxStream};
use materialize::materialize_all_pipeline_outputs;
use opentelemetry::metrics::Meter;

use crate::{
    plan::{PlanExecutionContext, QueryIdx, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder},
        worker::WorkerId,
    },
    statistics::stats::{DefaultRuntimeStats, RuntimeStatsRef},
    utils::channel::{Receiver, ReceiverStream},
};

#[cfg(feature = "python")]
mod actor_udf;
mod aggregate;
mod concat;
mod distinct;
mod explode;
mod filter;
mod glob_scan_source;
mod in_memory_source;
mod into_batches;
mod into_partitions;
mod join;
mod limit;
pub(crate) mod materialize;
mod monotonically_increasing_id;
mod pivot;
mod project;
mod sample;
mod scan_source;
mod shuffles;
mod sink;
mod sort;
mod top_n;
mod translate;
mod udf;
mod unpivot;
mod vllm;
mod window;

pub(crate) use translate::logical_plan_to_pipeline_node;
pub(crate) type NodeID = u32;
pub(crate) type NodeName = &'static str;

/// The materialized output of a completed pipeline node.
/// Contains both the partition data as well as metadata about the partition.
/// Right now, the only metadata is the worker id that has it so we can try
/// to schedule follow-up pipeline nodes on the same worker.
#[derive(Clone, Debug)]
pub(crate) struct MaterializedOutput {
    partition: Vec<PartitionRef>,
    worker_id: WorkerId,
}

impl MaterializedOutput {
    pub fn new(partition: Vec<PartitionRef>, worker_id: WorkerId) -> Self {
        Self {
            partition,
            worker_id,
        }
    }

    #[allow(dead_code)]
    pub fn partitions(&self) -> &[PartitionRef] {
        &self.partition
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn into_inner(self) -> (Vec<PartitionRef>, WorkerId) {
        (self.partition, self.worker_id)
    }

    pub fn split_into_materialized_outputs(&self) -> Vec<Self> {
        self.partition
            .iter()
            .map(|partition| Self::new(vec![partition.clone()], self.worker_id.clone()))
            .collect()
    }

    pub fn num_rows(&self) -> usize {
        self.partition
            .iter()
            .map(|partition| partition.num_rows())
            .sum()
    }

    pub fn size_bytes(&self) -> usize {
        self.partition
            .iter()
            .map(|partition| partition.size_bytes())
            .sum()
    }

    pub fn into_in_memory_scan_with_psets(
        materialized_outputs: Vec<Self>,
        schema: SchemaRef,
        node_id: NodeID,
    ) -> (LocalPhysicalPlanRef, HashMap<String, Vec<PartitionRef>>) {
        Self::into_in_memory_scan_with_psets_and_context(
            materialized_outputs,
            schema,
            node_id,
            None,
        )
    }

    pub fn into_in_memory_scan_with_psets_and_context(
        materialized_outputs: Vec<Self>,
        schema: SchemaRef,
        node_id: NodeID,
        additional: Option<HashMap<String, String>>,
    ) -> (LocalPhysicalPlanRef, HashMap<String, Vec<PartitionRef>>) {
        let total_size_bytes = materialized_outputs
            .iter()
            .map(|output| output.size_bytes())
            .sum::<usize>();
        let total_num_rows = materialized_outputs
            .iter()
            .map(|output| output.num_rows())
            .sum::<usize>();

        let info = InMemoryInfo::new(
            schema,
            node_id.to_string(),
            None,
            materialized_outputs.len(),
            total_size_bytes,
            total_num_rows,
            None,
            None,
        );

        let in_memory_scan = LocalPhysicalPlan::in_memory_scan(
            info,
            StatsState::NotMaterialized,
            LocalNodeContext {
                origin_node_id: Some(node_id as usize),
                additional,
            },
        );

        let partition_refs = materialized_outputs
            .into_iter()
            .flat_map(|output| output.into_inner().0)
            .collect::<Vec<_>>();
        let psets = HashMap::from([(node_id.to_string(), partition_refs)]);

        (in_memory_scan, psets)
    }
}

#[derive(Clone)]
pub(super) struct PipelineNodeConfig {
    pub schema: SchemaRef,
    pub execution_config: Arc<DaftExecutionConfig>,
    pub clustering_spec: ClusteringSpecRef,
}

impl PipelineNodeConfig {
    pub fn new(
        schema: SchemaRef,
        execution_config: Arc<DaftExecutionConfig>,
        clustering_spec: ClusteringSpecRef,
    ) -> Self {
        Self {
            schema,
            execution_config,
            clustering_spec,
        }
    }
}

#[derive(Clone)]
pub(super) struct PipelineNodeContext {
    pub query_idx: QueryIdx,
    pub query_id: QueryID,
    pub node_id: NodeID,
    pub node_name: NodeName,
    pub node_type: NodeType,
    pub node_category: NodeCategory,
}

impl PipelineNodeContext {
    pub fn new(
        query_idx: QueryIdx,
        query_id: QueryID,
        node_id: NodeID,
        node_name: NodeName,
        node_type: NodeType,
        node_category: NodeCategory,
    ) -> Self {
        Self {
            query_idx,
            query_id,
            node_id,
            node_name,
            node_type,
            node_category,
        }
    }

    pub fn to_hashmap(&self) -> HashMap<String, String> {
        HashMap::from([
            ("query_id".to_string(), self.query_id.to_string()),
            ("node_id".to_string(), self.node_id.to_string()),
            ("node_name".to_string(), self.node_name.to_string()),
        ])
    }
}

pub(crate) trait PipelineNodeImpl: Send + Sync {
    fn context(&self) -> &PipelineNodeContext;
    fn config(&self) -> &PipelineNodeConfig;
    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(DefaultRuntimeStats::new(meter, self.node_id()))
    }

    fn children(&self) -> Vec<DistributedPipelineNode>;
    fn produce_tasks(self: Arc<Self>, plan_context: &mut PlanExecutionContext)
    -> TaskBuilderStream;
    fn name(&self) -> NodeName {
        self.context().node_name
    }
    fn node_id(&self) -> NodeID {
        self.context().node_id
    }
    fn multiline_display(&self, verbose: bool) -> Vec<String>;
}

#[derive(Clone)]
pub(crate) struct DistributedPipelineNode {
    op: Arc<dyn PipelineNodeImpl>,
    children: Vec<DistributedPipelineNode>,
}

impl DistributedPipelineNode {
    pub fn new(op: Arc<dyn PipelineNodeImpl>) -> Self {
        let children = op.children();
        Self { op, children }
    }

    pub fn context(&self) -> &PipelineNodeContext {
        self.op.context()
    }
    fn config(&self) -> &PipelineNodeConfig {
        self.op.config()
    }
    pub fn node_id(&self) -> NodeID {
        self.op.node_id()
    }
    pub fn name(&self) -> NodeName {
        self.op.name()
    }
    pub fn num_partitions(&self) -> usize {
        self.op.config().clustering_spec.num_partitions()
    }
    pub fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        self.op.runtime_stats(meter)
    }
    pub fn produce_tasks(self, plan_context: &mut PlanExecutionContext) -> TaskBuilderStream {
        self.op.produce_tasks(plan_context)
    }
    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

impl ConcreteTreeNode for DistributedPipelineNode {
    fn children(&self) -> Vec<&Self> {
        self.children.iter().collect()
    }

    fn take_children(mut self) -> (Self, Vec<Self>) {
        let children = std::mem::take(&mut self.children);
        (self, children)
    }

    fn with_new_children(self, children: Vec<Self>) -> DaftResult<Self> {
        Ok(Self {
            op: self.op.clone(),
            children,
        })
    }
}

impl TreeDisplay for DistributedPipelineNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        match level {
            DisplayLevel::Compact => self.get_name(),
            DisplayLevel::Default => self.op.multiline_display(false).join("\n"),
            DisplayLevel::Verbose => self.op.multiline_display(true).join("\n"),
        }
    }

    fn repr_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.node_id(),
            "type": self.op.name(),
            "name": self.name(),
            "category": "Physical",
            "children": self.children.iter().map(|child| child.repr_json()).collect::<Vec<_>>(),
        })
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        self.children
            .iter()
            .map(|child| child.as_tree_display())
            .collect()
    }

    fn get_name(&self) -> String {
        self.context().node_name.to_string()
    }
}

/// Visualize a distributed pipeline as Mermaid markdown
pub fn viz_distributed_pipeline_mermaid(
    root: &DistributedPipelineNode,
    display_type: DisplayLevel,
    bottom_up: bool,
    subgraph_options: Option<SubgraphOptions>,
) -> String {
    let mut output = String::new();
    let mut visitor =
        MermaidDisplayVisitor::new(&mut output, display_type, bottom_up, subgraph_options);
    visitor.fmt(root).unwrap();
    output
}

/// Visualize a distributed pipeline as ASCII text
pub fn viz_distributed_pipeline_ascii(root: &DistributedPipelineNode, simple: bool) -> String {
    let mut s = String::new();
    let level = if simple {
        DisplayLevel::Compact
    } else {
        DisplayLevel::Default
    };
    fmt_tree_gitstyle(root, 0, &mut s, level).unwrap();
    s
}

pub(crate) struct TaskBuilderStream {
    task_builder_stream: BoxStream<'static, SwordfishTaskBuilder>,
}

impl From<Receiver<SwordfishTaskBuilder>> for TaskBuilderStream {
    fn from(receiver: Receiver<SwordfishTaskBuilder>) -> Self {
        let task_builder_stream = ReceiverStream::new(receiver).boxed();
        Self {
            task_builder_stream,
        }
    }
}

impl TaskBuilderStream {
    fn new(task_builder_stream: BoxStream<'static, SwordfishTaskBuilder>) -> Self {
        Self {
            task_builder_stream,
        }
    }

    pub fn materialize(
        self,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        query_idx: QueryIdx,
        task_id_counter: TaskIDCounter,
    ) -> impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin + 'static {
        let stream = self
            .task_builder_stream
            .map(move |builder| builder.build(query_idx, &task_id_counter));
        materialize_all_pipeline_outputs(stream, scheduler_handle, None)
    }

    pub fn pipeline_instruction<F>(self, node: Arc<dyn PipelineNodeImpl>, plan_builder: F) -> Self
    where
        F: Fn(LocalPhysicalPlanRef) -> LocalPhysicalPlanRef + Send + Sync + 'static,
    {
        let task_builder_stream = self
            .task_builder_stream
            .map(move |builder| builder.map_plan(node.as_ref(), &plan_builder))
            .boxed();
        Self::new(task_builder_stream)
    }
}

impl Stream for TaskBuilderStream {
    type Item = SwordfishTaskBuilder;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.task_builder_stream.poll_next_unpin(cx)
    }
}
