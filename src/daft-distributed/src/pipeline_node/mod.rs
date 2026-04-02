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
    Meter, QueryID,
    ops::{NodeCategory, NodeType},
};
use common_partitioning::PartitionRef;
use common_treenode::ConcreteTreeNode;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{partitioning::ClusteringSpecRef, stats::StatsState};
use daft_schema::schema::SchemaRef;
use futures::{Stream, StreamExt, stream::BoxStream};
use materialize::{materialize_all_pipeline_outputs, task_outputs_from_pipeline};
use serde::{Deserialize, Serialize};

use crate::{
    plan::{PlanExecutionContext, QueryIdx, TaskIDCounter},
    scheduling::{
        scheduler::SchedulerHandle,
        task::{SwordfishTask, SwordfishTaskBuilder, TaskID},
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
pub(crate) mod metrics;
mod monotonically_increasing_id;
mod pivot;
mod project;
mod random_shuffle;
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
pub(crate) type NodeName = Arc<str>;
/// Fingerprint identifying tasks with functionally identical plans.
/// Tasks sharing a fingerprint can reuse the same pipeline.
pub(crate) type PlanFingerprint = u32;

/// The materialized output of a completed pipeline node.
/// Contains both the partition data as well as metadata about the partition.
/// Right now, the only metadata is the worker id that has it so we can try
/// to schedule follow-up pipeline nodes on the same worker.
#[derive(Clone, Debug)]
pub(crate) struct MaterializedOutput {
    partition: Vec<PartitionRef>,
    worker_id: WorkerId,
    ip_address: String,
    task_id: TaskID,
}

impl MaterializedOutput {
    pub fn new(
        partition: Vec<PartitionRef>,
        worker_id: WorkerId,
        ip_address: String,
        task_id: TaskID,
    ) -> Self {
        Self {
            partition,
            worker_id,
            ip_address,
            task_id,
        }
    }

    #[allow(dead_code)]
    pub fn partitions(&self) -> &[PartitionRef] {
        &self.partition
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    #[allow(dead_code)]
    pub fn ip_address(&self) -> &String {
        &self.ip_address
    }

    pub fn into_inner(self) -> (Vec<PartitionRef>, WorkerId, String) {
        (self.partition, self.worker_id, self.ip_address)
    }

    pub fn split_into_materialized_outputs(&self) -> Vec<Self> {
        self.partition
            .iter()
            .map(|partition| Self {
                partition: vec![partition.clone()],
                worker_id: self.worker_id.clone(),
                ip_address: self.ip_address.clone(),
                task_id: self.task_id,
            })
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
    ) -> (LocalPhysicalPlanRef, Vec<PartitionRef>) {
        Self::build_in_memory_scan(
            materialized_outputs,
            schema,
            node_id,
            LocalNodeContext::new(Some(node_id as usize)),
        )
    }

    pub fn into_in_memory_scan_with_psets_and_phase(
        materialized_outputs: Vec<Self>,
        schema: SchemaRef,
        node_id: NodeID,
        node_phase: impl Into<String>,
    ) -> (LocalPhysicalPlanRef, Vec<PartitionRef>) {
        Self::build_in_memory_scan(
            materialized_outputs,
            schema,
            node_id,
            LocalNodeContext::new(Some(node_id as usize)).with_phase(node_phase),
        )
    }

    fn build_in_memory_scan(
        materialized_outputs: Vec<Self>,
        schema: SchemaRef,
        node_id: NodeID,
        local_ctx: LocalNodeContext,
    ) -> (LocalPhysicalPlanRef, Vec<PartitionRef>) {
        let total_size_bytes = materialized_outputs
            .iter()
            .map(|output| output.size_bytes())
            .sum::<usize>();

        let in_memory_scan = LocalPhysicalPlan::in_memory_scan(
            node_id,
            schema,
            total_size_bytes,
            StatsState::NotMaterialized,
            local_ctx,
        );

        let partition_refs = materialized_outputs
            .into_iter()
            .flat_map(|output| output.into_inner().0)
            .collect::<Vec<_>>();

        (in_memory_scan, partition_refs)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FlightShufflePartitionRef {
    pub shuffle_id: u64,
    pub partition_idx: usize,
    pub server_address: String,
    pub cache_id: u32,
    pub num_rows: usize,
    pub size_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ShufflePartitionRef {
    Flight(FlightShufflePartitionRef),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ShuffleWriteOutput {
    pub partitions: Vec<ShufflePartitionRef>,
    worker_id: WorkerId,
    task_id: TaskID,
}

impl ShuffleWriteOutput {
    pub fn new(partitions: Vec<ShufflePartitionRef>, worker_id: WorkerId, task_id: TaskID) -> Self {
        Self {
            partitions,
            worker_id,
            task_id,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum TaskOutput {
    Materialized(MaterializedOutput),
    ShuffleWrite(ShuffleWriteOutput),
}

impl TaskOutput {
    pub fn into_materialized(self) -> DaftResult<MaterializedOutput> {
        match self {
            Self::Materialized(materialized_output) => Ok(materialized_output),
            Self::ShuffleWrite(_) => Err(common_error::DaftError::InternalError(
                "Expected materialized task output but received shuffle write output".to_string(),
            )),
        }
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
    fn make_runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(DefaultRuntimeStats::new(meter, self.context()))
    }

    fn children(&self) -> Vec<DistributedPipelineNode>;
    fn produce_tasks(self: Arc<Self>, plan_context: &mut PlanExecutionContext)
    -> TaskBuilderStream;
    fn name(&self) -> NodeName {
        self.context().node_name.clone()
    }
    fn node_id(&self) -> NodeID {
        self.context().node_id
    }
    fn phase(&self) -> Option<&str> {
        None
    }
    fn multiline_display(&self, verbose: bool) -> Vec<String>;
}

#[derive(Clone)]
pub(crate) struct DistributedPipelineNode {
    op: Arc<dyn PipelineNodeImpl>,
    runtime_stats: RuntimeStatsRef,
    children: Vec<DistributedPipelineNode>,
}

impl DistributedPipelineNode {
    pub fn new(op: Arc<dyn PipelineNodeImpl>, meter: &Meter) -> Self {
        let children = op.children();
        let runtime_stats = op.make_runtime_stats(meter);
        Self {
            op,
            runtime_stats,
            children,
        }
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
    pub fn runtime_stats(&self) -> RuntimeStatsRef {
        self.runtime_stats.clone()
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
            op: self.op,
            children,
            runtime_stats: self.runtime_stats,
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
        let mut json = serde_json::json!({
            "id": self.node_id(),
            "type": self.op.name(),
            "name": self.name(),
            "category": "Physical",
            "children": self.children.iter().map(|child| child.repr_json()).collect::<Vec<_>>(),
        });
        if let Some(phase) = self.op.phase() {
            json["phase"] = serde_json::json!(phase);
        }
        json
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

    pub fn task_outputs(
        self,
        scheduler_handle: SchedulerHandle<SwordfishTask>,
        query_idx: QueryIdx,
        task_id_counter: TaskIDCounter,
    ) -> impl Stream<Item = DaftResult<TaskOutput>> + Send + Unpin + 'static {
        let stream = self
            .task_builder_stream
            .map(move |builder| builder.build(query_idx, &task_id_counter));
        task_outputs_from_pipeline(stream, scheduler_handle, None)
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

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use common_daft_config::DaftExecutionConfig;
    use common_metrics::{
        QueryID,
        ops::{NodeCategory, NodeType},
    };
    use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
    use daft_logical_plan::{ClusteringSpec, stats::StatsState};
    use daft_schema::schema::Schema;
    use futures::{StreamExt, stream};

    use super::*;
    use crate::scheduling::task::SwordfishTaskBuilder;

    /// Mock pipeline node for tests. Implements PipelineNodeImpl with minimal setup.
    pub struct MockNode {
        config: PipelineNodeConfig,
        context: PipelineNodeContext,
    }

    impl MockNode {
        pub fn new(node_id: NodeID) -> Self {
            Self {
                config: PipelineNodeConfig::new(
                    Arc::new(Schema::empty()),
                    Arc::new(DaftExecutionConfig::default()),
                    Arc::new(ClusteringSpec::unknown()),
                ),
                context: PipelineNodeContext::new(
                    0,
                    QueryID::default(),
                    node_id,
                    Arc::from("Mock"),
                    NodeType::Project,
                    NodeCategory::Intermediate,
                ),
            }
        }
    }

    impl PipelineNodeImpl for MockNode {
        fn context(&self) -> &PipelineNodeContext {
            &self.context
        }
        fn config(&self) -> &PipelineNodeConfig {
            &self.config
        }
        fn children(&self) -> Vec<DistributedPipelineNode> {
            vec![]
        }
        fn produce_tasks(
            self: Arc<Self>,
            _: &mut crate::plan::PlanExecutionContext,
        ) -> TaskBuilderStream {
            unimplemented!()
        }
        fn multiline_display(&self, _: bool) -> Vec<String> {
            vec![]
        }
    }

    pub fn make_builder(node: &MockNode, fp: PlanFingerprint) -> SwordfishTaskBuilder {
        let plan = LocalPhysicalPlan::in_memory_scan(
            0,
            Arc::new(Schema::empty()),
            0,
            StatsState::NotMaterialized,
            LocalNodeContext::default(),
        );
        SwordfishTaskBuilder::new(plan, node, fp)
    }

    pub fn make_source_stream(node: &Arc<MockNode>, n: usize) -> TaskBuilderStream {
        let fp = node.node_id();
        let node = node.clone();
        TaskBuilderStream::new(stream::iter((0..n).map(move |_| make_builder(&node, fp))).boxed())
    }

    pub async fn collect_fingerprints(stream: TaskBuilderStream) -> Vec<PlanFingerprint> {
        stream.map(|b| b.fingerprint()).collect().await
    }

    #[tokio::test]
    async fn chaining_nodes_produce_same_fingerprint_across_tasks() {
        let source = Arc::new(MockNode::new(10));
        let fps = collect_fingerprints(
            make_source_stream(&source, 3)
                .pipeline_instruction(Arc::new(MockNode::new(20)), |p| p)
                .pipeline_instruction(Arc::new(MockNode::new(30)), |p| p),
        )
        .await;
        assert!(fps.iter().all(|fp| *fp == fps[0]));
    }

    #[tokio::test]
    async fn join_produces_same_fingerprint_across_tasks() {
        let (left_src, right_src) = (Arc::new(MockNode::new(10)), Arc::new(MockNode::new(20)));
        let join: Arc<MockNode> = Arc::new(MockNode::new(30));
        let j = join.clone();
        let fps = collect_fingerprints(TaskBuilderStream::new(
            make_source_stream(&left_src, 3)
                .zip(make_source_stream(&right_src, 3))
                .map(move |(l, r)| SwordfishTaskBuilder::combine_with(&l, &r, j.as_ref(), |l, _| l))
                .boxed(),
        ))
        .await;
        assert!(fps.iter().all(|fp| *fp == fps[0]));
    }

    #[tokio::test]
    async fn fingerprint_override_produces_different_fingerprints_across_tasks() {
        let source = Arc::new(MockNode::new(10));
        let node: Arc<dyn PipelineNodeImpl> = Arc::new(MockNode::new(20));
        let counter = std::sync::atomic::AtomicU32::new(0);
        let fps = collect_fingerprints(TaskBuilderStream::new(
            make_source_stream(&source, 4)
                .map(move |b| {
                    let id = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    b.map_plan(node.as_ref(), |p| p).extend_fingerprint(id)
                })
                .boxed(),
        ))
        .await;
        let unique: std::collections::HashSet<_> = fps.into_iter().collect();
        assert_eq!(unique.len(), 4);
    }
}
