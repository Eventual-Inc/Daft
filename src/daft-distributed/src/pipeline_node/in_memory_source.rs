use std::{collections::HashMap, sync::Arc};

use common_metrics::ops::{NodeCategory, NodeType};
use common_partitioning::PartitionRef;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{ClusteringSpec, InMemoryInfo, stats::StatsState};
use futures::{StreamExt, stream};
use opentelemetry::metrics::Meter;

use super::{PipelineNodeContext, PipelineNodeImpl, scan_source::SourceStats};
use crate::{
    pipeline_node::{
        DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig, TaskBuilderStream,
    },
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
    statistics::stats::RuntimeStatsRef,
};

pub(crate) struct InMemorySourceNode {
    config: PipelineNodeConfig,
    context: PipelineNodeContext,
    info: InMemoryInfo,
    input_psets: Arc<HashMap<String, Vec<PartitionRef>>>,
}

impl InMemorySourceNode {
    const NODE_NAME: NodeName = "InMemorySource";

    pub fn new(
        node_id: NodeID,
        plan_config: &PlanConfig,
        info: InMemoryInfo,
        input_psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            plan_config.query_idx,
            plan_config.query_id.clone(),
            node_id,
            Self::NODE_NAME,
            NodeType::InMemoryScan,
            NodeCategory::Source,
        );

        let num_partitions = input_psets.values().map(|pset| pset.len()).sum::<usize>();

        let config = PipelineNodeConfig::new(
            info.source_schema.clone(),
            plan_config.config.clone(),
            Arc::new(ClusteringSpec::unknown_with_num_partitions(num_partitions)),
        );
        Self {
            config,
            context,
            info,
            input_psets,
        }
    }

    pub fn into_node(self) -> DistributedPipelineNode {
        DistributedPipelineNode::new(Arc::new(self))
    }

    fn make_in_memory_source_task(
        self: &Arc<Self>,
        partition_ref: PartitionRef,
    ) -> SwordfishTaskBuilder {
        let info = InMemoryInfo::new(
            self.info.source_schema.clone(),
            self.info.cache_key.clone(),
            None,
            1,
            partition_ref.size_bytes(),
            partition_ref.num_rows(),
            None,
            None,
        );
        let in_memory_scan = LocalPhysicalPlan::in_memory_scan(
            info,
            StatsState::NotMaterialized,
            LocalNodeContext {
                origin_node_id: Some(self.node_id() as usize),
                additional: None,
            },
        );

        let psets = HashMap::from([(self.info.cache_key.clone(), vec![partition_ref])]);
        SwordfishTaskBuilder::new(in_memory_scan, self.as_ref()).with_psets(psets)
    }
}

impl PipelineNodeImpl for InMemorySourceNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<DistributedPipelineNode> {
        vec![]
    }

    fn multiline_display(&self, _verbose: bool) -> Vec<String> {
        let mut res = vec![];
        res.push("InMemorySource:".to_string());
        res.push(format!(
            "Schema = {}",
            self.info.source_schema.short_string()
        ));
        res.push(format!("Size bytes = {}", self.info.size_bytes));
        res
    }

    fn produce_tasks(
        self: Arc<Self>,
        _plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let partition_refs = self
            .input_psets
            .get(&self.info.cache_key)
            .expect("InMemorySourceNode::produce_tasks: Expected in-memory input is not available in partition set")
            .clone();

        let slf = self.clone();
        let builders_iter = partition_refs
            .into_iter()
            .map(move |partition_ref| slf.make_in_memory_source_task(partition_ref));

        TaskBuilderStream::new(stream::iter(builders_iter).boxed())
    }

    fn runtime_stats(&self, meter: &Meter) -> RuntimeStatsRef {
        Arc::new(SourceStats::new(meter, self.node_id()))
    }
}
