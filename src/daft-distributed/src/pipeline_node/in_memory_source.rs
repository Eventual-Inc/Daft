use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{ClusteringSpec, InMemoryInfo, stats::StatsState};

use super::{PipelineNodeContext, PipelineNodeImpl, TaskBuilderStream};
use crate::{
    pipeline_node::{DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig},
    plan::{PlanConfig, PlanExecutionContext},
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::{Sender, create_channel},
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

    async fn execution_loop(
        self: Arc<Self>,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        let partition_refs = self.input_psets.get(&self.info.cache_key).expect("InMemorySourceNode::execution_loop: Expected in-memory input is not available in partition set").clone();

        for partition_ref in partition_refs {
            let builder = self.make_in_memory_source_task(partition_ref);
            if result_tx.send(builder).await.is_err() {
                break;
            }
        }

        Ok(())
    }

    fn make_in_memory_source_task(
        self: &Arc<Self>,
        partition_ref: PartitionRef,
    ) -> SwordfishTaskBuilder {
        let source_id = self.info.cache_key.clone();
        let in_memory_scan = LocalPhysicalPlan::in_memory_scan(
            source_id.clone(),
            self.info.source_schema.clone(),
            partition_ref.size_bytes(),
            StatsState::NotMaterialized,
            LocalNodeContext {
                origin_node_id: Some(self.node_id() as usize),
                additional: None,
            },
        );

        let psets = HashMap::from([(source_id, vec![partition_ref])]);
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
        plan_context: &mut PlanExecutionContext,
    ) -> TaskBuilderStream {
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(result_tx);
        plan_context.spawn(execution_loop);

        TaskBuilderStream::from(result_rx)
    }
}
