use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::{LocalNodeContext, LocalPhysicalPlan};
use daft_logical_plan::{ClusteringSpec, InMemoryInfo, stats::StatsState};

use super::{PipelineNodeContext, PipelineNodeImpl, SubmittableTaskStream};
use crate::{
    pipeline_node::{DistributedPipelineNode, NodeID, NodeName, PipelineNodeConfig},
    plan::{PlanConfig, PlanExecutionContext, TaskIDCounter},
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
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
        result_tx: Sender<SubmittableTask<SwordfishTask>>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        let partition_refs = self.input_psets.get(&self.info.cache_key).expect("InMemorySourceNode::execution_loop: Expected in-memory input is not available in partition set").clone();

        for partition_ref in partition_refs {
            let task = self.make_task_for_partition_refs(
                vec![partition_ref],
                TaskContext::from((&self.context, task_id_counter.next())),
            );
            if result_tx.send(SubmittableTask::new(task)).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    fn make_task_for_partition_refs(
        &self,
        partition_refs: Vec<PartitionRef>,
        task_context: TaskContext,
    ) -> SwordfishTask {
        let mut total_size_bytes = 0;
        let mut total_num_rows = 0;
        for partition_ref in &partition_refs {
            total_size_bytes += partition_ref.size_bytes();
            total_num_rows += partition_ref.num_rows();
        }
        let info = InMemoryInfo::new(
            self.info.source_schema.clone(),
            self.info.cache_key.clone(),
            None,
            1,
            total_size_bytes,
            total_num_rows,
            None,
            None,
        );
        let in_memory_source_plan = LocalPhysicalPlan::in_memory_scan(
            info,
            StatsState::NotMaterialized,
            LocalNodeContext {
                origin_node_id: Some(self.node_id() as usize),
                additional: None,
            },
        );
        let psets = HashMap::from([(self.info.cache_key.clone(), partition_refs.clone())]);
        SwordfishTask::new(
            task_context,
            in_memory_source_plan,
            self.config.execution_config.clone(),
            psets,
            // TODO: Replace with WorkerAffinity based on the psets location
            // Need to get that from `ray.experimental.get_object_locations(object_refs)`
            SchedulingStrategy::Spread,
            self.context.to_hashmap(),
        )
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
    ) -> SubmittableTaskStream {
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(result_tx, plan_context.task_id_counter());
        plan_context.spawn(execution_loop);

        SubmittableTaskStream::from(result_rx)
    }
}
