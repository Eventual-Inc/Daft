use std::{collections::HashMap, sync::Arc};

use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use daft_local_plan::LocalPhysicalPlan;
use daft_logical_plan::{stats::StatsState, ClusteringSpec, InMemoryInfo};

use super::{DistributedPipelineNode, PipelineNodeContext, PipelineOutput, RunningPipelineNode};
use crate::{
    pipeline_node::{NodeID, NodeName, PipelineNodeConfig},
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask, TaskContext},
    },
    stage::{StageConfig, StageExecutionContext, TaskIDCounter},
    utils::channel::{create_channel, Sender},
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
        stage_config: &StageConfig,
        info: InMemoryInfo,
        input_psets: Arc<HashMap<String, Vec<PartitionRef>>>,
        logical_node_id: Option<NodeID>,
    ) -> Self {
        let context = PipelineNodeContext::new(
            stage_config,
            node_id,
            Self::NODE_NAME,
            vec![],
            vec![],
            logical_node_id,
        );

        let num_partitions = input_psets.values().map(|pset| pset.len()).sum::<usize>();

        let config = PipelineNodeConfig::new(
            info.source_schema.clone(),
            stage_config.config.clone(),
            Arc::new(ClusteringSpec::unknown_with_num_partitions(num_partitions)),
        );
        Self {
            config,
            context,
            info,
            input_psets,
        }
    }

    pub fn arced(self) -> Arc<dyn DistributedPipelineNode> {
        Arc::new(self)
    }

    async fn execution_loop(
        self: Arc<Self>,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
        task_id_counter: TaskIDCounter,
    ) -> DaftResult<()> {
        let partition_refs = self.input_psets.get(&self.info.cache_key).expect("InMemorySourceNode::execution_loop: Expected in-memory input is not available in partition set").clone();

        for partition_ref in partition_refs {
            let task = self.make_task_for_partition_refs(
                vec![partition_ref],
                TaskContext::from((&self.context, task_id_counter.next())),
            )?;
            if result_tx
                .send(PipelineOutput::Task(SubmittableTask::new(task)))
                .await
                .is_err()
            {
                break;
            }
        }
        Ok(())
    }

    fn make_task_for_partition_refs(
        &self,
        partition_refs: Vec<PartitionRef>,
        task_context: TaskContext,
    ) -> DaftResult<SwordfishTask> {
        let mut total_size_bytes = 0;
        let mut total_num_rows = 0;
        for partition_ref in &partition_refs {
            total_size_bytes += partition_ref.size_bytes()?.unwrap_or(0);
            total_num_rows += partition_ref.num_rows().unwrap_or(0);
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
        let in_memory_source_plan =
            LocalPhysicalPlan::in_memory_scan(info, StatsState::NotMaterialized);
        let psets = HashMap::from([(self.info.cache_key.clone(), partition_refs.clone())]);
        let task = SwordfishTask::new(
            task_context,
            in_memory_source_plan,
            self.config.execution_config.clone(),
            psets,
            // TODO: Replace with WorkerAffinity based on the psets location
            // Need to get that from `ray.experimental.get_object_locations(object_refs)`
            SchedulingStrategy::Spread,
            self.context.to_hashmap(),
        );
        Ok(task)
    }

    fn multiline_display(&self) -> Vec<String> {
        let mut res = vec![];
        res.push("InMemorySource:".to_string());
        res.push(format!(
            "Schema = {}",
            self.info.source_schema.short_string()
        ));
        res.push(format!("Size bytes = {}", self.info.size_bytes));
        res
    }
}

impl DistributedPipelineNode for InMemorySourceNode {
    fn context(&self) -> &PipelineNodeContext {
        &self.context
    }

    fn config(&self) -> &PipelineNodeConfig {
        &self.config
    }

    fn children(&self) -> Vec<Arc<dyn DistributedPipelineNode>> {
        vec![]
    }

    fn start(self: Arc<Self>, stage_context: &mut StageExecutionContext) -> RunningPipelineNode {
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.execution_loop(result_tx, stage_context.task_id_counter());
        stage_context.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

impl TreeDisplay for InMemorySourceNode {
    fn display_as(&self, level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();
        match level {
            DisplayLevel::Compact => {
                writeln!(display, "{}", self.context.node_name).unwrap();
            }
            _ => {
                let multiline_display = self.multiline_display().join("\n");
                writeln!(display, "{}", multiline_display).unwrap();
            }
        }
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }

    fn get_name(&self) -> String {
        self.context.node_name.to_string()
    }
}
