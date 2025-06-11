use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, InMemoryInfo};

use super::{DistributedPipelineNode, PipelineOutput, RunningPipelineNode};
use crate::{
    pipeline_node::NodeID,
    plan::PlanID,
    scheduling::task::{SchedulingStrategy, SwordfishTask},
    stage::{StageContext, StageID},
    utils::channel::{create_channel, Sender},
};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct InMemorySourceNode {
    plan_id: PlanID,
    stage_id: StageID,
    node_id: NodeID,
    config: Arc<DaftExecutionConfig>,
    info: InMemoryInfo,
    plan: LocalPhysicalPlanRef,
    input_psets: Arc<HashMap<String, Vec<PartitionRef>>>,
}

impl InMemorySourceNode {
    #[allow(dead_code)]
    pub fn new(
        plan_id: PlanID,
        stage_id: StageID,
        node_id: usize,
        config: Arc<DaftExecutionConfig>,
        info: InMemoryInfo,
        plan: LocalPhysicalPlanRef,
        input_psets: Arc<HashMap<String, Vec<PartitionRef>>>,
    ) -> Self {
        Self {
            plan_id,
            stage_id,
            node_id,
            config,
            info,
            plan,
            input_psets,
        }
    }

    async fn execution_loop(
        self,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
    ) -> DaftResult<()> {
        let partition_refs = self.input_psets.get(&self.info.cache_key).expect("InMemorySourceNode::execution_loop: Expected in-memory input is not available in partition set").clone();

        for partition_ref in partition_refs {
            let task = self.make_task_for_partition_ref(partition_ref)?;
            if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    fn make_task_for_partition_ref(
        &self,
        partition_ref: PartitionRef,
    ) -> DaftResult<SwordfishTask> {
        let info = InMemoryInfo::new(
            self.plan.schema().clone(),
            self.info.cache_key.clone(),
            None,
            1,
            partition_ref.size_bytes()?.expect("make_task_for_partition_ref: Expect that the input partition ref for a in-memory source node has a known size"),
            partition_ref.num_rows()?,
            None,
            None,
        );
        let in_memory_source = LocalPhysicalPlan::in_memory_scan(info, StatsState::NotMaterialized);
        // the first operator of physical_plan has to be a scan
        let transformed_plan = self
            .plan
            .clone()
            .transform_up(|p| match p.as_ref() {
                LocalPhysicalPlan::PlaceholderScan(_) => {
                    Ok(Transformed::yes(in_memory_source.clone()))
                }
                _ => Ok(Transformed::no(p)),
            })?
            .data;
        let psets = HashMap::from([(self.info.cache_key.clone(), vec![partition_ref])]);
        let context = HashMap::from([
            ("plan_id".to_string(), self.plan_id.to_string()),
            ("stage_id".to_string(), format!("{}", self.stage_id)),
            ("node_id".to_string(), format!("{}", self.node_id)),
        ]);
        let task = SwordfishTask::new(
            transformed_plan,
            self.config.clone(),
            psets,
            // TODO: Replace with WorkerAffinity based on the psets location
            // Need to get that from `ray.experimental.get_object_locations(object_refs)`
            SchedulingStrategy::Spread,
            context,
        );
        Ok(task)
    }
}

impl DistributedPipelineNode for InMemorySourceNode {
    fn name(&self) -> &'static str {
        "InMemorySourceNode"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        vec![]
    }

    fn start(&mut self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.clone().execution_loop(result_tx);
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }
}
