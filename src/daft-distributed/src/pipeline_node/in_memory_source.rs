use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_display::{tree::TreeDisplay, DisplayLevel};
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, InMemoryInfo};

use super::{DistributedPipelineNode, PipelineOutput, RunningPipelineNode};
use crate::{
    pipeline_node::NodeID,
    plan::PlanID,
    scheduling::{
        scheduler::SubmittableTask,
        task::{SchedulingStrategy, SwordfishTask},
    },
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
            let task = self.make_task_for_partition_refs(vec![partition_ref])?;
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
    ) -> DaftResult<SwordfishTask> {
        let mut total_size_bytes = 0;
        let mut total_num_rows = 0;
        for partition_ref in &partition_refs {
            total_size_bytes += partition_ref.size_bytes()?.unwrap_or(0);
            total_num_rows += partition_ref.num_rows().unwrap_or(0);
        }
        let info = InMemoryInfo::new(
            self.plan.schema().clone(),
            self.info.cache_key.clone(),
            None,
            1,
            total_size_bytes,
            total_num_rows,
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
        let psets = HashMap::from([(self.info.cache_key.clone(), partition_refs.clone())]);
        let context = HashMap::from([
            ("plan_id".to_string(), self.plan_id.to_string()),
            ("stage_id".to_string(), format!("{}", self.stage_id)),
            ("node_id".to_string(), format!("{}", self.node_id)),
            ("node_name".to_string(), self.name().to_string()),
        ]);
        let task = SwordfishTask::new(
            transformed_plan,
            self.config.clone(),
            psets,
            // TODO: Replace with WorkerAffinity based on the psets location
            // Need to get that from `ray.experimental.get_object_locations(object_refs)`
            SchedulingStrategy::Spread,
            context,
            self.node_id,
        );
        Ok(task)
    }
}

impl DistributedPipelineNode for InMemorySourceNode {
    fn name(&self) -> &'static str {
        "DistributedInMemoryScan"
    }

    fn children(&self) -> Vec<&dyn DistributedPipelineNode> {
        vec![]
    }

    fn start(&self, stage_context: &mut StageContext) -> RunningPipelineNode {
        let (result_tx, result_rx) = create_channel(1);
        let execution_loop = self.clone().execution_loop(result_tx);
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }
    fn plan_id(&self) -> &PlanID {
        &self.plan_id
    }

    fn stage_id(&self) -> &StageID {
        &self.stage_id
    }

    fn node_id(&self) -> &NodeID {
        &self.node_id
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

impl TreeDisplay for InMemorySourceNode {
    fn display_as(&self, _level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        writeln!(display, "{}", self.name()).unwrap();
        writeln!(display, "Node ID: {}", self.node_id).unwrap();
        let plan = self.make_task_for_partition_refs(vec![]).unwrap().plan();
        writeln!(display, "Local Plan: {}", plan.single_line_display()).unwrap();
        display
    }

    fn get_children(&self) -> Vec<&dyn TreeDisplay> {
        vec![]
    }

    fn get_name(&self) -> String {
        self.name().to_string()
    }
}
