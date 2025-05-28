use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode};
use daft_local_plan::{LocalPhysicalPlan, LocalPhysicalPlanRef};
use daft_logical_plan::{stats::StatsState, InMemoryInfo};

use super::{DistributedPipelineNode, PipelineOutput, RunningPipelineNode};
use crate::{
    scheduling::task::{SchedulingStrategy, SwordfishTask},
    stage::StageContext,
    utils::channel::{create_channel, Sender},
};

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct InMemorySourceNode {
    node_id: usize,
    config: Arc<DaftExecutionConfig>,
    info: InMemoryInfo,
    plan: LocalPhysicalPlanRef,
    input_psets: HashMap<String, Vec<PartitionRef>>,
}

impl InMemorySourceNode {
    #[allow(dead_code)]
    pub fn new(
        node_id: usize,
        config: Arc<DaftExecutionConfig>,
        info: InMemoryInfo,
        plan: LocalPhysicalPlanRef,
        input_psets: HashMap<String, Vec<PartitionRef>>,
    ) -> Self {
        Self {
            node_id,
            config,
            info,
            plan,
            input_psets,
        }
    }

    async fn execution_loop(
        plan: LocalPhysicalPlanRef,
        config: Arc<DaftExecutionConfig>,
        in_memory_info: InMemoryInfo,
        psets: HashMap<String, Vec<PartitionRef>>,
        result_tx: Sender<PipelineOutput>,
    ) -> DaftResult<()> {
        let partition_refs = psets.get(&in_memory_info.cache_key).unwrap().clone();
        for partition_ref in partition_refs {
            let task = make_task_for_partition_ref(
                plan.clone(),
                partition_ref,
                in_memory_info.cache_key.clone(),
                config.clone(),
            )?;
            if result_tx.send(PipelineOutput::Task(task)).await.is_err() {
                break;
            }
        }
        Ok(())
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
        let execution_loop = Self::execution_loop(
            self.plan.clone(),
            self.config.clone(),
            self.info.clone(),
            std::mem::take(&mut self.input_psets),
            result_tx,
        );
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }
}

fn make_task_for_partition_ref(
    plan: LocalPhysicalPlanRef,
    partition_ref: PartitionRef,
    cache_key: String,
    config: Arc<DaftExecutionConfig>,
) -> DaftResult<SwordfishTask> {
    let info = InMemoryInfo::new(
        plan.schema().clone(),
        cache_key.clone(),
        None,
        1,
        partition_ref.size_bytes()?.unwrap(),
        partition_ref.num_rows()?,
        None,
        None,
    );
    let in_memory_source = LocalPhysicalPlan::in_memory_scan(info, StatsState::NotMaterialized);
    // the first operator of physical_plan has to be a scan
    let transformed_plan = plan
        .transform_up(|p| match p.as_ref() {
            LocalPhysicalPlan::PlaceholderScan(_) => Ok(Transformed::yes(in_memory_source.clone())),
            _ => Ok(Transformed::no(p)),
        })?
        .data;
    let mut psets = HashMap::new();
    psets.insert(cache_key, vec![partition_ref]);
    let task = SwordfishTask::new(transformed_plan, config, psets, SchedulingStrategy::Spread);
    Ok(task)
}
