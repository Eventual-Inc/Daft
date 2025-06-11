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
    input_psets: Arc<HashMap<String, Vec<PartitionRef>>>,
}

impl InMemorySourceNode {
    #[allow(dead_code)]
    pub fn new(
        node_id: usize,
        config: Arc<DaftExecutionConfig>,
        info: InMemoryInfo,
        plan: LocalPhysicalPlanRef,
        input_psets: Arc<HashMap<String, Vec<PartitionRef>>>,
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
        psets: Arc<HashMap<String, Vec<PartitionRef>>>,
        result_tx: Sender<PipelineOutput<SwordfishTask>>,
    ) -> DaftResult<()> {
        let partition_refs = psets.get(&in_memory_info.cache_key).expect("InMemorySourceNode::execution_loop: Expected in-memory input is not available in partition set").clone();
        for partition_ref in partition_refs {
            let task = make_task_for_partition_ref(
                plan.clone(),
                vec![partition_ref],
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

impl TreeDisplay for InMemorySourceNode {
    fn display_as(&self, _level: DisplayLevel) -> String {
        use std::fmt::Write;
        let mut display = String::new();

        writeln!(display, "{}", self.name()).unwrap();
        writeln!(display, "Node ID: {}", self.node_id).unwrap();
        let plan = make_task_for_partition_ref(
            self.plan.clone(),
            vec![],
            self.info.cache_key.clone(),
            self.config.clone(),
        )
        .unwrap()
        .plan();
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

impl DistributedPipelineNode for InMemorySourceNode {
    fn name(&self) -> &'static str {
        "DistributedInMemoryScan"
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
            self.input_psets.clone(),
            result_tx,
        );
        stage_context.joinset.spawn(execution_loop);

        RunningPipelineNode::new(result_rx)
    }

    fn as_tree_display(&self) -> &dyn TreeDisplay {
        self
    }
}

fn make_task_for_partition_ref(
    plan: LocalPhysicalPlanRef,
    partition_refs: Vec<PartitionRef>,
    cache_key: String,
    config: Arc<DaftExecutionConfig>,
) -> DaftResult<SwordfishTask> {
    let num_rows = partition_refs
        .iter()
        .map(|p| p.num_rows().unwrap_or(0))
        .sum::<usize>();
    let size_bytes = partition_refs
        .iter()
        .map(|p| {
            let size = p.size_bytes()?;
            Ok(size.unwrap_or(0))
        })
        .sum::<DaftResult<usize>>()?;
    let info = InMemoryInfo::new(
        plan.schema().clone(),
        cache_key.clone(),
        None,
        partition_refs.len(),
        size_bytes,
        num_rows,
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
    let psets = HashMap::from([(cache_key, partition_refs)]);
    let task = SwordfishTask::new(
        transformed_plan,
        config,
        psets,
        // TODO: Replace with WorkerAffinity based on the psets location
        // Need to get that from `ray.experimental.get_object_locations(object_refs)`
        SchedulingStrategy::Spread,
    );
    Ok(task)
}
