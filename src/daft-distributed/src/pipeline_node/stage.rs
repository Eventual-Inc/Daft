use serde::Serialize;

use super::{DistributedPipelineNode, NodeID};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct StageDescriptor {
    pub id: usize,
    pub name: String,
    pub operator_node_ids: Vec<NodeID>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ShuffleDescriptor {
    pub write_stage_id: usize,
    pub read_stage_id: usize,
    pub strategy: String,
    pub num_partitions: usize,
}

pub(crate) struct StagePlan {
    pub stages: Vec<StageDescriptor>,
    pub shuffles: Vec<ShuffleDescriptor>,
}

pub(crate) fn extract_stage_plan(root: &DistributedPipelineNode) -> StagePlan {
    let mut stages = Vec::new();
    let mut shuffles = Vec::new();
    let mut current_stage_ops = Vec::new();
    let mut stage_counter = 0;

    collect_stages(
        root,
        &mut stages,
        &mut shuffles,
        &mut current_stage_ops,
        &mut stage_counter,
    );

    if !current_stage_ops.is_empty() {
        stages.push(StageDescriptor {
            id: stage_counter,
            name: format!("Stage {stage_counter}"),
            operator_node_ids: current_stage_ops,
        });
    }

    StagePlan { stages, shuffles }
}

fn collect_stages(
    node: &DistributedPipelineNode,
    stages: &mut Vec<StageDescriptor>,
    shuffles: &mut Vec<ShuffleDescriptor>,
    current_stage_ops: &mut Vec<NodeID>,
    stage_counter: &mut usize,
) {
    let children = node.children_ref();

    for child in children {
        collect_stages(child, stages, shuffles, current_stage_ops, stage_counter);
    }

    if let Some(shuffle_info) = node.shuffle_info() {
        let write_stage_id = *stage_counter;
        stages.push(StageDescriptor {
            id: write_stage_id,
            name: format!("Stage {write_stage_id}"),
            operator_node_ids: std::mem::take(current_stage_ops),
        });

        *stage_counter += 1;
        let read_stage_id = *stage_counter;

        let strategy = shuffle_info
            .get("strategy")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();
        let num_partitions = shuffle_info
            .get("num_partitions")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;

        shuffles.push(ShuffleDescriptor {
            write_stage_id,
            read_stage_id,
            strategy,
            num_partitions,
        });
    }

    current_stage_ops.push(node.node_id());
}
