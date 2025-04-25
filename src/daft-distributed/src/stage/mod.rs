use std::sync::Arc;

use collect::CollectStage;
use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::LogicalPlanRef;
use shuffle_map::ShuffleMapStage;

mod collect;
mod shuffle_map;

pub enum Stage {
    Collect(CollectStage),
    #[allow(dead_code)]
    ShuffleMap(ShuffleMapStage),
}

pub fn split_at_stage_boundary(
    plan: &LogicalPlanRef,
    config: &Arc<DaftExecutionConfig>,
) -> DaftResult<(Stage, Option<LogicalPlanRef>)> {
    struct StageBoundarySplitter {
        next_stage: Option<Stage>,
        _config: Arc<DaftExecutionConfig>,
    }

    impl TreeNodeRewriter for StageBoundarySplitter {
        type Node = LogicalPlanRef;

        fn f_down(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            Ok(Transformed::no(node))
        }

        fn f_up(&mut self, node: Self::Node) -> DaftResult<Transformed<Self::Node>> {
            // TODO: Implement stage boundary splitting. Stage boundaries will be defined by the presence of a repartition, or the root of the plan.
            // If it is the root of the plan, we will return a collect stage.
            // If it is a repartition, we will return a shuffle map stage.
            Ok(Transformed::no(node))
        }
    }

    let mut splitter = StageBoundarySplitter {
        next_stage: None,
        _config: config.clone(),
    };

    let transformed = plan.clone().rewrite(&mut splitter)?;

    if let Some(next_stage) = splitter.next_stage {
        Ok((next_stage, Some(transformed.data)))
    } else {
        // make collect stage
        let plan = transformed.data;
        let collect_stage = CollectStage::new(plan, config.clone());
        Ok((Stage::Collect(collect_stage), None))
    }
}
