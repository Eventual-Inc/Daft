use std::{collections::HashMap, sync::Arc};

use common_daft_config::DaftExecutionConfig;
use common_error::DaftResult;
use common_partitioning::PartitionRef;
use common_treenode::{Transformed, TreeNode, TreeNodeRewriter};
use daft_logical_plan::LogicalPlanRef;

use crate::{dispatcher::TaskDispatcherHandle, program::logical_plan_to_programs};

pub enum Stage {
    Collect(CollectStage),
    #[allow(dead_code)]
    ShuffleMap(ShuffleMapStage),
}
pub struct CollectStage {
    logical_plan: LogicalPlanRef,
    config: Arc<DaftExecutionConfig>,
}

impl CollectStage {
    pub fn new(logical_plan: LogicalPlanRef, config: Arc<DaftExecutionConfig>) -> Self {
        Self {
            logical_plan,
            config,
        }
    }
}

impl CollectStage {
    pub fn spawn_stage_programs(
        &self,
        mut psets: HashMap<String, Vec<PartitionRef>>,
        task_dispatcher_handle: TaskDispatcherHandle,
        joinset: &mut tokio::task::JoinSet<DaftResult<()>>,
    ) -> DaftResult<tokio::sync::mpsc::Receiver<PartitionRef>> {
        let programs = logical_plan_to_programs(self.logical_plan.clone())?;
        let config = self.config.clone();
        let mut next_receiver = None;
        for program in programs {
            next_receiver = Some(program.spawn_program(
                task_dispatcher_handle.clone(),
                config.clone(),
                std::mem::take(&mut psets),
                joinset,
                next_receiver.take(),
            ));
        }
        Ok(next_receiver.unwrap())
    }
}

pub struct ShuffleMapStage {
    #[allow(dead_code)]
    logical_plan: LogicalPlanRef,
    #[allow(dead_code)]
    config: Arc<DaftExecutionConfig>,
}

impl ShuffleMapStage {
    #[allow(dead_code)]
    pub fn new(logical_plan: LogicalPlanRef, config: Arc<DaftExecutionConfig>) -> Self {
        Self {
            logical_plan,
            config,
        }
    }

    pub fn spawn_stage_programs(
        &self,
        _psets: HashMap<String, Vec<PartitionRef>>,
        _task_dispatcher_handle: TaskDispatcherHandle,
        _joinset: &mut tokio::task::JoinSet<DaftResult<()>>,
    ) -> DaftResult<tokio::sync::mpsc::Receiver<PartitionRef>> {
        todo!()
    }
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
