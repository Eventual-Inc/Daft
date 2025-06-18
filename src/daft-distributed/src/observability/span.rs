use std::sync::Arc;

use crate::{
    observability::{HooksManager, PlanEvent},
    pipeline_node::DistributedPipelineNode,
    plan::DistributedPhysicalPlan,
    stage::Stage,
};

/// A `span` that represents the lifetime of a `Plan`.
pub(crate) struct PlanSpan<'a> {
    plan: &'a DistributedPhysicalPlan,
    plan_id: &'a str,
    hooks_manager: &'a HooksManager,
}

impl<'a> PlanSpan<'a> {
    pub(crate) fn new(
        plan: &'a DistributedPhysicalPlan,
        plan_id: &'a str,
        hooks_manager: &'a HooksManager,
    ) -> Self {
        hooks_manager.emit(&PlanEvent::PlanStarted { plan, plan_id });
        Self {
            plan,
            plan_id,
            hooks_manager,
        }
    }
}

impl Drop for PlanSpan<'_> {
    fn drop(&mut self) {
        self.hooks_manager.emit(&PlanEvent::PlanCompleted {
            plan: self.plan,
            plan_id: self.plan_id,
        });
    }
}

/// A `span` that represents the lifetime of a `Stage`.
pub(crate) struct StageSpan<'a> {
    stage: &'a Stage,
    hooks_manager: &'a HooksManager,
    plan_id: &'a str,
}

impl<'a> StageSpan<'a> {
    pub(crate) fn new(stage: &'a Stage, plan_id: &'a str, hooks_manager: &'a HooksManager) -> Self {
        hooks_manager.emit(&PlanEvent::StageStarted { stage, plan_id });
        Self {
            stage,
            hooks_manager,
            plan_id,
        }
    }
    pub(crate) fn new_pipeline_span(
        &self,
        pipeline_node: Arc<dyn DistributedPipelineNode>,
    ) -> PipelineNodeSpan {
        PipelineNodeSpan::new(pipeline_node, self.hooks_manager.clone())
    }
}

impl Drop for StageSpan<'_> {
    fn drop(&mut self) {
        self.hooks_manager.emit(&PlanEvent::StageCompleted {
            stage: self.stage,
            plan_id: self.plan_id,
        });
    }
}

/// A `span` that represents the lifetime of a `PipelineNode`.
/// Unlike the other spans, `PipelineNodeSpan` requires an owned context due to async lifetimes
pub(crate) struct PipelineNodeSpan {
    pipeline_node: Arc<dyn DistributedPipelineNode>,
    hooks_manager: HooksManager,
}

impl PipelineNodeSpan {
    pub(crate) fn new(
        pipeline_node: Arc<dyn DistributedPipelineNode>,
        hooks_manager: HooksManager,
    ) -> Self {
        hooks_manager.emit(&PlanEvent::PipelineNodeStarted {
            pipeline_node: &pipeline_node,
        });
        Self {
            pipeline_node,
            hooks_manager,
        }
    }
}

impl Drop for PipelineNodeSpan {
    fn drop(&mut self) {
        self.hooks_manager.emit(&PlanEvent::PipelineNodeCompleted {
            pipeline_node: &self.pipeline_node,
        });
    }
}
