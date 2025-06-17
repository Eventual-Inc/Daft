mod pipeline_node;
mod plan;
#[cfg(feature = "python")]
pub mod python;
mod scheduling;
mod stage;
pub(crate) mod utils;

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_error::DaftResult;
use futures::Stream;
#[cfg(feature = "python")]
pub use python::register_modules;

use crate::{pipeline_node::DistributedPipelineNode, plan::DistributedPhysicalPlan, stage::Stage};

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
///
pub(crate) struct StageSpan<'a> {
    stage: &'a Stage,
    hooks_manager: &'a HooksManager,
    plan_id: &'a str,
}

impl<'a> StageSpan<'a> {
    pub(crate) fn new(stage: &'a Stage, plan_id: &'a str, hooks_manager: &'a HooksManager) -> Self {
        hooks_manager.emit(&PlanEvent::StageStarted {
            stage: &stage,
            plan_id: &plan_id,
        });
        Self {
            stage,
            plan_id,
            hooks_manager,
        }
    }
}

impl Drop for StageSpan<'_> {
    fn drop(&mut self) {
        self.hooks_manager.emit(&PlanEvent::StageCompleted {
            stage: &self.stage,
            plan_id: &self.plan_id,
        });
    }
}

pub(crate) struct PipelineNodeSpan {
    pipeline_node: Arc<dyn DistributedPipelineNode>,
    hooks_manager: HooksManager,
}

impl std::fmt::Debug for PipelineNodeSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineNodeSpan").finish()
    }
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

#[allow(dead_code)]
pub(crate) enum PlanEvent<'a> {
    PlanStarted {
        plan: &'a DistributedPhysicalPlan,
        plan_id: &'a str,
    },
    PlanCompleted {
        plan: &'a DistributedPhysicalPlan,
        plan_id: &'a str,
    },
    StageStarted {
        stage: &'a Stage,
        plan_id: &'a str,
    },
    StageCompleted {
        stage: &'a Stage,
        plan_id: &'a str,
    },
    PipelineNodeStarted {
        pipeline_node: &'a Arc<dyn DistributedPipelineNode>,
    },
    PipelineNodeCompleted {
        pipeline_node: &'a Arc<dyn DistributedPipelineNode>,
    },
}

pub(crate) trait PlanObserver {
    fn on_event(&self, event: &PlanEvent) -> DaftResult<()>;
}

#[derive(Clone)]
pub struct HooksManager {
    subscribers: Vec<Arc<dyn PlanObserver + Send + Sync>>,
}

impl HooksManager {
    pub fn new() -> Self {
        Self {
            subscribers: vec![Arc::new(DebugObserver)],
        }
    }

    // pub(crate) fn add_subscriber(&mut self, subscriber: Arc<dyn PlanObserver + Send + Sync>) {
    //     self.subscribers.push(subscriber);
    // }

    pub(crate) fn emit(&self, event: &PlanEvent) {
        for subscriber in &self.subscribers {
            let _ = subscriber.on_event(event);
        }
    }
}

pub struct DebugObserver;

impl PlanObserver for DebugObserver {
    fn on_event(&self, event: &PlanEvent) -> DaftResult<()> {
        match event {
            PlanEvent::PlanStarted { plan_id, .. } => {
                println!("[DEBUG] Plan started: {}", plan_id);
            }
            PlanEvent::PlanCompleted { plan_id, .. } => {
                println!("[DEBUG] Plan completed: {}", plan_id);
            }
            PlanEvent::StageStarted { stage, .. } => {
                println!("[DEBUG] Stage started: {}", stage.id);
            }
            PlanEvent::StageCompleted { stage, .. } => {
                println!("[DEBUG] Stage completed: {}", stage.id);
            }
            PlanEvent::PipelineNodeStarted { pipeline_node } => {
                println!("[DEBUG] Pipeline node started: {}", pipeline_node.node_id())
            }
            PlanEvent::PipelineNodeCompleted { pipeline_node } => println!(
                "[DEBUG] Pipeline node completed: {}",
                pipeline_node.node_id()
            ),
        }
        Ok(())
    }
}

/// Wrapper around a stream that also holds a span context for the stage.
/// This ensures that proper hooks are called when the stage ends (stream is dropped).
pub struct TrackedSpanStream<S, Span> {
    inner: S,
    _span: Span,
}

impl<'a, S, Span: 'a> TrackedSpanStream<S, Span> {
    pub fn new(stream: S, span: Span) -> Self {
        Self {
            inner: stream,
            _span: span,
        }
    }
}

impl<S: Stream + Unpin, Span: Unpin> Stream for TrackedSpanStream<S, Span> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

impl<S: Unpin, Span: Unpin> Unpin for TrackedSpanStream<S, Span> {}
