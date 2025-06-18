use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use common_error::DaftResult;
use futures::Stream;

use crate::{pipeline_node::DistributedPipelineNode, plan::DistributedPhysicalPlan, stage::Stage};

pub mod span;

#[allow(
    dead_code,
    reason = "Observers for the plan events are Not Yet Implemented"
)]
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

/// Wrapper around a stream that also holds a span context for the stream.
/// This ensures that proper hooks for the span are called when the stream is dropped
pub struct TrackedSpanStream<S, Span> {
    inner: S,
    _span: Span,
}

impl<S, Span> TrackedSpanStream<S, Span> {
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

pub(crate) trait PlanObserver: Send + Sync {
    fn on_event(&self, event: &PlanEvent) -> DaftResult<()>;
}

#[derive(Clone)]
pub struct HooksManager {
    subscribers: Vec<Arc<dyn PlanObserver>>,
}

impl HooksManager {
    pub fn new() -> Self {
        let mut subscribers: Vec<Arc<dyn PlanObserver>> = Vec::new();

        if cfg!(debug_assertions)
            && std::env::var("DAFT_DEV_ENABLE_PLAN_EVENT_DBG")
                .map(|s| matches!(s.to_lowercase().as_ref(), "1" | "true"))
                .unwrap_or(false)
        {
            subscribers.push(Arc::new(DebugObserver));
        }

        Self { subscribers }
    }

    pub(crate) fn emit(&self, event: &PlanEvent) {
        for subscriber in &self.subscribers {
            let _ = subscriber.on_event(event);
        }
    }
}

#[cfg(debug_assertions)]
pub struct DebugObserver;

#[cfg(debug_assertions)]
impl PlanObserver for DebugObserver {
    fn on_event(&self, event: &PlanEvent) -> common_error::DaftResult<()> {
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
                println!("[DEBUG] Pipeline node started: {}", pipeline_node.node_id());
            }
            PlanEvent::PipelineNodeCompleted { pipeline_node } => println!(
                "[DEBUG] Pipeline node completed: {}",
                pipeline_node.node_id()
            ),
        }
        Ok(())
    }
}
