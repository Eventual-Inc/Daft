use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::Arc,
};

use common_error::DaftResult;
use common_runtime::{RuntimeRef, RuntimeTask};
use tracing::Instrument;

use crate::{
    runtime_stats::{RuntimeStatsContext, TimedFuture},
    MemoryManager,
};

pub struct LocalTaskSpawner<F: Future> {
    join_set: tokio::task::JoinSet<F::Output>,
    buffer_and_id_order: Option<(
        HashMap<tokio::task::Id, Result<F::Output, tokio::task::JoinError>>,
        VecDeque<tokio::task::Id>,
    )>,
}

impl<F: Future + 'static> LocalTaskSpawner<F> {
    pub fn new(maintain_order: bool) -> Self {
        let buffer_and_id_order = maintain_order.then(|| (HashMap::new(), VecDeque::new()));
        Self {
            join_set: tokio::task::JoinSet::new(),
            buffer_and_id_order,
        }
    }

    pub fn push_back(&mut self, future: F) {
        let abort_handle = self.join_set.spawn_local(future);
        if let Some((_buffer, id_order)) = &mut self.buffer_and_id_order {
            id_order.push_back(abort_handle.id());
        }
    }

    pub fn push_front(&mut self, future: F) {
        let abort_handle = self.join_set.spawn_local(future);
        if let Some((_buffer, id_order)) = &mut self.buffer_and_id_order {
            id_order.push_front(abort_handle.id());
        }
    }

    pub async fn next(&mut self) -> Option<Result<F::Output, tokio::task::JoinError>> {
        if let Some((buffer, id_order)) = &mut self.buffer_and_id_order {
            let next_id = match id_order.front() {
                Some(id) => id,
                None => {
                    return None;
                }
            };
            while !buffer.contains_key(&next_id) {
                let res = self
                    .join_set
                    .join_next_with_id()
                    .await
                    .expect("Joinset should not be empty if buffer is not empty");
                match res {
                    Ok((id, result)) => {
                        buffer.insert(id, Ok(result));
                    }
                    Err(e) => {
                        let id = e.id();
                        buffer.insert(id, Err(e));
                    }
                }
            }
            let result = buffer.remove(&next_id)?;
            id_order.pop_front().expect("id_order should not be empty");
            Some(result)
        } else {
            self.join_set.join_next().await
        }
    }

    pub fn len(&self) -> usize {
        self.join_set.len()
            + self
                .buffer_and_id_order
                .as_ref()
                .map_or(0, |(buffer, _)| buffer.len())
    }
}

pub(crate) struct ComputeTaskSpawner {
    runtime_ref: RuntimeRef,
    memory_manager: Arc<MemoryManager>,
    runtime_context: Arc<RuntimeStatsContext>,
    outer_span: tracing::Span,
}

impl ComputeTaskSpawner {
    pub fn new(
        runtime_ref: RuntimeRef,
        memory_manager: Arc<MemoryManager>,
        runtime_context: Arc<RuntimeStatsContext>,
        span: tracing::Span,
    ) -> Self {
        Self {
            runtime_ref,
            memory_manager,
            runtime_context,
            outer_span: span,
        }
    }

    pub fn spawn_with_memory_request<F, O>(
        &self,
        memory_request: u64,
        future: F,
        span: tracing::Span,
    ) -> RuntimeTask<DaftResult<O>>
    where
        F: Future<Output = DaftResult<O>> + Send + 'static,
        O: Send + 'static,
    {
        let instrumented = future.instrument(span);
        let timed_fut = TimedFuture::new(
            instrumented,
            self.runtime_context.clone(),
            self.outer_span.clone(),
        );
        let memory_manager = self.memory_manager.clone();
        self.runtime_ref.spawn(async move {
            let _permit = memory_manager.request_bytes(memory_request).await?;
            timed_fut.await
        })
    }

    pub fn spawn<F, O>(&self, future: F, inner_span: tracing::Span) -> RuntimeTask<DaftResult<O>>
    where
        F: Future<Output = DaftResult<O>> + Send + 'static,
        O: Send + 'static,
    {
        let instrumented = future.instrument(inner_span);
        let timed_fut = TimedFuture::new(
            instrumented,
            self.runtime_context.clone(),
            self.outer_span.clone(),
        );
        self.runtime_ref.spawn(timed_fut)
    }
}
